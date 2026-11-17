"""Request execution: deadline accounting, statement cancellation, recovery.

Cancellation-safe protocol
==========================

A request coroutine can be cancelled at *any* await point (client
disconnects, an upstream proxy cancels a slow query).  Abandoning a borrowed
connection at that moment is what leaks pool slots: the statement keeps
running on a backend nobody will return, and after two such cancellations
every later request queues until it times out.

Therefore every execution runs in its own **worker task** owned by the
executor, not by the request::

    request task --shield--> worker task --owns--> lease (one pool slot)

When the request is cancelled the shield leaves the worker (and its
statement) running, so the worker — not the dying request — performs the
full stop protocol.  Recovery itself runs in yet another *settler* task,
shielded from the worker, so a **second** cancellation landing during
rollback, replacement build or idle confirmation cannot interrupt the
cleanup half-way: the settler finishes and returns/discards the slot exactly
once.

Protocol for an abnormal stop (client deadline, server backstop, or
cancellation)::

    1. call libpq ``PQcancel`` to ask the server to abort the statement;
    2. await the statement task until it really ends (no coroutine may keep
       touching the connection);
    3. if it does not stop, close/discard the connection (which also aborts
       the backend query) and let the task unwind;
    4. prove the backend is IDLE (``ROLLBACK`` + transaction_status check);
    5. only then return the connection to the pool; otherwise discard it and
       build a replacement.

A late result is audit-logged but never replaces a timeout receipt, and a
connection that is still executing is never lent to the next borrower.

Transactions are committed *inside* the cursor block on purpose: psycopg's
``async with conn.cursor()`` rolls back any transaction still open when the
block exits, so committing afterwards would silently leave the backend in
an aborted/in-transaction state.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import psycopg

from .config import Settings
from .errors import QueryServiceError, QueryTimeout
from .pool import PoolLease, QueryConnectionPool

logger = logging.getLogger("executor")


@dataclass
class _Recovery:
    cancelled: bool
    discarded: bool
    late_rows: list[tuple] | None


class _ConnectionLost(Exception):
    """Internal signal: the backend died and the statement may be retried."""


class QueryExecutor:
    def __init__(self, pool: QueryConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        # Worker/settler tasks outlive their request task so cancellation can
        # never strand a connection.  Tracked so shutdown can drain them.
        self._workers: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    async def execute(self, sql: str, deadline: float) -> dict[str, Any]:
        """Execute one validated read-only statement against the pool.

        The returned receipt belongs to *this* request only.  If the request
        task is cancelled, the worker keeps going independently just long
        enough to stop the statement and return/discard the connection; the
        cancellation is re-raised to the caller immediately.
        """
        worker = self._track(
            asyncio.create_task(self._run(sql, deadline), name="query-worker")
        )
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            # Stop the worker promptly; its cancellation handler hands off to
            # a shielded settler that performs the statement-abort +
            # idle-confirmation + lease-release protocol.  Crucially no part
            # of that protocol depends on this coroutine staying alive.
            worker.cancel()
            raise

    async def drain(self, timeout: float | None = None) -> None:
        """Wait for in-flight workers/settlers (e.g. during shutdown)."""
        timeout = (
            self._settings.shutdown_drain_s if timeout is None else timeout
        )
        pending = [t for t in self._workers if not t.done()]
        if not pending:
            return
        logger.info("draining %d in-flight query task(s)", len(pending))
        _, still = await asyncio.wait(pending, timeout=timeout)
        if still:
            logger.warning(
                "%d task(s) did not finish draining; cancelling", len(still)
            )
            for task in still:
                task.cancel()
            await asyncio.wait(still, timeout=2.0)

    # ------------------------------------------------------------------ #
    # Worker: acquisition + one read-only retry
    # ------------------------------------------------------------------ #
    async def _run(self, sql: str, deadline: float) -> dict[str, Any]:
        """Worker body: borrow a lease, execute, retry once on backend loss.

        A dead-on-arrival backend (e.g. terminated out-of-band while idle)
        is discarded and rebuilt, then the read-only statement is retried
        once within the same total deadline.
        """
        last_conn_error: psycopg.Error | None = None
        for attempt in (1, 2):
            wait_started = time.monotonic()
            # Cancellation while queued happens here: asyncio's semaphore
            # waiter is undone (or handed straight to the next waiter) and
            # no connection was ever borrowed, so nothing needs cleanup.
            lease = await self._pool.acquire_lease(deadline)
            waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
            acquired_at = time.monotonic()
            try:
                return await self._drive(
                    lease, sql, deadline, waited_ms, acquired_at
                )
            except _ConnectionLost as exc:
                cause = exc.__cause__
                last_conn_error = (
                    cause
                    if isinstance(cause, psycopg.Error)
                    else psycopg.OperationalError(str(exc))
                )
                # The connection is unusable; drop it (slot returned exactly
                # once by the lease) and retry if budget remains.  A
                # cancellation while discarding means the worker is going
                # away: the lease was still settled exactly once, so no slot
                # is leaked.
                await lease.discard_and_replace()
                if attempt == 2 or deadline - time.monotonic() <= 0:
                    raise last_conn_error
                logger.warning(
                    "connection-level error, discarded; retrying read-only "
                    "statement once within budget: %s",
                    last_conn_error,
                )
                continue

        # Defensive: loop should either return or raise earlier.
        raise last_conn_error  # pragma: no cover

    async def _drive(
        self,
        lease: PoolLease,
        sql: str,
        deadline: float,
        waited_ms: float,
        acquired_at: float,
    ) -> dict[str, Any]:
        """Drive one statement on a borrowed lease and settle the lease."""
        conn = lease.conn
        statement: asyncio.Task | None = None
        settler: asyncio.Task | None = None
        cleanup_owned = False
        try:
            statement = self._track(
                asyncio.create_task(
                    self._run_statement(conn, sql, deadline),
                    name="query-statement",
                )
            )
            try:
                rows = await asyncio.wait_for(
                    asyncio.shield(statement),
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            except (asyncio.TimeoutError, TimeoutError):
                cleanup_owned = True
                settler = self._settle_task(lease, statement, "client deadline")
                recovery = await asyncio.shield(settler)
                raise self._timeout_receipt(
                    recovery, waited_ms, acquired_at, backstop=False
                )
            except psycopg.errors.QueryCanceled as exc:
                # The server-side statement_timeout backstop fired first.
                # Same recovery protocol, same 504 receipt.
                logger.warning("statement_timeout backstop fired: %s", exc)
                cleanup_owned = True
                settler = self._settle_task(lease, statement, "server backstop")
                recovery = await asyncio.shield(settler)
                raise self._timeout_receipt(
                    recovery, waited_ms, acquired_at, backstop=True
                )
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure (backend terminated/restarted).
                # The statement task has already finished; this backend must
                # never be reused.  Cleanup is delegated to the caller, which
                # discards the lease before its optional retry.
                lease.condemn()
                cleanup_owned = True
                raise _ConnectionLost(str(exc)) from exc
            except psycopg.Error:
                # Statement-level error: roll back, verify IDLE, then reuse
                # (or discard if that cannot be confirmed).
                cleanup_owned = True
                settler = self._settle_task(lease, statement, "statement error")
                await asyncio.shield(settler)
                raise
            except asyncio.CancelledError:
                # Request cancelled (or service shutting down).  Hand off to
                # a shielded settler and unwind immediately; do not await
                # cleanup here because the task is being cancelled.
                cleanup_owned = True
                settler = self._settle_task(lease, statement, "cancelled")
                raise
            else:
                cleanup_owned = True
                await lease.release_healthy()
                elapsed_ms = round(
                    (time.monotonic() - acquired_at) * 1000, 3
                )
                return {
                    "status": "ok",
                    "executed": True,
                    "wait_ms": waited_ms,
                    "execution_ms": elapsed_ms,
                    "row_count": len(rows),
                    "rows": [self._jsonable(row) for row in rows],
                }
        finally:
            # Last-resort guard.  ``release_*`` is idempotent, so this is a
            # no-op after the normal paths.  If no settler is already running
            # and the lease is still held, hand it to one; a second
            # cancellation landing in the settler cannot lose the slot (the
            # lease releases in ``finally``) nor lend an executing backend
            # (a lease that cannot be confirmed is retired, never reused).
            if not cleanup_owned and not lease.released:
                if statement is not None:
                    self._settle_task(lease, statement, "finalizer")
                else:
                    # No statement task was ever created: nothing is running
                    # on the connection, so return it directly.
                    await lease.release_healthy()

    # ------------------------------------------------------------------ #
    # Statement + settlement tasks
    # ------------------------------------------------------------------ #
    async def _run_statement(
        self, conn: psycopg.AsyncConnection, sql: str, deadline: float
    ) -> list[tuple[Any, ...]]:
        # Server-side backstop, deliberately *after* the request deadline:
        # application cancellation normally wins; statement_timeout only
        # intervenes if PQcancel is delayed.
        backstop_ms = max(
            1,
            int(
                (deadline - time.monotonic()) * 1000
                + self._settings.statement_timeout_grace_ms
            ),
        )
        async with conn.cursor() as cur:
            await cur.execute("BEGIN READ ONLY")
            await cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (str(backstop_ms),),
            )
            await cur.execute(sql)
            rows = await cur.fetchall()
            # Commit before the cursor block exits, or psycopg's cursor
            # teardown would implicitly ROLLBACK this txn.
            await cur.execute("COMMIT")
            return rows

    def _settle_task(
        self, lease: PoolLease, statement: asyncio.Task, reason: str
    ) -> asyncio.Task:
        """Start recovery in a task independent of the caller."""
        return self._track(
            asyncio.create_task(
                self._recover(lease, statement, reason=reason),
                name=f"query-settle-{reason}",
            )
        )

    def _track(self, task: asyncio.Task) -> asyncio.Task:
        self._workers.add(task)
        task.add_done_callback(self._workers.discard)
        task.add_done_callback(self._log_task_failure)
        return task

    @staticmethod
    def _log_task_failure(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        if isinstance(exc, QueryServiceError):
            # Domain errors are the receipt, delivered to the client by the
            # worker (or expected when the client has gone away).
            return
        if isinstance(exc, psycopg.Error):
            # QueryCanceled and connection loss are expected outcomes of the
            # cancellation protocol and are logged with their context there.
            return
        logger.error(
            "background query task %s failed: %r", task.get_name(), exc
        )

    # ------------------------------------------------------------------ #
    # Recovery protocol
    # ------------------------------------------------------------------ #
    async def _recover(
        self, lease: PoolLease, statement: asyncio.Task, *, reason: str
    ) -> _Recovery:
        """Stop the statement if needed, then settle the lease exactly once.

        Runs in its own task (see :meth:`_settle_task`), so cancellation of
        the request or of the worker does not interrupt it.  If it is called
        a second time the lease claim makes the final release idempotent and
        re-observing a finished statement is harmless.
        """
        if lease.released:
            return _Recovery(False, True, None)

        try:
            return await self._recover_unchecked(lease, statement, reason)
        finally:
            # If recovery itself was cancelled or blew up before settling,
            # the connection's state is unconfirmed.  Never lend it again:
            # release the slot and retire (close) the backend, letting the
            # pool lazily rebuild capacity.
            if not lease.released:
                logger.warning(
                    "%s: recovery interrupted; retiring connection", reason
                )
                lease.abandon()

    async def _recover_unchecked(
        self, lease: PoolLease, statement: asyncio.Task, reason: str
    ) -> _Recovery:
        """The recovery protocol proper; see :meth:`_recover`."""
        conn = lease.conn
        cancelled = False
        late_rows: list[tuple] | None = None

        if not statement.done():
            logger.warning("%s: cancelling statement on backend", reason)
            # 1) libpq cancel request.  PQcancel opens a separate connection
            #    to the server and is a blocking socket call; run it off the
            #    loop.  It is best-effort: failure leads to discard below.
            cancelled = await self._cancel_statement(conn)
            # 2) Wait for the statement task to actually finish so no
            #    coroutine keeps touching this connection.
            settled = await self._await_statement(statement)
            if not settled:
                # 3) The statement ignored PQcancel.  Closing the connection
                #    also aborts the backend query; then let the task unwind
                #    against the closed connection so it cannot outlive this
                #    call.
                logger.error(
                    "statement did not stop after cancel; discarding connection"
                )
                await lease.discard_and_replace()
                await self._force_unwind(statement)
                return _Recovery(cancelled, True, None)
        else:
            # Statement already ended; ask the server to abandon anything it
            # may still have pending, then inspect the (late) outcome.
            cancelled = self._cancel_statement_now(conn)

        # The transaction may only be inspected once the statement task has
        # ended; otherwise the connection might still be executing and must
        # not be lent or trusted.
        if not statement.done():
            logger.error(
                "statement task still running after bounded wait; discarding"
            )
            await lease.discard_and_replace()
            await self._force_unwind(statement)
            return _Recovery(cancelled, True, None)

        if not statement.cancelled():
            try:
                late_rows = statement.result()
            except BaseException:
                late_rows = None
            if late_rows is not None and reason in (
                "client deadline",
                "server backstop",
            ):
                # The statement finished before/while the cancel took effect.
                # Its result must NOT replace the timeout receipt.
                logger.warning(
                    "statement completed before cancel took effect; late "
                    "result with %d rows ignored, timeout receipt stands",
                    len(late_rows),
                )

        # 4) Prove transaction end.  If we cannot, drop the backend.
        idle = await self._pool.confirm_idle_or_discard(conn)
        if idle:
            await lease.release_healthy()
            return _Recovery(cancelled, False, late_rows)

        logger.warning("could not confirm backend idle; discarding connection")
        await lease.discard_and_replace()
        return _Recovery(cancelled, True, late_rows)

    async def _cancel_statement(self, conn: psycopg.AsyncConnection) -> bool:
        try:
            await asyncio.to_thread(conn.cancel)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PQcancel failed; connection will be discarded")
            return False

    @staticmethod
    def _cancel_statement_now(conn: psycopg.AsyncConnection) -> bool:
        """Best-effort synchronous PQcancel for an already-finished task."""
        try:
            conn.cancel()
            return True
        except Exception:
            return False

    async def _await_statement(self, statement: asyncio.Task) -> bool:
        """Wait (bounded) for the statement task to finish.

        Returns ``True`` if it finished, ``False`` if it is still running.
        Exceptions raised by the statement are consumed.
        """
        try:
            await asyncio.wait_for(
                asyncio.shield(statement),
                timeout=self._settings.cancel_grace_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            return False
        except asyncio.CancelledError:
            raise
        except BaseException:
            # QueryCanceled / connection errors are the expected outcomes.
            pass
        return statement.done()

    async def _force_unwind(self, statement: asyncio.Task) -> None:
        """Ensure a statement task cannot outlive a discarded connection."""
        if statement.done():
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(statement),
                timeout=self._settings.cancel_unwind_grace_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            statement.cancel()
            await asyncio.gather(statement, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except BaseException:
            pass

    @staticmethod
    def _timeout_receipt(
        recovery: _Recovery,
        waited_ms: float,
        acquired_at: float,
        *,
        backstop: bool,
    ) -> QueryTimeout:
        elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
        detail = (
            "语句执行超过总截止时间"
            if not backstop
            else "语句触发服务端 statement_timeout 兜底"
        )
        if recovery.discarded:
            detail += "；无法确认后端空闲，连接已弃置并补建"
        return QueryTimeout(
            detail + "，已请求取消",
            extra={
                "status": "timeout",
                "executed": True,
                "cancelled": recovery.cancelled,
                "connection_discarded": recovery.discarded,
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "late_result_ignored": recovery.late_rows is not None,
            },
        )

    @staticmethod
    def _jsonable(row: tuple) -> list[Any]:
        out: list[Any] = []
        for value in row:
            # Common non-JSON types from fault logs.
            if hasattr(value, "isoformat"):
                out.append(value.isoformat())
            elif isinstance(value, (bytes, memoryview)):
                out.append(bytes(value).hex())
            else:
                out.append(value)
        return out
