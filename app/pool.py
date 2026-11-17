"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  A bounded
  semaphore serialises requests so a third request *waits* instead of
  opening another backend.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire_lease` takes an absolute
  monotonic deadline).  If the deadline passes while queued, no statement
  is ever sent on any connection.  If the *request task* is cancelled while
  queued, the waiter is removed and the slot is handed back; no connection
  is borrowed and none is leaked.
* A borrower receives a :class:`PoolLease`.  The lease owns exactly one
  semaphore slot for its whole life and its release methods are
  **idempotent**: the slot is returned in a ``finally`` block, exactly once,
  even if the releasing coroutine is cancelled half-way through a rollback
  or a replacement build.  This is what keeps cancellation at any stage
  (statement running, statement just finished, rollback, rebuild) from
  either leaking a slot or releasing it twice.
* After a timeout/cancellation the handler must prove the backend is idle
  again (``transaction_status == IDLE`` after a successful ``ROLLBACK``)
  before the connection is returned to the pool.  If that cannot be
  confirmed the connection is closed and discarded, and a replacement is
  built so the pool keeps its capacity of two (or lazily rebuilt on the next
  acquire if the rebuild itself fails).
* A lease that is released *without* a confirmed outcome (e.g. a second
  cancellation interrupted recovery) never lends its connection to anyone:
  the connection is retired (closed) in the background and the pool lazily
  rebuilds capacity.
* Every connection is opened in a read-only, read-committed session.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import weakref
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireTimeout

logger = logging.getLogger("querypool")

# Settlement outcomes of a lease.
SETTLED_RETURNED = "returned"
SETTLED_DISCARDED = "discarded"
SETTLED_UNRESOLVED = "unresolved"


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool


class PoolLease:
    """Exactly-once ownership of one pool slot and its connection.

    ``release_healthy`` puts the connection back only after the caller has
    confirmed it is idle; ``discard_and_replace`` never reuses it.  Both are
    idempotent and release the semaphore in ``finally``, so cleanup can be
    re-entered after a second cancellation without losing or duplicating a
    slot.  ``abandon`` (used only when recovery itself was cancelled) retires
    the connection in the background: it is never handed to another borrower.
    """

    __slots__ = ("_pool", "conn", "_released", "_settled", "outcome", "condemned")

    def __init__(self, pool: "QueryConnectionPool", conn: AsyncConnection) -> None:
        self._pool = pool
        self.conn = conn
        self._released = False
        self._settled = False
        self.outcome: str | None = None
        # Set when the connection must never be reused (e.g. a connection
        # level failure), even if it happens to report IDLE.
        self.condemned = False

    @property
    def released(self) -> bool:
        return self._released

    def condemn(self) -> None:
        """Never lend this connection again, whatever its state says."""
        self.condemned = True

    async def release_healthy(self) -> None:
        """Return a connection only after confirming it is IDLE.

        If the transaction state cannot be confirmed healthy, the connection
        is discarded and replaced instead of being handed to the next
        borrower.  The slot is released exactly once either way.
        """
        if not self._claim():
            return
        try:
            conn = self.conn
            if self._pool._closed:
                # Shutting down: never build a replacement.
                self.outcome = SETTLED_DISCARDED
                await self._pool._discard_locked(conn)
                self._settled = True
            elif self.condemned:
                self.outcome = SETTLED_DISCARDED
                await self._pool._discard_and_rebuild(conn)
            elif self._pool._is_idle(conn):
                # No await between the check and the hand-back, so the
                # connection cannot start executing again in between.  Mark
                # settled *before* the hand-back: once the connection is in
                # the free list, a later cancellation must not route it to
                # retirement (which would double-account a live connection).
                self.outcome = SETTLED_RETURNED
                self._settled = True
                self._pool._free.put_nowait(conn)
            else:
                self.outcome = SETTLED_DISCARDED
                await self._pool._discard_and_rebuild(conn)
                self._settled = True
        finally:
            self._complete()

    async def discard_and_replace(self) -> None:
        """Drop the connection (never reuse it) and best-effort rebuild."""
        if not self._claim():
            return
        try:
            self.outcome = SETTLED_DISCARDED
            await self._pool._discard_and_rebuild(self.conn)
            self._settled = True
        finally:
            self._complete()

    def abandon(self) -> None:
        """Release the slot without ever lending the connection again.

        Used when the recovery protocol itself was cancelled.  The backend is
        retired asynchronously (closing a libpq connection aborts any query
        still running on it); the pool lazily rebuilds capacity.
        """
        if not self._claim():
            return
        self.outcome = SETTLED_UNRESOLVED
        self._complete()

    def _claim(self) -> bool:
        # Synchronous check-and-set: two cleanup paths racing on the same
        # lease (e.g. a settler and the drive finalizer) can never both do
        # the release, so the slot is never returned twice.
        if self._released:
            return False
        self._released = True
        return True

    def _complete(self) -> None:
        self._pool._borrowed.pop(self.conn, None)
        self._pool._slots.release()
        if not self._settled:
            # The connection's fate was not confirmed (recovery was itself
            # cancelled or interrupted).  Never lend it again: retire it in
            # the background.  A task is used because no await is possible
            # while unwinding from a cancellation.
            try:
                task = asyncio.get_running_loop().create_task(
                    self._pool._retire(self.conn), name="retire-connection"
                )
            except RuntimeError:  # pragma: no cover - loop already gone
                logger.error("no running loop; connection leaked at shutdown")
            else:
                self._pool._retire_tasks.add(task)
                task.add_done_callback(self._pool._retire_tasks.discard)
        elif self.outcome == SETTLED_DISCARDED:
            # The slot is free again but the physical backend is gone; make
            # sure capacity is refilled even if the cleanup task that
            # discarded it is cancelled right now.
            self._pool._schedule_restore()


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # One slot per *borrower*.  Acquiring a slot guarantees the right to
        # use one physical backend, even if that backend must be (re)built.
        self._slots = asyncio.BoundedSemaphore(settings.pool_size)
        self._free: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        # Lease per borrowed connection.  It makes the release-by-connection
        # API idempotent: a connection can only be returned once because its
        # lease is removed from this map on release.
        self._borrowed: "weakref.WeakKeyDictionary[AsyncConnection, PoolLease]" = (
            weakref.WeakKeyDictionary()
        )
        # Number of physical backends currently tracked (idle + in use).
        # It can temporarily fall below pool_size when a rebuild fails.
        self._backends = 0
        self._closed = False
        self._lock = asyncio.Lock()
        # Serialises capacity decisions together with connection creation so
        # a lazy build (on acquire) and a background refill can never both
        # build and exceed the physical limit of ``pool_size``.
        self._build_lock = asyncio.Lock()
        self._restore_task: asyncio.Task | None = None
        # Connections already accounted for as discarded (idempotency guard).
        self._retired: "weakref.WeakSet[AsyncConnection]" = weakref.WeakSet()
        # Background connection-retirement tasks (created by leases that were
        # released without a confirmed outcome), drained on shutdown.
        self._retire_tasks: set[asyncio.Task] = set()
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._build_lock:
            async with self._lock:
                created: list[AsyncConnection] = []
                try:
                    for _ in range(self._settings.pool_size):
                        created.append(await self._make_connection())
                except BaseException:
                    # Do not leak half of a pool that failed to open.
                    for conn in created:
                        await self._discard_locked(conn)
                    raise
                for conn in created:
                    self._free.put_nowait(conn)
        logger.info(
            "pool opened: size=%d created=%d",
            self._settings.pool_size,
            self._total_created,
        )

    async def close(self) -> None:
        # Let background refills/retirements finish so discarded backends are
        # really closed before the pool is declared closed.
        pending = [
            t for t in self._retire_tasks
            if not t.done() and t is not asyncio.current_task()
        ]
        if pending:
            await asyncio.wait(pending, timeout=5.0)
        async with self._build_lock:
            async with self._lock:
                self._closed = True
                while True:
                    try:
                        conn = self._free.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._backends -= 1
                    await self._safe_close(conn)
        logger.info("pool closed")

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=len(self._borrowed),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
        )

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self) -> AsyncConnection:
        """Open a fresh connection pinned to a read-only session.

        Cancellation-safe: if setup is interrupted (or fails) the partially
        built backend is closed instead of being leaked.
        """
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            self._settings.database_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        try:
            # Session-level safety net: even an accidental COMMIT cannot write.
            await conn.execute("SET default_transaction_read_only = on")
            await conn.execute(
                "SET default_transaction_isolation = 'read committed'"
            )
            # Async connections expose a setter coroutine, not a writable
            # property.
            await conn.set_autocommit(False)
        except BaseException:
            await self._safe_close(conn)
            raise
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._backends += 1
        self._total_created += 1
        logger.info(
            "connection created (backends=%d total_created=%d)",
            self._backends,
            self._total_created,
        )
        return conn

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def acquire_lease(self, deadline: float) -> PoolLease:
        """Reserve one slot and one connection, waiting until ``deadline``.

        Time spent here counts against the request's total deadline.  On
        queue timeout *or cancellation* the semaphore waiter is undone and
        nothing has been sent to the database for this request.
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")

        # ``slot_held`` tracks ownership explicitly.  ``asyncio.timeout``
        # (unlike wait_for) never swallows an unrelated external
        # cancellation, and asyncio's semaphore hands the slot back when its
        # waiter is cancelled.  The flag covers the remaining race where the
        # slot is granted in the same tick the deadline expires (or a
        # cancellation is delivered right after the grant): the slot is then
        # released here, exactly once, instead of being leaked.
        slot_held = False
        try:
            async with asyncio.timeout(remaining):
                await self._slots.acquire()
            slot_held = True
            conn = await self._take_idle_or_build()
        except TimeoutError as exc:
            if slot_held:
                slot_held = False
                self._slots.release()
            raise PoolAcquireTimeout(
                "等待连接超过总截止时间，排队期间已过期，语句未执行"
            ) from exc
        except BaseException:
            if slot_held:
                slot_held = False
                self._slots.release()
            raise
        # No await between obtaining the connection and recording the lease,
        # so the lease and its slot ownership cannot be torn apart.
        lease = PoolLease(self, conn)
        self._borrowed[conn] = lease
        return lease

    async def acquire(self, deadline: float) -> AsyncConnection:
        """Backwards-compatible connection-returning acquire.

        Ownership is still tracked by a lease; use the matching
        :meth:`release_healthy` / :meth:`discard_and_replace` wrappers.
        """
        lease = await self.acquire_lease(deadline)
        return lease.conn

    async def _take_idle_or_build(self) -> AsyncConnection:
        # Fast path: an idle connection is available.  ``get_nowait`` is
        # atomic; the IDLE check plus hand-out has no await in between.
        if self._free.qsize() > 0:
            conn = self._free.get_nowait()
            if self._is_idle(conn):
                return conn
            # An idle backend turned stale; replace it before handing out.
            logger.warning(
                "idle connection not healthy (closed=%s tx=%s), replacing",
                conn.closed,
                conn.info.transaction_status,
            )
            await self._discard_locked(conn)

        # Slow path: build under the capacity lock, re-checking the free list
        # so a concurrent background refill cannot make us exceed
        # ``pool_size`` physical backends.
        async with self._build_lock:
            if self._closed:
                raise PoolAcquireTimeout("连接池正在关闭")
            while not self._free.empty():
                conn = self._free.get_nowait()
                if self._is_idle(conn):
                    return conn
                await self._discard_locked(conn)
            # Pool shrank after an earlier discarded connection: rebuild
            # lazily within the caller's remaining deadline budget.
            logger.info("no idle backend; lazily rebuilding within pool")
            return await self._make_connection()

    async def release_healthy(self, conn: AsyncConnection) -> None:
        """Return a connection that has been confirmed IDLE to the pool."""
        lease = self._borrowed.get(conn)
        if lease is None:
            # Already released (e.g. repeated cancellation hitting the
            # cleanup path twice).  Never put the connection back twice.
            logger.debug("release_healthy ignored: connection already released")
            return
        await lease.release_healthy()

    async def discard_and_replace(self, conn: AsyncConnection) -> None:
        """Drop an unverifiable connection and build its replacement."""
        lease = self._borrowed.get(conn)
        if lease is None:
            logger.debug("discard_and_replace ignored: connection already released")
            return
        await lease.discard_and_replace()

    async def _discard_and_rebuild(self, conn: AsyncConnection) -> None:
        """Close ``conn`` and ensure a replacement is built.

        The caller's lease releases the slot in its own ``finally``; this
        helper never touches the semaphore.  The replacement build runs in a
        task detached from the caller: if the caller (e.g. a cleanup task) is
        itself cancelled mid-build, the build still completes and refills the
        pool, so capacity is not left shrunk behind the slot that was just
        returned.  Failing to build is not fatal: the pool lazily rebuilds on
        the next acquire.
        """
        logger.warning(
            "discarding connection (closed=%s tx=%s) and rebuilding",
            conn.closed,
            conn.info.transaction_status,
        )
        await self._discard_locked(conn)
        await self._ensure_restore()

    async def _ensure_restore(self) -> None:
        """Start a background refill unless one is already running."""
        task = self._restore_task
        if task is not None and not task.done():
            return
        try:
            task = asyncio.get_running_loop().create_task(
                self._restore_capacity(), name="pool-restore"
            )
        except RuntimeError:  # pragma: no cover - loop gone at shutdown
            return
        self._restore_task = task
        self._retire_tasks.add(task)
        task.add_done_callback(self._retire_tasks.discard)

    def _schedule_restore(self) -> None:
        """Fire-and-forget refill; safe to call from non-async cleanup."""
        try:
            asyncio.get_running_loop().create_task(self._ensure_restore())
        except RuntimeError:  # pragma: no cover - loop gone at shutdown
            pass

    async def _restore_capacity(self) -> None:
        """Build missing backends up to ``pool_size`` (idle + in use)."""
        async with self._build_lock:
            while not self._closed and self._backends < self._settings.pool_size:
                try:
                    fresh = await self._make_connection()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "failed to rebuild discarded connection; "
                        "pool will lazily rebuild on next acquire"
                    )
                    return
                else:
                    self._free.put_nowait(fresh)

    async def _retire(self, conn: AsyncConnection) -> None:
        """Close a connection whose lease was released without settlement."""
        logger.warning(
            "retiring connection released without confirmed outcome "
            "(closed=%s tx=%s); rebuilding capacity",
            conn.closed,
            conn.info.transaction_status,
        )
        await self._discard_locked(conn)
        if not self._closed:
            await self._ensure_restore()

    async def _discard_locked(self, conn: AsyncConnection) -> None:
        # Accounting is idempotent so a second cancellation re-entering
        # cleanup cannot make ``total_discarded`` drift from ``backends``.
        if not conn.closed:
            await self._safe_close(conn)
        if conn in self._retired:
            return
        self._retired.add(conn)
        self._backends -= 1
        self._total_discarded += 1

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Prove that no statement is running anymore (best effort).

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status == TransactionStatus.ACTIVE:
                # The backend is still executing.  Issuing ROLLBACK here
                # would queue behind (or block on) that statement — the very
                # connection we set out to reclaim.  Refuse instead; the
                # caller discards it, and closing aborts the backend query.
                logger.warning(
                    "backend still ACTIVE; refusing to reuse it"
                )
                return False
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except asyncio.CancelledError:
            # Recovery was interrupted; the caller's lease-finalizer decides
            # (it retires rather than lends the connection).
            raise
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
