"""Runtime configuration, read from environment variables.

The service intentionally keeps only *two* physical PostgreSQL connections.
Every value can be overridden through environment variables so the same
image runs both under Docker Compose and in the local self-test harness.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    # DSN of the *read-only* login role.  The role itself lacks the privilege
    # to modify data; the application additionally forces READ ONLY sessions.
    database_dsn: str = os.environ.get(
        "DATABASE_DSN",
        "postgresql://fault_ro:fault_ro@db:5432/faultdb",
    )
    pool_size: int = _int("POOL_SIZE", 2)
    connect_timeout: float = _float("CONNECT_TIMEOUT", 10.0)
    # Server-side statement_timeout acts only as a backstop.  It is set
    # slightly beyond the request deadline so application-side cancellation
    # is the primary mechanism and the backstop fires only if that fails.
    statement_timeout_grace_ms: int = _int("STATEMENT_TIMEOUT_GRACE_MS", 1250)
    # How long to wait for the server to acknowledge PQcancel before the
    # connection is considered still busy and is discarded.  Must stay
    # comfortably inside an upstream proxy's response timeout.
    cancel_grace_s: float = _float("CANCEL_GRACE_S", 3.0)
    # Extra time granted to a statement task after its connection was closed,
    # so it unwinds instead of lingering.
    cancel_unwind_grace_s: float = _float("CANCEL_UNWIND_GRACE_S", 1.0)
    # Time allowed for in-flight cancellation cleanups at shutdown.
    shutdown_drain_s: float = _float("SHUTDOWN_DRAIN_S", 5.0)
    # Hard cap on the request body so an analyst cannot submit huge payloads.
    max_sql_chars: int = _int("MAX_SQL_CHARS", 10_000)


settings = Settings()
