"""FastAPI application: a two-connection read-only fault-log query API.

Run locally::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Swagger UI is served at ``/docs``, ReDoc at ``/redoc``.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .errors import InvalidDeadlineError, QueryServiceError
from .executor import QueryExecutor
from .pool import QueryConnectionPool
from .schemas import HealthResponse, QueryRequest, QueryResponse
from .validation import validate_single_select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

pool = QueryConnectionPool(settings)
executor = QueryExecutor(pool, settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Eagerly open both connections at startup; close them on shutdown.
    await pool.open()
    try:
        yield
    finally:
        # Let cancellation cleanups finish before connections are closed, so
        # a cancelled request cannot leave a backend running.
        await executor.drain()
        await pool.close()


app = FastAPI(
    title="机房设备故障日志只读查询 API",
    description=(
        "值班分析员共用的只读 SQL 查询入口。固定两条 PostgreSQL 连接，"
        "提交单条 SELECT 与总截止时间；排队时间计入预算，超时立即取消语句，"
        "确认事务结束才复用连接，无法确认则弃置并补建。请求被客户端或上游"
        "取消时同样取消语句并完成连接收尾，不占用、不泄漏连接名额。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryServiceError)
async def _service_error_handler(_: Request, exc: QueryServiceError):
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": {"code": exc.code, "message": str(exc)},
            **exc.extra,
        },
    )


@app.exception_handler(Exception)
async def _unexpected_handler(_: Request, exc: Exception):
    logger.exception("unhandled error")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
async def health() -> HealthResponse:
    """连接池健康状态：容量、空闲/占用数、累计创建与弃置数。"""
    status = await pool.status()
    return HealthResponse(
        status="healthy",
        pool={
            "size": status.size,
            "idle": status.idle,
            "in_use": status.in_use,
            "total_created": status.total_created,
            "total_discarded": status.total_discarded,
            "closed": status.closed,
        },
    )


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["查询"],
    responses={
        400: {"description": "SQL 不符合单条只读 SELECT 约束"},
        422: {"description": "请求体或截止时间不合法"},
        499: {"description": "客户端断开，语句已取消、连接收尾中"},
        503: {"description": "排队超时 / 连接被弃置"},
        504: {"description": "语句执行超过总截止时间"},
    },
)
async def query(
    body: QueryRequest, request: Request
) -> QueryResponse | JSONResponse:
    """执行一条只读 SELECT，``timeout_ms`` 是包含排队时间的总预算。"""
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)

    # Absolute monotonic deadline: waiting + execution share one budget.
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    # A deadline that expires while queued surfaces as a 503
    # acquire_timeout; an execution deadline as a 504 query_timeout.
    execute = asyncio.create_task(executor.execute(sql, deadline))
    cancel_reason = {"disconnect": False}
    watcher = asyncio.create_task(
        _watch_disconnect(request, execute, cancel_reason),
        name="disconnect-watch",
    )
    try:
        result = await execute
    except asyncio.CancelledError:
        if cancel_reason["disconnect"]:
            # The client is gone: there is nobody to deliver a receipt to.
            # The executor has already handed cleanup to its own shielded
            # task (statement cancelled, connection returned or discarded), so
            # answer 499 instead of letting CancelledError surface as an ASGI
            # error; the response write is discarded by the server.
            logger.info("query cancelled by client disconnect; 499 receipt")
            return JSONResponse(
                status_code=499,
                content={
                    "error": {
                        "code": "client_disconnected",
                        "message": "客户端已断开；语句已请求取消，连接收尾中",
                    }
                },
            )
        # Shutdown or an explicit cancellation of the handler.  The executor
        # has already handed cleanup to its own shielded task.
        logger.info("request cancelled; statement cleanup in progress")
        raise
    finally:
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await watcher
    return QueryResponse(**result)


async def _watch_disconnect(
    request: Request, execute: asyncio.Task, reason: dict[str, bool]
) -> None:
    """Cancel the query as soon as the client goes away.

    ASGI servers (uvicorn included) do not cancel a running endpoint when
    the peer disconnects; they only deliver an ``http.disconnect`` message.
    Without this watcher a query whose caller vanished would keep a scarce
    connection until its own deadline, which is exactly how two cancelled
    slow queries can occupy both slots.  Cancelling the execute task triggers
    the executor's shielded stop-and-release protocol.
    """
    while not execute.done():
        if await request.is_disconnected():
            logger.info("client disconnected; cancelling query")
            reason["disconnect"] = True
            execute.cancel()
            return
        # ``is_disconnected`` does not block; poll at a bounded rate.
        try:
            await asyncio.wait_for(asyncio.shield(execute), timeout=0.1)
        except asyncio.TimeoutError:
            continue
        except BaseException:
            return
