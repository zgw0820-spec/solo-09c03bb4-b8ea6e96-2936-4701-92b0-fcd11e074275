"""Cancellation-focused protocol tests (no HTTP server required).

These reproduce the incident: an upstream cancels requests that have already
started slow queries.  Every scenario checks that no pool slot is lost and
that a subsequent query still gets its own result.

Scenarios:
  A. cancel a request whose statement is executing: the statement is
     cancelled, the transaction is confirmed ended, the slot is returned and
     the very next query succeeds;
  B. cancel several times in a row, including during a *rebuild* triggered by
     a killed backend, and verify capacity never shrinks permanently;
  C. cancel while queued: no connection is borrowed, no statement is sent;
  D. a second cancellation delivered while the first cleanup is running
     neither leaks nor double-releases the slot;
  E. a connection that is still executing is never lent to another request.

Run:  python tests/test_cancel.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.executor import QueryExecutor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def wait_for_pool(pool: QueryConnectionPool, timeout: float = 12.0):
    """Wait until both backends are idle again (background cleanup done)."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        st = await pool.status()
        last = st
        if st.idle == st.size and st.in_use == 0:
            return st
        await asyncio.sleep(0.05)
    return last


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="cancel-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    def running_fault_ro() -> int:
        with psycopg.connect(fault_admin, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE usename='fault_ro' AND state='active'"
            )
            return cur.fetchone()[0]

    print("=== A. 执行中取消后连接归还未泄漏 ===")
    task = asyncio.create_task(
        executor.execute("SELECT pg_sleep(30)", time.monotonic() + 20)
    )
    await asyncio.sleep(0.5)
    check("慢查询确已发给数据库", running_fault_ro() >= 1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    st = await wait_for_pool(pool)
    check("取消后两条连接都回到空闲", st.idle == 2 and st.in_use == 0, st)
    check("取消后池容量仍为 2", st.size == 2, st)
    await asyncio.sleep(0.2)
    check("数据库侧已无活跃的 cancelled 查询", running_fault_ro() == 0,
          running_fault_ro())
    ok = await executor.execute("SELECT 'A-AFTER'", time.monotonic() + 5)
    check("取消后的接续查询拿到自身结果", ok["rows"] == [["A-AFTER"]], ok)

    print("=== B. 连续两次取消（复现事故）后接续查询 ===")
    discarded_before = (await pool.status()).total_discarded
    tasks = [
        asyncio.create_task(
            executor.execute(f"SELECT 'B{i}', pg_sleep(30)",
                             time.monotonic() + 20)
        )
        for i in range(2)
    ]
    await asyncio.sleep(0.6)
    check("两条慢查询同时占用两个名额", (await pool.status()).in_use == 2,
          await pool.status())
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
    st = await wait_for_pool(pool)
    check("两次取消后名额全部收回", st.idle == 2 and st.in_use == 0, st)
    ok = await executor.execute("SELECT 'B-AFTER'", time.monotonic() + 5)
    check("事故后新查询不再排队超时", ok["rows"] == [["B-AFTER"]], ok)
    st = await pool.status()
    check("确认事务结束后正常复用（无新增弃置）",
          st.total_discarded == discarded_before, st)

    print("=== C. 排队中取消不占连接、不发语句 ===")
    holders = [
        asyncio.create_task(
            executor.execute(f"SELECT 'H{i}', pg_sleep(1.5)",
                             time.monotonic() + 10)
        )
        for i in range(2)
    ]
    await asyncio.sleep(0.3)
    st_before = await pool.status()
    queued = asyncio.create_task(
        executor.execute("SELECT 'NEVER'", time.monotonic() + 10)
    )
    await asyncio.sleep(0.2)
    check("第三个请求确实在排队（未借到连接）",
          (await pool.status()).in_use == st_before.in_use, await pool.status())
    queued.cancel()
    try:
        await queued
    except asyncio.CancelledError:
        pass
    results = await asyncio.gather(*holders)
    check("占池查询不受影响", {r["rows"][0][0] for r in results} == {"H0", "H1"},
          results)
    st = await wait_for_pool(pool)
    check("排队取消后名额仍为 2", st.idle == 2 and st.in_use == 0, st)
    ok = await executor.execute(
        "SELECT count(*) FROM fault_logs WHERE message LIKE '%NEVER%'",
        time.monotonic() + 5,
    )
    check("排队取消的语句从未发送（无副作用）", ok["rows"] == [[0]], ok)

    print("=== D. 清理期间再次取消不丢失/不重复释放名额 ===")
    for round_no in range(5):
        t = asyncio.create_task(
            executor.execute(
                f"SELECT 'D{round_no}', pg_sleep(30)", time.monotonic() + 20
            )
        )
        await asyncio.sleep(0.2)
        t.cancel()                     # first cancellation -> cleanup starts
        await asyncio.sleep(0)         # let the settler begin (PQcancel)
        for settle_task in list(executor._workers):
            # Simulate an upstream that also cancels the cleanup coroutine.
            settle_task.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        st = await wait_for_pool(pool)
        ok = await executor.execute("SELECT 'D-AFTER'", time.monotonic() + 5)
        check(f"第 {round_no + 1} 轮：清理中再取消后名额为 2 且查询成功",
              st.idle == 2 and st.in_use == 0 and ok["rows"] == [["D-AFTER"]],
              (st, ok))

    print("=== D2. 在 ROLLBACK/空闲确认窗口内再次取消 ===")
    # Deterministically cancel the settler while it is inside the idleness
    # confirmation (after PQcancel, at the ROLLBACK/state check).  The lease
    # must still release exactly once and never lend the connection.
    original = pool.confirm_idle_or_discard
    hit = {"n": 0}

    async def cancellable_confirm(conn):
        hit["n"] += 1
        cancel_me = asyncio.current_task()
        # Cancel ourselves right before proving idleness: worst-case window.
        cancel_me.cancel()
        return await original(conn)

    pool.confirm_idle_or_discard = cancellable_confirm
    try:
        for round_no in range(3):
            t = asyncio.create_task(
                executor.execute("SELECT pg_sleep(30)", time.monotonic() + 20)
            )
            await asyncio.sleep(0.2)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            st = await wait_for_pool(pool)
            ok = await executor.execute("SELECT 'D2-AFTER'", time.monotonic() + 5)
            check(
                f"第 {round_no + 1} 轮：确认窗口内取消后名额为 2 且查询成功",
                st.idle == 2 and st.in_use == 0
                and ok["rows"] == [["D2-AFTER"]],
                (st, ok),
            )
        check("确实在空闲确认窗口注入了取消", hit["n"] >= 3, hit)
    finally:
        pool.confirm_idle_or_discard = original

    print("=== E. 后端被带外终止：弃置补建且仍成功返回自身结果 ===")
    # Kill one idle pooled backend, then run fast queries until both
    # backends have been cycled.  Whichever request draws the dead backend
    # must discard it, rebuild, retry once, and still return its own rows.
    with psycopg.connect(fault_admin, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename='fault_ro' AND pid <> pg_backend_pid()"
        )
        pids = [r[0] for r in cur.fetchall()]
        check("池内存在两条 fault_ro 后端", len(pids) == 2, pids)
        cur.execute("SELECT pg_terminate_backend(%s)", (pids[0],))
        check("成功终止一个后端", cur.fetchone()[0] is True)
    markers = []
    statuses = []
    for i in range(4):
        r = await executor.execute(f"SELECT 'E-{i}'", time.monotonic() + 5)
        statuses.append(r["status"])
        markers.append(r["rows"][0][0])
    check("借出死连接时客户端无感重试成功", statuses == ["ok"] * 4, statuses)
    check("每次返回自身结果", markers == [f"E-{i}" for i in range(4)], markers)
    st = await wait_for_pool(pool)
    check("弃置后补建回两条空闲连接", st.idle == 2 and st.size == 2, st)
    ok = await executor.execute("SELECT 'E-AFTER'", time.monotonic() + 5)
    check("补建后查询正常", ok["rows"] == [["E-AFTER"]], ok)

    print("=== F. 全程不变量：物理后端数 == 池容量、名额不漂移 ===")
    st = await pool.status()
    with psycopg.connect(fault_admin, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE usename='fault_ro'"
        )
        physical = cur.fetchone()[0]
    check("物理后端数等于池容量 (2)", physical == 2, physical)
    check("空闲 + 占用 = 容量，创建数 - 弃置数 = 当前后端数",
          st.idle + st.in_use == st.size
          and st.total_created - st.total_discarded == physical,
          (st, physical))
    check("信号量名额恰好收回 2 个且无残留租约（超额释放会抛错）",
          pool._slots._value == 2 and len(pool._borrowed) == 0,
          (pool._slots._value, len(pool._borrowed)))

    await executor.drain()
    await pool.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
