"""HTTP-level cancellation test: clients abort slow queries mid-flight.

This reproduces the operational incident against a *running* server: an
upstream proxy cancels two in-flight slow queries, then later callers must
still be served instead of queueing until their own deadlines expire.

    python tests/test_disconnect.py          # against http://127.0.0.1:8000

Checks after the aborts:
  * the server reports both connections idle again (nothing leaked);
  * no cancelled statement is still running in pg_stat_activity (when an
    ADMIN_DSN is available);
  * the next queries succeed promptly with their own rows;
  * a query that legitimately exceeds its own budget still returns 504.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

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


def get(path: str, timeout: float = 10.0):
    with urllib.request.urlopen(BASE + path, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode())


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def start_then_disconnect(payload: dict, wait_s: float) -> None:
    """Send a request and close the socket mid-flight (no response read)."""
    data = json.dumps(payload).encode()
    host, port = "127.0.0.1", 8000
    sock = socket.create_connection((host, port), timeout=10)
    head = (
        f"POST /query HTTP/1.1\r\nHost: {host}:{port}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n"
    )
    sock.sendall(head.encode() + data)
    time.sleep(wait_s)              # let the server start the statement
    sock.close()                    # upstream cancels: client vanished


def admin_pids() -> list[int] | None:
    dsn = os.environ.get("ADMIN_DSN")
    if not dsn:
        f = os.path.join(tempfile.gettempdir(), "faultdb_admin_dsn")
        if os.path.exists(f):
            dsn = open(f, encoding="utf-8").read().strip()
    if not dsn:
        return None
    try:
        import psycopg
    except ImportError:
        return None
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity WHERE usename='fault_ro' "
            "AND state='active' AND query LIKE '%pg_sleep%'"
        )
        return [r[0] for r in cur.fetchall()]


def wait_healthy_idle(timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    body = None
    while time.monotonic() < deadline:
        _, body = get("/health")
        if body["pool"]["idle"] == body["pool"]["size"] and \
                body["pool"]["in_use"] == 0:
            return body
        time.sleep(0.1)
    return body


def main() -> int:
    print(f"disconnect test against {BASE}")
    status, body = get("/health")
    check("启动时两条连接空闲",
          status == 200 and body["pool"]["idle"] == 2, body)

    print("\n=== 1. 两个已开始的慢查询被上游取消 ===")
    for i in range(2):
        start_then_disconnect(
            {"sql": f"SELECT 'DC{i}', pg_sleep(30)", "timeout_ms": 25000},
            wait_s=0.6,
        )
    health = wait_healthy_idle()
    check("客户端断开后两个名额都收回",
          health["pool"]["idle"] == 2 and health["pool"]["in_use"] == 0,
          health)
    pids = admin_pids()
    if pids is None:
        print("  SKIP  数据库活动查询检查（无 ADMIN_DSN）")
    else:
        check("数据库侧无仍在运行的被取消语句", pids == [], pids)

    print("\n=== 2. 接续查询不被积压拖垮 ===")
    t0 = time.monotonic()
    status, body = post(
        "/query", {"sql": "SELECT 'AFTER-DC'", "timeout_ms": 2000}
    )
    took = time.monotonic() - t0
    check("新查询 200 且拿到自身结果",
          status == 200 and body["rows"] == [["AFTER-DC"]], body)
    check("新查询无需排队即完成 (<1s)", took < 1.0, f"took={took:.2f}s")

    print("\n=== 3. 混合：取消与正常请求交替 ===")
    results: list = []

    def normal(tag: str) -> None:
        s, b = post("/query", {
            "sql": f"SELECT '{tag}', pg_sleep(0.6)", "timeout_ms": 5000,
        })
        results.append((tag, s, b))

    threads = [threading.Thread(target=normal, args=(f"N{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    start_then_disconnect(
        {"sql": "SELECT 'GHOST', pg_sleep(30)", "timeout_ms": 25000},
        wait_s=0.5,
    )
    for t in threads:
        t.join()
    check("并发正常请求全部成功",
          len(results) == 2 and all(s == 200 for _, s, _ in results), results)
    check("各自拿到自身结果",
          {b["rows"][0][0] for _, _, b in results} == {"N0", "N1"}, results)
    health = wait_healthy_idle()
    check("混合场景后名额全部收回",
          health["pool"]["idle"] == 2 and health["pool"]["in_use"] == 0,
          health)

    print("\n=== 4. 原有超时语义保持不变 ===")
    t0 = time.monotonic()
    status, body = post(
        "/query", {"sql": "SELECT pg_sleep(3)", "timeout_ms": 800},
        timeout=15,
    )
    took = time.monotonic() - t0
    check("执行超时仍为 504",
          status == 504 and body.get("error", {}).get("code") == "query_timeout",
          body)
    check("超时及时返回 (<2.5s)", took < 2.5, f"took={took:.2f}s")
    health = wait_healthy_idle()
    check("超时后池仍满且空闲", health["pool"]["idle"] == 2, health)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
