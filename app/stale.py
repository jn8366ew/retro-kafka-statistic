"""C4 stale write 결정적 재현 — Kafka 없이 DB 트랜잭션 두 개의 순서를 강제한다.

    uv run python -m app.stale s1     # 잠금 없음 → stale write
    uv run python -m app.stale s2     # 행사별 잠금 (잠금이 첫 조회) → 정확, B 는 잠금 대기
    uv run python -m app.stale s3     # 잠금은 있지만 B 가 잠금 전에 읽었다 → 옛 스냅샷으로 stale write
    uv run python -m app.stale s3b    # s3 와 같은 순서, B 만 READ COMMITTED → 정확
    uv run python -m app.stale all

전용 행사 RACE-01 (예약 r-9001 하나)만 쓴다. 다른 실험의 데이터는 건드리지 않는다.
원천은 100 에서 120 으로 바뀌고, 최종 집계의 정답은 항상 120 이다.

작업 A·B 는 각자의 DB 연결을 가진 스레드다. db.recompute_event() 의 hook 지점에서
멈췄다가 메인 스레드가 풀어 주는 방식으로 순서를 고정한다 (타이밍 운이 아니라 매번 같은 결과).

<사용자 실험 지점>
  * s2 에서 A 를 "after-read" 가 아니라 "locked" 에서 멈추면 A 가 읽는 값이 달라진다. 왜 그런가?
  * s3 의 pre-read 를 원천이 아닌 전혀 다른 테이블(event_statistics) 조회로 바꿔도 stale write 가
    나는지 본다 — 스냅샷은 테이블 단위가 아니라 트랜잭션 단위다.
"""

from __future__ import annotations

import sys
import threading
import time

from . import db
from .schemas import ReservationEvent

CODE = "RACE-01"
RID = "r-9001"
_t0 = time.monotonic()


def log(who: str, msg: str) -> None:
    print(f"  {time.monotonic() - _t0:6.2f}s  {who:<4} {msg}", flush=True)


def set_source(amount: int) -> None:
    conn = db.connect()
    try:
        db.upsert_reservation(conn, ReservationEvent(RID, CODE, "RESERVATION_UPDATED",
                                                     {"status": "CONFIRMED", "amount": amount}))
        conn.commit()
    finally:
        conn.close()


def setup() -> None:
    """RACE-01 만 초기화: 원천 100, 집계 100."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reservations WHERE event_code = %s", (CODE,))
            cur.execute("DELETE FROM event_statistics WHERE event_code = %s", (CODE,))
        conn.commit()
    finally:
        conn.close()
    set_source(100)
    conn = db.connect()
    try:
        db.recompute_event(conn, CODE)
        conn.commit()
    finally:
        conn.close()


def read_state() -> tuple[int, int]:
    """(원천 합계, 집계 total_amount)"""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(amount),0) FROM reservations WHERE event_code=%s AND status='CONFIRMED'", (CODE,))
            src = int(cur.fetchone()[0])
            cur.execute("SELECT total_amount FROM event_statistics WHERE event_code=%s", (CODE,))
            row = cur.fetchone()
        return src, (int(row[0]) if row else -1)
    finally:
        conn.close()


class Worker(threading.Thread):
    """집계 작업 하나 = DB 연결 하나 = 트랜잭션 하나."""

    def __init__(self, name: str, *, lock: bool, pause_at: tuple[str, ...] = (),
                 pre_read: bool = False, isolation: str | None = None) -> None:
        super().__init__(name=name, daemon=True)
        self.lock, self.pre_read, self.isolation = lock, pre_read, isolation
        self.gates = {p: (threading.Event(), threading.Event()) for p in pause_at}
        self.result: tuple[int, int] | None = None
        self.error: Exception | None = None
        self.conn_id: int | None = None

    def hook(self, point: str) -> None:
        log(self.name, point)
        if point in self.gates:
            reached, release = self.gates[point]
            reached.set()
            if not release.wait(timeout=30):
                raise TimeoutError(f"{self.name}: {point} 에서 풀리지 않음")

    def wait_at(self, point: str) -> None:
        if not self.gates[point][0].wait(timeout=10):
            raise TimeoutError(f"{self.name} 가 {point} 에 도달하지 않음")

    def release(self, point: str) -> None:
        self.gates[point][1].set()

    def run(self) -> None:
        conn = db.connect()
        self.conn_id = conn.thread_id()
        try:
            with conn.cursor() as cur:
                if self.isolation:
                    cur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {self.isolation}")
                    log(self.name, f"격리 수준 {self.isolation}")
                if self.pre_read:
                    # 잠금 전에 한 일반 SELECT — REPEATABLE READ 에서는 여기서 스냅샷이 고정된다
                    cur.execute("SELECT COALESCE(SUM(amount),0) FROM reservations WHERE event_code=%s AND status='CONFIRMED'", (CODE,))
                    log(self.name, f"pre-read (잠금 전 조회) 원천={cur.fetchone()[0]}")
                    self.hook("pre-read")
            self.result = db.recompute_event(conn, CODE, lock=self.lock, hook=self.hook)
            conn.commit()
            log(self.name, f"COMMIT  읽은 원천 {self.result[0]} 을 집계에 저장")
        except Exception as exc:  # noqa: BLE001 — 실험 결과로 보고한다
            conn.rollback()
            self.error = exc
            log(self.name, f"ROLLBACK {exc!r}")
        finally:
            conn.close()


def wait_blocked(worker: Worker, timeout: float = 10.0) -> dict:
    """worker 가 잠금 대기에 들어갈 때까지 performance_schema 를 본다."""
    admin = db.connect(admin=True)
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            st = db.lock_status(admin)
            if st["waits"]:
                return st
            time.sleep(0.1)
        raise TimeoutError(f"{worker.name} 가 잠금 대기에 들어가지 않음")
    finally:
        admin.close()


def show_locks(st: dict, names: dict[int, str]) -> None:
    for lk in st["locks"]:
        who = names.get(lk["conn_id"], f"conn{lk['conn_id']}")
        log("LOCK", f"{who}: {lk['mode']} {lk['status']} row={lk['row']}")


def conn_names(*workers: Worker) -> dict[int, str]:
    # performance_schema.threads.PROCESSLIST_ID = 연결 id
    return {w.conn_id: w.name for w in workers if w.conn_id is not None}


def verdict(title: str) -> bool:
    src, stat = read_state()
    ok = src == stat
    print(f"  => 원천 {src} / 집계 {stat}  {'OK' if ok else 'STALE WRITE — 오래된 값이 최신 값을 덮었다'}\n")
    return ok


# ---------------------------------------------------------------------------
# 시나리오
# ---------------------------------------------------------------------------


def s1() -> bool:
    print("[s1] 잠금 없음: A 가 100 을 읽고 멈춘 사이 원천 120, B 가 120 저장, A 가 뒤늦게 100 저장")
    setup()
    a = Worker("A", lock=False, pause_at=("after-read",))
    a.start(); a.wait_at("after-read")
    set_source(120); log("SRC", "원천 100 → 120 커밋")
    b = Worker("B", lock=False)
    b.start(); b.join()
    a.release("after-read"); a.join()
    return verdict("s1")


def s2() -> bool:
    print("[s2] 행사별 잠금 (잠금이 트랜잭션의 첫 조회): A 가 잠금을 쥔 채 멈춤, B 는 대기")
    setup()
    a = Worker("A", lock=True, pause_at=("after-read",))
    a.start(); a.wait_at("after-read")
    set_source(120); log("SRC", "원천 100 → 120 커밋 (집계 잠금은 원천 변경을 막지 않는다)")
    b = Worker("B", lock=True)
    b.start()
    st = wait_blocked(b)
    show_locks(st, conn_names(a, b))
    a.release("after-read"); a.join()
    b.join()
    return verdict("s2")


def _s3(isolation: str | None) -> bool:
    setup()
    b = Worker("B", lock=True, pause_at=("pre-read",), pre_read=True, isolation=isolation)
    b.start(); b.wait_at("pre-read")
    set_source(120); log("SRC", "원천 100 → 120 커밋")
    a = Worker("A", lock=True, pause_at=("after-read",))
    a.start(); a.wait_at("after-read")
    b.release("pre-read")
    st = wait_blocked(b)
    show_locks(st, conn_names(a, b))
    a.release("after-read"); a.join()
    b.join()
    return verdict("s3")


def s3() -> bool:
    print("[s3] 잠금은 있지만 B 가 잠금 전에 한 번 읽었다 (REPEATABLE READ 스냅샷 고정)")
    return _s3(None)


def s3b() -> bool:
    print("[s3b] s3 와 같은 순서, B 만 READ COMMITTED — 잠금 후 조회가 최신 커밋을 본다")
    return _s3("READ COMMITTED")


SCENARIOS = {"s1": s1, "s2": s2, "s3": s3, "s3b": s3b}


def main() -> None:
    names = sys.argv[1:] or ["all"]
    if names == ["all"]:
        names = list(SCENARIOS)
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        sys.exit(f"알 수 없는 시나리오 {unknown}. 가능: {list(SCENARIOS)} 또는 all")
    results = {n: SCENARIOS[n]() for n in names}
    print("요약:", "  ".join(f"{n}={'OK' if ok else 'STALE'}" for n, ok in results.items()))


if __name__ == "__main__":
    main()
