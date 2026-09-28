"""복구 배치 — Kafka 없이 원천에서 집계를 다시 계산한다 (C4).

    uv run python -m app.recover                      # 모든 행사 재계산 (잠금 없음)
    uv run python -m app.recover --lock               # 컨슈머와 같은 행사별 잠금 규칙
    uv run python -m app.recover --event-code EXPO-02 --hold-after-read-ms 6000

실시간 컨슈머(app.aggregate)와 **같은 저장 절차(db.recompute_event)** 를 쓴다.
잠금 규칙은 모든 쓰기 경로가 같이 지켜야 의미가 있다 — 컨슈머만 --lock 이고 복구 배치가
잠금 없이 돌면, 복구 배치가 오래된 값을 뒤늦게 덮을 수 있다 (C4 R1).
"""

from __future__ import annotations

import sys
import time

from . import db


def _arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def event_codes(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT event_code FROM reservations ORDER BY event_code")
        rows = [r[0] for r in cur.fetchall()]
    conn.commit()  # 목록 조회의 스냅샷을 끊는다 — 아래 행사별 트랜잭션이 옛 스냅샷을 물려받지 않게
    return rows


def main() -> None:
    lock = "--lock" in sys.argv
    hold_ms = int(_arg("--hold-after-read-ms", "0"))
    only = _arg("--event-code")

    def hook(point: str) -> None:
        if point == "locked":
            print(f"[recover] locked", flush=True)
        if point == "after-read" and hold_ms:
            print(f"[recover] hold {hold_ms}ms (원천을 읽었고 아직 저장 전)", flush=True)
            time.sleep(hold_ms / 1000)

    conn = db.connect()
    try:
        codes = [only] if only else event_codes(conn)
        print(f"[recover] start lock={lock} hold_after_read_ms={hold_ms} codes={codes}", flush=True)
        for code in codes:
            try:
                total, count = db.recompute_event(conn, code, lock=lock, hook=hook)
                conn.commit()
                print(f"[recover] db-commit event_code={code} total_amount={total} reservation_count={count}", flush=True)
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                print(f"[recover] fail event_code={code} error={exc!r}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
