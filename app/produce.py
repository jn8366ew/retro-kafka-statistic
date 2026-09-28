"""고정 이벤트 묶음 발행 CLI — C1·C2 검증의 입력.

    uv run python -m app.produce            # 고정 12건: 원천 DB 갱신 → 발행, 예측/실제 파티션 대조표
    uv run python -m app.produce --dup      # 같은 event_id 를 두 번 발행하는 멱등성 실험
    uv run python -m app.produce --one r-1003 GALA-01 CONFIRMED 9700   # 한 건만 (C2 E3 후속 이벤트)
    uv run python -m app.produce --kafka-only   # C1 재현: 원천 DB 를 건드리지 않고 발행만

원천 갱신 → 발행 순서 (C2 부터, dev-scope 결정):
  실제 업무 앱처럼 reservations 를 먼저 커밋하고 이벤트를 보낸다. 컨슈머는 이벤트를
  "어느 행사를 재계산할지" 알리는 신호로만 쓰고 원천을 다시 읽는다.
  두 쓰기는 원자적이지 않다 — DB 커밋 후 발행 전에 죽으면 이벤트가 사라진다.
  이것은 Transactional Outbox(dev-plan E03)의 문제이며 C2 에서는 한계로만 기록한다.

--dup 실험이 보여주는 것 (dev-scope.md 1장, 면접 문서 Q4):
  enable.idempotence=true 는 **전송 재시도**의 중복만 막는다. 애플리케이션이 같은
  event_id 로 produce() 를 두 번 호출하면 두 건 다 저장된다 (오프셋이 2개 찍힌다).
  업무 이벤트의 중복 제거는 프로듀서 설정이 아니라 소비 쪽 설계의 몫이다.

<사용자 실험 지점>
  * FIXED_EVENTS 의 reservation_id 를 바꿔 파티션 배치가 어떻게 변하는지 본다.
  * config.py 의 RKL_PARTITIONER 를 consistent_random 으로 바꾸면 같은 키가
    다른 파티션으로 간다 (murmur2 vs CRC32 — partitioning.py 주석).
"""

from __future__ import annotations

import sys

from . import db
from .config import settings
from .partitioning import crc32_partition, murmur2_partition
from .producer import build_producer, produce_sync
from .schemas import ReservationEvent

# 행사 2개 × 예약 6개. 같은 행사(GALA-01)의 예약들이 서로 다른 파티션으로
# 흩어지는 것을 보는 게 목적이다 — 키가 예약 ID 이기 때문이다 (면접 문서 Q13).
FIXED_EVENTS: list[ReservationEvent] = [
    ReservationEvent("r-1001", "GALA-01", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 10000}),
    ReservationEvent("r-1002", "GALA-01", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 12000}),
    ReservationEvent("r-1003", "GALA-01", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 9000}),
    ReservationEvent("r-1004", "EXPO-02", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 30000}),
    ReservationEvent("r-1005", "EXPO-02", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 28000}),
    ReservationEvent("r-1006", "EXPO-02", "RESERVATION_CREATED", {"status": "CONFIRMED", "amount": 31000}),
    ReservationEvent("r-1001", "GALA-01", "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 11000}),
    ReservationEvent("r-1002", "GALA-01", "RESERVATION_CANCELLED", {"status": "CANCELLED", "amount": 0}),
    ReservationEvent("r-1004", "EXPO-02", "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 29000}),
    ReservationEvent("r-1003", "GALA-01", "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 9500}),
    ReservationEvent("r-1005", "EXPO-02", "RESERVATION_CANCELLED", {"status": "CANCELLED", "amount": 0}),
    ReservationEvent("r-1006", "EXPO-02", "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 32000}),
]


def predicted(key: str) -> tuple[int, int]:
    kb = key.encode("utf-8")
    return (
        murmur2_partition(kb, settings.num_partitions),
        crc32_partition(kb, settings.num_partitions),
    )


class _Source:
    """원천 DB 쓰기. --kafka-only 면 아무것도 하지 않는다."""

    def __init__(self, enabled: bool) -> None:
        self.conn = db.connect() if enabled else None

    def write(self, ev: ReservationEvent) -> None:
        if self.conn is None:
            return
        db.upsert_reservation(self.conn, ev)
        self.conn.commit()  # 발행 전에 원천이 먼저 확정된다

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()


def run(events: list[ReservationEvent], source: _Source) -> list[dict]:
    producer = build_producer()
    rows = []
    print(f"{'key':<8} {'event_id':<18} {'type':<22} {'예측(murmur2/crc32)':<20} {'실제 p/offset'}")
    for ev in events:
        m_pred, c_pred = predicted(ev.key)
        source.write(ev)
        res = produce_sync(producer, settings.topic_events, ev.key, ev.to_bytes())
        match = "OK" if res["partition"] == m_pred else "MISMATCH!"
        print(
            f"{ev.key:<8} {ev.event_id:<18} {ev.event_type:<22} "
            f"{m_pred} / {c_pred:<14} {res['partition']}/{res['offset']}  {match}"
        )
        rows.append({**res, "event_id": ev.event_id, "predicted_murmur2": m_pred})
    return rows


def run_dup(source: _Source) -> None:
    """같은 event_id(같은 바이트)를 두 번 발행한다. 두 건 다 저장되는 것을 확인한다."""
    ev = ReservationEvent("r-1001", "GALA-01", "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 15000})
    producer = build_producer()
    source.write(ev)  # 원천은 한 번만 바뀐다. 중복은 발행 쪽에서만 생긴다
    print(f"같은 event_id 두 번 발행: {ev.event_id} (enable.idempotence=true 상태)")
    for i in (1, 2):
        res = produce_sync(producer, settings.topic_events, ev.key, ev.to_bytes())
        print(f"  {i}번째 발행 → partition {res['partition']}, offset {res['offset']}")
    print("→ 오프셋이 2개 = 두 건 다 저장됐다. 멱등 프로듀서는 애플리케이션 재발행을 막지 않는다.")


def one_event(args: list[str]) -> ReservationEvent:
    """--one RESERVATION_ID EVENT_CODE STATUS AMOUNT"""
    rid, code, status, amount = args
    etype = "RESERVATION_CANCELLED" if status == "CANCELLED" else "RESERVATION_UPDATED"
    return ReservationEvent(rid, code, etype, {"status": status, "amount": int(amount)})


def main() -> None:
    source = _Source(enabled="--kafka-only" not in sys.argv)
    try:
        if "--dup" in sys.argv:
            run_dup(source)
            return
        if "--one" in sys.argv:
            i = sys.argv.index("--one")
            events = [one_event(sys.argv[i + 1 : i + 5])]
        else:
            events = FIXED_EVENTS
        rows = run(events, source)
    finally:
        source.close()
    parts = {r["partition"] for r in rows}
    mismatch = [r for r in rows if r["partition"] != r["predicted_murmur2"]]
    print(f"\n발행 {len(rows)}건 (브로커 확인 기준), 파티션 {sorted(parts)}, 예측 불일치 {len(mismatch)}건")


if __name__ == "__main__":
    main()
