"""C2 결과 검증 — 로그가 아니라 실제 DB·Kafka 상태를 비교한다.

    uv run python -m app.verify --group c2-e1

네 가지를 따로 판정한다 (dev-plan 18장: 처리 시도 중복 ≠ 금액 중복 누적):

  1. 집계 정합성  reservations 에서 파이썬으로 독립 계산한 기대값 vs event_statistics
  2. 누락          토픽의 모든 위치(partition/offset) 중 이 그룹의 처리 기록이 0건인 것
  3. 재처리        처리 기록이 2건 이상인 위치 — 집계가 맞으면 "중복 처리됐지만 누적은 안 됨"
  4. 커밋 위치     그룹 커밋 위치 vs high watermark

위치를 event_id 가 아니라 (partition, offset) 으로 대조한다. --dup 처럼 같은 event_id 가
두 위치에 있을 수 있기 때문이다.
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, TopicPartition

from . import db
from .config import settings
from .topics import group_offsets


def expected_statistics(conn) -> dict[str, tuple[int, int]]:
    """원천에서 직접 계산. 컨슈머의 SQL 을 재사용하지 않는다 (같은 버그를 같이 믿지 않도록)."""
    with conn.cursor() as cur:
        cur.execute("SELECT event_code, status, amount FROM reservations")
        rows = cur.fetchall()
    out: dict[str, list[int]] = {}
    for code, status, amount in rows:
        acc = out.setdefault(code, [0, 0])
        if status == "CONFIRMED":
            acc[0] += amount
            acc[1] += 1
    return {code: (t, c) for code, (t, c) in out.items()}


def actual_statistics(conn) -> dict[str, tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT event_code, total_amount, reservation_count FROM event_statistics")
        return {code: (int(t), int(c)) for code, t, c in cur.fetchall()}


def attempts_by_position(conn, group: str) -> Counter:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partition_no, offset_no, COUNT(*) FROM processing_attempts"
            " WHERE group_id = %s GROUP BY partition_no, offset_no",
            (group,),
        )
        return Counter({(int(p), int(o)): int(n) for p, o, n in cur.fetchall()})


def topic_positions(topic: str, timeout: float = 15.0) -> dict[tuple[int, int], dict]:
    """토픽 전체(low~high)를 읽어 위치 → 이벤트 요약. 그룹에 가입하지 않고 assign 으로 읽는다."""
    c = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": "rkl-verify-probe",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    try:
        parts = sorted(c.list_topics(topic, timeout=10).topics[topic].partitions.keys())
        ends: dict[int, int] = {}
        assigns = []
        for p in parts:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
            if high > low:
                assigns.append(TopicPartition(topic, p, low))
                ends[p] = high
        c.assign(assigns)
        out: dict[tuple[int, int], dict] = {}
        pending = set(ends)
        deadline = time.monotonic() + timeout
        while pending and time.monotonic() < deadline:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pending.discard(msg.partition())
                continue
            try:
                doc = json.loads(msg.value())
                summary = {"event_id": doc.get("event_id"), "event_code": doc.get("event_code"),
                           "key": msg.key().decode() if msg.key() else None}
            except (ValueError, TypeError):
                summary = {"event_id": None, "event_code": None, "raw": repr(msg.value()[:40])}
            out[(msg.partition(), msg.offset())] = summary
            if msg.offset() + 1 >= ends[msg.partition()]:
                pending.discard(msg.partition())
        if pending:
            raise TimeoutError(f"토픽을 끝까지 읽지 못함: 파티션 {sorted(pending)}")
        return out
    finally:
        c.close()


def verify(group: str) -> dict:
    conn = db.connect()
    try:
        expected = expected_statistics(conn)
        actual = actual_statistics(conn)
        attempts = attempts_by_position(conn, group)
    finally:
        conn.close()

    stats = []
    for code in sorted(set(expected) | set(actual)):
        e, a = expected.get(code), actual.get(code)
        stats.append({"event_code": code, "expected": e, "actual": a, "ok": e == a})

    positions = topic_positions(settings.topic_events)
    missing = [{"pos": f"p{p}/o{o}", **positions[(p, o)]} for (p, o) in sorted(positions) if attempts[(p, o)] == 0]
    reprocessed = [
        {"pos": f"p{p}/o{o}", "attempts": attempts[(p, o)], **positions.get((p, o), {})}
        for (p, o) in sorted(attempts) if attempts[(p, o)] >= 2
    ]
    offsets = group_offsets(group, settings.topic_events)
    commit_caught_up = all(r["lag"] == 0 for r in offsets["partitions"])

    return {
        "group": group,
        "verdict": {
            "statistics": "OK" if all(s["ok"] for s in stats) else "MISMATCH",
            "missing_positions": len(missing),
            "reprocessed_positions": len(reprocessed),
            "commit_caught_up": commit_caught_up,
        },
        "statistics": stats,
        "missing": missing,
        "reprocessed": reprocessed,
        "offsets": offsets["partitions"],
        "topic_positions": len(positions),
        "attempt_rows": sum(attempts.values()),
    }


def main() -> None:
    if "--group" not in sys.argv:
        sys.exit("사용법: uv run python -m app.verify --group GROUP")
    group = sys.argv[sys.argv.index("--group") + 1]
    print(json.dumps(verify(group), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
