"""소비 CLI — C1 은 관찰만 한다 (DB 반영은 C2 부터).

    uv run python -m app.consume                  # 기본 그룹(stat-aggregator)으로 소비
    uv run python -m app.consume --group other    # 다른 그룹 = 독립 소비 확인
    uv run python -m app.consume --tag worker-2   # 같은 그룹 인스턴스 추가 (분담 관찰)

한 줄에 다음을 구분해 찍는다 (dev-plan.md 18장 "유지할 기술적 구분"):
  * 소비 위치: topic/partition/offset — Kafka 안의 위치
  * event_id / 메시지 키: 업무 변경 식별자 / 파티션 배치 기준
  * 커밋: 처리 후 명시적으로 호출한 결과. 수신했다고 커밋된 것이 아니다.

C1 커밋 정책: 폴링 루프마다 처리(출력) 후 동기 commit(). 커밋 시점을 조작하는
실험은 C2 에서 이 파일을 분기해 수행한다.
"""

from __future__ import annotations

import json
import signal
import sys

from confluent_kafka import Consumer, KafkaError

from .config import consumer_config, settings
from .schemas import ReservationEvent

_running = True


def _stop(_sig, _frame):
    global _running
    _running = False


def _arg(name: str) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None


def on_assign(consumer, partitions):
    print(f"[assign] {[f'{p.topic}/{p.partition}' for p in partitions]}")


def on_revoke(consumer, partitions):
    print(f"[revoke] {[f'{p.topic}/{p.partition}' for p in partitions]}")


def main() -> None:
    group = _arg("--group") or settings.consumer_group
    tag = _arg("--tag") or "worker-1"

    consumer = Consumer(consumer_config(group_id=group, instance_tag=tag))
    consumer.subscribe([settings.topic_events], on_assign=on_assign, on_revoke=on_revoke)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"소비 시작: topic={settings.topic_events} group={group} tag={tag} (Ctrl+C 로 정상 종료)")
    processed = 0
    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[error] {msg.error()}", file=sys.stderr)
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                ev = ReservationEvent.from_bytes(msg.value())
                desc = f"event_id={ev.event_id} event_code={ev.event_code} type={ev.event_type}"
            except (ValueError, json.JSONDecodeError) as exc:
                # C1 에서는 관찰만. 역직렬화 실패의 격리(DLQ)는 C3 의 주제다.
                desc = f"역직렬화 실패: {exc!r} raw={msg.value()[:40]!r}"

            print(f"p{msg.partition()}/o{msg.offset()} key={key} {desc}")
            processed += 1

            # 처리(여기서는 출력) 후 동기 커밋. asynchronous=False 라야 커밋 실패가
            # 예외로 드러난다 — 인자 없는 commit() 은 기본이 비동기다 (면접 문서 Q2).
            consumer.commit(asynchronous=False)
    finally:
        # 정상 종료: 진행 중이던 폴링을 마치고 그룹에서 떠난다 → 즉시 재배정
        consumer.close()
        print(f"종료. 처리 {processed}건 (커밋된 위치는 다음 기동의 재개 지점)")


if __name__ == "__main__":
    main()
