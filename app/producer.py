"""Producer 래퍼 (kafka-basic 에서 가져와 축소).

핵심 구분 두 가지 (dev-plan.md 4장 "공통 관측 정보"):
  * produce() 호출 = 로컬 버퍼에 넣은 시점. 발행 성공이 아니다.
  * 딜리버리 리포트 = 브로커의 저장 확인. 이때의 partition/offset 이 실제 결과다.

`produce_sync()` 는 브로커 확인까지 기다렸다가 partition/offset/timestamp 를 돌려준다.
멱등 프로듀서(enable.idempotence=true)를 켜 두지만, 이것은 **전송 재시도**의 중복만
막는다 — 애플리케이션이 같은 event_id 를 두 번 발행하면 두 건 다 저장된다 (C1 실험).
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer

from .config import settings


def build_producer(client_id: str = "rkl-producer") -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            # librdkafka 기본(consistent_random)이 아니라 Java 호환 murmur2 를 명시한다
            "partitioner": settings.partitioner,
            "client.id": client_id,
            "acks": "all",  # 단일 브로커(RF 1)라 acks=1 과 물리적으로 같다 — dev-scope.md 1장
            "enable.idempotence": True,
            "linger.ms": 0,
        }
    )


def produce_sync(
    producer: Producer,
    topic: str,
    key: str,
    value: Any,
    *,
    timeout: float = 10.0,
) -> dict:
    """한 건 발행하고 브로커 확인(ack)까지 기다린 뒤 배치 결과를 반환한다."""
    result: dict[str, Any] = {}

    def on_delivery(err, msg):
        if err is not None:
            result["error"] = str(err)
            return
        _ts_type, ts = msg.timestamp()
        result.update(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), timestamp=ts)

    raw = value if isinstance(value, (bytes, bytearray)) else json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
    producer.produce(topic, key=key.encode("utf-8"), value=raw, on_delivery=on_delivery)

    remaining = producer.flush(timeout)
    if remaining > 0:
        raise TimeoutError(f"{remaining}건이 {timeout}s 안에 전송되지 않았습니다. 브로커가 떠 있나요? ({settings.bootstrap_servers})")
    if "error" in result:
        raise RuntimeError(f"발행 실패: {result['error']}")

    result["key"] = key
    return result
