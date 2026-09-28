"""토픽 정의와 AdminClient 헬퍼.

토픽은 하나다: reservation.events (파티션 2, 키=예약 ID).
kafka-basic 과 달리 관찰용 극단 튜닝을 넣지 않는다 — segment.ms 등 전부 기본값.
C5(트랜잭션)에서 출력 토픽이 필요해지면 그때 추가한다.

describe_topics() / watermarks() 는 kafka-basic 에서 가져온 관측 헬퍼다.
"""

from __future__ import annotations

import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from .config import settings


def topic_specs() -> dict[str, dict]:
    # config 를 비워 둔다 = 브로커 기본값. describe_topics() 의 overridden_config 가
    # 비어 있는 것 자체가 "기본값에서 벗어난 게 없다"는 확인이 된다.
    return {
        settings.topic_events: {"partitions": settings.num_partitions, "config": {}},
    }


def get_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.bootstrap_servers})


def bootstrap_topics() -> list[dict]:
    """토픽을 생성한다. 이미 있으면 건드리지 않고 exists 로 보고한다."""
    admin = get_admin()
    specs = topic_specs()
    existing = set(admin.list_topics(timeout=10).topics.keys())

    to_create = [
        NewTopic(name, num_partitions=spec["partitions"], replication_factor=1, config=spec["config"])
        for name, spec in specs.items()
        if name not in existing
    ]

    results = [{"topic": name, "status": "exists"} for name in specs if name in existing]

    if to_create:
        futures = admin.create_topics(to_create)
        for name, fut in futures.items():
            try:
                fut.result()
                results.append({"topic": name, "status": "created"})
            except Exception as exc:  # 동시에 두 번 호출한 경우 등
                results.append({"topic": name, "status": "error", "detail": str(exc)})

    results.sort(key=lambda r: r["topic"])
    return results


def describe_topics(names: list[str] | None = None) -> list[dict]:
    """브로커에 **실제로 적용된** 토픽 설정을 읽어온다.

    is_default=False 인 항목만 추려서, 기본값에서 벗어난 값이 없는지 눈으로 확인한다.
    """
    admin = get_admin()
    names = names or list(topic_specs().keys())
    meta = admin.list_topics(timeout=10).topics

    resources = [ConfigResource(ConfigResource.Type.TOPIC, name) for name in names if name in meta]
    out: list[dict] = []

    for name in names:
        if name not in meta:
            out.append({"topic": name, "exists": False})

    if not resources:
        return out

    futures = admin.describe_configs(resources)
    for resource, fut in futures.items():
        entries = fut.result(timeout=10)
        name = resource.name
        overridden = {k: e.value for k, e in sorted(entries.items()) if not e.is_default}
        out.append(
            {
                "topic": name,
                "exists": True,
                "partitions": len(meta[name].partitions),
                "overridden_config": overridden,
                "cleanup_policy": entries["cleanup.policy"].value,
            }
        )

    out.sort(key=lambda r: r["topic"])
    return out


def watermarks(topic: str) -> dict:
    """파티션별 low/high watermark — lag 관측의 기초."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "rkl-watermark-probe",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=10)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            return {"topic": topic, "exists": False}

        partitions = sorted(meta.topics[topic].partitions.keys())
        rows = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
            rows.append(
                {
                    "partition": p,
                    "low": low,
                    "high": high,
                    # high-low 는 "오프셋 범위"이지 실제 남아있는 메시지 수가 아니다.
                    "offset_span": high - low,
                }
            )
        return {"topic": topic, "exists": True, "partitions": rows}
    finally:
        consumer.close()


def reset_topic(topic: str, timeout: float = 30.0) -> dict:
    """실험 초기화: 토픽을 지우고 같은 스펙으로 다시 만든다.

    삭제는 비동기라 직후 생성이 "marked for deletion" 으로 거절될 수 있어 재시도한다.
    이전 실험의 이벤트·그룹 오프셋(존재하지 않는 파티션 위치)이 다음 실험에 섞이지 않게 한다.
    """
    admin = get_admin()
    spec = topic_specs()[topic]
    if topic in admin.list_topics(timeout=10).topics:
        admin.delete_topics([topic])[topic].result()

    deadline = time.monotonic() + timeout
    while True:
        new = NewTopic(topic, num_partitions=spec["partitions"], replication_factor=1, config=spec["config"])
        try:
            admin.create_topics([new])[topic].result()
            return {"topic": topic, "status": "recreated"}
        except KafkaException as exc:
            if time.monotonic() > deadline:
                raise
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            time.sleep(0.5)


def group_offsets(group: str, topic: str) -> dict:
    """그룹의 커밋 위치 vs high watermark. lag = high - committed (커밋 기준)."""
    consumer = Consumer(
        {"bootstrap.servers": settings.bootstrap_servers, "group.id": group, "enable.auto.commit": False}
    )
    try:
        meta = consumer.list_topics(topic, timeout=10)
        parts = sorted(meta.topics[topic].partitions.keys())
        committed = consumer.committed([TopicPartition(topic, p) for p in parts], timeout=10)
        rows = []
        for tp in committed:
            _low, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10, cached=False)
            c = tp.offset if tp.offset >= 0 else None  # 음수 = 커밋 기록 없음 (OFFSET_INVALID)
            rows.append({"partition": tp.partition, "committed": c, "high": high,
                         "lag": None if c is None else high - c})
        return {"group": group, "topic": topic, "partitions": rows}
    finally:
        consumer.close()
