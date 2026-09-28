"""파티셔너 계산기 — "이 키는 몇 번 파티션으로 가는가?"를 직접 계산해 본다.

실무에서 가장 자주 밟는 지뢰:

* Java 클라이언트 기본 파티셔너 = murmur2  ->  `toPositive(murmur2(key)) % N`
* librdkafka(= confluent-kafka Python) 기본 = consistent_random(CRC32) -> `crc32(key) % N`

즉 **같은 키를 Java 프로듀서와 Python 프로듀서에서 보내면 다른 파티션에 떨어진다.**
같은 토픽에 두 언어가 같이 쓰면 키 기반 순서 보장이 조용히 깨진다.

여기서 두 해시를 모두 순수 파이썬으로 계산해 `GET /partition-preview` 로 나란히 보여주고,
실제로 발행된 파티션과 대조할 수 있게 한다.
"""

from __future__ import annotations

import zlib

MASK32 = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """Kafka(Java) `org.apache.kafka.common.utils.Utils.murmur2` 의 파이썬 포팅.

    반환값은 부호 없는 32비트 정수(Java 의 int 를 0xFFFFFFFF 로 마스킹한 값).
    """
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24

    h = (seed ^ length) & MASK32
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4] & 0xFF)
            + ((data[i4 + 1] & 0xFF) << 8)
            + ((data[i4 + 2] & 0xFF) << 16)
            + ((data[i4 + 3] & 0xFF) << 24)
        )
        k = (k * m) & MASK32
        k ^= k >> r
        k = (k * m) & MASK32
        h = (h * m) & MASK32
        h ^= k

    # Java 쪽은 switch fall-through 라 아래처럼 누적 적용된다.
    tail = length & ~3
    extra = length % 4
    if extra >= 3:
        h ^= (data[tail + 2] & 0xFF) << 16
    if extra >= 2:
        h ^= (data[tail + 1] & 0xFF) << 8
    if extra >= 1:
        h ^= data[tail] & 0xFF
        h = (h * m) & MASK32

    h ^= h >> 13
    h = (h * m) & MASK32
    h ^= h >> 15
    return h & MASK32


def to_positive(value: int) -> int:
    """Java `Utils.toPositive` — 최상위 비트를 떨어뜨려 항상 양수로 만든다."""
    return value & 0x7FFFFFFF


def murmur2_partition(key: bytes, num_partitions: int) -> int:
    """Java 기본 파티셔너와 동일한 결과."""
    return to_positive(murmur2(key)) % num_partitions


def crc32_hash(key: bytes) -> int:
    return zlib.crc32(key) & MASK32


def crc32_partition(key: bytes, num_partitions: int) -> int:
    """librdkafka `consistent` / `consistent_random` 파티셔너와 동일한 결과."""
    return crc32_hash(key) % num_partitions


def preview(key: str, num_partitions: int) -> dict:
    """두 알고리즘의 해시와 파티션을 함께 계산해 비교용 dict 로 반환."""
    kb = key.encode("utf-8")
    m_hash = murmur2(kb)
    c_hash = crc32_hash(kb)
    m_part = to_positive(m_hash) % num_partitions
    c_part = c_hash % num_partitions
    return {
        "key": key,
        "num_partitions": num_partitions,
        "murmur2": {
            "hash": m_hash,
            "to_positive": to_positive(m_hash),
            "partition": m_part,
            "used_by": "Java 클라이언트 기본, librdkafka partitioner=murmur2_random",
        },
        "crc32": {
            "hash": c_hash,
            "partition": c_part,
            "used_by": "librdkafka 기본 (consistent / consistent_random)",
        },
        "same_partition": m_part == c_part,
    }
