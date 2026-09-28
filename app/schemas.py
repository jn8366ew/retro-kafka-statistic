"""이벤트 스키마 — dev-plan.md 4장에서 확정한 필드.

이벤트 ID, Kafka 메시지 키, 토픽·파티션·오프셋은 서로 다른 식별자다:
  * event_id        업무 변경 이벤트의 식별자. 같은 예약의 정상 변경마다 새로 발급한다.
  * reservation_id  예약 식별자 = Kafka 메시지 키 (면접 문서 Q5).
  * event_code      행사 식별자. Kafka 레코드가 아니라 업무의 "행사"다.

키가 예약 ID 이므로 같은 행사(event_code)의 이벤트가 서로 다른 파티션으로 흩어진다.
이것이 C4 stale write 의 근본 원인이다 (dev-scope.md 3장) — 키를 행사코드로 바꾸면
재현 자체가 안 되므로 바꾸지 않는다.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

SCHEMA_VERSION = 1

EVENT_TYPES = {"RESERVATION_CREATED", "RESERVATION_UPDATED", "RESERVATION_CANCELLED"}


@dataclass
class ReservationEvent:
    reservation_id: str
    event_code: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:12]}")
    occurred_at: int = field(default_factory=lambda: int(time.time() * 1000))  # epoch ms
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"알 수 없는 event_type: {self.event_type}")

    @property
    def key(self) -> str:
        """Kafka 메시지 키 = 예약 ID."""
        return self.reservation_id

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "ReservationEvent":
        doc = json.loads(raw.decode("utf-8"))
        return cls(**doc)
