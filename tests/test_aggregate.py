"""aggregate.py 의 순수 함수 — Kafka·DB 없이 돈다."""

from app.aggregate import Received, group_by_event_code, next_offsets
from app.schemas import ReservationEvent

T = "reservation.events"


def rec(p: int, o: int, rid: str, code: str) -> Received:
    ev = ReservationEvent(rid, code, "RESERVATION_UPDATED", {"status": "CONFIRMED", "amount": 1})
    return Received(T, p, o, rid, ev)


def test_group_by_event_code_merges_same_event_across_partitions():
    items = [rec(0, 5, "r-1", "GALA"), rec(1, 2, "r-2", "EXPO"), rec(1, 3, "r-3", "GALA")]
    grouped = group_by_event_code(items)
    assert list(grouped) == ["GALA", "EXPO"]  # 처음 나타난 순서
    assert [it.offset for it in grouped["GALA"]] == [5, 3]


def test_next_offsets_is_last_offset_plus_one_per_partition():
    items = [rec(0, 5, "r-1", "A"), rec(0, 7, "r-2", "A"), rec(1, 2, "r-3", "B")]
    assert next_offsets(items) == {(T, 0): 8, (T, 1): 3}


def test_next_offsets_ignores_arrival_order():
    items = [rec(0, 9, "r-1", "A"), rec(0, 4, "r-2", "A")]
    assert next_offsets(items) == {(T, 0): 10}


def test_next_offsets_empty():
    assert next_offsets([]) == {}
