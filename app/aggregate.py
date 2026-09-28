"""C2 집계 컨슈머 — 수동 커밋과 전달 보장.

    uv run python -m app.aggregate --group c2-e1                     # 정상 (process-first, 명시적 동기 커밋)
    uv run python -m app.aggregate --group c2-e2 --crash-at after-db-before-commit
    uv run python -m app.aggregate --group c2-e3 --order commit-first --crash-at after-commit-before-db
    uv run python -m app.aggregate --group c2-e4 --crash-at mid-batch
    uv run python -m app.aggregate --group c2-e5w --fail-event-code EXPO-02 --mode wrong-continue
    uv run python -m app.aggregate --group c2-e6 --slow-ms 12000 --max-poll-interval-ms 10000
    uv run python -m app.aggregate --group c2-e7 --commit-style noarg-async
    ... --idle-exit 5   # 5초 동안 새 메시지가 없으면 정상 종료 (실험 자동화용)

C4 (stale write 와 행사별 잠금):

    uv run python -m app.aggregate --group c4 --tag w0 --assign 0 --hold-after-read-ms 6000   # 원천 읽고 6초 쥠
    uv run python -m app.aggregate --group c4 --tag w1 --assign 1                            # 다른 파티션 담당
    ... --lock                       # 행사 제어 행 FOR UPDATE 후 원천 조회 (db.recompute_event)
    ... --lock-wait-timeout 3        # 이 세션의 innodb_lock_wait_timeout (기본 50초)

--assign 은 subscribe 대신 파티션을 직접 고정한다 (리밸런스 없음). 어느 워커가 어느 파티션을
맡는지 매번 같아야 두 워커의 교차를 재현할 수 있어서다. 커밋은 여전히 --group 으로 한다.

한 배치의 흐름 (기본 process-first):

    consume(최대 --batch 건) → 역직렬화 → 행사코드로 묶음 (배치 내 행사 통합)
      → 행사마다 DB 트랜잭션 { 원천 재조회 + 절대값 Upsert + processing_attempts } commit
      → 파티션별 (마지막 오프셋 + 1) 을 명시해 동기 커밋

커밋하는 오프셋은 "다음에 읽을 위치"다. 마지막으로 처리한 오프셋이 아니라 +1 이다.

실패 정책 (correct 모드): 한 행사라도 실패하면 **커밋하지 않고 종료**한다. 실패 구간을 넘어서
커밋하지 않는 가장 단순한 정책이다. 재시도·격리(DLQ)는 C3 의 주제다.
wrong-continue 모드는 실패를 로그만 남기고 배치 끝까지 커밋한다 — 면접 Q2 가 지목한 잘못된 구현.
별도 그룹(--group)으로 실행해 정상 실행 경로와 섞지 않는다.

<사용자 실험 지점>
  * --batch 를 1 로 두면 배치 일부 실패(mid-batch)가 성립하지 않는다. 왜 그런지 확인해 본다.
  * process_batch() 안의 DB commit 을 행사별이 아니라 배치 전체 한 번으로 바꾸면
    mid-batch 크래시 때 DB 에 남는 것이 어떻게 달라지는지 본다.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from . import db
from .config import consumer_config, settings
from .schemas import ReservationEvent

ORDERS = {"process-first", "commit-first"}
CRASH_POINTS = {"after-db-before-commit", "after-commit-before-db", "mid-batch"}
MODES = {"correct", "wrong-continue"}
COMMIT_STYLES = {"explicit-sync", "noarg-sync", "noarg-async"}


@dataclass(frozen=True)
class Received:
    """Kafka 안의 위치(topic/partition/offset)와 업무 이벤트를 함께 들고 다닌다."""

    topic: str
    partition: int
    offset: int
    key: str | None
    event: ReservationEvent


class ProcessingError(Exception):
    pass


# ---------------------------------------------------------------------------
# 순수 함수 (tests/test_aggregate.py)
# ---------------------------------------------------------------------------


def group_by_event_code(items: list[Received]) -> dict[str, list[Received]]:
    """배치 안의 이벤트를 행사코드로 묶는다. 순서는 행사가 처음 나타난 순서.

    같은 행사의 이벤트가 여러 건이어도 재계산은 한 번이면 된다 — 절대값 재계산이기 때문이다.
    """
    grouped: dict[str, list[Received]] = {}
    for it in items:
        grouped.setdefault(it.event.event_code, []).append(it)
    return grouped


def next_offsets(items: list[Received]) -> dict[tuple[str, int], int]:
    """파티션별 커밋할 위치 = 그 파티션에서 받은 마지막 오프셋 + 1."""
    out: dict[tuple[str, int], int] = {}
    for it in items:
        k = (it.topic, it.partition)
        out[k] = max(out.get(k, -1), it.offset + 1)
    return out


# ---------------------------------------------------------------------------
# 실행
# ---------------------------------------------------------------------------


def _arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def _choice(name: str, default: str, allowed: set[str]) -> str:
    v = _arg(name, default)
    if v not in allowed:
        sys.exit(f"{name} 는 {sorted(allowed)} 중 하나: {v!r}")
    return v


class Lab:
    def __init__(self) -> None:
        self.group = _arg("--group") or settings.consumer_group
        self.tag = _arg("--tag") or "worker-1"
        self.order = _choice("--order", "process-first", ORDERS)
        self.crash_at = _arg("--crash-at")
        if self.crash_at is not None and self.crash_at not in CRASH_POINTS:
            sys.exit(f"--crash-at 은 {sorted(CRASH_POINTS)} 중 하나")
        self.mode = _choice("--mode", "correct", MODES)
        self.commit_style = _choice("--commit-style", "explicit-sync", COMMIT_STYLES)
        self.fail_event_code = _arg("--fail-event-code")
        self.batch = int(_arg("--batch", "50"))
        self.slow_ms = int(_arg("--slow-ms", "0"))
        mpi = _arg("--max-poll-interval-ms")
        self.max_poll_interval_ms = int(mpi) if mpi else None
        idle = _arg("--idle-exit")
        self.idle_exit = float(idle) if idle else None
        # C4
        self.lock = "--lock" in sys.argv
        self.hold_after_read_ms = int(_arg("--hold-after-read-ms", "0"))
        lwt = _arg("--lock-wait-timeout")
        self.lock_wait_timeout = int(lwt) if lwt else None
        assign = _arg("--assign")
        self.assign = [int(p) for p in assign.split(",")] if assign else None
        self.run_id = uuid.uuid4().hex[:8]
        self.running = True
        # --idle-exit 은 할당을 받은 뒤부터 센다. 직전 인스턴스가 hard kill 됐으면
        # session.timeout(10s) 동안 할당이 오지 않는데, 그걸 "메시지 없음"으로 착각하면 안 된다.
        self.idle_since: float | None = None

    def log(self, stage: str, **fields) -> None:
        """한 줄 = 한 단계. 수신·DB 커밋·커밋 요청·커밋 결과를 서로 다른 단계로 찍는다."""
        kv = " ".join(f"{k}={v}" for k, v in fields.items())
        print(f"[{self.run_id} {self.tag}] {stage:<14} {kv}", flush=True)

    def crash(self, point: str) -> None:
        """close() 를 타지 않는 hard kill. 그룹 탈퇴 통보도, 커밋도 없이 사라진다."""
        self.log("CRASH", at=point, note="os._exit(137) — close() 없음, 재배정은 session.timeout(10s) 후")
        os._exit(137)


def build_consumer(lab: Lab) -> Consumer:
    cfg = consumer_config(
        group_id=lab.group, instance_tag=lab.tag, max_poll_interval_ms=lab.max_poll_interval_ms
    )

    def on_commit(err, partitions):
        # 비동기 커밋의 결과는 여기서만 보인다. commit() 호출은 이미 반환됐다.
        parts = [f"p{p.partition}@{p.offset}" + (f"!{p.error}" if p.error else "") for p in partitions]
        if err is not None:
            lab.log("commit-cb-fail", error=err, partitions=parts)
        else:
            lab.log("commit-cb-ok", partitions=parts)

    cfg["on_commit"] = on_commit
    return Consumer(cfg)


def receive(lab: Lab, msgs) -> list[Received]:
    items: list[Received] = []
    for msg in msgs:
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                lab.log("kafka-error", error=msg.error())
            continue
        key = msg.key().decode("utf-8") if msg.key() else None
        try:
            ev = ReservationEvent.from_bytes(msg.value())
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            # 역직렬화 실패는 행사코드를 모른다. C2 에서는 처리 실패로 취급한다 (격리는 C3).
            raise ProcessingError(f"역직렬화 실패 p{msg.partition()}/o{msg.offset()}: {exc!r}") from exc
        items.append(Received(msg.topic(), msg.partition(), msg.offset(), key, ev))
        lab.log("recv", pos=f"p{msg.partition()}/o{msg.offset()}", key=key,
                event_id=ev.event_id, event_code=ev.event_code)
    return items


def _hook(lab: Lab, code: str, point: str) -> None:
    """C4: 원천을 읽은 뒤 저장 전에 일부러 쥐고 있는다. --lock 이면 이 동안 행사 잠금도 쥔다."""
    if point == "locked":
        lab.log("locked", event_code=code)
    if point == "after-read" and lab.hold_after_read_ms:
        lab.log("hold", event_code=code, ms=lab.hold_after_read_ms, note="원천을 읽었고 아직 저장 전")
        time.sleep(lab.hold_after_read_ms / 1000)


def process_batch(lab: Lab, conn, items: list[Received]) -> None:
    """행사마다 DB 트랜잭션 하나. correct 모드는 실패 시 ProcessingError 를 올린다."""
    grouped = group_by_event_code(items)
    if lab.crash_at == "mid-batch" and len(grouped) < 2:
        lab.log("warn", note="mid-batch 크래시는 배치에 행사가 2개 이상일 때만 성립한다", codes=list(grouped))

    for idx, (code, group) in enumerate(grouped.items()):
        if idx == 1 and lab.crash_at == "mid-batch":
            lab.crash(f"mid-batch (행사 {list(grouped)[0]} DB 커밋 후, {code} 처리 전)")
        try:
            if code == lab.fail_event_code:
                raise ProcessingError(f"의도적 실패: event_code={code} (--fail-event-code)")
            total, count = db.recompute_event(conn, code, lock=lab.lock, hook=lambda p: _hook(lab, code, p))
            db.record_attempts(conn, (
                {
                    "run_id": lab.run_id, "group_id": lab.group, "instance": lab.tag, "mode": lab.mode,
                    "event_id": it.event.event_id, "reservation_id": it.event.reservation_id,
                    "event_code": code, "topic": it.topic, "partition": it.partition, "offset": it.offset,
                }
                for it in group
            ))
            conn.commit()
            lab.log("db-commit", event_code=code, events=len(group),
                    total_amount=total, reservation_count=count)
        except Exception as exc:
            conn.rollback()
            if lab.mode == "correct":
                lab.log("process-fail", event_code=code, error=repr(exc), note="커밋하지 않고 멈춘다")
                raise ProcessingError(str(exc)) from exc
            # 잘못된 구현: 실패를 삼키고 다음으로 넘어간다 → 뒤에서 이 구간까지 커밋된다
            lab.log("process-fail", event_code=code, error=repr(exc), note="wrong-continue: 무시하고 진행")


def commit(lab: Lab, consumer: Consumer, items: list[Received]) -> bool:
    offs = next_offsets(items)
    requested = [f"p{p}@{o}" for (_t, p), o in sorted(offs.items())]
    lab.log("commit-req", style=lab.commit_style, requested=requested)
    try:
        if lab.commit_style == "explicit-sync":
            tps = [TopicPartition(t, p, o) for (t, p), o in sorted(offs.items())]
            result = consumer.commit(offsets=tps, asynchronous=False)
        elif lab.commit_style == "noarg-sync":
            # 인자 없음 = 이 컨슈머가 소비한 위치(할당된 모든 파티션)를 통째로 커밋
            result = consumer.commit(asynchronous=False)
        else:
            # 인자 없는 commit() 의 기본값이 비동기다. 반환 시점엔 결과를 모른다 → on_commit 콜백
            consumer.commit(asynchronous=True)
            lab.log("commit-sent", note="비동기 — 결과는 commit-cb-* 로그에서")
            return True
    except KafkaException as exc:
        err = exc.args[0]
        lab.log("commit-fail", code=err.name(), error=err.str(), requested=requested)
        return False

    bad = [tp for tp in result if tp.error]
    done = [f"p{tp.partition}@{tp.offset}" + (f"!{tp.error}" if tp.error else "") for tp in result]
    if bad:
        lab.log("commit-fail", partitions=done)
        return False
    lab.log("commit-ok", committed=done)
    return True


def main() -> None:
    lab = Lab()
    consumer = build_consumer(lab)

    def on_assign(_c, partitions):
        lab.idle_since = time.monotonic()
        lab.log("assign", partitions=[f"p{p.partition}" for p in partitions])

    def on_revoke(_c, partitions):
        lab.log("revoke", partitions=[f"p{p.partition}" for p in partitions])

    def on_lost(_c, partitions):
        lab.log("lost", partitions=[f"p{p.partition}" for p in partitions],
                note="소비권 상실 — 이미 한 DB 쓰기는 취소되지 않는다")

    if lab.assign is not None:
        # 수동 할당: 그룹 멤버십·리밸런스 없음. 시작 위치는 그룹의 커밋 위치 (없으면 earliest)
        consumer.assign([TopicPartition(settings.topic_events, p) for p in lab.assign])
        lab.idle_since = time.monotonic()
        lab.log("assign", partitions=[f"p{p}" for p in lab.assign], note="수동 할당 (--assign)")
    else:
        consumer.subscribe([settings.topic_events], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)

    def _stop(_sig, _frame):
        lab.running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    conn = db.connect()
    if lab.lock_wait_timeout is not None:
        db.set_lock_wait_timeout(conn, lab.lock_wait_timeout)
    lab.log("start", group=lab.group, order=lab.order, mode=lab.mode, commit_style=lab.commit_style,
            crash_at=lab.crash_at, fail_event_code=lab.fail_event_code, batch=lab.batch,
            slow_ms=lab.slow_ms, max_poll_interval_ms=lab.max_poll_interval_ms)
    if lab.lock or lab.hold_after_read_ms or lab.lock_wait_timeout is not None:
        lab.log("start-c4", lock=lab.lock, hold_after_read_ms=lab.hold_after_read_ms,
                lock_wait_timeout=lab.lock_wait_timeout, assign=lab.assign)
    exit_code = 0
    try:
        while lab.running:
            msgs = consumer.consume(num_messages=lab.batch, timeout=1.0)
            items = receive(lab, msgs)
            if not items:
                if (lab.idle_exit is not None and lab.idle_since is not None
                        and time.monotonic() - lab.idle_since > lab.idle_exit):
                    lab.log("idle-exit", seconds=lab.idle_exit)
                    break
                continue
            lab.idle_since = time.monotonic()
            lab.log("batch", size=len(items), partitions=sorted({it.partition for it in items}),
                    event_codes=list(group_by_event_code(items)))

            if lab.order == "commit-first":
                # At-most-once: 처리 전에 커밋 — 처리 전에 죽으면 이 구간은 다시 오지 않는다
                commit(lab, consumer, items)
                if lab.crash_at == "after-commit-before-db":
                    lab.crash("after-commit-before-db")
                process_batch(lab, conn, items)
            else:
                # At-least-once: 처리 후 커밋 — 커밋 전에 죽으면 이 구간을 다시 처리한다
                process_batch(lab, conn, items)
                if lab.crash_at == "after-db-before-commit":
                    lab.crash("after-db-before-commit")
                if lab.slow_ms:
                    # 첫 배치에만 건다. 매 배치가 max.poll.interval 을 넘으면 재가입 → 같은 배치
                    # 재처리 → 또 초과 로 영원히 진행하지 못한다 (그것도 실제 장애 모습이다).
                    lab.log("slow", sleep_ms=lab.slow_ms, note="DB 저장 후 커밋 전 지연 (첫 배치만)")
                    time.sleep(lab.slow_ms / 1000)
                    lab.slow_ms = 0
                commit(lab, consumer, items)
    except ProcessingError as exc:
        lab.log("stop", reason=str(exc), note="미커밋 구간은 다음 기동 때 다시 처리된다")
        exit_code = 1
    finally:
        conn.close()
        consumer.close()  # 정상 종료: 그룹에서 즉시 떠난다 (자동 커밋이 꺼져 있어 커밋은 없다)
        lab.log("closed", exit_code=exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
