"""MySQL 접근 (PyMySQL, ORM 없음) — C2 부터 사용.

트랜잭션 경계는 호출하는 쪽이 정한다 (autocommit=False). C2 의 실험 대상이 바로
"DB 커밋과 Kafka 커밋의 순서"이므로 여기서 몰래 commit 하지 않는다.

격리 수준은 InnoDB 기본 REPEATABLE READ 를 그대로 쓴다. C4 의 스냅샷 함정이 이 기본값에서 생긴다.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

import pymysql

from .config import settings
from .schemas import ReservationEvent

Hook = Callable[[str], None]


def _noop(_point: str) -> None:
    pass


def connect(*, admin: bool = False) -> pymysql.connections.Connection:
    """admin=True 는 관측 전용 (performance_schema 는 lab 계정에 권한이 없다)."""
    return pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_admin_user if admin else settings.mysql_user,
        password=settings.mysql_admin_password if admin else settings.mysql_password,
        database=settings.mysql_database,
        autocommit=False,
        charset="utf8mb4",
    )


def upsert_reservation(conn, ev: ReservationEvent) -> None:
    """발행기용: 원천(예약)을 갱신한다. 실제 업무 앱이 하는 쓰기를 흉내낸다.

    행사 제어 행(event_lock)도 여기서 보장한다 — 행사가 생길 때 업무 쪽이 등록한다는 가정.
    집계 쪽이 잠그려는 순간에 행이 없으면 잠글 대상이 없어 보호가 성립하지 않는다.

    이미 있는 행에 무조건 INSERT IGNORE 를 하면 안 된다: 중복 키 검사가 그 행에 공유 잠금을
    요청하므로, 집계 작업이 FOR UPDATE 로 쥐고 있는 동안 **업무 쓰기가 막힌다** (C4 에서 실제로
    30초 막혔다). 그래서 잠금 없는 일반 SELECT 로 먼저 보고, 없을 때만 넣는다.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM event_lock WHERE event_code = %s", (ev.event_code,))
        if cur.fetchone() is None:
            cur.execute("INSERT IGNORE INTO event_lock (event_code) VALUES (%s)", (ev.event_code,))
        cur.execute(
            """
            INSERT INTO reservations (reservation_id, event_code, status, amount)
            VALUES (%s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE event_code = new.event_code,
                                    status = new.status,
                                    amount = new.amount
            """,
            (ev.reservation_id, ev.event_code, ev.payload["status"], ev.payload["amount"]),
        )


def recompute_event(conn, event_code: str, *, lock: bool = False, hook: Hook = _noop) -> tuple[int, int]:
    """원천을 재조회해 행사 집계를 절대값으로 덮어쓴다. (total_amount, reservation_count) 반환.

    이벤트의 금액을 더하지 않는다 — 같은 원천 상태에서는 몇 번을 실행해도 같은 값이 된다.
    이것이 At-least-once 재처리에서 금액이 중복 누적되지 않는 근거다 (면접 문서 Q1·Q10).

    lock=True (C4): 행사 제어 행을 FOR UPDATE 로 잡은 **뒤에** 원천을 읽는다.
      * 같은 행사를 계산하는 다른 작업은 이 트랜잭션이 끝날 때까지 기다린다.
      * 잠금이 이 트랜잭션의 **첫 조회**여야 한다. REPEATABLE READ 의 스냅샷은 첫 일반 SELECT
        시점에 만들어지므로, 잠금 전에 무엇이든 읽었다면 잠금 후의 원천 조회도 그 옛 스냅샷을 본다
        (C4 S3 — 잠갔는데도 stale write).
      * 이 잠금은 집계 작업끼리만 줄 세운다. 원천(reservations) 변경은 막지 않는다.

    hook: 실험용 끼어들기 지점 ("before-lock", "locked", "after-read"). 기본은 아무것도 안 함.
    """
    with conn.cursor() as cur:
        if lock:
            hook("before-lock")
            cur.execute("SELECT event_code FROM event_lock WHERE event_code = %s FOR UPDATE", (event_code,))
            if cur.fetchone() is None:
                raise LookupError(f"event_lock 에 제어 행이 없다: {event_code}")
            hook("locked")
        cur.execute(
            """
            SELECT COALESCE(SUM(amount), 0), COUNT(*)
              FROM reservations
             WHERE event_code = %s AND status = 'CONFIRMED'
            """,
            (event_code,),
        )
        total, count = cur.fetchone()
        hook("after-read")
        cur.execute(
            """
            INSERT INTO event_statistics (event_code, total_amount, reservation_count)
            VALUES (%s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE total_amount = new.total_amount,
                                    reservation_count = new.reservation_count
            """,
            (event_code, int(total), int(count)),
        )
    return int(total), int(count)


def set_lock_wait_timeout(conn, seconds: int) -> None:
    """이 세션의 잠금 대기 한도 (기본 50초). 초과하면 1205 에러 — 트랜잭션은 문장 단위로만 롤백된다."""
    with conn.cursor() as cur:
        cur.execute("SET SESSION innodb_lock_wait_timeout = %s", (seconds,))


def lock_status(admin_conn) -> dict:
    """performance_schema 로 event_lock 에 걸린 잠금과 대기를 본다 (admin 연결 필요)."""
    with admin_conn.cursor() as cur:
        cur.execute(
            """
            SELECT l.ENGINE_TRANSACTION_ID, t.PROCESSLIST_ID, l.LOCK_MODE, l.LOCK_STATUS, l.LOCK_DATA
              FROM performance_schema.data_locks l
              LEFT JOIN performance_schema.threads t ON t.THREAD_ID = l.THREAD_ID
             WHERE l.OBJECT_SCHEMA = %s AND l.OBJECT_NAME = 'event_lock' AND l.LOCK_TYPE = 'RECORD'
            """,
            (settings.mysql_database,),
        )
        locks = [
            {"trx": trx, "conn_id": pid, "mode": mode, "status": status, "row": data}
            for trx, pid, mode, status, data in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT REQUESTING_ENGINE_TRANSACTION_ID, BLOCKING_ENGINE_TRANSACTION_ID
              FROM performance_schema.data_lock_waits
            """
        )
        waits = [{"waiting_trx": w, "blocking_trx": b} for w, b in cur.fetchall()]
    admin_conn.commit()  # 다음 호출이 새 상태를 보도록 스냅샷을 끊는다
    return {"locks": locks, "waits": waits}


def record_attempts(conn, rows: Iterable[dict]) -> None:
    """처리 기록. 집계 Upsert 와 같은 트랜잭션에서 호출해야 의미가 있다."""
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO processing_attempts
                (run_id, group_id, instance, mode, event_id, reservation_id, event_code,
                 topic, partition_no, offset_no)
            VALUES (%(run_id)s, %(group_id)s, %(instance)s, %(mode)s, %(event_id)s,
                    %(reservation_id)s, %(event_code)s, %(topic)s, %(partition)s, %(offset)s)
            """,
            list(rows),
        )


def truncate_lab_tables(conn) -> list[str]:
    """실험 초기화. event_lock(C4 제어 행)은 남긴다."""
    tables = ["processing_attempts", "event_statistics", "reservations"]
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"TRUNCATE TABLE {t}")
    conn.commit()
    return tables
