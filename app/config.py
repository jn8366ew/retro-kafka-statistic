"""랩 전역 설정.

모든 값은 `RKL_` 접두사 환경변수로 덮어쓸 수 있다. PowerShell 예:

    $env:RKL_CONSUMER_GROUP = "observer-b"; uv run python -m app.consume
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RKL_", env_file=".env", extra="ignore")

    bootstrap_servers: str = "localhost:19092"

    # Java 클라이언트 기본 파티셔너와 같은 결과를 내도록 murmur2 를 기본값으로 둔다.
    # (librdkafka 기본은 consistent_random=CRC32 — partitioning.py 주석 참고)
    partitioner: str = "murmur2_random"

    # 면접 문서 Q5 "파티션 2개였던 것으로 기억" — dev-scope.md 3장에서 2로 확정
    num_partitions: int = 2

    topic_events: str = "reservation.events"

    consumer_group: str = "stat-aggregator"

    # MySQL (C2 부터 사용)
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 13306
    mysql_user: str = "lab"
    mysql_password: str = "lab"
    mysql_database: str = "reservation_lab"
    # 관측 전용 (C4). performance_schema.data_locks 는 lab 계정에 권한이 없다.
    mysql_admin_user: str = "root"
    mysql_admin_password: str = "labroot"


settings = Settings()


def consumer_config(
    group_id: str | None = None,
    instance_tag: str | None = None,
    max_poll_interval_ms: int | None = None,
) -> dict:
    """컨슈머 공통 설정. 커밋은 전부 수동이다 (C2 의 실험 대상).

    --- 관찰을 위해 기본값에서 벗어난 컨슈머 설정 (프로덕션 금지) ---
      session.timeout.ms     45s -> 10s  강제 종료 후 재배정 대기 단축 (C2)
      heartbeat.interval.ms  3s  -> 3s   session.timeout 의 1/3 이하 제약을 명시
      max.poll.interval.ms   300s -> 인자로 줄 때만  커밋 실패(리밸런스) 유발 실험 (C2 E6)
    이 목록에 없는 컨슈머 값은 전부 기본값이다.
    (브로커 쪽 튜닝은 docker-compose.yml 의 주석 블록에 있다)

    librdkafka 는 max.poll.interval.ms >= session.timeout.ms 를 요구한다 (여기서는 10s 이상).
    """
    cfg = {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id or settings.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 10_000,
        "heartbeat.interval.ms": 3_000,
    }
    if instance_tag:
        cfg["client.id"] = instance_tag
    if max_poll_interval_ms is not None:
        cfg["max.poll.interval.ms"] = max_poll_interval_ms
    return cfg
