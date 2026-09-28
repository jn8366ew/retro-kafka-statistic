"""관리 CLI.

    uv run python -m app.admin bootstrap        # 토픽 생성 (있으면 exists)
    uv run python -m app.admin topics           # 브로커에 실제 적용된 토픽 설정
    uv run python -m app.admin watermarks       # 파티션별 low/high watermark
    uv run python -m app.admin offsets GROUP    # 그룹 커밋 위치 vs high watermark (C2)
    uv run python -m app.admin reset            # 실험 초기화: 토픽 재생성 + 실습 테이블 TRUNCATE (C2)
    uv run python -m app.admin locks            # event_lock 에 걸린 잠금과 대기 (C4, root 로 관측)
"""

from __future__ import annotations

import json
import sys

from . import db
from .config import settings
from .topics import bootstrap_topics, describe_topics, group_offsets, reset_topic, watermarks


def reset() -> dict:
    """실험마다 깨끗한 상태. 컨슈머가 떠 있으면 먼저 끈다 (삭제된 토픽을 붙잡고 있게 된다)."""
    topic = reset_topic(settings.topic_events)
    conn = db.connect()
    try:
        tables = db.truncate_lab_tables(conn)
    finally:
        conn.close()
    return {"topic": topic, "truncated": tables}


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "topics"
    if cmd == "bootstrap":
        out = bootstrap_topics()
    elif cmd == "topics":
        out = describe_topics()
    elif cmd == "watermarks":
        out = watermarks(settings.topic_events)
    elif cmd == "offsets" and len(sys.argv) > 2:
        out = group_offsets(sys.argv[2], settings.topic_events)
    elif cmd == "reset":
        out = reset()
    elif cmd == "locks":
        admin_conn = db.connect(admin=True)
        try:
            out = db.lock_status(admin_conn)
        finally:
            admin_conn.close()
    else:
        print(__doc__)
        sys.exit(2)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
