-- C2 부터 사용하는 처리 기록. dev-scope.md 3장의 3개 테이블에 더한 4번째 테이블.
--
-- 행사별 집계 Upsert 와 **같은 DB 트랜잭션**에서 INSERT 한다. 따라서 행 하나 =
-- "DB 효과가 커밋된 처리 1회"다. 집계는 절대값 Upsert 라서 event_statistics 만 봐서는
-- 재처리가 있었는지 알 수 없다 — 처리 시도 중복과 금액 중복 누적을 구분하려면 이 기록이 필요하다.
--
-- 기존 mysql 볼륨에는 initdb 가 다시 돌지 않으므로 수동 적용:
--   Get-Content db/02-processing.sql | docker compose exec -T mysql mysql -ulab -plab reservation_lab

CREATE TABLE IF NOT EXISTS processing_attempts (
    id             BIGINT       AUTO_INCREMENT PRIMARY KEY,
    run_id         VARCHAR(32)  NOT NULL,  -- 컨슈머 프로세스 1회 기동의 식별자
    group_id       VARCHAR(64)  NOT NULL,
    instance       VARCHAR(32)  NOT NULL,
    mode           VARCHAR(32)  NOT NULL,  -- correct / wrong-continue
    event_id       VARCHAR(32)  NOT NULL,
    reservation_id VARCHAR(32)  NOT NULL,
    event_code     VARCHAR(32)  NOT NULL,
    topic          VARCHAR(64)  NOT NULL,
    partition_no   INT          NOT NULL,
    offset_no      BIGINT       NOT NULL,
    created_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    KEY idx_attempts_event_id (event_id),
    KEY idx_attempts_position (group_id, partition_no, offset_no)
);
