-- dev-scope.md 3장의 DB 스키마 3개.
-- C1 에서는 DDL 만 적용한다 (DB 를 실제로 쓰는 것은 C2 부터).
-- mysql 컨테이너 최초 기동 시 docker-entrypoint-initdb.d 로 자동 적용된다.
-- 볼륨을 지우지 않고 다시 적용하려면:
--   docker compose exec -T mysql mysql -ulab -plab reservation_lab < db/01-schema.sql

-- 원천. 재계산의 입력. (집계는 이 테이블을 재조회한 절대값으로 갱신한다 — 이벤트 금액 누적 금지)
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id VARCHAR(32)  PRIMARY KEY,
    event_code     VARCHAR(32)  NOT NULL,
    status         VARCHAR(16)  NOT NULL,  -- CONFIRMED / CANCELLED
    amount         INT          NOT NULL,
    updated_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_reservations_event_code (event_code)
);

-- 집계 결과. 절대값 Upsert 대상.
CREATE TABLE IF NOT EXISTS event_statistics (
    event_code        VARCHAR(32)  PRIMARY KEY,
    total_amount      INT          NOT NULL,
    reservation_count INT          NOT NULL,
    updated_at        TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

-- 행사별 제어 행. C4 전용.
-- event_statistics 에 아직 행이 없는 새 행사도 SELECT ... FOR UPDATE 로 잡을 대상이
-- 항상 존재하도록 미리 만들어 둔다 (dev-scope.md 3장).
CREATE TABLE IF NOT EXISTS event_lock (
    event_code VARCHAR(32)  PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
