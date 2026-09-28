# reservation-kafka-lab

예약·행사 집계를 예제로 Kafka 컨슈머의 **커밋·장애·동시성**을 직접 부딪혀 보는 개인 실습 저장소.

예약 이벤트를 Kafka 로 흘리고, 컨슈머가 행사별 집계를 MySQL 에 저장한다.
그 과정에서 일부러 죽이고, 실패시키고, 느리게 만들어 "무엇이 중복되고 무엇이 사라지는지"를 확인한다.

- Kafka 1대 (KRaft) · MySQL 8.4 · Python (confluent-kafka, PyMySQL) — `docker compose up -d` 로 기동
- 포트: Kafka 19092 · Kafka UI 18080 · MySQL 13306

## 진행 상황

| 단계 | 주제 | 상태 |
| --- | --- | --- |
| C1 | 환경, 키·파티션·오프셋 | 완료 |
| C2 | 수동 커밋과 전달 보장 (At-least-once / At-most-once, 커밋 실패, 리밸런스) | 구현·실험 완료, 직접 실습 중 (코드 과제 남음) |
| C3 | 재시도·DLQ | 예정 (선택) |
| C4 | stale write 와 행사별 잠금 | 구현·실험 완료 |
| C5 | Kafka 트랜잭션 / Exactly-once | 예정 (선택) |

계속 회고 진행중.. 