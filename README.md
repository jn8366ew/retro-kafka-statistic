# reservation-kafka-lab

예약·행사 집계를 예제로 Kafka 컨슈머의 **커밋·장애·동시성**을 직접 부딪혀 보는 개인 실습 저장소.

예약 이벤트를 Kafka 로 흘리고, 컨슈머가 행사별 집계를 MySQL 에 저장한다.
그 과정에서 일부러 죽이고, 실패시키고, 느리게 만들어 "무엇이 중복되고 무엇이 사라지는지"를 확인한다.
판정은 로그가 아니라 원천 DB 에서 독립 계산한 기대값과 실제 집계를 대조해서 한다.

- Kafka 1대 (KRaft) · MySQL 8.4 · Python (confluent-kafka, PyMySQL)
- 포트: Kafka 19092 · Kafka UI 18080 · MySQL 13306

## 한 것

| 단계 | 주제 | 상태 |
| --- | --- | --- |
| C1 | 키·파티션·오프셋 — 파티션 예측, 멱등 프로듀서의 한계 | 완료 |
| C2 | 수동 커밋과 전달 보장 — At-least-once / At-most-once, 커밋 실패, 리밸런스 | 실험 완료, 코드 과제 진행 중 |
| C3 | 재시도·DLQ | 예정 (선택) |
| C4 | stale write 와 행사별 잠금 | 실험 완료 |
| C5 | Kafka 트랜잭션 / Exactly-once | 예정 (선택) |

## 알게 된 것

- **lag 0 은 정상의 증거가 아니다.** 누락과 stale write 는 커밋 위치가 끝까지 가 있어도 생기고, 결과 대조로만 드러난다.
- **처리 시도 중복 ≠ 금액 중복.** 집계를 절대값으로 재계산하면 재처리는 무해하다.
- **재처리 멱등성과 동시 쓰기 정합성은 별개다.** 후자는 행사별 잠금이 필요하고, 잠금은 트랜잭션의 첫 조회여야 한다.

## 남은 것

- AWS DMS, MSK, 람다로 카프카 연결 설정하기



## 빠른 시작

```powershell
docker compose up -d
uv sync
uv run python -m app.admin bootstrap
uv run python -m app.produce
uv run python -m app.aggregate --group demo --idle-exit 4
uv run python -m app.verify --group demo
```

## 더 보기

- [notes/commands.md](notes/commands.md) — 구성, 디렉터리, 실습별 명령과 실험 인자
- [notes/results.md](notes/results.md) — C1·C2·C4 실험 결과 표
