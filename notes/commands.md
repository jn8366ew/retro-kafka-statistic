# 구성과 실행 명령

[README](../README.md) 의 상세판. 명령은 Windows PowerShell 기준이다.

## 구성

| 구성 요소 | 버전 / 설정 |
| --- | --- |
| Kafka | `confluentinc/cp-kafka:7.8.0`, KRaft 단일 노드 (broker+controller), RF 1 |
| Kafka UI | `ghcr.io/kafbat/kafka-ui` |
| MySQL | 8.4, InnoDB 기본 REPEATABLE READ |
| Python | ≥ 3.11, [uv](https://docs.astral.sh/uv/), confluent-kafka (librdkafka), PyMySQL, pydantic-settings |

| 포트 (호스트) | 용도 |
| --- | --- |
| 19092 | Kafka 브로커 |
| 18080 | Kafka UI |
| 13306 | MySQL (`lab` / `lab`, DB `reservation_lab`) |

데이터 흐름:

```
app.produce ── reservations 갱신 ─→ reservation.events (파티션 2, 키 = 예약 ID) ─→ app.aggregate ─→ event_statistics
                                                                                   (행사 원천 합계를 절대값 재계산)
```

| 테이블 | 역할 |
| --- | --- |
| `reservations` | 원천. 예약별 행사·상태·금액 |
| `event_statistics` | 집계 결과. 행사별 총액·건수 |
| `event_lock` | 행사별 잠금 제어 행 (`SELECT ... FOR UPDATE`, C4) |
| `processing_attempts` | 컨슈머 처리 시도 기록 — 누락·재처리 위치 판정용 (C2) |

스키마는 `db/01-schema.sql`, `db/02-processing.sql` — MySQL 최초 기동(빈 볼륨) 시 자동 적용된다.

## 디렉터리

```
app/
  admin.py         토픽 생성·설정 조회·watermark·그룹 오프셋·초기화·잠금 관측
  produce.py       고정 이벤트 12건 발행 (원천 DB 갱신 → 발행), --dup / --one / --kafka-only
  producer.py      Producer 래퍼 (딜리버리 리포트로 실제 partition/offset 확인)
  partitioning.py  murmur2 / crc32 파티션 예측 계산기
  consume.py       관찰용 컨슈머 (C1)
  aggregate.py     집계 컨슈머 — 커밋 순서·크래시·실패·지연·잠금 주입 (C2·C4)
  verify.py        집계 / 누락 / 재처리 / 커밋 위치 판정
  stale.py         stale write 결정적 재현 (DB 트랜잭션만, C4)
  recover.py       Kafka 없이 원천에서 집계 재계산하는 복구 배치 (C4)
  db.py            MySQL 접근 (트랜잭션 경계는 호출자가 정함)
  config.py        설정 (RKL_ 환경변수로 덮어쓰기)
db/                스키마 SQL
scripts/
  lab.ps1          PowerShell 단축 함수 (fresh / v / vv / off)
  dump_log.ps1     Kafka 세그먼트 파일 덤프
tests/             순수 로직 단위 테스트
```

## 실습별 명령

### C1 — 키·파티션·오프셋

```powershell
uv run python -m app.admin topics            # 브로커 적용 설정 (overridden_config 가 비어야 정상)
uv run python -m app.admin watermarks        # 파티션별 low/high watermark

uv run python -m app.produce                 # 발행 + murmur2/crc32 예측 vs 실제 파티션 대조
uv run python -m app.produce --dup           # 같은 event_id 두 번 발행

uv run python -m app.consume                 # 기본 그룹으로 소비, 건별 동기 커밋
uv run python -m app.consume --group other   # 다른 그룹 = 독립 소비
uv run python -m app.consume --tag worker-2  # 같은 그룹 두 번째 인스턴스 (파티션 분담 관찰)
```

### C2 — 수동 커밋과 전달 보장

실험 한 번의 순서 — 초기화 → 발행 → 집계 컨슈머 → 검증:

```powershell
uv run python -m app.admin reset             # 토픽 재생성 + 실습 테이블 TRUNCATE (컨슈머는 먼저 끈다)
uv run python -m app.produce
uv run python -m app.aggregate --group c2-e2 --crash-at after-db-before-commit --idle-exit 4
uv run python -m app.aggregate --group c2-e2 --idle-exit 4     # 같은 그룹으로 재기동
uv run python -m app.verify --group c2-e2
uv run python -m app.admin offsets c2-e2     # 그룹 커밋 위치 vs high watermark
```

`app.aggregate` 실험 인자 (전체 설명은 파일 docstring):

| 인자 | 값 |
| --- | --- |
| `--order` | `process-first`(기본, At-least-once) / `commit-first`(At-most-once) |
| `--crash-at` | `after-db-before-commit` / `after-commit-before-db` / `mid-batch` — `os._exit` 로 hard kill |
| `--fail-event-code X` | 해당 행사 처리 실패 주입 |
| `--mode` | `correct`(기본, 실패 시 커밋 없이 정지) / `wrong-continue`(실패 무시하고 커밋) |
| `--commit-style` | `explicit-sync`(기본) / `noarg-sync` / `noarg-async` |
| `--slow-ms`, `--max-poll-interval-ms` | 첫 배치 지연으로 그룹 이탈 → 커밋 실패 |
| `--idle-exit N` | 파티션 할당 이후 N초 새 메시지가 없으면 정상 종료 |

hard kill 뒤 같은 그룹으로 재기동하면 파티션 할당까지 ~10초 걸린다 (`session.timeout.ms`). 멈춘 게 아니다.

### C4 — stale write 와 행사별 잠금

DB 수준 결정적 재현 (Kafka 불필요):

```powershell
uv run python -m app.stale all    # s1 잠금 없음 / s2 잠금 / s3 잠금 전 조회 / s3b READ COMMITTED
```

Kafka 워커 두 개로 재현 (터미널 3개, 먼저 `reset` → `produce` → 초기 12건 처리):

```powershell
# 터미널 1, 2 — 파티션 고정 할당. --lock 을 빼고/넣고 비교
uv run python -m app.aggregate --group c4 --tag w0 --assign 0 --hold-after-read-ms 6000 --lock
uv run python -m app.aggregate --group c4 --tag w1 --assign 1 --lock
# 터미널 3 — p0 쪽 변경 → 1~2초 뒤 p1 쪽 변경
uv run python -m app.produce --one r-1005 EXPO-02 CONFIRMED 28000
uv run python -m app.produce --one r-1004 EXPO-02 CONFIRMED 50000
uv run python -m app.admin locks             # 잠금 보유(GRANTED) / 대기(WAITING)
uv run python -m app.verify --group c4
```

잠금 대기 타임아웃은 `--lock-wait-timeout N`, 복구 배치는
`uv run python -m app.recover [--lock] [--event-code X] [--hold-after-read-ms N]`.

### 단축 함수와 상태 보기

```powershell
. .\scripts\lab.ps1          # 창마다 한 번 (앞의 점+공백 필수)
fresh                        # reset + 고정 12건 발행
v c2-e1                      # 판정 4개
vv c2-e1                     # 판정 + 집계 + 누락/재처리 위치
off c2-e1                    # 그룹 커밋 위치 vs 끝 위치

start http://localhost:18080                          # Kafka UI
.\scripts\dump_log.ps1 -Partition 0 -ListOnly         # 세그먼트 파일 목록
docker exec -it rkl-mysql mysql -ulab -plab reservation_lab
```

## 주의

- **개발 전용 구성이다.** 단일 브로커 RF 1, 평문 비밀번호(`lab` / `labroot`), 관찰을 위해 바꾼 브로커 설정
  (`group.initial.rebalance.delay.ms=0`, `auto.create.topics.enable=false`)이 들어 있다. `docker-compose.yml` 주석 참고.
- 명령과 스크립트는 Windows PowerShell 기준이다. `python -m app.*` 명령 자체는 OS 와 무관하다.
- 잠금 관측(`app.admin locks`)은 `performance_schema` 권한 때문에 root 연결을 쓴다.
