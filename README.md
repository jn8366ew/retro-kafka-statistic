# reservation-kafka-lab

예약·행사 집계 도메인으로 Kafka 커밋·DLQ·잠금을 실험하는 랩.
범위와 근거는 `../kafka-basic/dev-scope.md` (C1~C5), 원안은 `../kafka-basic/dev-plan.md`.

- 브로커 1대 (KRaft, RF 1) / 파티션 2 / 메시지 키 = 예약 ID
- MySQL 8.4 (C2 부터 사용, 테이블 4개 — `db/01-schema.sql`, `db/02-processing.sql`) / PyMySQL / confluent-kafka
- 포트: Kafka **19092**, Kafka UI **18080**, MySQL **13306** — kafka-basic(9092/8080)과 동시 기동 가능

## 실행·종료

```powershell
docker compose up -d          # kafka + kafka-ui + mysql
docker compose ps             # healthcheck 확인
docker compose down           # 종료 (데이터 볼륨 유지)
docker compose down -v        # 종료 + 실습 데이터 초기화 (볼륨 삭제)
uv sync                       # Python 의존성
```

## 토픽 확인·발행·소비 (C1)

```powershell
uv run python -m app.admin bootstrap     # reservation.events 생성 (파티션 2, 설정은 전부 기본값)
uv run python -m app.admin topics        # 브로커에 실제 적용된 설정 (overridden_config 가 비어야 정상)
uv run python -m app.admin watermarks    # 파티션별 low/high watermark

uv run python -m app.produce             # 고정 12건 발행 + murmur2/crc32 예측 vs 실제 파티션 대조
uv run python -m app.produce --dup       # 같은 event_id 두 번 발행 (멱등 프로듀서의 한계 실험)

uv run python -m app.consume                   # 기본 그룹(stat-aggregator)으로 소비, 건별 동기 커밋
uv run python -m app.consume --group other     # 다른 그룹 = 독립 소비
uv run python -m app.consume --tag worker-2    # 같은 그룹 두 번째 인스턴스 (분담 관찰)
```

## 수동 커밋과 전달 보장 (C2)

처음 한 번, 기존 mysql 볼륨에 처리 기록 테이블을 적용한다 (빈 볼륨이면 자동):

```powershell
Get-Content db/02-processing.sql | docker compose exec -T mysql mysql -ulab -plab reservation_lab
```

실험 한 번의 순서 — 초기화, 발행(원천 DB 갱신 → 이벤트), 집계 컨슈머, 검증:

```powershell
uv run python -m app.admin reset                      # 토픽 재생성 + 실습 테이블 TRUNCATE (컨슈머는 먼저 끈다)
uv run python -m app.produce                          # 12건: reservations 갱신 후 발행
uv run python -m app.aggregate --group c2-e1 --idle-exit 4
uv run python -m app.verify --group c2-e1             # 집계 / 누락 / 재처리 / 커밋 위치
uv run python -m app.admin offsets c2-e1              # 그룹 커밋 위치 vs high watermark
```

`app.aggregate` 실험 인자 (전체 설명은 파일 docstring, 결과는 `docs/experiments/C2.md`):

| 인자 | 값 |
| --- | --- |
| `--order` | `process-first`(기본, At-least-once) / `commit-first`(At-most-once) |
| `--crash-at` | `after-db-before-commit` / `after-commit-before-db` / `mid-batch` — `os._exit` hard kill |
| `--fail-event-code X` | 해당 행사 처리 실패 주입 |
| `--mode` | `correct`(기본, 실패 시 커밋 없이 정지) / `wrong-continue`(실패 무시하고 커밋 — 별도 그룹으로) |
| `--commit-style` | `explicit-sync`(기본) / `noarg-sync` / `noarg-async` |
| `--slow-ms`, `--max-poll-interval-ms` | 첫 배치 지연으로 그룹 이탈 → 커밋 실패 |
| `--idle-exit N` | 할당 이후 N초 무소식이면 정상 종료 |

hard kill 뒤 같은 그룹으로 재기동하면 파티션 할당까지 ~10초 걸린다 (session.timeout). 멈춘 게 아니다.

**직접 따라 하는 실습 절차는 [`docs/practice-C2/`](docs/practice-C2/README.md)** — 챕터 5개 (준비와 도구 / 전달 보장 / 실패 처리와 커밋 / 리밸런스 / 코드 고쳐 보기).

## stale write 와 행사별 잠금 (C4)

DB 수준 결정적 재현 (전용 행사 RACE-01 만 쓴다, Kafka 불필요):

```powershell
uv run python -m app.stale all       # s1 잠금 없음 / s2 잠금 / s3 잠금 전 조회(스냅샷 함정) / s3b READ COMMITTED
```

Kafka 워커 두 개로 재현 (터미널 3개, 먼저 `app.admin reset` → `app.produce` → 초기 12건 처리):

```powershell
# 터미널 1, 2 — 파티션 고정 할당. --lock 을 빼고/넣고 비교
uv run python -m app.aggregate --group c4 --tag w0 --assign 0 --hold-after-read-ms 6000 --lock
uv run python -m app.aggregate --group c4 --tag w1 --assign 1 --lock
# 터미널 3 — p0 쪽 변경 → 1~2초 뒤 p1 쪽 변경
uv run python -m app.produce --one r-1005 EXPO-02 CONFIRMED 28000
uv run python -m app.produce --one r-1004 EXPO-02 CONFIRMED 50000
uv run python -m app.admin locks                   # 누가 잠금을 쥐고(GRANTED) 누가 기다리나(WAITING)
uv run python -m app.verify --group c4
```

복구 배치도 같은 저장 절차를 쓴다: `uv run python -m app.recover [--lock] [--event-code X] [--hold-after-read-ms N]`.
결과는 `docs/experiments/C4.md`.

상태 들여다보기:

```powershell
# Kafka UI
start http://localhost:18080

# 세그먼트 파일 직접 덤프
.\scripts\dump_log.ps1 -Partition 0 -ListOnly

# MySQL (스키마는 db/01-schema.sql, 최초 기동 시 자동 적용)
docker exec -it rkl-mysql mysql -ulab -plab reservation_lab
```

## 빠른 확인 (Windows PowerShell)

프로젝트로 이동:

```powershell
cd C:\Users\FAMILY\projs\reservation-kafka-lab
```

상태·구성 검토:

```powershell
git status                                   # 이 랩의 작업 트리 (아직 최초 커밋 전)
git -C ..\kafka-basic diff dev-scope.md      # 범위 문서에서 바뀐 부분
Get-ChildItem -Recurse app, db, docs         # 파일 구성 훑어보기
```

실행 상태 점검:

```powershell
docker compose ps                            # rkl-kafka / rkl-kafka-ui / rkl-mysql
uv run python -m app.admin topics            # 토픽 설정 (overridden_config 비어야 정상)
uv run python -m app.admin watermarks        # 파티션별 오프셋 범위
docker exec -it rkl-mysql mysql -ulab -plab reservation_lab   # DB 접속 (SHOW TABLES; 등)
```

### 터미널 배치

소비기는 종료할 때까지 터미널 하나를 계속 차지한다. 기본 2개, 그룹 실험은 3개를 띄운다
(모든 터미널에서 먼저 `cd C:\Users\FAMILY\projs\reservation-kafka-lab`).

| 터미널 | 역할 | 명령 |
| --- | --- | --- |
| 1 | 소비기 (계속 실행) | `uv run python -m app.consume` |
| 2 | 발행기·관리 명령 | `uv run python -m app.produce`, `app.admin ...` |
| 3 (선택) | 같은 그룹 2번째 인스턴스 — 파티션 분담 관찰 | `uv run python -m app.consume --tag worker-2` |
| 3 (선택) | 또는 다른 그룹 — 독립 소비 관찰 | `uv run python -m app.consume --group other` |

소비기 종료는 Ctrl+C. 터미널 3에서 인스턴스를 켜고 끌 때 터미널 1의 `[assign]`/`[revoke]`
로그로 리밸런싱이 보인다.

## 오류 구분 (L01 완료 기준)

- **환경 연결 오류**: `produce`/`consume` 가 `브로커가 떠 있나요?` 또는 접속 타임아웃을 낸다 →
  compose 상태·포트(19092)를 확인한다.
- **업무 처리 오류**: 소비 로그에 `역직렬화 실패: ...` 로 찍힌다. 소비는 계속된다 (격리는 C3).

## 실험 기록

`docs/experiments/` 에 실습별 기록을 남긴다 (`dev-plan.md` 17장 D 템플릿).
파일이 존재한다는 이유만으로 검증 완료로 표시하지 않는다 — 실행 못 한 항목은 실행 미확인.
