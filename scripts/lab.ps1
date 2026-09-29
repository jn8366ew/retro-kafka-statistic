# C2 실습 단축 함수. 프로젝트 루트에서 창을 열 때마다 한 번:  . .\scripts\lab.ps1
# (앞의 점+공백이 있어야 함수가 현재 창에 남는다)

# 실험 시작: 토픽·테이블 초기화 후 고정 12건 발행
function fresh { uv run python -m app.admin reset | Out-Null; uv run python -m app.produce }

# 판정 4개만 보기
function v($g) { (uv run python -m app.verify --group $g | Out-String | ConvertFrom-Json).verdict }

# 판정 + 집계 + 누락/재처리 위치까지
function vv($g) {
  $r = uv run python -m app.verify --group $g | Out-String | ConvertFrom-Json
  $r.verdict
  $r.statistics | Format-Table event_code, @{n='expected';e={$_.expected -join '/'}}, @{n='actual';e={$_.actual -join '/'}}, ok
  "missing:";     $r.missing     | Format-Table pos, event_code, key
  "reprocessed:"; $r.reprocessed | Format-Table pos, attempts, event_code, key
}

# 그룹 커밋 위치 vs 끝 위치
function off($g) { (uv run python -m app.admin offsets $g | Out-String | ConvertFrom-Json).partitions | Format-Table }

Write-Host "lab functions loaded: fresh, v, vv, off"
