<#
.SYNOPSIS
    Kafka 세그먼트 파일(.log)을 직접 덤프한다.
    컴팩션 전/후를 비교해 stale write 가 "물리적으로" 사라졌음을 증명하는 용도.

.EXAMPLE
    # 컴팩션 전
    .\scripts\dump_log.ps1 -Partition 1 -Out dumps\before.txt

    # (10초 대기 -> POST /scenarios/force-roll -> 몇 초 더 대기)

    # 컴팩션 후
    .\scripts\dump_log.ps1 -Partition 1 -Out dumps\after.txt

    # 남아있는 레코드만 비교
    Select-String -Path dumps\before.txt -Pattern '^\| offset'
    Select-String -Path dumps\after.txt  -Pattern '^\| offset'

.EXAMPLE
    # 세그먼트 파일 목록만 보기 (.log 가 여러 개면 롤이 일어났다는 뜻)
    .\scripts\dump_log.ps1 -Partition 1 -ListOnly
#>
[CmdletBinding()]
param(
    [string]$Topic = "reservation.events",
    [int]$Partition = 0,
    [string]$Out = "",
    [switch]$ListOnly
)

$ErrorActionPreference = "Stop"
$dir = "/var/lib/kafka/data/$Topic-$Partition"

if ($ListOnly) {
    docker compose exec -T kafka ls -la $dir
    return
}

# NOTE: PowerShell 5.1 은 네이티브 명령에 따옴표가 들어간 인자를 넘길 때 인용을 깨뜨린다.
# 그래서 셸 파이프라인을 컨테이너 안에서 조립하지 않고, 파일 목록을 여기서 만들어
# 따옴표 없는 인자로만 kafka-dump-log 에 넘긴다.
$listing = docker compose exec -T kafka sh -c "ls -1 $dir/*.log"
$files = $listing -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ }

if (-not $files) {
    Write-Error "no .log segment found in $dir"
    return
}

$joined = $files -join ","
Write-Host "segments ($($files.Count)): $joined"

$result = docker compose exec -T kafka kafka-dump-log --files $joined --print-data-log

if ($Out) {
    $outDir = Split-Path -Parent $Out
    if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }
    $result | Set-Content -Path $Out -Encoding UTF8
    Write-Host "saved: $Out" -ForegroundColor Green
    # 실제로 남아있는 레코드만 요약 (컴팩션 전후 비교 지점)
    $result | Select-String -Pattern '^\| offset'
} else {
    $result
}
