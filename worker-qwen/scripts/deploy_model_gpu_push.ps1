<#
.SYNOPSIS
    Qwen 모델 워커(GPU) 이미지를 빌드하고 예시 private registry에 푸시합니다.

.DESCRIPTION
    푸시만 실패했다면 -SkipBuild 로 재실행한다. 이미 만들어진 로컬 이미지를 그대로
    태깅/푸시하므로 수 GB 재빌드를 건너뛴다. 레지스트리에 이미 올라간 레이어도
    다시 전송하지 않는다.

    빌드는 기본적으로 레이어 캐시를 쓴다. requirements.txt 나 Dockerfile 이 바뀌면
    pip 레이어는 알아서 무효화되고, 소스만 바뀌었으면 마지막 COPY 레이어만 다시 굽는다.
    -NoCache 는 캐시가 실제로 오염됐다고 판단될 때만 쓴다: 6GB 가 넘는 torch cu124
    레이어를 매번 다시 받아 굽고, 직전 15GB 이미지를 고아(<none>)로 남긴다.
#>

# 1. param 블록이 반드시 코드의 가장 처음에 와야 합니다.
param(
    [string]$Tag = "v1.0",
    [switch]$SkipBuild,
    [switch]$NoCache,
    # 빌드 캐시 상한. WSL2 VHDX 는 한 번 커지면 자동으로 줄지 않으므로 상한을 둔다.
    [string]$MaxBuildCache = "20GB"
)

# 2. 그 다음 에러 설정 및 변수 선언을 진행합니다.
$ErrorActionPreference = "Stop"

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

# 변수 설정
$Registry = "registry.example.com:5000"
$Type = "gpu"
$ImageName = "ai-tts-queuetts-model-worker-qwen-${Type}"
$FullImage = "${Registry}/${ImageName}:${Tag}"
# docker-compose.${Type}.yml 이 빌드하는 로컬 이미지 이름 (태그 source)
$BuiltImage = "queuetts-tts-model-worker-qwen:${Type}"

if ($SkipBuild) {
    Write-Host "=== 빌드 건너뜀 (-SkipBuild): 기존 ${BuiltImage} 사용 ===" -ForegroundColor Yellow
    docker image inspect "${BuiltImage}" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "로컬 이미지 ${BuiltImage} 가 없습니다. -SkipBuild 없이 실행하세요." }
}
else {
    $buildArgs = @("-f", "docker-compose.${Type}.yml", "build")
    if ($NoCache) {
        Write-Host "=== 이미지 빌드 (--no-cache: torch 레이어까지 전부 재빌드) ===" -ForegroundColor Yellow
        $buildArgs += "--no-cache"
    }
    else {
        Write-Host "=== 이미지 빌드 (레이어 캐시 사용) ===" -ForegroundColor Cyan
    }

    docker compose @buildArgs

    if ($LASTEXITCODE -ne 0) { throw "Docker build failed" }
}

Write-Host "=== 태깅: ${BuiltImage} -> ${FullImage} ===" -ForegroundColor Cyan
docker tag "${BuiltImage}" "${FullImage}"
if ($LASTEXITCODE -ne 0) { throw "Docker tag failed" }

Write-Host "=== Registry 푸시: ${FullImage} ===" -ForegroundColor Cyan
docker push "${FullImage}"

if ($LASTEXITCODE -ne 0) { throw "Docker push failed" }

# 푸시가 끝난 뒤에 정리한다. 실패했다면 로컬 이미지를 남겨 둬야 -SkipBuild 로 재시도할 수 있다.
# 여기서 지우는 건 태그를 잃은 이미지(<none>)뿐이다 — 직전 배포판이 새 빌드에 밀려난 것들.
# 이걸 안 하면 배포 1회마다 15GB 가 VHDX 에 영구히 쌓인다.
Write-Host "=== 정리: 고아 이미지 / 빌드 캐시 ===" -ForegroundColor Cyan
docker image prune -f
docker builder prune -f --max-used-space $MaxBuildCache

docker system df

Write-Host "=== 완료: ${FullImage} ===" -ForegroundColor Green
Write-Host "VHDX 는 이미지를 지워도 자동으로 줄지 않는다. 실제 회수는 README 의 sparse 설정 참고." -ForegroundColor DarkGray
