<#
.SYNOPSIS
    Gateway API 이미지를 빌드하고 예시 private registry에 푸시합니다.
#>

# 1. param 블록이 반드시 코드의 가장 처음에 와야 합니다.
param(
    [string]$Tag = "v1.0",
    [string]$Profile = "dev"
)

# 2. 그 다음 에러 설정 및 변수 선언을 진행합니다.
$ErrorActionPreference = "Stop"

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

# 변수 설정
$Registry = "registry.example.com:5000"
$ImageName = "ai-tts-queuetts-gateway"
$FullImage = "${Registry}/${ImageName}:${Tag}"
# docker-compose.yml 이 빌드하는 로컬 이미지 이름 (태그 source)
$BuiltImage = "queuetts-tts-gateway:latest"

# docker-compose.yml 의 build arg(PROFILE)로 전달되어 application.yml 환경이 결정된다.
$env:PROFILE = $Profile

Write-Host "=== 이미지 빌드 (profile=${Profile}) ===" -ForegroundColor Cyan
docker compose -f "docker-compose.yml" build --no-cache

if ($LASTEXITCODE -ne 0) { throw "Docker build failed" }

Write-Host "=== 태깅: ${BuiltImage} -> ${FullImage} ===" -ForegroundColor Cyan
docker tag "${BuiltImage}" "${FullImage}"
if ($LASTEXITCODE -ne 0) { throw "Docker tag failed" }

Write-Host "=== Registry 푸시: ${FullImage} ===" -ForegroundColor Cyan
docker push "${FullImage}"

if ($LASTEXITCODE -ne 0) { throw "Docker push failed" }

Write-Host "=== 완료: ${FullImage} ===" -ForegroundColor Green
