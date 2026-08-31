@echo off
setlocal
set "REPO_ROOT=%~dp0.."
set "COMPOSE_FILE=%REPO_ROOT%\docker-compose.cpu.yml"

if not "%~1"=="" (
    docker compose -f "%COMPOSE_FILE%" %*
    exit /b %ERRORLEVEL%
)

docker compose -f "%COMPOSE_FILE%" build ai-tts-queuetts-1
if errorlevel 1 exit /b %ERRORLEVEL%

docker compose -f "%COMPOSE_FILE%" up -d
exit /b %ERRORLEVEL%
