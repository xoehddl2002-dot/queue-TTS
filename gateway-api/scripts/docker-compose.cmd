@echo off
setlocal
set "REPO_ROOT=%~dp0.."
set "COMPOSE_FILE=%REPO_ROOT%\docker-compose.yml"

if not "%~1"=="" (
    docker compose -f "%COMPOSE_FILE%" %*
    exit /b %ERRORLEVEL%
)

docker compose -f "%COMPOSE_FILE%" build gateway-api
if errorlevel 1 exit /b %ERRORLEVEL%

docker compose -f "%COMPOSE_FILE%" up -d
exit /b %ERRORLEVEL%
