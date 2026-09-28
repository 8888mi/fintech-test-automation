я@echo off
rem Windows stand-in for the Makefile: starts Docker Desktop if needed,
rem brings up Kafka + the payments service, then runs pytest.
rem
rem   run-tests.bat                 whole suite, parallel, one rerun (as in CI)
rem   run-tests.bat api             tests/api only
rem   run-tests.bat integration     tests/integration only
rem   run-tests.bat ui --headed     tests/ui; extra arguments go straight to pytest
rem   run-tests.bat install         poetry install + Chromium
rem   run-tests.bat kafka-ui        browser view of Kafka on http://localhost:8080
rem   run-tests.bat down            stop the environment (incl. kafka-ui) and drop its volumes
rem
rem The environment is left running after the tests so reruns start instantly.

setlocal EnableDelayedExpansion
cd /d "%~dp0"

set "SUITE=%~1"
if "%SUITE%"=="" set "SUITE=all"

rem Everything after the first argument is passed to pytest untouched.
set "EXTRA="
set "FIRST=1"
for %%A in (%*) do (
    if defined FIRST (set "FIRST=") else set "EXTRA=!EXTRA! %%A"
)

if /i "%SUITE%"=="install" goto :install
if /i "%SUITE%"=="down" goto :down
if /i "%SUITE%"=="kafka-ui" goto :kafka_ui

if /i "%SUITE%"=="all" (
    set "PYTEST_ARGS=-n auto --reruns 1 --reruns-delay 2"
) else if /i "%SUITE%"=="api" (
    set "PYTEST_ARGS=tests/api -n auto"
) else if /i "%SUITE%"=="integration" (
    set "PYTEST_ARGS=tests/integration"
) else if /i "%SUITE%"=="ui" (
    set "PYTEST_ARGS=tests/ui"
) else (
    echo Unknown suite "%SUITE%". Use: all, api, integration, ui, install, kafka-ui, down.
    exit /b 2
)

call :ensure_docker || exit /b 1

echo.
echo === Starting Kafka and the payments service...
docker compose up -d --wait
if errorlevel 1 (
    echo docker compose failed. Service logs: docker compose logs payments
    exit /b 1
)

echo.
echo === poetry run pytest %PYTEST_ARGS%%EXTRA%
poetry run pytest %PYTEST_ARGS%%EXTRA%
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (echo === Tests passed.) else (echo === Tests failed, pytest exit code %RC%.)
echo Report: allure serve allure-results      Stop environment: run-tests.bat down
exit /b %RC%


:install
poetry install || exit /b 1
poetry run playwright install chromium
exit /b %ERRORLEVEL%


:kafka_ui
call :ensure_docker || exit /b 1
docker compose --profile tools up -d --wait kafka-ui || exit /b 1
echo Kafka UI: http://localhost:8080
start "" http://localhost:8080
exit /b 0


:down
call :ensure_docker || exit /b 1
rem --profile tools so an opt-in kafka-ui container is removed as well.
docker compose --profile tools down -v
exit /b %ERRORLEVEL%


:ensure_docker
docker info >nul 2>&1 && exit /b 0
set "DOCKER_EXE=C:\Program Files\Docker\Docker\Docker Desktop.exe"
if not exist "%DOCKER_EXE%" (
    echo Docker Desktop not found at "%DOCKER_EXE%".
    exit /b 1
)
echo === Starting Docker Desktop...
start "" "%DOCKER_EXE%"
rem Poll for up to ~2 minutes; the daemon usually answers within 30 seconds.
rem ping is used as the delay because `timeout` fails when stdin is redirected.
for /l %%i in (1,1,40) do (
    ping -n 4 127.0.0.1 >nul
    docker info >nul 2>&1 && (echo Docker is ready. & exit /b 0)
)
echo Docker daemon did not respond within 2 minutes.
exit /b 1
