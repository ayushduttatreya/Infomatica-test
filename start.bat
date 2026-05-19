@echo off
REM =============================================================================
REM start.bat – XClarity ETL NiFi Migration (Windows)
REM Starts Docker Compose services then invokes main.py
REM
REM Usage:
REM   start.bat [--modules all]
REM =============================================================================

setlocal EnableDelayedExpansion

set SCRIPT_DIR=%~dp0
set NIFI_URL=%NIFI_URL%
if "%NIFI_URL%"=="" set NIFI_URL=http://localhost:8080

set MODULES=%MODULES%
if "%MODULES%"=="" set MODULES=all

set NIFI_READY_TIMEOUT=180
set ELAPSED=0

echo [%TIME%] XClarity ETL NiFi Migration – start.bat

REM ---------------------------------------------------------------------------
REM 1. Pre-flight checks
REM ---------------------------------------------------------------------------
where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] docker not found on PATH
    exit /b 1
)

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] python not found on PATH
    exit /b 1
)

REM Prefer docker compose v2 plugin
docker compose version >nul 2>&1
if errorlevel 1 (
    where docker-compose >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Neither "docker compose" nor "docker-compose" found
        exit /b 1
    )
    set COMPOSE_CMD=docker-compose
) else (
    set COMPOSE_CMD=docker compose
)

echo [%TIME%] Using compose: %COMPOSE_CMD%

REM ---------------------------------------------------------------------------
REM 2. Create required directories
REM ---------------------------------------------------------------------------
if not exist "%SCRIPT_DIR%data\inbound"  mkdir "%SCRIPT_DIR%data\inbound"
if not exist "%SCRIPT_DIR%data\outbound" mkdir "%SCRIPT_DIR%data\outbound"
if not exist "%SCRIPT_DIR%data\error"    mkdir "%SCRIPT_DIR%data\error"
if not exist "%SCRIPT_DIR%drivers"       mkdir "%SCRIPT_DIR%drivers"
if not exist "%SCRIPT_DIR%sql"           mkdir "%SCRIPT_DIR%sql"
if not exist "%SCRIPT_DIR%monitoring"    mkdir "%SCRIPT_DIR%monitoring"
if not exist "%SCRIPT_DIR%schemas"       mkdir "%SCRIPT_DIR%schemas"

REM Generate minimal init.sql if missing
if not exist "%SCRIPT_DIR%sql\init.sql" (
    echo [%TIME%] Generating minimal sql\init.sql...
    (
        echo -- XClarity ETL lookup tables
        echo CREATE TABLE IF NOT EXISTS exchange_rates (
        echo     from_currency VARCHAR^(5^),
        echo     to_currency   VARCHAR^(5^),
        echo     rate_date     DATE,
        echo     exchange_rate NUMERIC^(12,4^),
        echo     PRIMARY KEY ^(from_currency, to_currency, rate_date^)
        echo ^);
        echo.
        echo CREATE TABLE IF NOT EXISTS product_categories (
        echo     category_id        VARCHAR^(10^) PRIMARY KEY,
        echo     category_name      VARCHAR^(50^),
        echo     parent_category_id VARCHAR^(10^),
        echo     level              INTEGER,
        echo     description        TEXT
        echo ^);
        echo.
        echo CREATE TABLE IF NOT EXISTS tgt_customer_scd2 (
        echo     customer_id    VARCHAR^(10^),
        echo     first_name     VARCHAR^(50^),
        echo     last_name      VARCHAR^(50^),
        echo     email          VARCHAR^(100^),
        echo     phone          VARCHAR^(20^),
        echo     status         VARCHAR^(10^),
        echo     effective_date TIMESTAMP,
        echo     end_date       TIMESTAMP,
        echo     is_current     CHAR^(1^),
        echo     PRIMARY KEY ^(customer_id, effective_date^)
        echo ^);
    ) > "%SCRIPT_DIR%sql\init.sql"
)

REM Generate minimal prometheus.yml if missing
if not exist "%SCRIPT_DIR%monitoring\grafana\provisioning" (
    mkdir "%SCRIPT_DIR%monitoring\grafana\provisioning"
)
if not exist "%SCRIPT_DIR%monitoring\prometheus.yml" (
    echo [%TIME%] Generating minimal monitoring\prometheus.yml...
    (
        echo global:
        echo   scrape_interval: 15s
        echo.
        echo scrape_configs:
        echo   - job_name: "nifi"
        echo     static_configs:
        echo       - targets:
        echo           - "nifi-1:9092"
        echo           - "nifi-2:9092"
        echo           - "nifi-3:9092"
    ) > "%SCRIPT_DIR%monitoring\prometheus.yml"
)

REM ---------------------------------------------------------------------------
REM 3. Start Docker Compose
REM ---------------------------------------------------------------------------
echo [%TIME%] Starting Docker Compose services...
pushd "%SCRIPT_DIR%"
%COMPOSE_CMD% up -d --remove-orphans
if errorlevel 1 (
    echo [ERROR] Docker Compose failed to start services
    exit /b 1
)
popd

REM ---------------------------------------------------------------------------
REM 4. Wait for NiFi to become ready
REM ---------------------------------------------------------------------------
echo [%TIME%] Waiting up to %NIFI_READY_TIMEOUT%s for NiFi at %NIFI_URL% ...

:WAIT_LOOP
curl -sf "%NIFI_URL%/nifi/" >nul 2>&1
if not errorlevel 1 goto NIFI_READY

if !ELAPSED! geq %NIFI_READY_TIMEOUT% (
    echo [ERROR] NiFi did not become ready within %NIFI_READY_TIMEOUT%s
    echo         Check: %COMPOSE_CMD% logs nifi-1
    exit /b 1
)
timeout /t 5 /nobreak >nul
set /a ELAPSED+=5
echo [%TIME%]   Still waiting... (%ELAPSED%s elapsed)
goto WAIT_LOOP

:NIFI_READY
echo [%TIME%] NiFi is ready!

REM ---------------------------------------------------------------------------
REM 5. Install Python dependencies
REM ---------------------------------------------------------------------------
echo [%TIME%] Installing Python dependencies from requirements.txt...
python -m pip install --quiet -r "%SCRIPT_DIR%requirements.txt"
if errorlevel 1 (
    echo [WARNING] pip install reported errors; continuing...
)

REM ---------------------------------------------------------------------------
REM 6. Run the migration
REM ---------------------------------------------------------------------------
echo [%TIME%] Invoking main.py (NIFI_URL=%NIFI_URL%, MODULES=%MODULES%)...
python "%SCRIPT_DIR%main.py" --nifi-url "%NIFI_URL%" --modules "%MODULES%" %*

if errorlevel 1 (
    echo [ERROR] main.py failed. Check migration_run.log for details.
    exit /b 1
)

echo [%TIME%] Migration deployment completed successfully.
endlocal
