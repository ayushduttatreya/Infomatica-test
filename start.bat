@echo off
REM start.bat — Start the XClarity ETL Informatica→NiFi migration pipeline (Windows)
REM Usage: start.bat

setlocal EnableDelayedExpansion

echo ╔══════════════════════════════════════════════╗
echo ║  XClarity ETL — start.bat                    ║
echo ╚══════════════════════════════════════════════╝

REM ---------------------------------------------------------------------------
REM 1. Pre-flight checks
REM ---------------------------------------------------------------------------
where docker >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: docker not found in PATH.
    exit /b 1
)

where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: python not found in PATH.
    exit /b 1
)

REM ---------------------------------------------------------------------------
REM 2. Create required data directories
REM ---------------------------------------------------------------------------
set SCRIPT_DIR=%~dp0

for %%D in (
    "data\source\customer"
    "data\source\sales"
    "data\source\product"
    "data\source\finance"
    "data\source\hr"
    "data\source\lookup"
    "data\output\customer"
    "data\output\sales"
    "data\output\product"
    "data\output\finance"
    "data\output\hr"
    "schemas"
    "sql"
) do (
    if not exist "%SCRIPT_DIR%%%D" mkdir "%SCRIPT_DIR%%%D"
)

REM ---------------------------------------------------------------------------
REM 3. Start Docker services
REM ---------------------------------------------------------------------------
echo.
echo [1/4] Starting Docker services...
cd /d "%SCRIPT_DIR%"

docker compose version >nul 2>&1
if %ERRORLEVEL% equ 0 (
    set COMPOSE_CMD=docker compose
) else (
    set COMPOSE_CMD=docker-compose
)

%COMPOSE_CMD% up -d nifi nifi-registry postgres
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to start Docker services.
    exit /b 1
)

REM ---------------------------------------------------------------------------
REM 4. Wait for NiFi (simple polling loop, up to 5 minutes)
REM ---------------------------------------------------------------------------
echo.
echo [2/4] Waiting for NiFi to be healthy (up to 5 minutes)...
set /a MAX_WAIT=300
set /a WAITED=0
set /a INTERVAL=10

:WAIT_LOOP
if %WAITED% geq %MAX_WAIT% (
    echo ERROR: NiFi did not become healthy within %MAX_WAIT%s.
    %COMPOSE_CMD% logs --tail=50 nifi
    exit /b 1
)

REM Check NiFi HTTPS endpoint
curl -k -s -o nul -w "%%{http_code}" https://localhost:8443/nifi > "%TEMP%\nifi_status.txt" 2>nul
set /p HTTP_CODE=<"%TEMP%\nifi_status.txt"
if "%HTTP_CODE%"=="200" goto NIFI_READY
if "%HTTP_CODE%"=="302" goto NIFI_READY

echo   Waiting for NiFi... (%WAITED%s elapsed)
timeout /t %INTERVAL% /nobreak >nul
set /a WAITED=WAITED+INTERVAL
goto WAIT_LOOP

:NIFI_READY
echo NiFi is responding (HTTP %HTTP_CODE%).

REM ---------------------------------------------------------------------------
REM 5. Install Python dependencies
REM ---------------------------------------------------------------------------
echo.
echo [3/4] Installing Python dependencies...

if not exist "%SCRIPT_DIR%.venv" (
    python -m venv "%SCRIPT_DIR%.venv"
)
call "%SCRIPT_DIR%.venv\Scripts\activate.bat"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r "%SCRIPT_DIR%requirements.txt"
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to install Python dependencies.
    exit /b 1
)

REM ---------------------------------------------------------------------------
REM 6. Run main.py
REM ---------------------------------------------------------------------------
echo.
echo [4/4] Running main.py...
cd /d "%SCRIPT_DIR%"
python main.py
set EXIT_CODE=%ERRORLEVEL%

echo.
if %EXIT_CODE% equ 0 (
    echo Migration pipeline completed successfully.
) else (
    echo Migration pipeline finished with errors ^(exit code %EXIT_CODE%^). Check xclarity_etl.log.
)

endlocal
exit /b %EXIT_CODE%
