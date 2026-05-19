@echo off
REM start.bat — XClarity ETL: start Docker stack, then run migration pipeline
REM Usage: start.bat [--no-docker]

setlocal enabledelayedexpansion

SET SCRIPT_DIR=%~dp0
SET LOG_FILE=%SCRIPT_DIR%migration_%DATE:~-4,4%%DATE:~-10,2%%DATE:~-7,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log
SET LOG_FILE=%LOG_FILE: =0%

PUSHD "%SCRIPT_DIR%"

echo ============================================== >> "%LOG_FILE%"
echo  XClarity ETL Migration -- %DATE% %TIME% >> "%LOG_FILE%"
echo ============================================== >> "%LOG_FILE%"
echo  XClarity ETL Migration -- %DATE% %TIME%

REM ---- Load .env if present -----------------------------------------------
IF EXIST "%SCRIPT_DIR%.env" (
    echo [INFO] Loading .env
    FOR /F "usebackq tokens=1,* delims==" %%A IN ("%SCRIPT_DIR%.env") DO (
        SET "%%A=%%B"
    )
)

IF NOT DEFINED NIFI_HOST SET NIFI_HOST=https://localhost:8443

SET SKIP_DOCKER=%1

REM ---- Start Docker stack --------------------------------------------------
IF NOT "%SKIP_DOCKER%"=="--no-docker" (
    echo [INFO] Starting Docker Compose stack...

    WHERE docker >nul 2>&1
    IF ERRORLEVEL 1 (
        echo [ERROR] docker not found on PATH. Install Docker Desktop.
        EXIT /B 1
    )

    docker compose up -d --remove-orphans
    IF ERRORLEVEL 1 (
        echo [ERROR] docker compose up failed.
        EXIT /B 1
    )

    echo [INFO] Waiting for NiFi to become healthy (up to 3 minutes)...
    SET RETRIES=36
    :NIFI_WAIT
    curl -sk "%NIFI_HOST%/nifi-api/system-diagnostics" >nul 2>&1
    IF ERRORLEVEL 1 (
        SET /A RETRIES=!RETRIES!-1
        IF !RETRIES! LEQ 0 (
            echo [ERROR] NiFi did not become healthy. Check: docker compose logs nifi-1
            EXIT /B 1
        )
        echo [INFO] NiFi not ready yet, retrying in 5s... (!RETRIES! attempts left)
        TIMEOUT /T 5 /NOBREAK >nul
        GOTO NIFI_WAIT
    )
    echo [INFO] NiFi is healthy.
) ELSE (
    echo [INFO] --no-docker flag set: skipping Docker Compose start.
)

REM ---- Install Python dependencies -----------------------------------------
echo [INFO] Installing Python dependencies...
pip install --quiet -r "%SCRIPT_DIR%requirements.txt"
IF ERRORLEVEL 1 (
    echo [ERROR] pip install failed.
    EXIT /B 1
)

REM ---- Run migration pipeline ----------------------------------------------
echo [INFO] Running migration pipeline...
python "%SCRIPT_DIR%main.py"
IF ERRORLEVEL 1 (
    echo [ERROR] Migration pipeline failed. See log: %LOG_FILE%
    EXIT /B 1
)

echo ==============================================
echo  Migration complete -- %DATE% %TIME%
echo  Log: %LOG_FILE%
echo ==============================================

POPD
endlocal
