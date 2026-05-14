@echo off
REM start.bat - Windows startup script for info-to-nifi project

echo ==========================================
echo info-to-nifi Pipeline Startup Script
echo ==========================================
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not installed
    echo Please install Docker Desktop from https://docs.docker.com/desktop/install/windows-install/
    pause
    exit /b 1
)

REM Check if Docker Compose is installed
docker-compose --version >nul 2>&1
if errorlevel 1 (
    docker compose version >nul 2>&1
    if errorlevel 1 (
        echo Error: Docker Compose is not installed
        echo Please install Docker Compose from https://docs.docker.com/compose/install/
        pause
        exit /b 1
    )
)

REM Check if Python 3 is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python 3 is not installed
    echo Please install Python 3 from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo All prerequisites are installed
echo.

REM Create necessary directories
echo Creating necessary directories...
if not exist "data" mkdir data
if not exist "templates" mkdir templates
if not exist "logs" mkdir logs

REM Start Docker Compose services
echo.
echo Starting Docker Compose services...
echo This may take a few minutes on first run...
echo.

docker compose version >nul 2>&1
if errorlevel 1 (
    docker-compose up -d
) else (
    docker compose up -d
)

if errorlevel 1 (
    echo Error: Failed to start Docker services
    pause
    exit /b 1
)

REM Wait for services to be ready
echo.
echo Waiting for services to be ready...
echo NiFi UI will be available at: http://localhost:8080/nifi
echo Username: admin
echo Password: ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB
echo.

REM Wait for NiFi to be ready
set MAX_WAIT=180
set WAIT_TIME=0

:wait_loop
if %WAIT_TIME% GEQ %MAX_WAIT% goto wait_timeout

curl -s -f http://localhost:8080/nifi >nul 2>&1
if not errorlevel 1 (
    echo NiFi is ready!
    goto nifi_ready
)

echo Waiting for NiFi to start... (%WAIT_TIME%/%MAX_WAIT% seconds^)
timeout /t 10 /nobreak >nul
set /a WAIT_TIME=%WAIT_TIME%+10
goto wait_loop

:wait_timeout
echo Warning: NiFi did not start within expected time
echo You can check the status with: docker logs nifi
goto continue_setup

:nifi_ready
echo.

:continue_setup
REM Check if virtual environment exists
if not exist "venv" (
    echo.
    echo Creating Python virtual environment...
    python -m venv venv
)

REM Activate virtual environment
echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Install/upgrade pip
echo.
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo.
echo Installing Python dependencies...
pip install -r requirements.txt

if errorlevel 1 (
    echo Error: Failed to install Python dependencies
    pause
    exit /b 1
)

REM Run the pipeline
echo.
echo ==========================================
echo Starting Pipeline Execution
echo ==========================================
echo.

python main.py

REM Capture exit code
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ==========================================
if %EXIT_CODE% EQU 0 (
    echo Pipeline completed successfully
) else (
    echo Pipeline completed with errors ^(exit code: %EXIT_CODE%^)
)
echo ==========================================
echo.
echo Docker services are still running.
echo To stop services, run: docker-compose down
echo To view NiFi UI, visit: http://localhost:8080/nifi
echo To view logs, run: docker logs nifi
echo.

pause
exit /b %EXIT_CODE%