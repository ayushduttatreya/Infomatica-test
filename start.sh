#!/bin/bash

# start.sh - Linux/Mac startup script for info-to-nifi project

set -e

echo "=========================================="
echo "info-to-nifi Pipeline Startup Script"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    echo "Please install Docker from https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    echo "Please install Docker Compose from https://docs.docker.com/compose/install/"
    exit 1
fi

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    echo "Please install Python 3 from https://www.python.org/downloads/"
    exit 1
fi

echo -e "${GREEN}All prerequisites are installed${NC}"
echo ""

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p data templates logs

# Start Docker Compose services
echo ""
echo "Starting Docker Compose services..."
echo "This may take a few minutes on first run..."

if docker compose version &> /dev/null; then
    docker compose up -d
else
    docker-compose up -d
fi

# Wait for services to be healthy
echo ""
echo "Waiting for services to be ready..."
echo "NiFi UI will be available at: http://localhost:8080/nifi"
echo "Username: admin"
echo "Password: ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
echo ""

# Wait for NiFi to be ready
MAX_WAIT=180
WAIT_TIME=0
while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    if curl -s -f http://localhost:8080/nifi > /dev/null 2>&1; then
        echo -e "${GREEN}NiFi is ready!${NC}"
        break
    fi
    echo "Waiting for NiFi to start... ($WAIT_TIME/$MAX_WAIT seconds)"
    sleep 10
    WAIT_TIME=$((WAIT_TIME + 10))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    echo -e "${YELLOW}Warning: NiFi did not start within expected time${NC}"
    echo "You can check the status with: docker logs nifi"
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Install/upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo ""
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Run the pipeline
echo ""
echo "=========================================="
echo "Starting Pipeline Execution"
echo "=========================================="
echo ""

python3 main.py

# Capture exit code
EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Pipeline completed successfully${NC}"
else
    echo -e "${RED}Pipeline completed with errors (exit code: $EXIT_CODE)${NC}"
fi
echo "=========================================="
echo ""
echo "Docker services are still running."
echo "To stop services, run: docker-compose down"
echo "To view NiFi UI, visit: http://localhost:8080/nifi"
echo "To view logs, run: docker logs nifi"
echo ""

exit $EXIT_CODE