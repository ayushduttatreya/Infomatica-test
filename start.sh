#!/usr/bin/env bash
# start.sh — XClarity ETL: start Docker stack, then run migration pipeline
# Usage: ./start.sh [--no-docker]  (--no-docker skips docker-compose if already running)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="$SCRIPT_DIR/migration_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=============================================="
echo " XClarity ETL Migration — $(date)"
echo "=============================================="

# ---- Load environment --------------------------------------------------------
if [ -f "$SCRIPT_DIR/.env" ]; then
    echo "[INFO] Loading .env"
    # shellcheck disable=SC2046
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

NIFI_HOST="${NIFI_HOST:-https://localhost:8443}"
SKIP_DOCKER="${1:-}"

# ---- Start Docker stack -------------------------------------------------------
if [ "$SKIP_DOCKER" != "--no-docker" ]; then
    echo "[INFO] Starting Docker Compose stack..."

    if ! command -v docker &>/dev/null; then
        echo "[ERROR] docker not found on PATH. Install Docker Desktop or Docker Engine."
        exit 1
    fi

    docker compose up -d --remove-orphans

    echo "[INFO] Waiting for NiFi to become healthy (up to 3 minutes)..."
    RETRIES=36  # 36 x 5s = 3 min
    until curl -sk "$NIFI_HOST/nifi-api/system-diagnostics" > /dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "[ERROR] NiFi did not become healthy in time. Check: docker compose logs nifi-1"
            exit 1
        fi
        echo "[INFO] NiFi not ready yet, retrying in 5s... ($RETRIES attempts left)"
        sleep 5
    done
    echo "[INFO] NiFi is healthy."
else
    echo "[INFO] --no-docker flag set: skipping Docker Compose start."
fi

# ---- Install Python dependencies ----------------------------------------------
echo "[INFO] Installing Python dependencies..."
pip install --quiet -r "$SCRIPT_DIR/requirements.txt"

# ---- Run migration pipeline --------------------------------------------------
echo "[INFO] Running migration pipeline..."
python "$SCRIPT_DIR/main.py"

echo "=============================================="
echo " Migration complete — $(date)"
echo " Log: $LOG_FILE"
echo "=============================================="
