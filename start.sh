#!/usr/bin/env bash
# start.sh — Start the XClarity ETL Informatica→NiFi migration pipeline (Linux/macOS)
# Usage: ./start.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔══════════════════════════════════════════════╗"
echo "║  XClarity ETL — start.sh                     ║"
echo "╚══════════════════════════════════════════════╝"

# ---------------------------------------------------------------------------
# 1. Pre-flight checks
# ---------------------------------------------------------------------------
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found in PATH."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v "docker compose" >/dev/null 2>&1 || \
    { echo "ERROR: docker-compose not found."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found."; exit 1; }

# ---------------------------------------------------------------------------
# 2. Create required data directories
# ---------------------------------------------------------------------------
mkdir -p \
    "${SCRIPT_DIR}/data/source/customer" \
    "${SCRIPT_DIR}/data/source/sales" \
    "${SCRIPT_DIR}/data/source/product" \
    "${SCRIPT_DIR}/data/source/finance" \
    "${SCRIPT_DIR}/data/source/hr" \
    "${SCRIPT_DIR}/data/source/lookup" \
    "${SCRIPT_DIR}/data/output/customer" \
    "${SCRIPT_DIR}/data/output/sales" \
    "${SCRIPT_DIR}/data/output/product" \
    "${SCRIPT_DIR}/data/output/finance" \
    "${SCRIPT_DIR}/data/output/hr" \
    "${SCRIPT_DIR}/schemas" \
    "${SCRIPT_DIR}/sql"

# ---------------------------------------------------------------------------
# 3. Start Docker services (NiFi, NiFi Registry, PostgreSQL)
# ---------------------------------------------------------------------------
echo ""
echo "[1/4] Starting Docker services…"
cd "${SCRIPT_DIR}"

# Use 'docker compose' (v2) or fall back to 'docker-compose' (v1)
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

${COMPOSE_CMD} up -d nifi nifi-registry postgres

# ---------------------------------------------------------------------------
# 4. Wait for NiFi to become healthy
# ---------------------------------------------------------------------------
echo ""
echo "[2/4] Waiting for NiFi to be healthy (up to 5 minutes)…"
MAX_WAIT=300
WAITED=0
INTERVAL=10

while true; do
    STATUS=$(${COMPOSE_CMD} ps --format json nifi 2>/dev/null | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('Health','unknown'))" 2>/dev/null || echo "unknown")
    if [ "${STATUS}" = "healthy" ]; then
        echo "NiFi is healthy."
        break
    fi
    if [ "${WAITED}" -ge "${MAX_WAIT}" ]; then
        echo "ERROR: NiFi did not become healthy within ${MAX_WAIT}s."
        ${COMPOSE_CMD} logs --tail=50 nifi
        exit 1
    fi
    echo "  Waiting for NiFi… (${WAITED}s elapsed, status=${STATUS})"
    sleep "${INTERVAL}"
    WAITED=$((WAITED + INTERVAL))
done

# ---------------------------------------------------------------------------
# 5. Install Python dependencies (inside a venv if possible)
# ---------------------------------------------------------------------------
echo ""
echo "[3/4] Installing Python dependencies…"
if [ ! -d "${SCRIPT_DIR}/.venv" ]; then
    python3 -m venv "${SCRIPT_DIR}/.venv"
fi
source "${SCRIPT_DIR}/.venv/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "${SCRIPT_DIR}/requirements.txt"

# ---------------------------------------------------------------------------
# 6. Run main.py
# ---------------------------------------------------------------------------
echo ""
echo "[4/4] Running main.py…"
cd "${SCRIPT_DIR}"
python3 main.py
EXIT_CODE=$?

echo ""
if [ "${EXIT_CODE}" -eq 0 ]; then
    echo "Migration pipeline completed successfully."
else
    echo "Migration pipeline finished with errors (exit code ${EXIT_CODE}). Check xclarity_etl.log."
fi

exit "${EXIT_CODE}"
