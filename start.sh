#!/usr/bin/env bash
# =============================================================================
# start.sh – XClarity ETL NiFi Migration
# Starts Docker Compose services then invokes main.py
#
# Usage:
#   chmod +x start.sh
#   ./start.sh [--nifi-url http://localhost:8080] [--modules all]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NIFI_URL="${NIFI_URL:-http://localhost:8080}"
MODULES="${MODULES:-all}"
NIFI_READY_TIMEOUT=180  # seconds

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
die()  { log "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Pre-flight checks
# ---------------------------------------------------------------------------
command -v docker  >/dev/null 2>&1 || die "docker not found on PATH"
command -v python3 >/dev/null 2>&1 || die "python3 not found on PATH"

# Detect docker compose (v2 plugin or standalone)
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    die "Neither 'docker compose' nor 'docker-compose' found"
fi

log "Using compose command: $COMPOSE_CMD"

# ---------------------------------------------------------------------------
# 2. Create directories expected by docker-compose.yml
# ---------------------------------------------------------------------------
mkdir -p "$SCRIPT_DIR/data/inbound"
mkdir -p "$SCRIPT_DIR/data/outbound"
mkdir -p "$SCRIPT_DIR/data/error"
mkdir -p "$SCRIPT_DIR/drivers"
mkdir -p "$SCRIPT_DIR/sql"
mkdir -p "$SCRIPT_DIR/monitoring"
mkdir -p "$SCRIPT_DIR/schemas"

# Provide a minimal init.sql if not present
if [ ! -f "$SCRIPT_DIR/sql/init.sql" ]; then
    log "Generating minimal sql/init.sql..."
    cat > "$SCRIPT_DIR/sql/init.sql" <<'SQL'
-- XClarity ETL lookup tables
CREATE TABLE IF NOT EXISTS exchange_rates (
    from_currency VARCHAR(5),
    to_currency   VARCHAR(5),
    rate_date     DATE,
    exchange_rate NUMERIC(12,4),
    PRIMARY KEY (from_currency, to_currency, rate_date)
);

CREATE TABLE IF NOT EXISTS product_categories (
    category_id        VARCHAR(10) PRIMARY KEY,
    category_name      VARCHAR(50),
    parent_category_id VARCHAR(10),
    level              INTEGER,
    description        TEXT
);

CREATE TABLE IF NOT EXISTS tgt_customer_scd2 (
    customer_id    VARCHAR(10),
    first_name     VARCHAR(50),
    last_name      VARCHAR(50),
    email          VARCHAR(100),
    phone          VARCHAR(20),
    status         VARCHAR(10),
    effective_date TIMESTAMP,
    end_date       TIMESTAMP,
    is_current     CHAR(1),
    PRIMARY KEY (customer_id, effective_date)
);
SQL
fi

# Provide a minimal prometheus config if not present
mkdir -p "$SCRIPT_DIR/monitoring/grafana/provisioning"
if [ ! -f "$SCRIPT_DIR/monitoring/prometheus.yml" ]; then
    log "Generating minimal monitoring/prometheus.yml..."
    cat > "$SCRIPT_DIR/monitoring/prometheus.yml" <<'YAML'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "nifi"
    static_configs:
      - targets:
          - "nifi-1:9092"
          - "nifi-2:9092"
          - "nifi-3:9092"
YAML
fi

# ---------------------------------------------------------------------------
# 3. Start Docker Compose
# ---------------------------------------------------------------------------
log "Starting Docker Compose services..."
cd "$SCRIPT_DIR"
$COMPOSE_CMD up -d --remove-orphans

log "Waiting up to ${NIFI_READY_TIMEOUT}s for NiFi to be ready at $NIFI_URL ..."
ELAPSED=0
until curl -sf "$NIFI_URL/nifi/" >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$NIFI_READY_TIMEOUT" ]; then
        die "NiFi did not become ready within ${NIFI_READY_TIMEOUT}s. Check: docker compose logs nifi-1"
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    log "  Still waiting... (${ELAPSED}s elapsed)"
done
log "NiFi is ready!"

# ---------------------------------------------------------------------------
# 4. Install Python dependencies
# ---------------------------------------------------------------------------
log "Installing Python dependencies from requirements.txt..."
python3 -m pip install --quiet -r "$SCRIPT_DIR/requirements.txt" \
    || log "WARNING: pip install failed; continuing anyway"

# ---------------------------------------------------------------------------
# 5. Run the migration
# ---------------------------------------------------------------------------
log "Invoking main.py (NIFI_URL=$NIFI_URL, MODULES=$MODULES)..."
python3 "$SCRIPT_DIR/main.py" \
    --nifi-url "$NIFI_URL" \
    --modules  "$MODULES" \
    "$@"

EXIT_CODE=$?
if [ "$EXIT_CODE" -eq 0 ]; then
    log "Migration deployment completed successfully."
else
    log "ERROR: main.py exited with code $EXIT_CODE. Check migration_run.log for details."
fi

exit "$EXIT_CODE"
