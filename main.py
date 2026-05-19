"""
main.py – XClarity ETL NiFi Migration Runner
Discovers and runs all src/*.py modules in dependency order.

Deployment order (per spec.md Phase plan):
  Phase 0 (Infrastructure):
    1. controller_services  – shared CS: CSV readers/writers, JDBC, Redis, schema registry
    2. schemas              – register all 27+ Avro source/target schemas

  Phase 1 (Foundation):
    3. hr         – HR module (simple, standalone)
    4. operations – Operations module (simple, depends on Sales + Product in data, but deployable early)

  Phase 2 (Core Business Logic):
    5. finance    – Finance module (standalone, complex)
    6. sales      – Sales module (depends on Customer)

  Phase 3 (Complex Transforms):
    7. customer   – Customer module (SCD2, PII masking, churn)
    8. product    – Product module (hierarchy, lifecycle, recommendations)

Usage:
  python main.py [--nifi-url http://localhost:8080] [--modules all|hr,sales,...]
"""

import argparse
import importlib
import logging
import os
import sys
import time
from typing import List

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("migration_run.log", encoding="utf-8"),
    ],
)

# ---------------------------------------------------------------------------
# Module deployment order (dependency-aware)
# ---------------------------------------------------------------------------

DEPLOYMENT_ORDER: List[str] = [
    "controller_services",  # shared NiFi controller services first
    "schemas",              # Avro schema registration
    "hr",                   # Phase 1: simple standalone
    "operations",           # Phase 1: simple, reads Sales/Product data only
    "finance",              # Phase 2: complex, standalone
    "sales",                # Phase 2: depends on Customer data
    "customer",             # Phase 3: complex SCD2, PII
    "product",              # Phase 3: complex hierarchy, lifecycle
]

SRC_DIR = os.path.join(os.path.dirname(__file__), "src")


def discover_modules() -> List[str]:
    """Return all module names found in src/ that match the deployment order list."""
    available = []
    for name in DEPLOYMENT_ORDER:
        module_path = os.path.join(SRC_DIR, f"{name}.py")
        if os.path.isfile(module_path):
            available.append(name)
        else:
            logger.warning("Module file not found, skipping: src/%s.py", name)
    return available


def run_module(module_name: str, nifi_url: str) -> bool:
    """
    Import and execute the run() function from src/<module_name>.py.

    Returns True on success, False on error.
    """
    logger.info("=" * 60)
    logger.info("Running module: %s", module_name)
    logger.info("=" * 60)

    start = time.time()
    try:
        # Add src/ to the Python path for imports
        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        mod = importlib.import_module(module_name)
        run_fn = getattr(mod, "run", None)

        if run_fn is None:
            logger.error("Module %s has no run() function – skipping", module_name)
            return False

        run_fn(nifi_url=nifi_url)
        elapsed = time.time() - start
        logger.info("Module %s completed successfully in %.1fs", module_name, elapsed)
        return True

    except Exception as exc:
        elapsed = time.time() - start
        logger.error(
            "Module %s FAILED after %.1fs: %s",
            module_name, elapsed, exc,
            exc_info=True,
        )
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="XClarity ETL – NiFi migration runner. Deploys all domain Process Groups."
    )
    parser.add_argument(
        "--nifi-url",
        default=os.environ.get("NIFI_URL", "http://localhost:8080"),
        help="NiFi base URL (default: http://localhost:8080 or $NIFI_URL)",
    )
    parser.add_argument(
        "--modules",
        default="all",
        help=(
            "Comma-separated list of modules to run, or 'all' for full deployment. "
            f"Available: {', '.join(DEPLOYMENT_ORDER)}"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List modules that would be deployed without executing them",
    )
    args = parser.parse_args()

    nifi_url: str = args.nifi_url.rstrip("/")
    dry_run: bool = args.dry_run

    # Determine which modules to run
    if args.modules.strip().lower() == "all":
        modules_to_run = discover_modules()
    else:
        requested = [m.strip() for m in args.modules.split(",") if m.strip()]
        # Preserve dependency order for requested subset
        modules_to_run = [m for m in DEPLOYMENT_ORDER if m in requested]
        unknown = set(requested) - set(DEPLOYMENT_ORDER)
        if unknown:
            logger.warning("Unknown modules (ignored): %s", ", ".join(sorted(unknown)))

    if not modules_to_run:
        logger.error("No valid modules selected for deployment.")
        return 1

    logger.info("NiFi URL     : %s", nifi_url)
    logger.info("Dry run      : %s", dry_run)
    logger.info("Modules (%d) : %s", len(modules_to_run), ", ".join(modules_to_run))

    if dry_run:
        logger.info("Dry run mode – no changes applied.")
        return 0

    # Run modules in order
    results = {}
    for module_name in modules_to_run:
        success = run_module(module_name, nifi_url=nifi_url)
        results[module_name] = success

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("DEPLOYMENT SUMMARY")
    logger.info("=" * 60)
    passed = [m for m, ok in results.items() if ok]
    failed = [m for m, ok in results.items() if not ok]

    for m in passed:
        logger.info("  [OK]   %s", m)
    for m in failed:
        logger.error("  [FAIL] %s", m)

    logger.info("")
    logger.info("Passed: %d / %d", len(passed), len(results))

    if failed:
        logger.error("Failed modules: %s", ", ".join(failed))
        return 1

    logger.info("All modules deployed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
