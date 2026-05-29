"""
main.py
XClarity ETL — Informatica → Apache NiFi migration runner.

Discovers and executes all src/*.py modules in dependency order:
  1. schemas    — write Avro schema files
  2. extract    — NiFi infrastructure setup (controller services, PGs, param contexts)
  3. finance    — PG_Finance (no upstream domain dependencies)
  4. product    — PG_Product
  5. customer   — PG_Customer
  6. sales      — PG_Sales
  7. hr_operations — PG_HR_Operations

Each module exposes a `run()` function that is invoked here.
Errors in individual modules are logged and do not stop subsequent modules.
"""

import importlib
import logging
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging setup (before any module imports that configure their own loggers)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("xclarity_etl.log", mode="a"),
    ],
)
logger = logging.getLogger("xclarity_etl.main")

# ---------------------------------------------------------------------------
# Module execution order
# Dependency order matches spec.md §7.4 (Finance first, HR_Ops last)
# ---------------------------------------------------------------------------
MODULE_ORDER = [
    "src.schemas",
    "src.extract",
    "src.finance",
    "src.product",
    "src.customer",
    "src.sales",
    "src.hr_operations",
]


def discover_modules() -> list[str]:
    """
    Discover all src/*.py module names.
    Returns them sorted in the required execution order;
    any extra modules found (not in MODULE_ORDER) are appended at the end.
    """
    src_dir = Path(__file__).parent / "src"
    found = {
        f"src.{p.stem}"
        for p in src_dir.glob("*.py")
        if p.stem != "__init__"
    }
    # Modules in MODULE_ORDER first (preserving order), then anything extra
    ordered = [m for m in MODULE_ORDER if m in found]
    extras = sorted(found - set(MODULE_ORDER))
    return ordered + extras


def run_module(module_name: str) -> bool:
    """
    Import and call `run()` on a single module.
    Returns True on success, False on error.
    """
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("Starting module: %s", module_name)
    start = time.monotonic()
    try:
        mod = importlib.import_module(module_name)
        if not hasattr(mod, "run"):
            logger.warning("Module '%s' has no run() function — skipped.", module_name)
            return True
        mod.run()
        elapsed = time.monotonic() - start
        logger.info("Module '%s' completed in %.1fs.", module_name, elapsed)
        return True
    except Exception as exc:
        elapsed = time.monotonic() - start
        logger.error(
            "Module '%s' FAILED after %.1fs: %s",
            module_name, elapsed, exc,
            exc_info=True,
        )
        return False


def main() -> int:
    """
    Main entry point.
    Runs all modules in order; returns 0 if all succeed, 1 if any fail.
    """
    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║  XClarity ETL — Informatica → NiFi Migration ║")
    logger.info("╚══════════════════════════════════════════════╝")

    modules = discover_modules()
    logger.info("Execution plan (%d modules): %s", len(modules), modules)

    results: dict[str, bool] = {}
    overall_start = time.monotonic()

    for module_name in modules:
        success = run_module(module_name)
        results[module_name] = success

    total_elapsed = time.monotonic() - overall_start
    failed = [m for m, ok in results.items() if not ok]
    succeeded = [m for m, ok in results.items() if ok]

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("Run complete in %.1fs.", total_elapsed)
    logger.info("Succeeded (%d): %s", len(succeeded), succeeded)

    if failed:
        logger.error("FAILED    (%d): %s", len(failed), failed)
        return 1

    logger.info("All modules completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
