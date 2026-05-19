"""
main.py — XClarity ETL migration runner.

Discovers and executes all src/*.py modules in dependency order:

  Phase 0 (Infrastructure):
    controller_services  — register NiFi Controller Services + Redis pre-load
    schemas              — register all Avro schemas

  Phase 1 (Foundation flows — HR + Operations first):
    hr
    operations

  Phase 2 (Core business logic):
    finance
    sales

  Phase 3 (Complex transforms):
    customer
    product

Each module must expose a `run()` function.
Errors in any module are logged and re-raised to halt the pipeline.
"""

import importlib
import logging
import sys
import time
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

# Load .env file if present (NIFI_HOST, REDIS_HOST, JDBC_URL, etc.)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Execution order following spec.md §8 Timeline + §3 Process Group Dependencies
# ---------------------------------------------------------------------------
EXECUTION_ORDER: list[str] = [
    # Phase 0: shared infrastructure
    "controller_services",
    "schemas",
    # Phase 1: simple, standalone domains
    "hr",
    "operations",
    # Phase 2: core business logic
    "finance",
    "sales",
    # Phase 3: complex transforms (depend on Customer → Sales → Product)
    "customer",
    "product",
]


def _load_module(module_name: str) -> Callable[[], None]:
    """Import src/<module_name>.py and return its run() callable."""
    # Ensure src/ is on the path
    src_path = Path(__file__).parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    logger.info("Importing module: %s", module_name)
    mod = importlib.import_module(module_name)
    if not hasattr(mod, "run"):
        raise AttributeError(f"Module 'src/{module_name}.py' does not expose a run() function.")
    return mod.run


def discover_modules() -> list[str]:
    """
    Return the ordered list of src/*.py module names (excluding __init__).
    Modules not in EXECUTION_ORDER are appended at the end in alphabetical order.
    """
    src_path = Path(__file__).parent / "src"
    all_modules = sorted(
        p.stem for p in src_path.glob("*.py")
        if p.stem != "__init__" and not p.stem.startswith("_")
    )
    ordered = [m for m in EXECUTION_ORDER if m in all_modules]
    extra = [m for m in all_modules if m not in EXECUTION_ORDER]
    return ordered + extra


def run_all() -> None:
    """Discover and run all src modules in dependency order."""
    modules = discover_modules()
    logger.info("Discovered %d modules: %s", len(modules), modules)

    results: dict[str, str] = {}
    overall_start = time.monotonic()

    for module_name in modules:
        t0 = time.monotonic()
        logger.info("=" * 60)
        logger.info("Starting module: %s", module_name)
        try:
            run_fn = _load_module(module_name)
            run_fn()
            elapsed = time.monotonic() - t0
            results[module_name] = f"OK ({elapsed:.1f}s)"
            logger.info("Finished module: %s  [%.1fs]", module_name, elapsed)
        except Exception as exc:
            elapsed = time.monotonic() - t0
            results[module_name] = f"FAILED ({elapsed:.1f}s): {exc}"
            logger.error(
                "Module %s FAILED after %.1fs: %s",
                module_name, elapsed, exc,
                exc_info=True,
            )
            # Halt on failure — downstream modules depend on upstream completion
            _print_summary(results, time.monotonic() - overall_start)
            sys.exit(1)

    _print_summary(results, time.monotonic() - overall_start)


def _print_summary(results: dict[str, str], total_seconds: float) -> None:
    logger.info("=" * 60)
    logger.info("EXECUTION SUMMARY  (total: %.1fs)", total_seconds)
    logger.info("=" * 60)
    for module, status in results.items():
        logger.info("  %-30s  %s", module, status)
    logger.info("=" * 60)


if __name__ == "__main__":
    run_all()
