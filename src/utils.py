"""
src/utils.py
Shared utilities: date functions, NiFi connection helpers, logging config.
Provides NiFi-compatible equivalents for Informatica date functions used
across all mappings (DATE_DIFF, TO_DATE, TO_CHAR), plus PII masking,
MD5 hashing, and nipyapi convenience wrappers.
"""

import hashlib
import logging
import os
import re
import time
import uuid
from datetime import datetime, date
from typing import Optional, Union

import nipyapi
from nipyapi import config as nifi_config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("xclarity_etl.utils")


# ---------------------------------------------------------------------------
# NiFi connection
# ---------------------------------------------------------------------------
NIFI_BASE_URL = os.environ.get("NIFI_BASE_URL", "https://localhost:8443/nifi-api")
NIFI_USERNAME = os.environ.get("NIFI_USERNAME", "admin")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD", "adminpassword123")
NIFI_VERIFY_SSL = os.environ.get("NIFI_VERIFY_SSL", "false").lower() == "true"


def connect_nifi(retries: int = 10, delay: float = 10.0) -> None:
    """Connect to NiFi and authenticate. Retries on failure."""
    nifi_config.nifi_config.host = NIFI_BASE_URL
    nifi_config.nifi_config.verify_ssl = NIFI_VERIFY_SSL

    for attempt in range(1, retries + 1):
        try:
            nipyapi.security.service_login(
                service="nifi",
                username=NIFI_USERNAME,
                password=NIFI_PASSWORD,
            )
            logger.info("Connected to NiFi at %s", NIFI_BASE_URL)
            return
        except Exception as exc:
            logger.warning(
                "NiFi connect attempt %d/%d failed: %s", attempt, retries, exc
            )
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to NiFi at {NIFI_BASE_URL} after {retries} attempts."
    )


def get_root_pg_id() -> str:
    """Return the root process-group ID."""
    root = nipyapi.canvas.get_root_pg_id()
    logger.debug("Root PG ID: %s", root)
    return root


def get_or_create_pg(name: str, parent_pg_id: Optional[str] = None) -> object:
    """
    Return the NiFi process group named *name* under *parent_pg_id*,
    creating it if it does not exist.
    """
    parent_id = parent_pg_id or get_root_pg_id()
    existing = nipyapi.canvas.get_process_group(name, greedy=False)
    if existing:
        logger.info("Process group '%s' already exists.", name)
        return existing
    pg = nipyapi.canvas.create_process_group(
        parent_pg=nipyapi.canvas.get_process_group(parent_id, greedy=False)
        or _pg_entity_from_id(parent_id),
        new_pg_name=name,
        location=(0, 0),
    )
    logger.info("Created process group '%s' (id=%s).", name, pg.id)
    return pg


def _pg_entity_from_id(pg_id: str) -> object:
    """Fetch a PG entity by raw ID string."""
    return nipyapi.canvas.get_process_group(pg_id, greedy=False)


def generate_batch_id() -> str:
    """Return a unique batch identifier for ETL run stamping."""
    return f"BATCH_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8].upper()}"


# ---------------------------------------------------------------------------
# Informatica date function equivalents
# ---------------------------------------------------------------------------

def date_diff_days(d1: Union[date, datetime, str], d2: Union[date, datetime, str]) -> int:
    """
    NiFi/Python equivalent of Informatica DATE_DIFF(d1, d2, 'D').
    Returns the integer number of days between d1 and d2 (d1 - d2).
    """
    d1 = _coerce_date(d1)
    d2 = _coerce_date(d2)
    return (d1 - d2).days


def date_diff_months(d1: Union[date, datetime, str], d2: Union[date, datetime, str]) -> int:
    """
    Informatica DATE_DIFF(d1, d2, 'MM') equivalent.
    Returns whole months between d1 and d2.
    """
    d1 = _coerce_date(d1)
    d2 = _coerce_date(d2)
    return (d1.year - d2.year) * 12 + (d1.month - d2.month)


def to_date(value: str, fmt: str = "%Y-%m-%d") -> date:
    """
    Informatica TO_DATE(str, fmt) equivalent.
    Parses *value* with *fmt* and returns a Python date.
    """
    try:
        return datetime.strptime(value.strip(), fmt).date()
    except ValueError as exc:
        logger.error("to_date failed for value=%r fmt=%r: %s", value, fmt, exc)
        raise


def to_char(dt: Union[date, datetime], fmt: str = "%Y-%m") -> str:
    """
    Informatica TO_CHAR(date, 'YYYY-MM') equivalent.
    Formats *dt* with Python strftime *fmt*.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.strftime(fmt)


def _coerce_date(value: Union[date, datetime, str]) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    # Try ISO format first, then fallback
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Cannot coerce to date: {value!r}")


# ---------------------------------------------------------------------------
# PII masking  (m_customer_privacy_mask)
# Reproduces Informatica SUBSTR/RPAD/INSTR/LENGTH expressions exactly.
# ---------------------------------------------------------------------------

def mask_first_name(first_name: str) -> str:
    """
    Mask first_name: show first 2 chars, replace remainder with '*'.
    Informatica: SUBSTR(first_name,1,2) || RPAD('*', LENGTH(first_name)-2, '*')
    """
    if not first_name:
        return first_name
    visible = first_name[:2]
    padding = "*" * max(0, len(first_name) - 2)
    return visible + padding


def mask_email(email: str) -> str:
    """
    Mask email: show first char + '***' + '@' + domain.
    Informatica: SUBSTR(email,1,1) || '***' || SUBSTR(email, INSTR(email,'@'))
    """
    if not email:
        return email
    at_pos = email.find("@")
    if at_pos < 0:
        return email[0:1] + "***"
    return email[0:1] + "***" + email[at_pos:]


def mask_phone(phone: str) -> str:
    """
    Mask phone: replace all but last 4 digits with '*'.
    Informatica: RPAD('*', LENGTH(phone)-4, '*') || SUBSTR(phone, LENGTH(phone)-3)
    """
    if not phone:
        return phone
    digits_only = re.sub(r"\D", "", phone)
    if len(digits_only) <= 4:
        return phone  # too short to mask meaningfully
    last_four = digits_only[-4:]
    return "*" * (len(digits_only) - 4) + last_four


# ---------------------------------------------------------------------------
# MD5 audit hashing  (m_finance_audit_trail)
# Reproduces Informatica MD5() over concatenated GL key fields.
# ---------------------------------------------------------------------------

def md5_audit_hash(entry_id: str, account_code: str, debit: str, credit: str) -> str:
    """
    Compute lowercase hex MD5 digest of concatenated key fields.
    Matches Informatica: MD5(entry_id || account_code || debit || credit)
    """
    raw = f"{entry_id}{account_code}{debit}{credit}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Validation helpers  (m_customer_validate)
# ---------------------------------------------------------------------------

# RFC-5321-style pattern matching Informatica REG_MATCH expression
EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)


def validate_email(email: str) -> bool:
    """Return True if email matches the expected RFC-like regex pattern."""
    if not email:
        return False
    return bool(EMAIL_REGEX.match(email.strip()))


def validate_phone(phone: str) -> bool:
    """
    Return True if stripping formatting characters yields >= 10 digits.
    Matches Informatica phone validation logic.
    """
    if not phone:
        return False
    digits = re.sub(r"\D", "", phone)
    return len(digits) >= 10


# ---------------------------------------------------------------------------
# Segmentation / classification helpers
# ---------------------------------------------------------------------------

def customer_segment(total_spend: float) -> str:
    """platinum >=400, gold >=200, silver >=100, bronze otherwise."""
    if total_spend >= 400:
        return "platinum"
    if total_spend >= 200:
        return "gold"
    if total_spend >= 100:
        return "silver"
    return "bronze"


def churn_risk(last_transaction_date: Optional[Union[date, str]], status: str) -> str:
    """high if last_transaction >90 days ago or null; medium if inactive; else low."""
    if last_transaction_date is None:
        return "high"
    try:
        days_ago = date_diff_days(date.today(), _coerce_date(last_transaction_date))
    except ValueError:
        return "high"
    if days_ago > 90:
        return "high"
    if status and status.lower() == "inactive":
        return "medium"
    return "low"


def tax_rate_for_region(region: str) -> float:
    """Regional tax rates matching Informatica IIF cascade."""
    mapping = {
        "Northeast": 0.08,
        "West": 0.0725,
        "Midwest": 0.065,
    }
    return mapping.get(region, 0.07)


def expense_category(account_code: int) -> str:
    """DECODE account_code to expense category (account codes 6100-6500)."""
    decode = {
        6100: "Payroll",
        6200: "Facilities",
        6300: "Utilities",
        6400: "Marketing",
        6500: "Technology",
    }
    return decode.get(account_code, "Other")


def lifecycle_stage(launch_date: Union[date, str], status: str) -> str:
    """
    introduction <6m, growth 6-18m, maturity >18m, end_of_life if discontinued.
    """
    if status and status.lower() == "discontinued":
        return "end_of_life"
    try:
        months = date_diff_months(date.today(), _coerce_date(launch_date))
    except ValueError:
        return "unknown"
    if months < 6:
        return "introduction"
    if months < 18:
        return "growth"
    return "maturity"


def review_sentiment(avg_rating: float) -> str:
    """positive >=4.0, neutral >=3.0, negative <3.0."""
    if avg_rating >= 4.0:
        return "positive"
    if avg_rating >= 3.0:
        return "neutral"
    return "negative"


def stock_alert(quantity_on_hand: int, reorder_level: int) -> str:
    """critical <=50% of reorder_level, low <=reorder_level, ok otherwise."""
    if quantity_on_hand <= reorder_level * 0.5:
        return "critical"
    if quantity_on_hand <= reorder_level:
        return "low"
    return "ok"


def budget_variance_status(variance_pct: float) -> str:
    """under_budget <-5%, on_budget within ±5%, over_budget >+5%."""
    if variance_pct < -5.0:
        return "under_budget"
    if variance_pct > 5.0:
        return "over_budget"
    return "on_budget"


def performance_tier(weighted_score: float) -> str:
    """exceptional >=4.5, strong >=3.8, meets >=3.0, below."""
    if weighted_score >= 4.5:
        return "exceptional"
    if weighted_score >= 3.8:
        return "strong"
    if weighted_score >= 3.0:
        return "meets"
    return "below"


def turnover_risk(status: str, weighted_score: float, tenure_months: int) -> str:
    """high if terminated; medium if low score or <12m tenure; else low."""
    if status and status.lower() == "terminated":
        return "high"
    if weighted_score < 3.0 or tenure_months < 12:
        return "medium"
    return "low"


def quality_grade(pass_rate: float) -> str:
    """A >=99.5, B >=98, C >=95, F otherwise."""
    if pass_rate >= 99.5:
        return "A"
    if pass_rate >= 98.0:
        return "B"
    if pass_rate >= 95.0:
        return "C"
    return "F"


def vendor_tier(composite_score: float) -> str:
    """preferred >=80, approved >=60, probationary otherwise."""
    if composite_score >= 80:
        return "preferred"
    if composite_score >= 60:
        return "approved"
    return "probationary"
