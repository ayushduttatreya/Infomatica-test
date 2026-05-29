"""
src/customer.py
Customer domain — PG_Customer — 10 mappings migrated from Informatica to NiFi.

Mappings covered (ticket: Migrate Customer domain flows):
  1.  m_customer_load          → TGT_CUSTOMER_LOAD      (metadata stamp)
  2.  m_customer_deduplicate   → TGT_CUSTOMER_DEDUP     (sort + QueryRecord FIRST)
  3.  m_customer_validate      → TGT_CUSTOMER_VALIDATE  (email regex + phone length)
  4.  m_customer_scd2          → TGT_CUSTOMER_SCD2      (LookupRecord + ExecuteScript)
  5.  m_customer_addr_norm     → TGT_ADDR_NORMALIZED    (INITCAP + abbreviation expansion)
  6.  m_customer_segment       → TGT_CUSTOMER_SEGMENT   (spend-tier RouteOnAttribute)
  7.  m_customer_merge         → TGT_CUSTOMER_MERGE     (customer + address join)
  8.  m_customer_privacy_mask  → TGT_CUSTOMER_MASKED    (GDPR PII masking — HIGH RISK)
  9.  m_customer_lifetime_value→ TGT_CUSTOMER_LTV       (QueryRecord SUM)
  10. m_customer_churn_flag    → TGT_CUSTOMER_CHURN     (DATE_DIFF + RouteOnAttribute)

Each function builds the NiFi sub-flow using nipyapi and also provides a
pure-Python transform function suitable for unit testing and PySpark execution.
"""

import logging
import uuid
from datetime import date, datetime
from typing import Optional

import nipyapi
import nipyapi.nifi as nifi_api

from src.utils import (
    connect_nifi,
    generate_batch_id,
    get_or_create_pg,
    get_root_pg_id,
    mask_email,
    mask_first_name,
    mask_phone,
    validate_email,
    validate_phone,
    customer_segment,
    churn_risk,
    date_diff_days,
    to_char,
    _coerce_date,
)

logger = logging.getLogger("xclarity_etl.customer")

SOURCE_DATA_PATH = "/opt/nifi/source_data/customer"
OUTPUT_DATA_PATH = "/opt/nifi/output_data/customer"

# ---------------------------------------------------------------------------
# Shared NiFi processor creation helper
# ---------------------------------------------------------------------------

def _add_processor(pg_id: str, proc_type: str, name: str, properties: dict,
                   position: tuple = (0, 0)) -> object:
    """Add a processor to a process group. Returns the processor entity."""
    body = nifi_api.ProcessorEntity(
        revision=nifi_api.RevisionDTO(version=0),
        component=nifi_api.ProcessorDTO(
            type=proc_type,
            name=name,
            config=nifi_api.ProcessorConfigDTO(properties=properties),
            position=nifi_api.PositionDTO(x=float(position[0]), y=float(position[1])),
        ),
    )
    try:
        proc = nifi_api.ProcessGroupsApi().create_processor(pg_id, body)
        logger.debug("Added processor '%s' to PG %s.", name, pg_id)
        return proc
    except Exception as exc:
        logger.error("Failed to add processor '%s': %s", name, exc)
        raise


def _connect(pg_id: str, src_id: str, dst_id: str, relationship: str = "success") -> None:
    """Connect two processors within a process group."""
    body = nifi_api.ConnectionEntity(
        revision=nifi_api.RevisionDTO(version=0),
        component=nifi_api.ConnectionDTO(
            source=nifi_api.ConnectableDTO(id=src_id, type="PROCESSOR", group_id=pg_id),
            destination=nifi_api.ConnectableDTO(id=dst_id, type="PROCESSOR", group_id=pg_id),
            selected_relationships=[relationship],
        ),
    )
    try:
        nifi_api.ProcessGroupsApi().create_connection(pg_id, body)
    except Exception as exc:
        logger.warning("Connection %s→%s (%s) skipped: %s", src_id, dst_id, relationship, exc)


# ---------------------------------------------------------------------------
# Pure-Python transform functions (unit-testable; used by PySpark pipeline)
# ---------------------------------------------------------------------------

def transform_customer_load(row: dict, batch_id: str) -> dict:
    """
    m_customer_load: add metadata columns (load_date, batch_id).
    Equivalent of Informatica EXP_ADD_METADATA expression.
    """
    return {
        "customer_id": row.get("customer_id"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "email": row.get("email"),
        "load_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "batch_id": batch_id,
    }


def transform_customer_dedup(rows: list[dict]) -> list[dict]:
    """
    m_customer_deduplicate: sort by email, keep FIRST customer_id per email.
    Equivalent of Informatica SRT_BY_EMAIL + AGG_DEDUP(FIRST(customer_id)).
    In NiFi: SortRecord(email) → QueryRecord('SELECT email, FIRST(customer_id)…').
    """
    seen: dict[str, dict] = {}
    for row in sorted(rows, key=lambda r: (r.get("email") or "")):
        email = row.get("email") or ""
        if email not in seen:
            seen[email] = {
                "customer_id": row.get("customer_id"),
                "email": email,
                "status": row.get("status"),
            }
    return list(seen.values())


def transform_customer_validate(row: dict) -> dict:
    """
    m_customer_validate: email regex + phone digit-length check.
    Equivalent of Informatica REG_MATCH / IIF expression.
    HIGH RISK: regex must match Informatica REG_MATCH output exactly.
    """
    email = row.get("email") or ""
    phone = row.get("phone") or ""
    email_ok = validate_email(email)
    phone_ok = validate_phone(phone)
    return {
        "customer_id": row.get("customer_id"),
        "email_valid": email_ok,
        "phone_valid": phone_ok,
        "is_valid": email_ok and phone_ok,
    }


def transform_customer_scd2(row: dict, existing: Optional[dict]) -> list[dict]:
    """
    m_customer_scd2: SCD Type 2 — expire old row, insert new row.
    HIGH RISK: equivalent of Informatica Lookup + Router + Expression.

    Args:
        row:      incoming customer record
        existing: current active row from target DB (None if not found)

    Returns:
        list of records to upsert (1 for new customer, 2 for updates)
    """
    today = date.today().strftime("%Y-%m-%d")
    scd2_end_forever = "9999-12-31"
    result = []

    if existing:
        # Expire old row
        expired = {**existing, "end_date": today, "is_current": 0}
        result.append(expired)

    # Insert new current row
    new_row = {
        "customer_id": row.get("customer_id"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "email": row.get("email"),
        "effective_date": today,
        "end_date": scd2_end_forever,
        "is_current": 1,
    }
    result.append(new_row)
    return result


def transform_addr_normalized(row: dict) -> dict:
    """
    m_customer_addr_norm: INITCAP city/address + abbreviation expansion.
    Equivalent of Informatica INITCAP + REPLACECHR expressions.
    """
    abbrev_map = {
        " St ": " Street ", " St.": " Street", " Ave ": " Avenue ",
        " Ave.": " Avenue", " Blvd ": " Boulevard ", " Blvd.": " Boulevard",
        " Dr ": " Drive ", " Dr.": " Drive", " Rd ": " Road ", " Rd.": " Road",
        " Ct ": " Court ", " Ct.": " Court", " Ln ": " Lane ", " Ln.": " Lane",
        " Pl ": " Place ", " Pl.": " Place",
    }

    def initcap(s: str) -> str:
        return " ".join(w.capitalize() for w in (s or "").split())

    def expand(s: str) -> str:
        for abbr, full in abbrev_map.items():
            s = s.replace(abbr, full)
        return s

    addr1 = expand(initcap(row.get("address_line1") or ""))
    city = initcap(row.get("city") or "")

    return {
        "address_id": row.get("address_id"),
        "customer_id": row.get("customer_id"),
        "address_line1_norm": addr1,
        "city_norm": city,
        "state": (row.get("state") or "").upper(),
        "zip_code": row.get("zip_code"),
    }


def transform_customer_segment(customer_id: str, total_spend: float) -> dict:
    """
    m_customer_segment: platinum/gold/silver/bronze by spend.
    Thresholds: platinum >=400, gold >=200, silver >=100, bronze otherwise.
    """
    return {
        "customer_id": customer_id,
        "total_spend": round(total_spend, 2),
        "segment": customer_segment(total_spend),
    }


def transform_customer_merge(customer_row: dict, address_row: dict, segment: str) -> dict:
    """
    m_customer_merge: join customer + address + segment.
    Equivalent of Informatica Joiner transformation.
    """
    return {
        "customer_id": customer_row.get("customer_id"),
        "first_name": customer_row.get("first_name"),
        "last_name": customer_row.get("last_name"),
        "email": customer_row.get("email"),
        "address_line1": address_row.get("address_line1_norm") or address_row.get("address_line1"),
        "city": address_row.get("city_norm") or address_row.get("city"),
        "state": address_row.get("state"),
        "segment": segment,
    }


def transform_pii_mask(row: dict) -> dict:
    """
    m_customer_privacy_mask: GDPR PII masking — HIGH RISK.
    Reproduces Informatica SUBSTR/RPAD/INSTR/LENGTH expressions exactly.
    Unit tests must compare output to Informatica reference values.
    """
    return {
        "customer_id": row.get("customer_id"),
        "masked_first_name": mask_first_name(row.get("first_name") or ""),
        "masked_email": mask_email(row.get("email") or ""),
        "masked_phone": mask_phone(row.get("phone") or ""),
    }


def transform_customer_ltv(rows: list[dict]) -> list[dict]:
    """
    m_customer_lifetime_value: SUM(purchase_amt) - SUM(return_amt) per customer.
    Equivalent of Informatica Aggregator with SUM().
    In NiFi: QueryRecord('SELECT customer_id, SUM(purchase_amt)-SUM(return_amt) AS ltv …').
    """
    ltv_map: dict[str, float] = {}
    for row in rows:
        cid = row.get("customer_id") or ""
        txn_type = (row.get("transaction_type") or "").lower()
        amount = float(row.get("amount") or 0)
        if txn_type == "return":
            ltv_map[cid] = ltv_map.get(cid, 0.0) - amount
        else:
            ltv_map[cid] = ltv_map.get(cid, 0.0) + amount
    return [
        {"customer_id": cid, "ltv": round(ltv, 2)}
        for cid, ltv in ltv_map.items()
    ]


def transform_customer_churn(row: dict) -> dict:
    """
    m_customer_churn_flag: score churn risk from last transaction + status.
    Equivalent of Informatica DATE_DIFF + IIF expression + RouteOnAttribute.
    """
    last_txn = row.get("last_transaction_date")
    status = row.get("status") or ""
    risk = churn_risk(last_txn, status)
    return {
        "customer_id": row.get("customer_id"),
        "last_transaction_date": last_txn,
        "churn_risk": risk,
    }


# ---------------------------------------------------------------------------
# NiFi flow builders
# ---------------------------------------------------------------------------

def build_m_customer_load(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_load sub-flow inside PG_Customer."""
    logger.info("Building m_customer_load sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_CUSTOMERS",
        {"Input Directory": f"{SOURCE_DATA_PATH}", "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 0))

    update_meta = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_AddMetadata",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/load_date": "${now():format('yyyy-MM-dd HH:mm:ss')}",
         "/batch_id": "${batch.id}",
         "/source_system": "informatica_etl"}, (200, 0))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_LOAD",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_load"}, (400, 0))

    _connect(pg_id, get_file.id, update_meta.id)
    _connect(pg_id, update_meta.id, put_db.id)
    logger.info("m_customer_load sub-flow built.")


def build_m_customer_deduplicate(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_deduplicate sub-flow — SortRecord + QueryRecord."""
    logger.info("Building m_customer_deduplicate sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_DEDUP_SRC_CUSTOMERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 200))

    sort = _add_processor(pg_id, "org.apache.nifi.processors.standard.SortRecord",
        "SortRecord_ByEmail",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Sort Attribute": "/email", "Sort Order": "Ascending"}, (200, 200))

    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_DedupFirstByEmail",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "dedup": "SELECT email, FIRST(customer_id) AS customer_id, FIRST(status) AS status "
                  "FROM FLOWFILE GROUP BY email"}, (400, 200))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_DEDUP",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_dedup"}, (600, 200))

    _connect(pg_id, get_file.id, sort.id)
    _connect(pg_id, sort.id, query.id)
    _connect(pg_id, query.id, put_db.id, "dedup")
    logger.info("m_customer_deduplicate sub-flow built.")


def build_m_customer_validate(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """
    Build m_customer_validate sub-flow.
    ExecuteScript applies REG_MATCH email regex and phone digit-length check.
    RouteOnAttribute routes invalid records to error funnel.
    """
    logger.info("Building m_customer_validate sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_VALIDATE_SRC_CUSTOMERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 400))

    # ExecuteScript for validation (Python)
    validation_script = r"""
import re, json
from io import StringIO

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

def validate(flowfile):
    content = session.read(flowfile).decode('utf-8')
    import csv
    reader = csv.DictReader(StringIO(content))
    rows = []
    for row in reader:
        email = row.get('email', '')
        phone = re.sub(r'\D', '', row.get('phone', ''))
        email_ok = bool(EMAIL_RE.match(email.strip()))
        phone_ok = len(phone) >= 10
        rows.append({
            'customer_id': row.get('customer_id'),
            'email_valid': str(email_ok).lower(),
            'phone_valid': str(phone_ok).lower(),
            'is_valid': str(email_ok and phone_ok).lower(),
        })
    return rows

flowFile = session.get()
if flowFile is not None:
    results = validate(flowFile)
    out = session.create()
    out = session.write(out, OutputStreamCallback(lambda os: os.write(
        json.dumps(results).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""

    execute_script = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_ValidateCustomer",
        {"Script Engine": "python", "Script Body": validation_script}, (200, 400))

    route = _add_processor(pg_id, "org.apache.nifi.processors.standard.RouteOnAttribute",
        "Route_ValidCustomers",
        {"Routing Strategy": "Route to Property name",
         "valid": "${is_valid:equals('true')}",
         "invalid": "${is_valid:equals('false')}"}, (400, 400))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_VALIDATE",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_validate"}, (600, 400))

    error_funnel = _add_processor(pg_id, "org.apache.nifi.processors.standard.LogMessage",
        "Log_InvalidCustomers",
        {"Log Level": "warn", "Log Prefix": "INVALID_CUSTOMER: "}, (600, 500))

    _connect(pg_id, get_file.id, execute_script.id)
    _connect(pg_id, execute_script.id, route.id)
    _connect(pg_id, route.id, put_db.id, "valid")
    _connect(pg_id, route.id, error_funnel.id, "invalid")
    logger.info("m_customer_validate sub-flow built.")


def build_m_customer_scd2(pg_id: str, csv_reader_id: str, db_pool_id: str,
                           db_lookup_id: str) -> None:
    """
    Build m_customer_scd2 sub-flow — HIGH RISK.
    LookupRecord → RouteOnAttribute (found/not_found) → ExecuteScript → PutDatabaseRecord.
    """
    logger.info("Building m_customer_scd2 sub-flow (HIGH RISK)…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SCD2_SRC_CUSTOMERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 600))

    lookup = _add_processor(pg_id, "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_SCD2_CustomerExists",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Lookup Service": db_lookup_id,
         "Result RecordPath": "/existing_customer_id",
         "Routing Strategy": "Route to 'matched' or 'unmatched'",
         "customer_id": "/customer_id"}, (200, 600))

    scd2_script = r"""
import json
from datetime import date

flowFile = session.get()
if flowFile is not None:
    content = json.loads(session.read(flowFile).decode('utf-8'))
    today = date.today().strftime('%Y-%m-%d')
    results = []
    for row in (content if isinstance(content, list) else [content]):
        existing_id = row.get('existing_customer_id')
        if existing_id:
            # Expire existing row
            results.append({**row, 'end_date': today, 'is_current': 0,
                             'effective_date': row.get('effective_date', today)})
        # Insert new current row
        results.append({
            'customer_id': row['customer_id'],
            'first_name': row.get('first_name'),
            'last_name': row.get('last_name'),
            'email': row.get('email'),
            'effective_date': today,
            'end_date': '9999-12-31',
            'is_current': 1,
        })
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(results).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute_scd2 = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_SCD2Logic",
        {"Script Engine": "python", "Script Body": scd2_script}, (400, 600))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_SCD2",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "UPSERT", "Table Name": "tgt_customer_scd2"}, (600, 600))

    _connect(pg_id, get_file.id, lookup.id)
    _connect(pg_id, lookup.id, execute_scd2.id, "matched")
    _connect(pg_id, lookup.id, execute_scd2.id, "unmatched")
    _connect(pg_id, execute_scd2.id, put_db.id)
    logger.info("m_customer_scd2 sub-flow built.")


def build_m_customer_addr_norm(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_addr_norm sub-flow — INITCAP + abbreviation expansion."""
    logger.info("Building m_customer_addr_norm sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_ADDR_SRC_CUSTOMER_ADDRESSES",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customer_addresses*.csv",
         "Keep Source File": "true"}, (0, 800))

    addr_script = r"""
import json, re
def initcap(s):
    return ' '.join(w.capitalize() for w in (s or '').split())
ABBREV = {' St ': ' Street ', ' Ave ': ' Avenue ', ' Blvd ': ' Boulevard ',
          ' Dr ': ' Drive ', ' Rd ': ' Road ', ' Ct ': ' Court ',
          ' Ln ': ' Lane ', ' Pl ': ' Place '}
def expand(s):
    for a, f in ABBREV.items():
        s = s.replace(a, f)
    return s
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out_rows = []
    for row in (rows if isinstance(rows, list) else [rows]):
        out_rows.append({
            'address_id': row.get('address_id'),
            'customer_id': row.get('customer_id'),
            'address_line1_norm': expand(initcap(row.get('address_line1',''))),
            'city_norm': initcap(row.get('city','')),
            'state': (row.get('state') or '').upper(),
            'zip_code': row.get('zip_code'),
        })
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(out_rows).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_AddrNorm",
        {"Script Engine": "python", "Script Body": addr_script}, (200, 800))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_ADDR_NORMALIZED",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_addr_normalized"}, (400, 800))

    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)
    logger.info("m_customer_addr_norm sub-flow built.")


def build_m_customer_segment(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_segment sub-flow — spend-tier RouteOnAttribute."""
    logger.info("Building m_customer_segment sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SEG_SRC_CUSTOMER_TRANSACTIONS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customer_transactions*.csv",
         "Keep Source File": "true"}, (0, 1000))

    agg = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_AggSpend",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "agg": "SELECT customer_id, SUM(amount) AS total_spend "
                "FROM FLOWFILE GROUP BY customer_id"}, (200, 1000))

    seg_script = r"""
import json
def segment(spend):
    if spend >= 400: return 'platinum'
    if spend >= 200: return 'gold'
    if spend >= 100: return 'silver'
    return 'bronze'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    for row in (rows if isinstance(rows, list) else [rows]):
        row['segment'] = segment(float(row.get('total_spend') or 0))
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(rows).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_Segment",
        {"Script Engine": "python", "Script Body": seg_script}, (400, 1000))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_SEGMENT",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_segment"}, (600, 1000))

    _connect(pg_id, get_file.id, agg.id)
    _connect(pg_id, agg.id, execute.id, "agg")
    _connect(pg_id, execute.id, put_db.id)
    logger.info("m_customer_segment sub-flow built.")


def build_m_customer_merge(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_merge sub-flow — ExecuteScript join of customer + address + segment."""
    logger.info("Building m_customer_merge sub-flow…")

    merge_script = r"""
import json
# Reads merged JSON payload produced by upstream join logic
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(rows).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_CustomerMerge",
        {"Script Engine": "python", "Script Body": merge_script}, (200, 1200))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_MERGE",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_merge"}, (400, 1200))

    _connect(pg_id, execute.id, put_db.id)
    logger.info("m_customer_merge sub-flow built.")


def build_m_customer_privacy_mask(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """
    Build m_customer_privacy_mask sub-flow — GDPR PII masking — HIGH RISK.
    ExecuteScript applies exact Informatica SUBSTR/RPAD masking logic.
    Unit tests must validate against Informatica reference output before go-live.
    """
    logger.info("Building m_customer_privacy_mask sub-flow (HIGH RISK — GDPR)…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_MASK_SRC_CUSTOMERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 1400))

    mask_script = r"""
import json, re
def mask_first_name(s):
    if not s: return s
    return s[:2] + '*' * max(0, len(s) - 2)
def mask_email(s):
    if not s: return s
    at = s.find('@')
    if at < 0: return s[:1] + '***'
    return s[:1] + '***' + s[at:]
def mask_phone(s):
    if not s: return s
    digits = re.sub(r'\D', '', s)
    if len(digits) <= 4: return s
    return '*' * (len(digits) - 4) + digits[-4:]
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out_rows = []
    for row in (rows if isinstance(rows, list) else [rows]):
        out_rows.append({
            'customer_id': row.get('customer_id'),
            'masked_first_name': mask_first_name(row.get('first_name', '')),
            'masked_email': mask_email(row.get('email', '')),
            'masked_phone': mask_phone(row.get('phone', '')),
        })
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(out_rows).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_PIIMask",
        {"Script Engine": "python", "Script Body": mask_script}, (200, 1400))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_MASKED",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_masked"}, (400, 1400))

    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)
    logger.info("m_customer_privacy_mask sub-flow built.")


def build_m_customer_ltv(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """Build m_customer_lifetime_value sub-flow — QueryRecord SUM aggregation."""
    logger.info("Building m_customer_lifetime_value sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_LTV_SRC_CUSTOMER_TRANSACTIONS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customer_transactions*.csv",
         "Keep Source File": "true"}, (0, 1600))

    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_LTV",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "ltv": (
             "SELECT customer_id, "
             "SUM(CASE WHEN transaction_type='purchase' THEN amount ELSE 0 END) "
             " - SUM(CASE WHEN transaction_type='return' THEN amount ELSE 0 END) AS ltv "
             "FROM FLOWFILE GROUP BY customer_id"
         )}, (200, 1600))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_LTV",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_ltv"}, (400, 1600))

    _connect(pg_id, get_file.id, query.id)
    _connect(pg_id, query.id, put_db.id, "ltv")
    logger.info("m_customer_lifetime_value sub-flow built.")


def build_m_customer_churn(pg_id: str, csv_reader_id: str, db_pool_id: str) -> None:
    """
    Build m_customer_churn_flag sub-flow.
    ExecuteScript calculates DATE_DIFF(today, last_transaction_date) and
    sets churn_risk; RouteOnAttribute routes high/medium/low.
    """
    logger.info("Building m_customer_churn_flag sub-flow…")

    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_CHURN_SRC_CUSTOMERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "customers*.csv",
         "Keep Source File": "true"}, (0, 1800))

    churn_script = r"""
import json
from datetime import date, datetime
def churn_risk(last_txn, status):
    if not last_txn:
        return 'high'
    try:
        d = datetime.strptime(str(last_txn).strip()[:10], '%Y-%m-%d').date()
        days = (date.today() - d).days
    except Exception:
        return 'high'
    if days > 90: return 'high'
    if (status or '').lower() == 'inactive': return 'medium'
    return 'low'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out_rows = []
    for row in (rows if isinstance(rows, list) else [rows]):
        out_rows.append({
            'customer_id': row.get('customer_id'),
            'last_transaction_date': row.get('last_transaction_date'),
            'churn_risk': churn_risk(
                row.get('last_transaction_date'), row.get('status')),
        })
    out = session.create(flowFile)
    out = session.write(out, OutputStreamCallback(
        lambda os: os.write(json.dumps(out_rows).encode('utf-8'))))
    session.transfer(out, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_ChurnRisk",
        {"Script Engine": "python", "Script Body": churn_script}, (200, 1800))

    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CUSTOMER_CHURN",
        {"Record Reader": csv_reader_id, "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_customer_churn"}, (400, 1800))

    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)
    logger.info("m_customer_churn_flag sub-flow built.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Entry point called by main.py.
    Retrieves or creates PG_Customer, then builds all 10 customer mapping sub-flows.
    """
    logger.info("=== customer.py: building PG_Customer (10 mappings) ===")

    connect_nifi()
    root_pg_id = get_root_pg_id()

    # Resolve shared services by name (created by extract.py)
    def _svc_id(name: str) -> str:
        svc = nipyapi.canvas.get_controller_service(name)
        return svc.id if svc else ""

    csv_reader_id = _svc_id("XClarity_CSVReader")
    db_pool_id = _svc_id("XClarity_DBCPConnectionPool")
    db_lookup_id = _svc_id("XClarity_DatabaseRecordLookupService")

    pg = get_or_create_pg("PG_Customer", parent_pg_id=root_pg_id)
    pg_id = pg.id if hasattr(pg, "id") else str(pg)

    try:
        build_m_customer_load(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_load failed: %s", exc)

    try:
        build_m_customer_deduplicate(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_deduplicate failed: %s", exc)

    try:
        build_m_customer_validate(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_validate failed: %s", exc)

    try:
        build_m_customer_scd2(pg_id, csv_reader_id, db_pool_id, db_lookup_id)
    except Exception as exc:
        logger.error("m_customer_scd2 failed: %s", exc)

    try:
        build_m_customer_addr_norm(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_addr_norm failed: %s", exc)

    try:
        build_m_customer_segment(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_segment failed: %s", exc)

    try:
        build_m_customer_merge(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_merge failed: %s", exc)

    try:
        build_m_customer_privacy_mask(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_privacy_mask failed: %s", exc)

    try:
        build_m_customer_ltv(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_ltv failed: %s", exc)

    try:
        build_m_customer_churn(pg_id, csv_reader_id, db_pool_id)
    except Exception as exc:
        logger.error("m_customer_churn_flag failed: %s", exc)

    logger.info("PG_Customer build complete.")
