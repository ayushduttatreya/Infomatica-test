"""
src/finance.py
Finance domain — PG_Finance — 10 mappings migrated from Informatica to NiFi.

Mappings covered (ticket: Migrate Finance domain flows):
  1.  m_finance_gl_load          → TGT_GL              (passthrough)
  2.  m_finance_ap_load          → TGT_AP              (passthrough)
  3.  m_finance_ar_load          → TGT_AR              (passthrough)
  4.  m_finance_journal_validate → TGT_JOURNAL_VALID   (ABS(debits-credits)<0.01)
  5.  m_finance_currency_convert → TGT_CURRENCY_CONV   (LookupRecord exchange rate)
  6.  m_finance_budget_variance  → TGT_BUDGET_VAR      (actual-budget / pct / status)
  7.  m_finance_expense_cat      → TGT_EXPENSE_CAT     (DECODE account_code)
  8.  m_finance_revenue_rec      → TGT_REVENUE_REC     (TO_CHAR period stamp)
  9.  m_finance_consolidation    → TGT_CONSOLIDATED    (QueryRecord GROUP BY account_code)
  10. m_finance_audit_trail      → TGT_AUDIT_TRAIL     (MD5 hash — MEDIUM RISK)
"""

import logging
from datetime import datetime

import nipyapi
import nipyapi.nifi as nifi_api

from src.utils import (
    budget_variance_status,
    connect_nifi,
    expense_category,
    get_or_create_pg,
    get_root_pg_id,
    md5_audit_hash,
    to_char,
)

logger = logging.getLogger("xclarity_etl.finance")

SOURCE_DATA_PATH = "/opt/nifi/source_data/finance"
OUTPUT_DATA_PATH = "/opt/nifi/output_data/finance"


def _add_processor(pg_id, proc_type, name, properties, position=(0, 0)):
    body = nifi_api.ProcessorEntity(
        revision=nifi_api.RevisionDTO(version=0),
        component=nifi_api.ProcessorDTO(
            type=proc_type, name=name,
            config=nifi_api.ProcessorConfigDTO(properties=properties),
            position=nifi_api.PositionDTO(x=float(position[0]), y=float(position[1])),
        ),
    )
    try:
        return nifi_api.ProcessGroupsApi().create_processor(pg_id, body)
    except Exception as exc:
        logger.error("Failed to add processor '%s': %s", name, exc)
        raise


def _connect(pg_id, src_id, dst_id, relationship="success"):
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
        logger.warning("Connection skipped: %s", exc)


# ---------------------------------------------------------------------------
# Pure-Python transform functions
# ---------------------------------------------------------------------------

def transform_gl_load(row: dict) -> dict:
    """m_finance_gl_load: passthrough general ledger."""
    return {
        "entry_id": row.get("entry_id"),
        "account_code": row.get("account_code"),
        "entry_date": row.get("entry_date"),
        "debit_amount": row.get("debit_amount"),
        "credit_amount": row.get("credit_amount"),
        "currency": row.get("currency"),
    }


def transform_ap_load(row: dict) -> dict:
    """m_finance_ap_load: passthrough accounts payable."""
    return {
        "invoice_id": row.get("invoice_id"),
        "vendor_id": row.get("vendor_id"),
        "amount": row.get("amount"),
        "status": row.get("status"),
    }


def transform_ar_load(row: dict) -> dict:
    """m_finance_ar_load: passthrough accounts receivable."""
    return {
        "invoice_id": row.get("invoice_id"),
        "customer_id": row.get("customer_id"),
        "amount": row.get("amount"),
        "status": row.get("status"),
    }


def transform_journal_validate(rows: list[dict]) -> list[dict]:
    """
    m_finance_journal_validate: ABS(SUM(debits) - SUM(credits)) < 0.01.
    Equivalent of Informatica Aggregator + Filter.
    """
    from collections import defaultdict
    totals: dict = defaultdict(lambda: {"debit": 0.0, "credit": 0.0})
    for row in rows:
        eid = row.get("entry_id") or ""
        totals[eid]["debit"] += float(row.get("debit_amount") or 0)
        totals[eid]["credit"] += float(row.get("credit_amount") or 0)
    result = []
    for eid, v in totals.items():
        diff = abs(v["debit"] - v["credit"])
        result.append({
            "entry_id": eid,
            "total_debits": round(v["debit"], 2),
            "total_credits": round(v["credit"], 2),
            "balance_status": "balanced" if diff < 0.01 else "imbalanced",
        })
    return result


def transform_currency_convert(row: dict, exchange_rate: float) -> dict:
    """
    m_finance_currency_convert: multiply debit/credit by exchange_rate.
    USD passes through unchanged.
    Equivalent of Informatica Lookup + Expression.
    """
    currency = (row.get("currency") or "USD").upper()
    debit = float(row.get("debit_amount") or 0)
    credit = float(row.get("credit_amount") or 0)
    rate = 1.0 if currency == "USD" else exchange_rate
    return {
        "entry_id": row.get("entry_id"),
        "currency": currency,
        "exchange_rate": rate,
        "debit_usd": round(debit * rate, 2),
        "credit_usd": round(credit * rate, 2),
    }


def transform_budget_variance(row: dict) -> dict:
    """
    m_finance_budget_variance: variance_amount + variance_pct + status.
    Status: under_budget <-5%, on_budget ±5%, over_budget >+5%.
    """
    actual = float(row.get("actual_amount") or 0)
    budget = float(row.get("budget_amount") or 0)
    variance = round(actual - budget, 2)
    pct = round((variance / budget * 100), 2) if budget else 0.0
    return {
        "budget_id": row.get("budget_id"),
        "variance_amount": variance,
        "variance_pct": pct,
        "variance_status": budget_variance_status(pct),
    }


def transform_expense_cat(row: dict) -> dict:
    """
    m_finance_expense_cat: DECODE account_code to expense category.
    6100=Payroll, 6200=Facilities, 6300=Utilities, 6400=Marketing,
    6500=Technology, else Other.
    """
    code = int(row.get("account_code") or 0)
    return {
        "entry_id": row.get("entry_id"),
        "account_code": code,
        "expense_category": expense_category(code),
    }


def transform_revenue_rec(row: dict) -> dict:
    """m_finance_revenue_rec: stamp recognition_period as YYYY-MM."""
    entry_date = row.get("entry_date") or ""
    period = entry_date[:7] if len(entry_date) >= 7 else "Unknown"
    return {
        "entry_id": row.get("entry_id"),
        "recognition_period": period,
        "debit_amount": row.get("debit_amount"),
    }


def transform_consolidation(rows: list[dict]) -> list[dict]:
    """
    m_finance_consolidation: SUM debits+credits across all entity_id per account_code.
    """
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {"name": "", "debit": 0.0, "credit": 0.0})
    for row in rows:
        code = row.get("account_code") or 0
        agg[code]["name"] = row.get("account_name") or ""
        agg[code]["debit"] += float(row.get("debit_amount") or 0)
        agg[code]["credit"] += float(row.get("credit_amount") or 0)
    return [
        {
            "account_code": code,
            "account_name": v["name"],
            "total_debits": round(v["debit"], 2),
            "total_credits": round(v["credit"], 2),
            "net_balance": round(v["debit"] - v["credit"], 2),
        }
        for code, v in agg.items()
    ]


def transform_audit_trail(row: dict) -> dict:
    """
    m_finance_audit_trail: MD5 hash of key GL fields + SYSDATE.
    MEDIUM RISK: must match Informatica MD5() output for reference records.
    """
    audit_hash = md5_audit_hash(
        str(row.get("entry_id") or ""),
        str(row.get("account_code") or ""),
        str(row.get("debit_amount") or ""),
        str(row.get("credit_amount") or ""),
    )
    return {
        "entry_id": row.get("entry_id"),
        "audit_hash": audit_hash,
        "audit_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "etl_system": "XClarity_ETL",
        "action": "load",
    }


# ---------------------------------------------------------------------------
# NiFi flow builders
# ---------------------------------------------------------------------------

def build_m_finance_gl_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_gl_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 0))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_GL",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_gl"}, (200, 0))
    _connect(pg_id, gf.id, put.id)


def build_m_finance_ap_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_ap_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_ACCOUNTS_PAYABLE",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "accounts_payable*.csv",
         "Keep Source File": "true"}, (0, 200))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_AP",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_ap"}, (200, 200))
    _connect(pg_id, gf.id, put.id)


def build_m_finance_ar_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_ar_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_ACCOUNTS_RECEIVABLE",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "accounts_receivable*.csv",
         "Keep Source File": "true"}, (0, 400))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_AR",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_ar"}, (200, 400))
    _connect(pg_id, gf.id, put.id)


def build_m_finance_journal_validate(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_journal_validate sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_VAL_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 600))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_JournalBalance",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "balance": (
             "SELECT entry_id, SUM(debit_amount) AS total_debits, "
             "SUM(credit_amount) AS total_credits, "
             "CASE WHEN ABS(SUM(debit_amount) - SUM(credit_amount)) < 0.01 "
             "THEN 'balanced' ELSE 'imbalanced' END AS balance_status "
             "FROM FLOWFILE GROUP BY entry_id"
         )}, (200, 600))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_JOURNAL_VALID",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_journal_valid"}, (400, 600))
    _connect(pg_id, gf.id, query.id)
    _connect(pg_id, query.id, put.id, "balance")


def build_m_finance_currency_convert(pg_id, csv_reader_id, db_pool_id, db_lookup_id):
    """
    m_finance_currency_convert: LookupRecord (from_currency+rate_date) → convert.
    MEDIUM RISK: composite lookup key must match exactly.
    """
    logger.info("Building m_finance_currency_convert sub-flow (MEDIUM RISK)…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_CURR_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 800))
    # Build composite key attribute before LookupRecord
    build_key = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateAttribute",
        "UpdateAttr_BuildLookupKey",
        {"lookup_key": "${currency}_${entry_date:substring(0,10)}"}, (200, 800))
    lookup = _add_processor(pg_id, "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_ExchangeRate",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Lookup Service": db_lookup_id,
         "Result RecordPath": "/exchange_rate",
         "lookup_key": "${lookup_key}"}, (400, 800))
    conv_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        cur = (row.get('currency') or 'USD').upper()
        rate = float(row.get('exchange_rate') or 1.0)
        if cur == 'USD': rate = 1.0
        out.append({
            'entry_id': row.get('entry_id'),
            'currency': cur,
            'exchange_rate': rate,
            'debit_usd': round(float(row.get('debit_amount') or 0) * rate, 2),
            'credit_usd': round(float(row.get('credit_amount') or 0) * rate, 2),
        })
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_CurrencyConvert",
        {"Script Engine": "python", "Script Body": conv_script}, (600, 800))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CURRENCY_CONV",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_currency_conv"}, (800, 800))
    _connect(pg_id, gf.id, build_key.id)
    _connect(pg_id, build_key.id, lookup.id)
    _connect(pg_id, lookup.id, execute.id, "matched")
    _connect(pg_id, lookup.id, execute.id, "unmatched")
    _connect(pg_id, execute.id, put.id)


def build_m_finance_budget_variance(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_budget_variance sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_BUDGET",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "budget*.csv",
         "Keep Source File": "true"}, (0, 1000))
    bv_script = r"""
import json
def status(pct):
    if pct < -5: return 'under_budget'
    if pct > 5: return 'over_budget'
    return 'on_budget'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        actual = float(row.get('actual_amount') or 0)
        budget = float(row.get('budget_amount') or 0)
        variance = round(actual - budget, 2)
        pct = round(variance / budget * 100, 2) if budget else 0.0
        out.append({'budget_id': row.get('budget_id'),
                    'variance_amount': variance, 'variance_pct': pct,
                    'variance_status': status(pct)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_BudgetVariance",
        {"Script Engine": "python", "Script Body": bv_script}, (200, 1000))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_BUDGET_VAR",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_budget_var"}, (400, 1000))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_finance_expense_cat(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_expense_cat sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_EC_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 1200))
    ec_script = r"""
import json
DECODE = {6100:'Payroll',6200:'Facilities',6300:'Utilities',6400:'Marketing',6500:'Technology'}
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = [{'entry_id': r.get('entry_id'),
            'account_code': int(r.get('account_code') or 0),
            'expense_category': DECODE.get(int(r.get('account_code') or 0), 'Other')}
           for r in (rows if isinstance(rows, list) else [rows])]
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_ExpenseCat",
        {"Script Engine": "python", "Script Body": ec_script}, (200, 1200))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_EXPENSE_CAT",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_expense_cat"}, (400, 1200))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_finance_revenue_rec(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_revenue_rec sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_RR_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 1400))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_RecognitionPeriod",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/recognition_period": "${entry_date:substring(0,7)}"}, (200, 1400))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_REVENUE_REC",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_revenue_rec"}, (400, 1400))
    _connect(pg_id, gf.id, update.id)
    _connect(pg_id, update.id, put.id)


def build_m_finance_consolidation(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_finance_consolidation sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_CON_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 1600))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_Consolidation",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "consol": (
             "SELECT account_code, FIRST(account_name) AS account_name, "
             "SUM(debit_amount) AS total_debits, SUM(credit_amount) AS total_credits, "
             "SUM(debit_amount) - SUM(credit_amount) AS net_balance "
             "FROM FLOWFILE GROUP BY account_code"
         )}, (200, 1600))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CONSOLIDATED",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_consolidated"}, (400, 1600))
    _connect(pg_id, gf.id, query.id)
    _connect(pg_id, query.id, put.id, "consol")


def build_m_finance_audit_trail(pg_id, csv_reader_id, db_pool_id):
    """
    m_finance_audit_trail: MD5 audit hash — MEDIUM RISK.
    ExecuteScript uses hashlib.md5 to replicate Informatica MD5() exactly.
    Reference records must be validated against Informatica output before go-live.
    """
    logger.info("Building m_finance_audit_trail sub-flow (MEDIUM RISK)…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_AUDIT_SRC_GENERAL_LEDGER",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "general_ledger*.csv",
         "Keep Source File": "true"}, (0, 1800))
    audit_script = r"""
import json, hashlib
from datetime import datetime
def md5_hash(entry_id, account_code, debit, credit):
    raw = f'{entry_id}{account_code}{debit}{credit}'
    return hashlib.md5(raw.encode('utf-8')).hexdigest()
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        out.append({
            'entry_id': row.get('entry_id'),
            'audit_hash': md5_hash(
                str(row.get('entry_id','')), str(row.get('account_code','')),
                str(row.get('debit_amount','')), str(row.get('credit_amount',''))),
            'audit_timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'etl_system': 'XClarity_ETL',
            'action': 'load',
        })
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_AuditTrail",
        {"Script Engine": "python", "Script Body": audit_script}, (200, 1800))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_AUDIT_TRAIL",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_audit_trail"}, (400, 1800))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    logger.info("=== finance.py: building PG_Finance (10 mappings) ===")
    connect_nifi()
    root_pg_id = get_root_pg_id()

    def _svc_id(name):
        svc = nipyapi.canvas.get_controller_service(name)
        return svc.id if svc else ""

    csv_reader_id = _svc_id("XClarity_CSVReader")
    db_pool_id = _svc_id("XClarity_DBCPConnectionPool")
    db_lookup_id = _svc_id("XClarity_DatabaseRecordLookupService")

    pg = get_or_create_pg("PG_Finance", parent_pg_id=root_pg_id)
    pg_id = pg.id if hasattr(pg, "id") else str(pg)

    builders_no_lookup = [
        build_m_finance_gl_load,
        build_m_finance_ap_load,
        build_m_finance_ar_load,
        build_m_finance_journal_validate,
        build_m_finance_budget_variance,
        build_m_finance_expense_cat,
        build_m_finance_revenue_rec,
        build_m_finance_consolidation,
        build_m_finance_audit_trail,
    ]
    for builder in builders_no_lookup:
        try:
            builder(pg_id, csv_reader_id, db_pool_id)
        except Exception as exc:
            logger.error("%s failed: %s", builder.__name__, exc)

    try:
        build_m_finance_currency_convert(pg_id, csv_reader_id, db_pool_id, db_lookup_id)
    except Exception as exc:
        logger.error("build_m_finance_currency_convert failed: %s", exc)

    logger.info("PG_Finance build complete.")
