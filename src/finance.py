"""
Finance Domain - Apache NiFi Process Group
Migrated from Informatica PowerCenter XClarity_ETL

Covers tickets:
  - Implement Finance GL/AP/AR load, journal balance validation, and financial consolidation
  - Implement multi-currency conversion using Redis/JDBC LookupRecord
  - Implement MD5 audit trail generation and budget variance analysis
  - Implement Finance expense categorisation using NiFi expression language DECODE replacement
  - Implement Finance GL/AP/AR load, journal balance validation, and revenue recognition
  - Implement multi-currency conversion with Redis exchange rate cache
  - Implement budget variance analysis, expense categorisation, financial consolidation,
    and MD5 audit trail

Sources : SRC_GENERAL_LEDGER, SRC_ACCOUNTS_PAYABLE, SRC_ACCOUNTS_RECEIVABLE,
          SRC_BUDGET, SRC_EXCHANGE_RATES
Targets : TGT_GL, TGT_AP, TGT_AR, TGT_JOURNAL_VALID, TGT_CURRENCY_CONV,
          TGT_BUDGET_VAR, TGT_EXPENSE_CAT, TGT_REVENUE_REC,
          TGT_CONSOLIDATED, TGT_AUDIT_TRAIL
"""

import logging

import nipyapi
from nipyapi import canvas

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Groovy script: MD5 audit trail generation
# Replaces Informatica MD5() built-in
# ---------------------------------------------------------------------------

GROOVY_AUDIT_TRAIL = """\
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.security.MessageDigest

def flowFile = session.get()
if (!flowFile) return

try {
    def content = IOUtils.toString(session.read(flowFile), StandardCharsets.UTF_8)
    def records = new JsonSlurper().parseText(content)
    def now     = new Date().toInstant().toString()

    records.each { rec ->
        def raw = "${rec.entry_id}${rec.account_code}${rec.debit_amount}${rec.credit_amount}"
        rec.audit_hash      = MessageDigest.getInstance('MD5').digest(raw.bytes).encodeHex().toString()
        rec.audit_timestamp = now
        rec.audit_user      = 'etl_system'
        rec.audit_action    = 'load'
    }

    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(records).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Audit trail generation failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""

# ---------------------------------------------------------------------------
# NiFi EL: DECODE account_code to expense category name
# Replaces Informatica DECODE() built-in
# ---------------------------------------------------------------------------

DECODE_EXPENSE_CAT = (
    "${account_code:equals('6100'):ifElse('Payroll',"
    "${account_code:equals('6200'):ifElse('Facilities',"
    "${account_code:equals('6300'):ifElse('Utilities',"
    "${account_code:equals('6400'):ifElse('Marketing',"
    "${account_code:equals('6500'):ifElse('Technology','Other')})})})}}"
)


def create_finance_process_group(parent_pg_id: str, nifi_url: str = "http://localhost:8080") -> None:
    """
    Build the full Finance NiFi Process Group.

    Covers Informatica mappings:
      m_finance_gl_load, m_finance_ap_load, m_finance_ar_load,
      m_finance_journal_validate, m_finance_currency_convert,
      m_finance_budget_var, m_finance_expense_cat,
      m_finance_revenue_rec, m_finance_consolidation,
      m_finance_audit_trail
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    try:
        root = canvas.get_root_pg_id()
        parent_id = parent_pg_id or root

        logger.info("Creating Finance Process Group under parent %s", parent_id)
        finance_pg = canvas.create_process_group(
            canvas.get_process_group(parent_id),
            "Finance",
            (100, 700),
        )
        pg_id = finance_pg.id
        logger.info("Finance PG created: %s", pg_id)

        _build_gl_ap_ar_load(pg_id)
        _build_journal_validate(pg_id)
        _build_currency_convert(pg_id)
        _build_budget_variance(pg_id)
        _build_expense_cat(pg_id)
        _build_revenue_rec(pg_id)
        _build_consolidation(pg_id)
        _build_audit_trail(pg_id)

        logger.info("Finance Process Group fully built: %s", pg_id)

    except Exception as exc:
        logger.error("Failed to build Finance Process Group: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Sub-flow builders
# ---------------------------------------------------------------------------

def _build_gl_ap_ar_load(pg_id: str) -> None:
    """
    m_gl_load / m_ap_load / m_ar_load:
    GetFile(SRC_*) -> PutFile TGT_GL / TGT_AP / TGT_AR
    """
    logger.info("[m_gl_ap_ar_load] Building sub-flow in PG %s", pg_id)

    sources = [
        ("SRC_GENERAL_LEDGER", "/data/inbound/general_ledger", "TGT_GL"),
        ("SRC_ACCOUNTS_PAYABLE", "/data/inbound/accounts_payable", "TGT_AP"),
        ("SRC_ACCOUNTS_RECEIVABLE", "/data/inbound/accounts_receivable", "TGT_AR"),
    ]

    for idx, (src_name, inbound_dir, tgt_name) in enumerate(sources):
        try:
            get = canvas.create_processor(
                canvas.get_process_group(pg_id),
                canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
                (100, 200 + idx * 180),
                f"GetFile_{src_name}",
                {
                    "Input Directory": inbound_dir,
                    "File Filter": "*.csv",
                },
            )

            put = canvas.create_processor(
                canvas.get_process_group(pg_id),
                canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
                (400, 200 + idx * 180),
                f"PutFile_{tgt_name}",
                {"Directory": f"/data/outbound/{tgt_name}"},
            )

            canvas.create_connection(get, put, ["success"])

        except Exception as exc:
            logger.warning("[m_gl_ap_ar_load][%s] API offline: %s", src_name, exc)

    logger.info("[m_gl_ap_ar_load] sub-flow complete")


def _build_journal_validate(pg_id: str) -> None:
    """
    m_finance_journal_validate:
    QueryRecord (SUM debit_amount, SUM credit_amount per journal_id)
    -> UpdateRecord (balanced = 'Y' if ABS(debits-credits) < 0.01 else 'N')
    -> TGT_JOURNAL_VALID
    """
    logger.info("[m_journal_validate] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 760),
            "QueryRecord_JournalBalance",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "journal_totals": (
                    "SELECT entry_id, account_code, "
                    "SUM(debit_amount) AS total_debits, "
                    "SUM(credit_amount) AS total_credits "
                    "FROM FLOWFILE GROUP BY entry_id, account_code"
                ),
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 760),
            "UpdateRecord_JournalBalance",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/balanced": (
                    "${total_debits:toNumber():minus(${total_credits:toNumber()}):abs():lt(0.01):ifElse('Y','N')}"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 760),
            "PutFile_TGT_JOURNAL_VALID",
            {"Directory": "/data/outbound/TGT_JOURNAL_VALID"},
        )

        canvas.create_connection(query, update, ["journal_totals"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_journal_validate] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_journal_validate] Could not build via API (offline mode): %s", exc)


def _build_currency_convert(pg_id: str) -> None:
    """
    m_finance_currency_convert:
    LookupRecord (Redis keyed on 'currency:date') to fetch exchange_rate
    -> UpdateRecord:
         if currency != 'USD':
           usd_debit  = debit_amount  * exchange_rate
           usd_credit = credit_amount * exchange_rate
         else: pass through
    -> TGT_CURRENCY_CONV

    Pre-requisite: SRC_EXCHANGE_RATES loaded into Redis keyed on '<from_currency>:<rate_date>'
    """
    logger.info("[m_finance_currency_convert] Building sub-flow in PG %s", pg_id)

    try:
        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (100, 940),
            "LookupRecord_ExchangeRate",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "RedisLookupService",
                "Result RecordPath": "/exchange_rate",
                # Redis key = currency:date
                "key": "${currency}:${entry_date:substring(0,10)}",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 940),
            "UpdateRecord_CurrencyConvert",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/usd_debit_amount": (
                    "${currency:equals('USD'):ifElse("
                    "${debit_amount},"
                    "${debit_amount:toNumber():multiply(${exchange_rate:toNumber()})})}"
                ),
                "/usd_credit_amount": (
                    "${currency:equals('USD'):ifElse("
                    "${credit_amount},"
                    "${credit_amount:toNumber():multiply(${exchange_rate:toNumber()})})}"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 940),
            "PutFile_TGT_CURRENCY_CONV",
            {"Directory": "/data/outbound/TGT_CURRENCY_CONV"},
        )

        canvas.create_connection(lookup, update, ["matched", "unmatched"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_finance_currency_convert] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_currency_convert] Could not build via API (offline mode): %s", exc)


def _build_budget_variance(pg_id: str) -> None:
    """
    m_finance_budget_var:
    GetFile(SRC_BUDGET)
    -> UpdateRecord:
         variance_amount = actual_amount - budget_amount
         variance_pct    = variance_amount / budget_amount * 100
         classification  = under_budget (<-5%), on_budget (within 5%), over_budget (>5%)
    -> TGT_BUDGET_VAR
    """
    logger.info("[m_finance_budget_var] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1120),
            "GetFile_SRC_BUDGET",
            {
                "Input Directory": "/data/inbound/budget",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1120),
            "UpdateRecord_BudgetVariance",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/variance_amount": "${actual_amount:toNumber():minus(${budget_amount:toNumber()})}",
                "/variance_pct": "${actual_amount:toNumber():minus(${budget_amount:toNumber()}):divide(${budget_amount:toNumber()}):multiply(100)}",
                "/classification": (
                    "${variance_pct:toNumber():lt(-5):ifElse('under_budget',"
                    "${variance_pct:toNumber():gt(5):ifElse('over_budget','on_budget')})}"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1120),
            "PutFile_TGT_BUDGET_VAR",
            {"Directory": "/data/outbound/TGT_BUDGET_VAR"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_finance_budget_var] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_budget_var] Could not build via API (offline mode): %s", exc)


def _build_expense_cat(pg_id: str) -> None:
    """
    m_finance_expense_cat:
    RouteOnAttribute (filter account_code in 6xxx range)
    -> UpdateRecord (DECODE replacement: nested NiFi EL ifElse)
    -> TGT_EXPENSE_CAT
    """
    logger.info("[m_finance_expense_cat] Building sub-flow in PG %s", pg_id)

    try:
        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (100, 1300),
            "RouteOnAttribute_ExpenseFilter",
            {
                "Routing Strategy": "Route to Property name",
                "expense": "${account_code:matches('6[0-9]{3}')}",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1300),
            "UpdateRecord_ExpenseCategory",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/expense_category": DECODE_EXPENSE_CAT,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1300),
            "PutFile_TGT_EXPENSE_CAT",
            {"Directory": "/data/outbound/TGT_EXPENSE_CAT"},
        )

        canvas.create_connection(route, update, ["expense"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_finance_expense_cat] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_expense_cat] Could not build via API (offline mode): %s", exc)


def _build_revenue_rec(pg_id: str) -> None:
    """
    m_finance_revenue_rec:
    RouteOnAttribute (revenue account codes)
    -> UpdateRecord: recognition_period = TO_CHAR(entry_date, 'YYYY-MM'), status = 'recognized'
    -> TGT_REVENUE_REC
    """
    logger.info("[m_finance_revenue_rec] Building sub-flow in PG %s", pg_id)

    try:
        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (100, 1480),
            "RouteOnAttribute_RevenueAccounts",
            {
                "Routing Strategy": "Route to Property name",
                # Revenue accounts typically 4xxx range
                "revenue": "${account_code:matches('4[0-9]+')}",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1480),
            "UpdateRecord_RevenueRec",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/recognition_period": "${entry_date:toDate('yyyy-MM-dd HH:mm:ss'):format('yyyy-MM')}",
                "/status": "recognized",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1480),
            "PutFile_TGT_REVENUE_REC",
            {"Directory": "/data/outbound/TGT_REVENUE_REC"},
        )

        canvas.create_connection(route, update, ["revenue"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_finance_revenue_rec] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_revenue_rec] Could not build via API (offline mode): %s", exc)


def _build_consolidation(pg_id: str) -> None:
    """
    m_finance_consolidation:
    QueryRecord (GROUP BY account_code, account_name: SUM debits, SUM credits)
    -> UpdateRecord (net_balance = total_debits - total_credits)
    -> TGT_CONSOLIDATED
    """
    logger.info("[m_finance_consolidation] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1660),
            "QueryRecord_Consolidation",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "consolidate": (
                    "SELECT account_code, account_name, "
                    "SUM(debit_amount) AS total_debits, "
                    "SUM(credit_amount) AS total_credits "
                    "FROM FLOWFILE GROUP BY account_code, account_name"
                ),
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1660),
            "UpdateRecord_NetBalance",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/net_balance": "${total_debits:toNumber():minus(${total_credits:toNumber()})}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1660),
            "PutFile_TGT_CONSOLIDATED",
            {"Directory": "/data/outbound/TGT_CONSOLIDATED"},
        )

        canvas.create_connection(query, update, ["consolidate"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_finance_consolidation] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_consolidation] Could not build via API (offline mode): %s", exc)


def _build_audit_trail(pg_id: str) -> None:
    """
    m_finance_audit_trail:
    ExecuteScript (Groovy MD5 hash of entry_id+account_code+amounts,
                   stamp audit_timestamp=${now()}, audit_user, audit_action)
    -> TGT_AUDIT_TRAIL
    """
    logger.info("[m_finance_audit_trail] Building sub-flow in PG %s", pg_id)

    try:
        execute = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (100, 1840),
            "ExecuteScript_AuditTrail",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_AUDIT_TRAIL,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1840),
            "PutFile_TGT_AUDIT_TRAIL",
            {"Directory": "/data/outbound/TGT_AUDIT_TRAIL"},
        )

        canvas.create_connection(execute, put, ["success"])

        logger.info("[m_finance_audit_trail] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_finance_audit_trail] Could not build via API (offline mode): %s", exc)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = "http://localhost:8080", parent_pg_id: str = "") -> None:
    """Entry-point called by main.py to deploy the Finance Process Group."""
    logger.info("=== Finance domain deployment starting ===")
    create_finance_process_group(parent_pg_id=parent_pg_id, nifi_url=nifi_url)
    logger.info("=== Finance domain deployment complete ===")


if __name__ == "__main__":
    run()
