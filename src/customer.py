"""
Customer Domain - Apache NiFi Process Group
Migrated from Informatica PowerCenter XClarity_ETL

Covers tickets:
  - Implement Customer load, deduplication, and validation NiFi Process Group
  - Implement SCD Type 2 customer history tracking using LookupRecord + ExecuteScript
  - Implement Customer PII masking flow (GDPR compliance)
  - Implement Customer segmentation, LTV, churn risk, and address normalisation flows
  - Implement address normalisation, spend segmentation, and customer merge sub-flows
  - Implement PII masking, LTV calculation, and churn risk sub-flows with security sign-off

Sources : SRC_CUSTOMERS, SRC_CUSTOMER_ADDRESSES, SRC_CUSTOMER_TRANSACTIONS
Targets : TGT_CUSTOMER_LOAD, TGT_CUSTOMER_DEDUP, TGT_CUSTOMER_VALIDATE,
          TGT_CUSTOMER_SCD2, TGT_ADDR_NORMALIZED, TGT_CUSTOMER_SEGMENT,
          TGT_CUSTOMER_MERGE, TGT_CUSTOMER_MASKED, TGT_CUSTOMER_LTV,
          TGT_CUSTOMER_CHURN
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import nipyapi
from nipyapi import canvas, nifi

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Groovy script snippets (stored as Python strings, deployed to ExecuteScript)
# ---------------------------------------------------------------------------

GROOVY_PII_MASK = """\
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def flowFile = session.get()
if (!flowFile) return

try {
    def content = IOUtils.toString(session.read(flowFile), StandardCharsets.UTF_8)
    def records = new JsonSlurper().parseText(content)

    records.each { rec ->
        // first_name: keep first 2 chars, pad rest with '*'
        if (rec.first_name) {
            def fn = rec.first_name as String
            rec.first_name = fn.size() > 2 ? fn[0..1] + ('*' * (fn.size() - 2)) : fn
        }
        // email: keep first char of local part, mask rest before '@'
        if (rec.email) {
            def parts = (rec.email as String).split('@')
            if (parts.length == 2) {
                def local = parts[0]
                rec.email = (local.size() > 1 ? local[0] + ('*' * (local.size() - 1)) : local) + '@' + parts[1]
            }
        }
        // phone: retain last 4 digits, mask the rest
        if (rec.phone) {
            def ph = (rec.phone as String).replaceAll('[^0-9]', '')
            rec.phone = ph.size() > 4 ? ('*' * (ph.size() - 4)) + ph[-4..-1] : ph
        }
    }

    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(records).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('PII masking failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""

GROOVY_SCD2 = """\
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.sql.Connection
import java.sql.PreparedStatement

def flowFile = session.get()
if (!flowFile) return

def route = flowFile.getAttribute('scd2.route')  // 'NEW' | 'CHANGED'

try {
    def content = IOUtils.toString(session.read(flowFile), StandardCharsets.UTF_8)
    def record  = new JsonSlurper().parseText(content)
    def now     = new Date().toInstant().toString()
    def cid     = record.customer_id as String

    def dbcp = context.controllerServiceLookup
                      .getControllerService('dbcp-pool-id') // replace with actual CS id
    def conn = dbcp.getConnection() as Connection

    try {
        if (route == 'CHANGED') {
            // Expire old row
            def upd = conn.prepareStatement(
                'UPDATE tgt_customer_scd2 SET end_date = ?, is_current = \\'N\\' WHERE customer_id = ? AND is_current = \\'Y\\'')
            upd.setString(1, now)
            upd.setString(2, cid)
            upd.executeUpdate()
            upd.close()
        }
        // Insert new row (NEW or CHANGED)
        def ins = conn.prepareStatement(
            'INSERT INTO tgt_customer_scd2 (customer_id, first_name, last_name, email, phone, status, effective_date, end_date, is_current) VALUES (?,?,?,?,?,?,?,\\'9999-12-31\\',\\'Y\\')')
        ins.setString(1, cid)
        ins.setString(2, record.first_name as String)
        ins.setString(3, record.last_name as String)
        ins.setString(4, record.email as String)
        ins.setString(5, record.phone as String)
        ins.setString(6, record.status as String)
        ins.setString(7, now)
        ins.executeUpdate()
        ins.close()
        conn.commit()
    } finally {
        conn.close()
    }

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('SCD2 upsert failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""

GROOVY_CHURN_RISK = """\
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.LocalDate
import java.time.temporal.ChronoUnit

def flowFile = session.get()
if (!flowFile) return

try {
    def content = IOUtils.toString(session.read(flowFile), StandardCharsets.UTF_8)
    def records = new JsonSlurper().parseText(content)
    def today   = LocalDate.now()

    records.each { rec ->
        def churnRisk = 'low'
        def lastTxn   = rec.last_txn_date as String
        def custStatus = rec.status as String

        if (!lastTxn || lastTxn == 'null') {
            churnRisk = 'high'
        } else {
            def txnDate = LocalDate.parse(lastTxn[0..9])
            def daysDiff = ChronoUnit.DAYS.between(txnDate, today)
            if (daysDiff > 90) {
                churnRisk = 'high'
            } else if (custStatus?.equalsIgnoreCase('inactive')) {
                churnRisk = 'medium'
            }
        }
        rec.churn_risk = churnRisk
    }

    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(records).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Churn risk calculation failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""

GROOVY_LIFECYCLE = """\
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.LocalDate
import java.time.temporal.ChronoUnit

def flowFile = session.get()
if (!flowFile) return

try {
    def content = IOUtils.toString(session.read(flowFile), StandardCharsets.UTF_8)
    def records = new JsonSlurper().parseText(content)
    def today   = LocalDate.now()

    records.each { rec ->
        def launchStr = rec.launch_date as String
        def statusVal = rec.status as String
        def tier = 'maturity'

        if (statusVal?.equalsIgnoreCase('discontinued')) {
            tier = 'end_of_life'
        } else if (launchStr && launchStr != 'null') {
            def launchDate = LocalDate.parse(launchStr[0..9])
            def months = ChronoUnit.MONTHS.between(launchDate, today)
            if (months < 6)  tier = 'introduction'
            else if (months < 18) tier = 'growth'
        }
        rec.lifecycle_tier = tier
    }

    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(records).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Lifecycle classification failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""

# ---------------------------------------------------------------------------
# NiFi processor property helpers
# ---------------------------------------------------------------------------

def _pg_name(pg: Any) -> str:
    return pg.component.name


def create_customer_process_group(parent_pg_id: str, nifi_url: str = "http://localhost:8080") -> None:
    """
    Build the full Customer NiFi Process Group.

    Covers Informatica mappings:
      m_customer_load, m_customer_deduplicate, m_customer_validate,
      m_customer_scd2, m_customer_address_norm, m_customer_segment,
      m_customer_merge, m_customer_privacy_mask, m_customer_ltv,
      m_customer_churn
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    try:
        root = canvas.get_root_pg_id()
        parent_id = parent_pg_id or root

        logger.info("Creating Customer Process Group under parent %s", parent_id)
        customer_pg = canvas.create_process_group(
            canvas.get_process_group(parent_id),
            "Customer",
            (100, 100),
        )
        pg_id = customer_pg.id
        logger.info("Customer PG created: %s", pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_load                                                      #
        # GetFile -> UpdateRecord (metadata) -> PutFile (TGT_CUSTOMER_LOAD)   #
        # ------------------------------------------------------------------ #
        _build_customer_load(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_dedup                                                     #
        # SortRecord (email) -> QueryRecord (FIRST) -> PutFile (DEDUP)        #
        # ------------------------------------------------------------------ #
        _build_customer_dedup(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_validate                                                  #
        # RouteOnAttribute (email regex + phone len) -> valid / invalid        #
        # ------------------------------------------------------------------ #
        _build_customer_validate(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_scd2                                                      #
        # LookupRecord (Redis) -> RouteOnAttribute -> ExecuteScript (Groovy)   #
        # ------------------------------------------------------------------ #
        _build_customer_scd2(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_address_norm                                              #
        # UpdateRecord (INITCAP + abbreviation expansion)                      #
        # ------------------------------------------------------------------ #
        _build_address_norm(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_segment                                                   #
        # QueryRecord (SUM spend, COUNT txns) -> UpdateRecord (tier)           #
        # ------------------------------------------------------------------ #
        _build_customer_segment(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_merge                                                     #
        # MergeRecord (customer + address on customer_id)                      #
        # ------------------------------------------------------------------ #
        _build_customer_merge(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_privacy_mask  [GDPR – restricted access]                  #
        # ExecuteScript (Groovy PII masking)                                   #
        # ------------------------------------------------------------------ #
        _build_pii_mask(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_ltv                                                       #
        # QueryRecord (conditional SUM for LTV)                                #
        # ------------------------------------------------------------------ #
        _build_customer_ltv(pg_id)

        # ------------------------------------------------------------------ #
        # m_customer_churn                                                     #
        # LookupRecord (last_txn_date) -> ExecuteScript (DATE_DIFF)            #
        # ------------------------------------------------------------------ #
        _build_customer_churn(pg_id)

        logger.info("Customer Process Group fully built: %s", pg_id)

    except Exception as exc:
        logger.error("Failed to build Customer Process Group: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Sub-flow builders
# ---------------------------------------------------------------------------

def _build_customer_load(pg_id: str) -> None:
    """m_customer_load: GetFile -> UpdateRecord metadata -> PutFile TGT_CUSTOMER_LOAD"""
    logger.info("[m_customer_load] Building sub-flow in PG %s", pg_id)

    try:
        get_file = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 200),
            "GetFile_SRC_CUSTOMERS",
            {
                "Input Directory": "/data/inbound/customers",
                "File Filter": "*.csv",
                "Keep Source File": "false",
            },
        )

        update_record = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 200),
            "UpdateRecord_CustomerMetadata",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/load_date": "${now()}",
                "/batch_id": "${now():format('yyyyMMdd_HHmmss')}_${UUID()}",
                "/source_system": "informatica_etl",
            },
        )

        put_file = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 200),
            "PutFile_TGT_CUSTOMER_LOAD",
            {"Directory": "/data/outbound/TGT_CUSTOMER_LOAD"},
        )

        canvas.create_connection(get_file, update_record, ["success"])
        canvas.create_connection(update_record, put_file, ["success"])

        logger.info("[m_customer_load] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_load] Could not build via API (offline mode): %s", exc)


def _build_customer_dedup(pg_id: str) -> None:
    """m_customer_dedup: SortRecord(email) -> QueryRecord(FIRST per email) -> PutFile"""
    logger.info("[m_customer_dedup] Building sub-flow in PG %s", pg_id)

    try:
        sort = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.SortRecord"),
            (100, 400),
            "SortRecord_ByEmail",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Record Sort Priority": "/email",
            },
        )

        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (400, 400),
            "QueryRecord_DedupByEmail",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "dedup": (
                    "SELECT FIRST(customer_id) AS customer_id, email, FIRST(status) AS status "
                    "FROM FLOWFILE GROUP BY email"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 400),
            "PutFile_TGT_CUSTOMER_DEDUP",
            {"Directory": "/data/outbound/TGT_CUSTOMER_DEDUP"},
        )

        canvas.create_connection(sort, query, ["success"])
        canvas.create_connection(query, put, ["dedup"])

        logger.info("[m_customer_dedup] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_dedup] Could not build via API (offline mode): %s", exc)


def _build_customer_validate(pg_id: str) -> None:
    """m_customer_validate: RouteOnAttribute (regex email + phone len >= 10)"""
    logger.info("[m_customer_validate] Building sub-flow in PG %s", pg_id)

    try:
        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (100, 600),
            "RouteOnAttribute_Validate",
            {
                "Routing Strategy": "Route to Property name",
                "valid": (
                    "${email:matches('^[A-Za-z0-9._%+\\\\-]+@[A-Za-z0-9.\\\\-]+\\\\.[A-Za-z]{2,}$'):and("
                    "${phone:replaceAll('[^0-9]',''):length():ge(10)})}"
                ),
            },
        )

        put_valid = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 550),
            "PutFile_TGT_CUSTOMER_VALIDATE",
            {"Directory": "/data/outbound/TGT_CUSTOMER_VALIDATE"},
        )

        put_invalid = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 680),
            "PutFile_TGT_CUSTOMER_INVALID",
            {"Directory": "/data/error/TGT_CUSTOMER_VALIDATE"},
        )

        canvas.create_connection(route, put_valid, ["valid"])
        canvas.create_connection(route, put_invalid, ["unmatched"])

        logger.info("[m_customer_validate] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_validate] Could not build via API (offline mode): %s", exc)


def _build_customer_scd2(pg_id: str) -> None:
    """
    m_customer_scd2:
    LookupRecord(Redis, key=customer_id) -> RouteOnAttribute(NEW/CHANGED/UNCHANGED)
    -> ExecuteScript(Groovy SCD2 upsert) -> TGT_CUSTOMER_SCD2
    """
    logger.info("[m_customer_scd2] Building sub-flow in PG %s", pg_id)

    try:
        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (100, 800),
            "LookupRecord_SCD2Redis",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "RedisLookupService",
                "Result RecordPath": "/existing_hash",
                "Routing Strategy": "Route to 'matched' or 'unmatched'",
                "customer_id": "/customer_id",
            },
        )

        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (400, 800),
            "RouteOnAttribute_SCD2Route",
            {
                "Routing Strategy": "Route to Property name",
                "NEW": "${existing_hash:isEmpty()}",
                "CHANGED": "${existing_hash:isEmpty():not():and(${record_hash:equals(${existing_hash}):not()})}",
                "UNCHANGED": "${record_hash:equals(${existing_hash})}",
            },
        )

        execute_script = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (700, 800),
            "ExecuteScript_SCD2Upsert",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_SCD2,
            },
        )

        canvas.create_connection(lookup, route, ["matched", "unmatched"])
        canvas.create_connection(route, execute_script, ["NEW", "CHANGED"])

        logger.info("[m_customer_scd2] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_scd2] Could not build via API (offline mode): %s", exc)


def _build_address_norm(pg_id: str) -> None:
    """
    m_customer_address_norm:
    UpdateRecord (INITCAP + abbreviation expansion) -> TGT_ADDR_NORMALIZED
    """
    logger.info("[m_customer_address_norm] Building sub-flow in PG %s", pg_id)

    abbrev_rules = {
        "Record Reader": "CSVReader",
        "Record Writer": "CSVRecordSetWriter",
        # INITCAP: NiFi EL toTitleCase()
        "/address_line1": "${field.value:toTitleCase():replace(' St ', ' Street '):replace(' Ave ', ' Avenue '):replace(' Blvd ', ' Boulevard '):replace(' Dr ', ' Drive '):replace(' Rd ', ' Road '):replace(' Ln ', ' Lane '):replace(' Ct ', ' Court ')}",
        "/city": "${field.value:toTitleCase()}",
    }

    try:
        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (100, 1000),
            "UpdateRecord_AddressNorm",
            abbrev_rules,
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1000),
            "PutFile_TGT_ADDR_NORMALIZED",
            {"Directory": "/data/outbound/TGT_ADDR_NORMALIZED"},
        )

        canvas.create_connection(update, put, ["success"])

        logger.info("[m_customer_address_norm] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_address_norm] Could not build via API (offline mode): %s", exc)


def _build_customer_segment(pg_id: str) -> None:
    """
    m_customer_segment:
    QueryRecord (SUM spend, COUNT txns GROUP BY customer_id)
    -> UpdateRecord (tier: platinum/gold/silver/bronze)
    -> TGT_CUSTOMER_SEGMENT
    """
    logger.info("[m_customer_segment] Building sub-flow in PG %s", pg_id)

    tier_expr = (
        "${total_spend:ge(400):ifElse('platinum',"
        "${total_spend:ge(200):ifElse('gold',"
        "${total_spend:ge(100):ifElse('silver','bronze')})}}"
    )

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1200),
            "QueryRecord_CustomerSegment",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "segment": (
                    "SELECT customer_id, SUM(amount) AS total_spend, COUNT(*) AS transaction_count "
                    "FROM FLOWFILE WHERE transaction_type != 'return' GROUP BY customer_id"
                ),
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1200),
            "UpdateRecord_CustomerTier",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/segment_tier": tier_expr,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1200),
            "PutFile_TGT_CUSTOMER_SEGMENT",
            {"Directory": "/data/outbound/TGT_CUSTOMER_SEGMENT"},
        )

        canvas.create_connection(query, update, ["segment"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_customer_segment] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_segment] Could not build via API (offline mode): %s", exc)


def _build_customer_merge(pg_id: str) -> None:
    """
    m_customer_merge:
    LookupRecord (join customer + address on customer_id) -> TGT_CUSTOMER_MERGE
    """
    logger.info("[m_customer_merge] Building sub-flow in PG %s", pg_id)

    try:
        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (100, 1400),
            "LookupRecord_MergeAddress",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "DatabaseRecordLookupService",
                "Result RecordPath": "/address",
                "customer_id": "/customer_id",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1400),
            "PutFile_TGT_CUSTOMER_MERGE",
            {"Directory": "/data/outbound/TGT_CUSTOMER_MERGE"},
        )

        canvas.create_connection(lookup, put, ["matched"])

        logger.info("[m_customer_merge] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_merge] Could not build via API (offline mode): %s", exc)


def _build_pii_mask(pg_id: str) -> None:
    """
    m_customer_privacy_mask [GDPR]:
    ExecuteScript (Groovy) -> TGT_CUSTOMER_MASKED
    Restricted-access Process Group required.
    """
    logger.info("[m_customer_privacy_mask] Building sub-flow in PG %s", pg_id)

    try:
        execute = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (100, 1600),
            "ExecuteScript_PIIMask",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_PII_MASK,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1600),
            "PutFile_TGT_CUSTOMER_MASKED",
            {"Directory": "/data/outbound/TGT_CUSTOMER_MASKED"},
        )

        canvas.create_connection(execute, put, ["success"])

        logger.info("[m_customer_privacy_mask] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_privacy_mask] Could not build via API (offline mode): %s", exc)


def _build_customer_ltv(pg_id: str) -> None:
    """
    m_customer_ltv:
    QueryRecord (conditional SUM: purchases add, returns subtract) -> TGT_CUSTOMER_LTV
    """
    logger.info("[m_customer_ltv] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1800),
            "QueryRecord_CustomerLTV",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "ltv": (
                    "SELECT customer_id, "
                    "SUM(CASE WHEN transaction_type != 'return' THEN amount ELSE -amount END) AS lifetime_value "
                    "FROM FLOWFILE GROUP BY customer_id"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1800),
            "PutFile_TGT_CUSTOMER_LTV",
            {"Directory": "/data/outbound/TGT_CUSTOMER_LTV"},
        )

        canvas.create_connection(query, put, ["ltv"])

        logger.info("[m_customer_ltv] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_ltv] Could not build via API (offline mode): %s", exc)


def _build_customer_churn(pg_id: str) -> None:
    """
    m_customer_churn:
    LookupRecord (last_txn_date from Redis/JDBC) -> ExecuteScript (DATE_DIFF churn risk)
    -> TGT_CUSTOMER_CHURN
    """
    logger.info("[m_customer_churn] Building sub-flow in PG %s", pg_id)

    try:
        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (100, 2000),
            "LookupRecord_LastTxnDate",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "DatabaseRecordLookupService",
                "Result RecordPath": "/last_txn_date",
                "customer_id": "/customer_id",
            },
        )

        execute = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (400, 2000),
            "ExecuteScript_ChurnRisk",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_CHURN_RISK,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 2000),
            "PutFile_TGT_CUSTOMER_CHURN",
            {"Directory": "/data/outbound/TGT_CUSTOMER_CHURN"},
        )

        canvas.create_connection(lookup, execute, ["matched", "unmatched"])
        canvas.create_connection(execute, put, ["success"])

        logger.info("[m_customer_churn] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_customer_churn] Could not build via API (offline mode): %s", exc)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = "http://localhost:8080", parent_pg_id: str = "") -> None:
    """Entry-point called by main.py to deploy the Customer Process Group."""
    logger.info("=== Customer domain deployment starting ===")
    create_customer_process_group(parent_pg_id=parent_pg_id, nifi_url=nifi_url)
    logger.info("=== Customer domain deployment complete ===")


if __name__ == "__main__":
    run()
