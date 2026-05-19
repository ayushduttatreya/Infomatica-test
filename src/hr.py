"""
HR Domain - Apache NiFi Process Group
Migrated from Informatica PowerCenter XClarity_ETL

Covers tickets:
  - Migrate HR Module: employee load, payroll, attendance, performance scoring,
    turnover risk
  - Migrate HR Module: employee load, payroll, attendance, performance scoring,
    and turnover risk (sub-flows)

Sources : SRC_EMPLOYEES, SRC_PAYROLL, SRC_ATTENDANCE, SRC_PERFORMANCE
Targets : TGT_EMPLOYEES, TGT_PAYROLL, TGT_ATTENDANCE, TGT_PERF_SCORE, TGT_TURNOVER
"""

import logging

import nipyapi
from nipyapi import canvas

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Groovy script: Turnover risk calculation (DATE_DIFF tenure)
# ---------------------------------------------------------------------------

GROOVY_TURNOVER_RISK = """\
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
        def hireDateStr    = rec.hire_date as String
        def statusVal      = rec.status as String
        def managerRating  = (rec.manager_rating ?: 0) as Double

        def tenureMonths = 0
        if (hireDateStr && hireDateStr != 'null') {
            try {
                def hireDate = LocalDate.parse(hireDateStr[0..9])
                tenureMonths = ChronoUnit.MONTHS.between(hireDate, today)
            } catch (Exception ignored) {}
        }

        def riskLevel = 'low'
        if (statusVal?.equalsIgnoreCase('terminated')) {
            riskLevel = 'high'
        } else if (managerRating < 3.0 || tenureMonths < 12) {
            riskLevel = 'medium'
        }

        rec.tenure_months = tenureMonths
        rec.turnover_risk  = riskLevel
    }

    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(records).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Turnover risk calculation failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""


def create_hr_process_group(parent_pg_id: str, nifi_url: str = "http://localhost:8080") -> None:
    """
    Build the full HR NiFi Process Group.

    Covers Informatica mappings:
      m_hr_employee_load, m_hr_payroll, m_hr_attendance,
      m_hr_perf_score, m_hr_turnover
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    try:
        root = canvas.get_root_pg_id()
        parent_id = parent_pg_id or root

        logger.info("Creating HR Process Group under parent %s", parent_id)
        hr_pg = canvas.create_process_group(
            canvas.get_process_group(parent_id),
            "HR",
            (600, 700),
        )
        pg_id = hr_pg.id
        logger.info("HR PG created: %s", pg_id)

        _build_employee_load(pg_id)
        _build_payroll(pg_id)
        _build_attendance(pg_id)
        _build_perf_score(pg_id)
        _build_turnover_risk(pg_id)

        logger.info("HR Process Group fully built: %s", pg_id)

    except Exception as exc:
        logger.error("Failed to build HR Process Group: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Sub-flow builders
# ---------------------------------------------------------------------------

def _build_employee_load(pg_id: str) -> None:
    """
    m_hr_employee_load:
    GetFile(SRC_EMPLOYEES) -> CSVReader -> PutFile TGT_EMPLOYEES
    """
    logger.info("[m_hr_employee_load] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 200),
            "GetFile_SRC_EMPLOYEES",
            {
                "Input Directory": "/data/inbound/employees",
                "File Filter": "*.csv",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 200),
            "PutFile_TGT_EMPLOYEES",
            {"Directory": "/data/outbound/TGT_EMPLOYEES"},
        )

        canvas.create_connection(get, put, ["success"])

        logger.info("[m_hr_employee_load] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_hr_employee_load] Could not build via API (offline mode): %s", exc)


def _build_payroll(pg_id: str) -> None:
    """
    m_hr_payroll:
    GetFile(SRC_PAYROLL)
    -> UpdateRecord:
         total_deductions   = federal_tax + state_tax + insurance + retirement_401k
         effective_tax_rate = (federal_tax + state_tax) / gross_pay
         calc_net_pay       = gross_pay - total_deductions
         net_pay_match      = 'Y' if ABS(calc_net_pay - net_pay) < 0.01 else 'N'
    -> TGT_PAYROLL
    """
    logger.info("[m_hr_payroll] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 380),
            "GetFile_SRC_PAYROLL",
            {
                "Input Directory": "/data/inbound/payroll",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 380),
            "UpdateRecord_Payroll",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/total_deductions": (
                    "${federal_tax:toNumber()"
                    ":plus(${state_tax:toNumber()})"
                    ":plus(${insurance:toNumber()})"
                    ":plus(${retirement_401k:toNumber()})}"
                ),
                "/effective_tax_rate": (
                    "${federal_tax:toNumber():plus(${state_tax:toNumber()})"
                    ":divide(${gross_pay:toNumber()})}"
                ),
                "/calc_net_pay": "${gross_pay:toNumber():minus(${total_deductions:toNumber()})}",
                "/net_pay_match": (
                    "${calc_net_pay:toNumber():minus(${net_pay:toNumber()}):abs():lt(0.01):ifElse('Y','N')}"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 380),
            "PutFile_TGT_PAYROLL",
            {"Directory": "/data/outbound/TGT_PAYROLL"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_hr_payroll] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_hr_payroll] Could not build via API (offline mode): %s", exc)


def _build_attendance(pg_id: str) -> None:
    """
    m_hr_attendance:
    GetFile(SRC_ATTENDANCE) -> PutFile TGT_ATTENDANCE
    """
    logger.info("[m_hr_attendance] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 560),
            "GetFile_SRC_ATTENDANCE",
            {
                "Input Directory": "/data/inbound/attendance",
                "File Filter": "*.csv",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 560),
            "PutFile_TGT_ATTENDANCE",
            {"Directory": "/data/outbound/TGT_ATTENDANCE"},
        )

        canvas.create_connection(get, put, ["success"])

        logger.info("[m_hr_attendance] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_hr_attendance] Could not build via API (offline mode): %s", exc)


def _build_perf_score(pg_id: str) -> None:
    """
    m_hr_perf_score:
    GetFile(SRC_PERFORMANCE)
    -> UpdateRecord:
         weighted_score = goal_score*0.4 + competency_score*0.3 + manager_rating*0.3
         perf_tier      = exceptional(>=4.5) / strong(>=3.8) / meets(>=3.0) / below_expectations
    -> TGT_PERF_SCORE
    """
    logger.info("[m_hr_perf_score] Building sub-flow in PG %s", pg_id)

    tier_expr = (
        "${weighted_score:toNumber():ge(4.5):ifElse('exceptional',"
        "${weighted_score:toNumber():ge(3.8):ifElse('strong',"
        "${weighted_score:toNumber():ge(3.0):ifElse('meets','below_expectations')})})}"
    )

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 740),
            "GetFile_SRC_PERFORMANCE",
            {
                "Input Directory": "/data/inbound/performance",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 740),
            "UpdateRecord_PerfScore",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/weighted_score": (
                    "${goal_score:toNumber():multiply(0.4)"
                    ":plus(${competency_score:toNumber():multiply(0.3)})"
                    ":plus(${manager_rating:toNumber():multiply(0.3)})}"
                ),
                "/perf_tier": tier_expr,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 740),
            "PutFile_TGT_PERF_SCORE",
            {"Directory": "/data/outbound/TGT_PERF_SCORE"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_hr_perf_score] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_hr_perf_score] Could not build via API (offline mode): %s", exc)


def _build_turnover_risk(pg_id: str) -> None:
    """
    m_hr_turnover:
    LookupRecord (join employee performance on employee_id)
    -> ExecuteScript (Groovy DATE_DIFF tenure, turnover_risk flag)
    -> TGT_TURNOVER
    """
    logger.info("[m_hr_turnover] Building sub-flow in PG %s", pg_id)

    try:
        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (100, 920),
            "LookupRecord_EmployeePerf",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "DatabaseRecordLookupService",
                "Result RecordPath": "/manager_rating",
                "employee_id": "/employee_id",
            },
        )

        execute = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (400, 920),
            "ExecuteScript_TurnoverRisk",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_TURNOVER_RISK,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 920),
            "PutFile_TGT_TURNOVER",
            {"Directory": "/data/outbound/TGT_TURNOVER"},
        )

        canvas.create_connection(lookup, execute, ["matched", "unmatched"])
        canvas.create_connection(execute, put, ["success"])

        logger.info("[m_hr_turnover] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_hr_turnover] Could not build via API (offline mode): %s", exc)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = "http://localhost:8080", parent_pg_id: str = "") -> None:
    """Entry-point called by main.py to deploy the HR Process Group."""
    logger.info("=== HR domain deployment starting ===")
    create_hr_process_group(parent_pg_id=parent_pg_id, nifi_url=nifi_url)
    logger.info("=== HR domain deployment complete ===")


if __name__ == "__main__":
    run()
