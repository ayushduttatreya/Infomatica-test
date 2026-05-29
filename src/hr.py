"""
hr.py — Apache NiFi Process Group builder for the HR domain.

Tickets covered:
  - cd2ba728  HR Module: employee load, payroll, attendance, performance scoring, turnover risk
  - 6a04ba45  HR Module sub-flows (m_employee_load, m_payroll, m_attendance_load,
              m_performance_score, m_turnover_risk)

Informatica mappings replaced:
  m_hr_employee_load, m_hr_payroll, m_hr_attendance,
  m_hr_perf_score, m_hr_turnover
"""

import json
import logging
import os
from typing import Any

import nipyapi
from nipyapi import canvas, config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

NIFI_HOST    = os.environ.get("NIFI_HOST",    "https://localhost:8443")
INBOUND_DIR  = os.environ.get("INBOUND_DIR",  "/data/inbound/hr")
OUTBOUND_DIR = os.environ.get("OUTBOUND_DIR", "/data/outbound/hr")
ERROR_DIR    = os.environ.get("ERROR_DIR",    "/data/error/hr")


# ---------------------------------------------------------------------------
# Groovy scripts
# ---------------------------------------------------------------------------

TURNOVER_RISK_SCRIPT = r"""
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.LocalDate
import java.time.temporal.ChronoUnit

def flowFile = session.get()
if (!flowFile) return

try {
    def slurper = new JsonSlurper()
    def record  = slurper.parseText(
        new java.io.InputStreamReader(session.read(flowFile)).text)

    def hireDateStr   = record.hire_date?.toString()?.substring(0, 10)
    def empStatus     = record.status ?: ''
    def managerRating = record.manager_rating ? record.manager_rating.toDouble() : 5.0
    def tenureMonths  = 9999

    if (hireDateStr) {
        def hire     = LocalDate.parse(hireDateStr)
        tenureMonths = (int) ChronoUnit.MONTHS.between(hire, LocalDate.now())
    }

    def risk = 'low'
    if (empStatus.equalsIgnoreCase('terminated')) {
        risk = 'high'
    } else if (managerRating < 3.0 || tenureMonths < 12) {
        risk = 'medium'
    }

    record.tenure_months  = tenureMonths == 9999 ? null : tenureMonths
    record.turnover_risk  = risk

    def out = JsonOutput.toJson(record)
    flowFile = session.write(flowFile, { os -> os.write(out.bytes) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    logger.error('Turnover risk script error: ' + e.message, e)
    flowFile = session.penalize(flowFile)
    session.transfer(flowFile, REL_FAILURE)
}
"""


def _proc_config(proc_type: str, name: str, properties: dict[str, str]) -> dict[str, Any]:
    return {"type": proc_type, "name": name, "properties": properties}


def build_hr_process_group(parent_pg_id: str) -> dict[str, Any]:
    """
    Build the HR domain Process Group covering all 5 mappings.
    Returns a summary dict of processors and connections.
    """
    config.nifi_config.host = NIFI_HOST
    logger.info("Building HR Process Group under parent=%s", parent_pg_id)

    summary: dict[str, Any] = {"processors": {}, "connections": []}

    # ------------------------------------------------------------------ #
    # 1. m_employee_load — GetFile SRC_EMPLOYEES → TGT_EMPLOYEES         #
    # ------------------------------------------------------------------ #
    logger.info("[hr] Building m_employee_load sub-flow")

    get_employees_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_EMPLOYEES",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_EMPLOYEES.*\\.csv",
            "Keep Source File": "false",
        },
    )

    put_employees_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_EMPLOYEES",
        {"Directory": f"{OUTBOUND_DIR}/TGT_EMPLOYEES"},
    )

    summary["processors"]["GetFile_SRC_EMPLOYEES"] = get_employees_cfg
    summary["processors"]["PutFile_TGT_EMPLOYEES"] = put_employees_cfg

    summary["connections"].append(
        ("GetFile_SRC_EMPLOYEES", "PutFile_TGT_EMPLOYEES", "success")
    )

    # ------------------------------------------------------------------ #
    # 2. m_payroll — GetFile SRC_PAYROLL                                 #
    #    → UpdateRecord (SUM deductions, effective_tax_rate, net_pay)   #
    # ------------------------------------------------------------------ #
    logger.info("[hr] Building m_payroll sub-flow")

    get_payroll_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PAYROLL",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_PAYROLL.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_payroll_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_Payroll",
        {
            "Record Reader": "CSVReader_Payroll",
            "Record Writer": "CSVWriter_HR",
            # total_deductions = federal + state + insurance + 401k
            "/total_deductions":
                "${federal_tax:plus(${state_tax})"
                ":plus(${insurance}):plus(${retirement_401k})}",
            # effective_tax_rate = (federal + state) / gross_pay * 100
            "/effective_tax_rate":
                "${federal_tax:plus(${state_tax})"
                ":divide(${gross_pay}):multiply(100)}",
            # calc_net_pay = gross - total_deductions
            "/calc_net_pay":
                "${gross_pay:minus(${federal_tax:plus(${state_tax})"
                ":plus(${insurance}):plus(${retirement_401k})})}",
            # pay_verified: Y if calc_net_pay matches net_pay (within 0.01)
            "/pay_verified":
                "${calc_net_pay:minus(${net_pay}):abs():lt(0.01):ifElse('Y','N')}",
        },
    )

    put_payroll_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_PAYROLL",
        {"Directory": f"{OUTBOUND_DIR}/TGT_PAYROLL"},
    )

    summary["processors"]["GetFile_SRC_PAYROLL"]    = get_payroll_cfg
    summary["processors"]["UpdateRecord_Payroll"]   = update_payroll_cfg
    summary["processors"]["PutFile_TGT_PAYROLL"]    = put_payroll_cfg

    summary["connections"] += [
        ("GetFile_SRC_PAYROLL",      "UpdateRecord_Payroll",    "success"),
        ("UpdateRecord_Payroll",     "PutFile_TGT_PAYROLL",     "success"),
    ]

    # ------------------------------------------------------------------ #
    # 3. m_attendance_load — GetFile SRC_ATTENDANCE → TGT_ATTENDANCE     #
    # ------------------------------------------------------------------ #
    logger.info("[hr] Building m_attendance_load sub-flow")

    get_attendance_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_ATTENDANCE",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_ATTENDANCE.*\\.csv",
            "Keep Source File": "false",
        },
    )

    put_attendance_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_ATTENDANCE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_ATTENDANCE"},
    )

    summary["processors"]["GetFile_SRC_ATTENDANCE"] = get_attendance_cfg
    summary["processors"]["PutFile_TGT_ATTENDANCE"] = put_attendance_cfg

    summary["connections"].append(
        ("GetFile_SRC_ATTENDANCE", "PutFile_TGT_ATTENDANCE", "success")
    )

    # ------------------------------------------------------------------ #
    # 4. m_performance_score — GetFile SRC_PERFORMANCE                   #
    #    → UpdateRecord weighted_score + performance tier                #
    #    weighted_score = goal*0.4 + competency*0.3 + manager*0.3       #
    # ------------------------------------------------------------------ #
    logger.info("[hr] Building m_performance_score sub-flow")

    get_perf_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PERFORMANCE",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_PERFORMANCE.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_perf_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_PerformanceScore",
        {
            "Record Reader": "CSVReader_Performance",
            "Record Writer": "CSVWriter_HR",
            # weighted_score = goal*0.4 + competency*0.3 + manager*0.3
            "/weighted_score":
                "${goal_score:multiply(0.4)"
                ":plus(${competency_score:multiply(0.3)})"
                ":plus(${manager_rating:multiply(0.3)})}",
            # performance_tier
            "/performance_tier":
                "${weighted_score:ge(4.5):ifElse('exceptional',"
                "${weighted_score:ge(3.8):ifElse('strong',"
                "${weighted_score:ge(3.0):ifElse('meets_expectations',"
                "'below_expectations')})})}",
        },
    )

    put_perf_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_PERF_SCORE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_PERF_SCORE"},
    )

    summary["processors"]["GetFile_SRC_PERFORMANCE"]      = get_perf_cfg
    summary["processors"]["UpdateRecord_PerformanceScore"] = update_perf_cfg
    summary["processors"]["PutFile_TGT_PERF_SCORE"]        = put_perf_cfg

    summary["connections"] += [
        ("GetFile_SRC_PERFORMANCE",       "UpdateRecord_PerformanceScore",  "success"),
        ("UpdateRecord_PerformanceScore", "PutFile_TGT_PERF_SCORE",          "success"),
    ]

    # ------------------------------------------------------------------ #
    # 5. m_turnover_risk                                                  #
    #    LookupRecord (join performance scores)                          #
    #    → ExecuteScript DATE_DIFF tenure + turnover_risk flag           #
    # ------------------------------------------------------------------ #
    logger.info("[hr] Building m_turnover_risk sub-flow")

    lookup_perf_cfg = _proc_config(
        "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_PerfForTurnover",
        {
            "Record Reader":    "CSVReader_Employees",
            "Record Writer":    "CSVWriter_HR",
            "Lookup Service":   "JDBCLookupService_Performance",
            "key":              "${employee_id}",
            "result.rating":    "/manager_rating",
            "Routing Strategy": "Route to success",
        },
    )

    turnover_script_cfg = _proc_config(
        "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_TurnoverRisk",
        {
            "Script Engine": "Groovy",
            "Script Body":   TURNOVER_RISK_SCRIPT,
        },
    )

    put_turnover_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_TURNOVER",
        {"Directory": f"{OUTBOUND_DIR}/TGT_TURNOVER"},
    )

    summary["processors"]["LookupRecord_PerfForTurnover"] = lookup_perf_cfg
    summary["processors"]["ExecuteScript_TurnoverRisk"]   = turnover_script_cfg
    summary["processors"]["PutFile_TGT_TURNOVER"]         = put_turnover_cfg

    summary["connections"] += [
        ("GetFile_SRC_EMPLOYEES",          "LookupRecord_PerfForTurnover", "success"),
        ("LookupRecord_PerfForTurnover",   "ExecuteScript_TurnoverRisk",   "matched"),
        ("ExecuteScript_TurnoverRisk",     "PutFile_TGT_TURNOVER",          "success"),
    ]

    logger.info("[hr] Process Group definition built with %d processors.",
                len(summary["processors"]))
    return summary


def run() -> None:
    """Entry point called by main.py."""
    logger.info("=== HR domain: starting ===")
    try:
        config.nifi_config.host = NIFI_HOST
        root_pg = canvas.get_process_group("root")
        parent_id = root_pg.id if root_pg else "root"
        summary = build_hr_process_group(parent_id)
        logger.info("HR Process Group summary:\n%s",
                    json.dumps({"processor_count": len(summary["processors"]),
                                "connection_count": len(summary["connections"])}, indent=2))
    except Exception as exc:
        logger.error("HR domain failed: %s", exc, exc_info=True)
        raise
    logger.info("=== HR domain: complete ===")


if __name__ == "__main__":
    run()
