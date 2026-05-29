"""
src/hr_operations.py
HR and Operations domain — PG_HR_Operations — 10 mappings migrated from Informatica to NiFi.

Mappings covered (ticket: Migrate HR and Operations domain flows):
  1.  m_hr_employee_load        → TGT_EMPLOYEES     (passthrough)
  2.  m_hr_payroll_calc         → TGT_PAYROLL       (net pay calculation)
  3.  m_hr_attendance_load      → TGT_ATTENDANCE    (passthrough)
  4.  m_hr_performance_score    → TGT_PERF_SCORE    (weighted scoring + tier)
  5.  m_hr_turnover_analysis    → TGT_TURNOVER      (LookupRecord + DATE_DIFF)
  6.  m_ops_shipping_load       → TGT_SHIPPING      (passthrough)
  7.  m_ops_delivery_sla        → TGT_DELIVERY      (transit_days + SLA flag)
  8.  m_ops_warehouse_reconcile → TGT_WH_RECONCILE  (accuracy_pct + needs_recount)
  9.  m_ops_quality_grade       → TGT_QUALITY       (defect_rate + grade)
  10. m_ops_vendor_scorecard    → TGT_VENDOR_SCORE  (composite score + tier)
"""

import logging
from datetime import date, datetime

import nipyapi
import nipyapi.nifi as nifi_api

from src.utils import (
    connect_nifi,
    date_diff_days,
    date_diff_months,
    get_or_create_pg,
    get_root_pg_id,
    performance_tier,
    quality_grade,
    turnover_risk,
    vendor_tier,
)

logger = logging.getLogger("xclarity_etl.hr_operations")

SOURCE_DATA_PATH_HR = "/opt/nifi/source_data/hr"
SOURCE_DATA_PATH_OPS = "/opt/nifi/source_data/hr"
OUTPUT_DATA_PATH = "/opt/nifi/output_data/hr"


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

def transform_employee_load(row: dict) -> dict:
    """m_hr_employee_load: passthrough employee master data."""
    return {
        "employee_id": row.get("employee_id"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "department": row.get("department"),
        "status": row.get("status"),
    }


def transform_payroll_calc(row: dict) -> dict:
    """
    m_hr_payroll_calc: derive total_deductions, effective_tax_rate, calc_net_pay.
    Equivalent of Informatica Expression transformation.
    """
    gross = float(row.get("gross_pay") or 0)
    fed_tax = float(row.get("federal_tax") or 0)
    state_tax = float(row.get("state_tax") or 0)
    insurance = float(row.get("insurance") or 0)
    retirement = float(row.get("retirement_401k") or 0)
    total_deductions = round(fed_tax + state_tax + insurance + retirement, 2)
    eff_tax_rate = round((fed_tax + state_tax) / gross * 100, 2) if gross else 0.0
    calc_net_pay = round(gross - total_deductions, 2)
    return {
        "payroll_id": row.get("payroll_id"),
        "employee_id": row.get("employee_id"),
        "gross_pay": gross,
        "total_deductions": total_deductions,
        "effective_tax_rate": eff_tax_rate,
        "calc_net_pay": calc_net_pay,
    }


def transform_attendance_load(row: dict) -> dict:
    """m_hr_attendance_load: passthrough attendance records."""
    return {
        "record_id": row.get("record_id"),
        "employee_id": row.get("employee_id"),
        "date": row.get("date"),
        "hours_worked": row.get("hours_worked"),
        "status": row.get("status"),
    }


def transform_performance_score(row: dict) -> dict:
    """
    m_hr_performance_score: weighted_score = goal*0.4 + competency*0.3 + manager*0.3.
    Tier: exceptional >=4.5, strong >=3.8, meets >=3.0, below.
    """
    goal = float(row.get("goal_score") or 0)
    comp = float(row.get("competency_score") or 0)
    mgr = float(row.get("manager_rating") or 0)
    weighted = round(goal * 0.4 + comp * 0.3 + mgr * 0.3, 2)
    return {
        "review_id": row.get("review_id"),
        "employee_id": row.get("employee_id"),
        "weighted_score": weighted,
        "performance_tier": performance_tier(weighted),
    }


def transform_turnover_risk(emp_row: dict, perf_score: float) -> dict:
    """
    m_hr_turnover_analysis: tenure_months via DATE_DIFF + performance lookup.
    high=terminated, medium=low score/<12m tenure, low otherwise.
    """
    hire_date = emp_row.get("hire_date") or ""
    try:
        tenure_months = date_diff_months(date.today(), hire_date)
    except Exception:
        tenure_months = 0
    status = emp_row.get("status") or ""
    risk = turnover_risk(status, perf_score, tenure_months)
    return {
        "employee_id": emp_row.get("employee_id"),
        "tenure_months": tenure_months,
        "weighted_score": perf_score,
        "turnover_risk": risk,
    }


def transform_shipping_load(row: dict) -> dict:
    """m_ops_shipping_load: passthrough shipping records."""
    return {
        "shipment_id": row.get("shipment_id"),
        "order_id": row.get("order_id"),
        "carrier": row.get("carrier"),
        "ship_date": row.get("ship_date"),
        "status": row.get("status"),
    }


def transform_delivery_sla(row: dict) -> dict:
    """
    m_ops_delivery_sla: transit_days = DATE_DIFF(delivery_date, ship_date, 'D').
    sla_met = transit_days <= 5.
    delivery_status from current status + sla_met flag.
    """
    ship = row.get("ship_date") or ""
    delivery = row.get("delivery_date") or ""
    try:
        transit_days = date_diff_days(delivery, ship)
    except Exception:
        transit_days = None
    status = (row.get("status") or "").lower()
    sla_met = (transit_days is not None and transit_days <= 5)
    if "delivered" in status:
        delivery_status = "delivered_on_time" if sla_met else "delivered_late"
    else:
        delivery_status = "in_transit"
    return {
        "shipment_id": row.get("shipment_id"),
        "transit_days": transit_days,
        "sla_met": sla_met,
        "delivery_status": delivery_status,
    }


def transform_wh_reconcile(row: dict) -> dict:
    """m_ops_warehouse_reconcile: accuracy_pct + needs_recount flag."""
    qty = int(row.get("quantity") or 0)
    disc = abs(int(row.get("discrepancy") or 0))
    accuracy_pct = round((1 - disc / qty) * 100, 2) if qty else 0.0
    return {
        "record_id": row.get("record_id"),
        "abs_discrepancy": disc,
        "accuracy_pct": accuracy_pct,
        "needs_recount": disc > 1,
    }


def transform_quality_grade_calc(row: dict) -> dict:
    """m_ops_quality_grade: defect_rate + quality_grade A/B/C/F."""
    defects = int(row.get("defect_count") or 0)
    batch = int(row.get("batch_size") or 1)
    pass_rate = float(row.get("pass_rate") or 0)
    defect_rate = round(defects / batch * 100, 2) if batch else 0.0
    return {
        "metric_id": row.get("metric_id"),
        "product_id": row.get("product_id"),
        "defect_rate": defect_rate,
        "quality_grade": quality_grade(pass_rate),
    }


def transform_vendor_score(row: dict) -> dict:
    """
    m_ops_vendor_scorecard: composite_score = on_time*0.4 + quality*0.4 + norm_spend*0.2.
    Tier: preferred >=80, approved >=60, probationary.
    """
    on_time = float(row.get("on_time_delivery_pct") or 0)
    quality = float(row.get("quality_score") or 0) * 10  # scale 0-5 → 0-50 → pct
    total_spend = float(row.get("total_spend") or 0)
    # Normalise spend to 0-100 using a max cap of 1,000,000
    norm_spend = min(total_spend / 10000, 100)
    composite = round(on_time * 0.4 + quality * 0.4 + norm_spend * 0.2, 2)
    return {
        "vendor_id": row.get("vendor_id"),
        "composite_score": composite,
        "vendor_tier": vendor_tier(composite),
    }


# ---------------------------------------------------------------------------
# NiFi flow builders
# ---------------------------------------------------------------------------

def build_m_hr_employee_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_hr_employee_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_EMPLOYEES",
        {"Input Directory": SOURCE_DATA_PATH_HR, "File Filter": "employees*.csv",
         "Keep Source File": "true"}, (0, 0))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_EMPLOYEES",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_employees"}, (200, 0))
    _connect(pg_id, gf.id, put.id)


def build_m_hr_payroll_calc(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_hr_payroll_calc sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PAYROLL",
        {"Input Directory": SOURCE_DATA_PATH_HR, "File Filter": "payroll*.csv",
         "Keep Source File": "true"}, (0, 200))
    payroll_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        gross = float(row.get('gross_pay') or 0)
        fed = float(row.get('federal_tax') or 0)
        state = float(row.get('state_tax') or 0)
        ins = float(row.get('insurance') or 0)
        ret = float(row.get('retirement_401k') or 0)
        deductions = round(fed + state + ins + ret, 2)
        eff_rate = round((fed + state) / gross * 100, 2) if gross else 0.0
        out.append({'payroll_id': row.get('payroll_id'),
                    'employee_id': row.get('employee_id'),
                    'gross_pay': gross,
                    'total_deductions': deductions,
                    'effective_tax_rate': eff_rate,
                    'calc_net_pay': round(gross - deductions, 2)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_PayrollCalc",
        {"Script Engine": "python", "Script Body": payroll_script}, (200, 200))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PAYROLL",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_payroll"}, (400, 200))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_hr_attendance_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_hr_attendance_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_ATTENDANCE",
        {"Input Directory": SOURCE_DATA_PATH_HR, "File Filter": "attendance*.csv",
         "Keep Source File": "true"}, (0, 400))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_ATTENDANCE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_attendance"}, (200, 400))
    _connect(pg_id, gf.id, put.id)


def build_m_hr_performance_score(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_hr_performance_score sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PERFORMANCE",
        {"Input Directory": SOURCE_DATA_PATH_HR, "File Filter": "performance*.csv",
         "Keep Source File": "true"}, (0, 600))
    perf_script = r"""
import json
def tier(s):
    if s >= 4.5: return 'exceptional'
    if s >= 3.8: return 'strong'
    if s >= 3.0: return 'meets'
    return 'below'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        goal = float(row.get('goal_score') or 0)
        comp = float(row.get('competency_score') or 0)
        mgr = float(row.get('manager_rating') or 0)
        ws = round(goal*0.4 + comp*0.3 + mgr*0.3, 2)
        out.append({'review_id': row.get('review_id'),
                    'employee_id': row.get('employee_id'),
                    'weighted_score': ws, 'performance_tier': tier(ws)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_PerfScore",
        {"Script Engine": "python", "Script Body": perf_script}, (200, 600))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PERF_SCORE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_perf_score"}, (400, 600))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_hr_turnover_analysis(pg_id, csv_reader_id, db_pool_id, db_lookup_id):
    logger.info("Building m_hr_turnover_analysis sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_TURN_SRC_EMPLOYEES",
        {"Input Directory": SOURCE_DATA_PATH_HR, "File Filter": "employees*.csv",
         "Keep Source File": "true"}, (0, 800))
    lookup = _add_processor(pg_id, "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_PerfScore",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Lookup Service": db_lookup_id,
         "Result RecordPath": "/weighted_score",
         "employee_id": "/employee_id"}, (200, 800))
    turnover_script = r"""
import json
from datetime import date, datetime
def risk(status, score, tenure):
    if (status or '').lower() == 'terminated': return 'high'
    if score < 3.0 or tenure < 12: return 'medium'
    return 'low'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        hire = row.get('hire_date', '')
        try:
            d = datetime.strptime(str(hire)[:10], '%Y-%m-%d').date()
            tenure = (date.today().year - d.year)*12 + (date.today().month - d.month)
        except:
            tenure = 0
        score = float(row.get('weighted_score') or 0)
        out.append({'employee_id': row.get('employee_id'),
                    'tenure_months': tenure,
                    'weighted_score': score,
                    'turnover_risk': risk(row.get('status'), score, tenure)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_TurnoverRisk",
        {"Script Engine": "python", "Script Body": turnover_script}, (400, 800))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_TURNOVER",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_turnover"}, (600, 800))
    _connect(pg_id, gf.id, lookup.id)
    _connect(pg_id, lookup.id, execute.id, "matched")
    _connect(pg_id, lookup.id, execute.id, "unmatched")
    _connect(pg_id, execute.id, put.id)


def build_m_ops_shipping_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_ops_shipping_load sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SHIPPING",
        {"Input Directory": SOURCE_DATA_PATH_OPS, "File Filter": "shipping*.csv",
         "Keep Source File": "true"}, (0, 1000))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_SHIPPING",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_shipping"}, (200, 1000))
    _connect(pg_id, gf.id, put.id)


def build_m_ops_delivery_sla(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_ops_delivery_sla sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SLA_SRC_SHIPPING",
        {"Input Directory": SOURCE_DATA_PATH_OPS, "File Filter": "shipping*.csv",
         "Keep Source File": "true"}, (0, 1200))
    sla_script = r"""
import json
from datetime import datetime
def transit(ship, delivery):
    try:
        d1 = datetime.strptime(str(delivery)[:10], '%Y-%m-%d').date()
        d2 = datetime.strptime(str(ship)[:10], '%Y-%m-%d').date()
        return (d1 - d2).days
    except: return None
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        days = transit(row.get('ship_date'), row.get('delivery_date'))
        sla = days is not None and days <= 5
        status = (row.get('status') or '').lower()
        if 'delivered' in status:
            ds = 'delivered_on_time' if sla else 'delivered_late'
        else:
            ds = 'in_transit'
        out.append({'shipment_id': row.get('shipment_id'),
                    'transit_days': days, 'sla_met': sla, 'delivery_status': ds})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_DeliverySLA",
        {"Script Engine": "python", "Script Body": sla_script}, (200, 1200))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_DELIVERY",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_delivery"}, (400, 1200))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_ops_warehouse_reconcile(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_ops_warehouse_reconcile sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_WAREHOUSE_INVENTORY",
        {"Input Directory": SOURCE_DATA_PATH_OPS, "File Filter": "warehouse_inventory*.csv",
         "Keep Source File": "true"}, (0, 1400))
    wh_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        qty = int(row.get('quantity') or 0)
        disc = abs(int(row.get('discrepancy') or 0))
        acc = round((1 - disc / qty) * 100, 2) if qty else 0.0
        out.append({'record_id': row.get('record_id'),
                    'abs_discrepancy': disc,
                    'accuracy_pct': acc,
                    'needs_recount': disc > 1})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_WHReconcile",
        {"Script Engine": "python", "Script Body": wh_script}, (200, 1400))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_WH_RECONCILE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_wh_reconcile"}, (400, 1400))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_ops_quality_grade(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_ops_quality_grade sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_QUALITY_METRICS",
        {"Input Directory": SOURCE_DATA_PATH_OPS, "File Filter": "quality_metrics*.csv",
         "Keep Source File": "true"}, (0, 1600))
    qg_script = r"""
import json
def grade(pass_rate):
    if pass_rate >= 99.5: return 'A'
    if pass_rate >= 98.0: return 'B'
    if pass_rate >= 95.0: return 'C'
    return 'F'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        defects = int(row.get('defect_count') or 0)
        batch = int(row.get('batch_size') or 1)
        pass_rate = float(row.get('pass_rate') or 0)
        defect_rate = round(defects / batch * 100, 2) if batch else 0.0
        out.append({'metric_id': row.get('metric_id'),
                    'product_id': row.get('product_id'),
                    'defect_rate': defect_rate,
                    'quality_grade': grade(pass_rate)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_QualityGrade",
        {"Script Engine": "python", "Script Body": qg_script}, (200, 1600))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_QUALITY",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_quality"}, (400, 1600))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


def build_m_ops_vendor_scorecard(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_ops_vendor_scorecard sub-flow…")
    gf = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_VENDORS",
        {"Input Directory": SOURCE_DATA_PATH_OPS, "File Filter": "vendors*.csv",
         "Keep Source File": "true"}, (0, 1800))
    vendor_script = r"""
import json
def tier(score):
    if score >= 80: return 'preferred'
    if score >= 60: return 'approved'
    return 'probationary'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        on_time = float(row.get('on_time_delivery_pct') or 0)
        quality = float(row.get('quality_score') or 0) * 10  # 0-5 scale to 0-50
        spend = float(row.get('total_spend') or 0)
        norm_spend = min(spend / 10000, 100)
        composite = round(on_time * 0.4 + quality * 0.4 + norm_spend * 0.2, 2)
        out.append({'vendor_id': row.get('vendor_id'),
                    'composite_score': composite,
                    'vendor_tier': tier(composite)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_VendorScore",
        {"Script Engine": "python", "Script Body": vendor_script}, (200, 1800))
    put = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_VENDOR_SCORE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_vendor_score"}, (400, 1800))
    _connect(pg_id, gf.id, execute.id)
    _connect(pg_id, execute.id, put.id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    logger.info("=== hr_operations.py: building PG_HR_Operations (10 mappings) ===")
    connect_nifi()
    root_pg_id = get_root_pg_id()

    def _svc_id(name):
        svc = nipyapi.canvas.get_controller_service(name)
        return svc.id if svc else ""

    csv_reader_id = _svc_id("XClarity_CSVReader")
    db_pool_id = _svc_id("XClarity_DBCPConnectionPool")
    db_lookup_id = _svc_id("XClarity_DatabaseRecordLookupService")

    pg = get_or_create_pg("PG_HR_Operations", parent_pg_id=root_pg_id)
    pg_id = pg.id if hasattr(pg, "id") else str(pg)

    builders_no_lookup = [
        build_m_hr_employee_load,
        build_m_hr_payroll_calc,
        build_m_hr_attendance_load,
        build_m_hr_performance_score,
        build_m_ops_shipping_load,
        build_m_ops_delivery_sla,
        build_m_ops_warehouse_reconcile,
        build_m_ops_quality_grade,
        build_m_ops_vendor_scorecard,
    ]
    for builder in builders_no_lookup:
        try:
            builder(pg_id, csv_reader_id, db_pool_id)
        except Exception as exc:
            logger.error("%s failed: %s", builder.__name__, exc)

    try:
        build_m_hr_turnover_analysis(pg_id, csv_reader_id, db_pool_id, db_lookup_id)
    except Exception as exc:
        logger.error("build_m_hr_turnover_analysis failed: %s", exc)

    logger.info("PG_HR_Operations build complete.")
