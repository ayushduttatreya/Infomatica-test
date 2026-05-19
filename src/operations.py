"""
operations.py — Apache NiFi Process Group builder for the Operations domain.

Tickets covered:
  - 7d039c8f  Operations Module: shipping, warehouse reconciliation,
              quality control, vendor scorecard
  - ed117368  Operations Module sub-flows (m_shipping_load, m_delivery_sla,
              m_warehouse_reconcile, m_quality_control, m_vendor_score)

Informatica mappings replaced:
  m_ops_shipping, m_ops_delivery_sla, m_ops_wh_reconcile,
  m_ops_quality, m_ops_vendor_score
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
INBOUND_DIR  = os.environ.get("INBOUND_DIR",  "/data/inbound/operations")
OUTBOUND_DIR = os.environ.get("OUTBOUND_DIR", "/data/outbound/operations")
ERROR_DIR    = os.environ.get("ERROR_DIR",    "/data/error/operations")


# ---------------------------------------------------------------------------
# Groovy scripts
# ---------------------------------------------------------------------------

DELIVERY_SLA_SCRIPT = r"""
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

    def shipDateStr     = record.ship_date?.toString()?.substring(0, 10)
    def deliveryDateStr = record.delivery_date?.toString()?.substring(0, 10)
    def transitDays     = null
    def slaMet          = 'N'
    def deliveryStatus  = record.status ?: 'in_transit'

    if (shipDateStr && deliveryDateStr) {
        def ship     = LocalDate.parse(shipDateStr)
        def delivery = LocalDate.parse(deliveryDateStr)
        transitDays  = (int) ChronoUnit.DAYS.between(ship, delivery)
        slaMet       = transitDays <= 5 ? 'Y' : 'N'
        deliveryStatus = slaMet == 'Y' ? 'delivered_on_time' : 'delivered_late'
    }

    record.transit_days = transitDays
    record.sla_met      = slaMet
    record.status       = deliveryStatus

    def out = JsonOutput.toJson(record)
    flowFile = session.write(flowFile, { os -> os.write(out.bytes) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    logger.error('Delivery SLA script error: ' + e.message, e)
    flowFile = session.penalize(flowFile)
    session.transfer(flowFile, REL_FAILURE)
}
"""


def _proc_config(proc_type: str, name: str, properties: dict[str, str]) -> dict[str, Any]:
    return {"type": proc_type, "name": name, "properties": properties}


def build_operations_process_group(parent_pg_id: str) -> dict[str, Any]:
    """
    Build the Operations domain Process Group covering all 5 mappings.
    Returns a summary dict of processors and connections.
    """
    config.nifi_config.host = NIFI_HOST
    logger.info("Building Operations Process Group under parent=%s", parent_pg_id)

    summary: dict[str, Any] = {"processors": {}, "connections": []}

    # ------------------------------------------------------------------ #
    # 1. m_shipping_load — GetFile SRC_SHIPPING → TGT_SHIPPING           #
    # ------------------------------------------------------------------ #
    logger.info("[operations] Building m_shipping_load sub-flow")

    get_shipping_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SHIPPING",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SHIPPING.*\\.csv",
            "Keep Source File": "false",
        },
    )

    put_shipping_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_SHIPPING",
        {"Directory": f"{OUTBOUND_DIR}/TGT_SHIPPING"},
    )

    summary["processors"]["GetFile_SRC_SHIPPING"] = get_shipping_cfg
    summary["processors"]["PutFile_TGT_SHIPPING"] = put_shipping_cfg

    summary["connections"].append(
        ("GetFile_SRC_SHIPPING", "PutFile_TGT_SHIPPING", "success")
    )

    # ------------------------------------------------------------------ #
    # 2. m_delivery_sla — ExecuteScript (DATE_DIFF transit_days)         #
    #    → sla_met flag + status → TGT_DELIVERY                         #
    # ------------------------------------------------------------------ #
    logger.info("[operations] Building m_delivery_sla sub-flow")

    delivery_script_cfg = _proc_config(
        "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_DeliverySLA",
        {
            "Script Engine": "Groovy",
            "Script Body":   DELIVERY_SLA_SCRIPT,
        },
    )

    put_delivery_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_DELIVERY",
        {"Directory": f"{OUTBOUND_DIR}/TGT_DELIVERY"},
    )

    summary["processors"]["ExecuteScript_DeliverySLA"] = delivery_script_cfg
    summary["processors"]["PutFile_TGT_DELIVERY"]       = put_delivery_cfg

    summary["connections"] += [
        ("GetFile_SRC_SHIPPING",       "ExecuteScript_DeliverySLA",  "success"),
        ("ExecuteScript_DeliverySLA",  "PutFile_TGT_DELIVERY",        "success"),
    ]

    # ------------------------------------------------------------------ #
    # 3. m_warehouse_reconcile — GetFile SRC_WAREHOUSE_INVENTORY         #
    #    → UpdateRecord abs_discrepancy, accuracy_pct, needs_recount    #
    # ------------------------------------------------------------------ #
    logger.info("[operations] Building m_warehouse_reconcile sub-flow")

    get_warehouse_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_WAREHOUSE_INVENTORY",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_WAREHOUSE_INVENTORY.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_wh_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_WHReconcile",
        {
            "Record Reader": "CSVReader_Warehouse",
            "Record Writer": "CSVWriter_Operations",
            # abs_discrepancy = ABS(discrepancy)
            "/abs_discrepancy":
                "${discrepancy:multiply(-1):max(${discrepancy})}",
            # accuracy_pct = (1 - abs_discrepancy / quantity) * 100
            "/accuracy_pct":
                "${quantity:minus(${discrepancy:multiply(-1):max(${discrepancy})})"
                ":divide(${quantity}):multiply(100)}",
            # needs_recount = abs_discrepancy > 1
            "/needs_recount":
                "${discrepancy:multiply(-1):max(${discrepancy}):gt(1):ifElse('Y','N')}",
        },
    )

    put_wh_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_WH_RECONCILE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_WH_RECONCILE"},
    )

    summary["processors"]["GetFile_SRC_WAREHOUSE_INVENTORY"] = get_warehouse_cfg
    summary["processors"]["UpdateRecord_WHReconcile"]         = update_wh_cfg
    summary["processors"]["PutFile_TGT_WH_RECONCILE"]         = put_wh_cfg

    summary["connections"] += [
        ("GetFile_SRC_WAREHOUSE_INVENTORY", "UpdateRecord_WHReconcile",  "success"),
        ("UpdateRecord_WHReconcile",         "PutFile_TGT_WH_RECONCILE",  "success"),
    ]

    # ------------------------------------------------------------------ #
    # 4. m_quality_control — GetFile SRC_QUALITY_METRICS                 #
    #    → UpdateRecord defect_rate + quality_grade A/B/C/F             #
    # ------------------------------------------------------------------ #
    logger.info("[operations] Building m_quality_control sub-flow")

    get_quality_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_QUALITY_METRICS",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_QUALITY_METRICS.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_quality_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_QualityGrade",
        {
            "Record Reader": "CSVReader_Quality",
            "Record Writer": "CSVWriter_Operations",
            # defect_rate = defect_count / batch_size * 100
            "/defect_rate":
                "${defect_count:divide(${batch_size}):multiply(100)}",
            # quality_grade: A(>=99.5%), B(>=98%), C(>=95%), F otherwise
            "/quality_grade":
                "${pass_rate:ge(99.5):ifElse('A',"
                "${pass_rate:ge(98):ifElse('B',"
                "${pass_rate:ge(95):ifElse('C','F')})})}",
        },
    )

    put_quality_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_QUALITY",
        {"Directory": f"{OUTBOUND_DIR}/TGT_QUALITY"},
    )

    summary["processors"]["GetFile_SRC_QUALITY_METRICS"] = get_quality_cfg
    summary["processors"]["UpdateRecord_QualityGrade"]   = update_quality_cfg
    summary["processors"]["PutFile_TGT_QUALITY"]         = put_quality_cfg

    summary["connections"] += [
        ("GetFile_SRC_QUALITY_METRICS",  "UpdateRecord_QualityGrade",  "success"),
        ("UpdateRecord_QualityGrade",    "PutFile_TGT_QUALITY",         "success"),
    ]

    # ------------------------------------------------------------------ #
    # 5. m_vendor_score — GetFile SRC_VENDORS                            #
    #    → UpdateRecord composite_score + vendor tier                    #
    #    composite = on_time*0.4 + quality*0.4 + spend_ratio*0.2        #
    # ------------------------------------------------------------------ #
    logger.info("[operations] Building m_vendor_score sub-flow")

    get_vendors_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_VENDORS",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_VENDORS.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_vendor_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_VendorScore",
        {
            "Record Reader": "CSVReader_Vendors",
            "Record Writer": "CSVWriter_Operations",
            # spend_ratio: normalised to 0-100, using 1M as baseline
            # composite_score = on_time*0.4 + quality_score*0.4 + spend_ratio*0.2
            "/composite_score":
                "${on_time_delivery_pct:multiply(0.4)"
                ":plus(${quality_score:multiply(0.4)})"
                ":plus(${total_spend:divide(1000000):multiply(100):multiply(0.2)})}",
            "/vendor_tier":
                "${composite_score:ge(80):ifElse('preferred',"
                "${composite_score:ge(60):ifElse('approved','probationary')})}",
        },
    )

    put_vendor_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_VENDOR_SCORE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_VENDOR_SCORE"},
    )

    summary["processors"]["GetFile_SRC_VENDORS"]       = get_vendors_cfg
    summary["processors"]["UpdateRecord_VendorScore"]  = update_vendor_cfg
    summary["processors"]["PutFile_TGT_VENDOR_SCORE"]  = put_vendor_cfg

    summary["connections"] += [
        ("GetFile_SRC_VENDORS",        "UpdateRecord_VendorScore",  "success"),
        ("UpdateRecord_VendorScore",   "PutFile_TGT_VENDOR_SCORE",  "success"),
    ]

    logger.info("[operations] Process Group definition built with %d processors.",
                len(summary["processors"]))
    return summary


def run() -> None:
    """Entry point called by main.py."""
    logger.info("=== Operations domain: starting ===")
    try:
        config.nifi_config.host = NIFI_HOST
        root_pg = canvas.get_process_group("root")
        parent_id = root_pg.id if root_pg else "root"
        summary = build_operations_process_group(parent_id)
        logger.info("Operations Process Group summary:\n%s",
                    json.dumps({"processor_count": len(summary["processors"]),
                                "connection_count": len(summary["connections"])}, indent=2))
    except Exception as exc:
        logger.error("Operations domain failed: %s", exc, exc_info=True)
        raise
    logger.info("=== Operations domain: complete ===")


if __name__ == "__main__":
    run()
