"""
sales.py — Apache NiFi Process Group builder for the Sales domain.

Tickets covered:
  - 86679eb8  Sales Module: order load, validation, discount, tax, commission,
              returns, revenue aggregation, pipeline
  - 0a69bf40  Sales order load, validation, discount, tax, commission sub-flows
  - a978d216  Sales returns, revenue aggregation, forecast load, pipeline weighting

Informatica mappings replaced:
  m_sales_load, m_sales_order, m_sales_validate, m_sales_discount_calc,
  m_sales_tax_calc, m_sales_commission, m_returns_process,
  m_revenue_agg, m_forecast_load, m_pipeline_weight
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
INBOUND_DIR  = os.environ.get("INBOUND_DIR",  "/data/inbound/sales")
OUTBOUND_DIR = os.environ.get("OUTBOUND_DIR", "/data/outbound/sales")
ERROR_DIR    = os.environ.get("ERROR_DIR",    "/data/error/sales")


def _proc_config(proc_type: str, name: str, properties: dict[str, str]) -> dict[str, Any]:
    return {"type": proc_type, "name": name, "properties": properties}


def build_sales_process_group(parent_pg_id: str) -> dict[str, Any]:
    """
    Build the Sales domain Process Group covering all 10 mappings.
    Returns a summary dict of processors and connections.
    """
    config.nifi_config.host = NIFI_HOST
    logger.info("Building Sales Process Group under parent=%s", parent_pg_id)

    summary: dict[str, Any] = {"processors": {}, "connections": []}

    # ------------------------------------------------------------------ #
    # 1. m_sales_load — GetFile SRC_SALES_ORDERS + SRC_SALES_LINE_ITEMS  #
    #    → UpdateRecord metadata stamp → TGT_SALES_ORDER / TGT_LINE_ITEMS#
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_sales_load sub-flow")

    get_orders_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_ORDERS",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SALES_ORDERS.*\\.csv",
            "Keep Source File": "false",
        },
    )

    get_lineitems_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_LINE_ITEMS",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SALES_LINE_ITEMS.*\\.csv",
            "Keep Source File": "false",
        },
    )

    update_order_meta_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_SalesOrderMeta",
        {
            "Record Reader":    "CSVReader_Sales",
            "Record Writer":    "CSVWriter_Sales",
            "/load_timestamp":  "${now()}",
            "/batch_id":        "${UUID()}",
            "/source_system":   "informatica_etl",
        },
    )

    put_sales_order_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_SALES_ORDER",
        {"Directory": f"{OUTBOUND_DIR}/TGT_SALES_ORDER"},
    )

    summary["processors"]["GetFile_SRC_SALES_ORDERS"]    = get_orders_cfg
    summary["processors"]["GetFile_SRC_SALES_LINE_ITEMS"]= get_lineitems_cfg
    summary["processors"]["UpdateRecord_SalesOrderMeta"] = update_order_meta_cfg
    summary["processors"]["PutFile_TGT_SALES_ORDER"]     = put_sales_order_cfg

    # ------------------------------------------------------------------ #
    # 2. m_sales_validate — RouteOnAttribute (amount > 0, IS_DATE check)  #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_sales_validate sub-flow")

    route_validate_cfg = _proc_config(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "RouteOnAttribute_SalesValidate",
        {
            "Routing Strategy":  "Route to Property name",
            "valid_amount":      "${total_amount:gt(0)}",
            "valid_date":        "${order_date:toDate('yyyy-MM-dd HH:mm:ss'):notNull()}",
            "is_valid":          "${valid_amount:and(${valid_date})}",
        },
    )

    put_sales_validate_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_SALES_VALIDATE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_SALES_VALIDATE"},
    )

    put_sales_invalid_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_SALES_INVALID",
        {"Directory": ERROR_DIR},
    )

    summary["processors"]["RouteOnAttribute_SalesValidate"] = route_validate_cfg
    summary["processors"]["PutFile_TGT_SALES_VALIDATE"]     = put_sales_validate_cfg
    summary["processors"]["PutFile_TGT_SALES_INVALID"]      = put_sales_invalid_cfg

    # ------------------------------------------------------------------ #
    # 3. m_discount_calc — UpdateRecord gross/discount/net amounts        #
    #    → TGT_LINE_ITEMS / TGT_DISCOUNT_CALC                            #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_discount_calc sub-flow")

    discount_calc_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_DiscountCalc",
        {
            "Record Reader":    "CSVReader_LineItems",
            "Record Writer":    "CSVWriter_Sales",
            # gross = quantity * unit_price
            "/gross_amount":    "${quantity:multiply(${unit_price})}",
            # discount = gross * discount_pct / 100
            "/discount_amount": "${quantity:multiply(${unit_price})"
                                ":multiply(${discount_pct}):divide(100)}",
            # net = gross - discount
            "/net_amount":      "${quantity:multiply(${unit_price})"
                                ":minus(${quantity:multiply(${unit_price})"
                                ":multiply(${discount_pct}):divide(100)})}",
        },
    )

    put_line_items_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_LINE_ITEMS",
        {"Directory": f"{OUTBOUND_DIR}/TGT_LINE_ITEMS"},
    )

    put_discount_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_DISCOUNT_CALC",
        {"Directory": f"{OUTBOUND_DIR}/TGT_DISCOUNT_CALC"},
    )

    summary["processors"]["UpdateRecord_DiscountCalc"] = discount_calc_cfg
    summary["processors"]["PutFile_TGT_LINE_ITEMS"]    = put_line_items_cfg
    summary["processors"]["PutFile_TGT_DISCOUNT_CALC"] = put_discount_cfg

    # ------------------------------------------------------------------ #
    # 4. m_tax_calc — UpdateRecord region-based tax rate                  #
    #    NE=8%, W=7.25%, MW=6.5%, default=7%                             #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_tax_calc sub-flow")

    tax_calc_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_TaxCalc",
        {
            "Record Reader": "CSVReader_Sales",
            "Record Writer": "CSVWriter_Sales",
            "/tax_rate":
                "${region:equals('Northeast'):ifElse(0.08,"
                "${region:equals('West'):ifElse(0.0725,"
                "${region:equals('Midwest'):ifElse(0.065,0.07)})})}",
            "/tax_amount":
                "${net_amount:multiply(${region:equals('Northeast'):ifElse(0.08,"
                "${region:equals('West'):ifElse(0.0725,"
                "${region:equals('Midwest'):ifElse(0.065,0.07)})})})}",
            "/total_with_tax":
                "${net_amount:plus(${net_amount:multiply(${region:equals('Northeast')"
                ":ifElse(0.08,${region:equals('West'):ifElse(0.0725,"
                "${region:equals('Midwest'):ifElse(0.065,0.07)})})})})}",
        },
    )

    put_tax_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_TAX_CALC",
        {"Directory": f"{OUTBOUND_DIR}/TGT_TAX_CALC"},
    )

    summary["processors"]["UpdateRecord_TaxCalc"] = tax_calc_cfg
    summary["processors"]["PutFile_TGT_TAX_CALC"] = put_tax_cfg

    # ------------------------------------------------------------------ #
    # 5. m_commission — UpdateRecord commission = total * 0.08           #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_commission sub-flow")

    commission_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_Commission",
        {
            "Record Reader": "CSVReader_Sales",
            "Record Writer": "CSVWriter_Sales",
            "/commission":   "${total_amount:multiply(0.08)}",
        },
    )

    put_commission_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_COMMISSION",
        {"Directory": f"{OUTBOUND_DIR}/TGT_COMMISSION"},
    )

    summary["processors"]["UpdateRecord_Commission"] = commission_cfg
    summary["processors"]["PutFile_TGT_COMMISSION"]  = put_commission_cfg

    # ------------------------------------------------------------------ #
    # 6. m_returns_process — GetFile SRC_SALES_RETURNS                   #
    #    → UpdateRecord processing_date + is_processed flag              #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_returns_process sub-flow")

    get_returns_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_RETURNS",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SALES_RETURNS.*\\.csv",
            "Keep Source File": "false",
        },
    )

    returns_update_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_Returns",
        {
            "Record Reader":    "CSVReader_Returns",
            "Record Writer":    "CSVWriter_Sales",
            "/processing_date": "${now()}",
            "/is_processed":    "Y",
        },
    )

    put_returns_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_RETURNS",
        {"Directory": f"{OUTBOUND_DIR}/TGT_RETURNS"},
    )

    summary["processors"]["GetFile_SRC_SALES_RETURNS"] = get_returns_cfg
    summary["processors"]["UpdateRecord_Returns"]       = returns_update_cfg
    summary["processors"]["PutFile_TGT_RETURNS"]        = put_returns_cfg

    # ------------------------------------------------------------------ #
    # 7. m_revenue_agg — QueryRecord SUM/COUNT GROUP BY region, period   #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_revenue_agg sub-flow")

    revenue_agg_cfg = _proc_config(
        "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_RevenueAgg",
        {
            "Record Reader": "CSVReader_Sales",
            "Record Writer": "CSVWriter_Sales",
            "REVENUE_AGG":
                "SELECT region, "
                "SUBSTRING(order_date, 1, 7) AS period, "
                "SUM(total_amount) AS revenue, "
                "COUNT(order_id) AS order_count "
                "FROM FLOWFILE "
                "GROUP BY region, SUBSTRING(order_date, 1, 7)",
        },
    )

    put_revenue_agg_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_REVENUE_AGG",
        {"Directory": f"{OUTBOUND_DIR}/TGT_REVENUE_AGG"},
    )

    summary["processors"]["QueryRecord_RevenueAgg"]  = revenue_agg_cfg
    summary["processors"]["PutFile_TGT_REVENUE_AGG"] = put_revenue_agg_cfg

    # ------------------------------------------------------------------ #
    # 8. m_forecast_load — GetFile SRC_SALES_FORECAST → TGT_FORECAST     #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_forecast_load sub-flow")

    get_forecast_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_FORECAST",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SALES_FORECAST.*\\.csv",
            "Keep Source File": "false",
        },
    )

    put_forecast_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_FORECAST",
        {"Directory": f"{OUTBOUND_DIR}/TGT_FORECAST"},
    )

    summary["processors"]["GetFile_SRC_SALES_FORECAST"] = get_forecast_cfg
    summary["processors"]["PutFile_TGT_FORECAST"]        = put_forecast_cfg

    # ------------------------------------------------------------------ #
    # 9. m_pipeline_weight — GetFile SRC_SALES_PIPELINE                  #
    #    → UpdateRecord weighted_amount = amount * probability / 100     #
    # ------------------------------------------------------------------ #
    logger.info("[sales] Building m_pipeline_weight sub-flow")

    get_pipeline_cfg = _proc_config(
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_PIPELINE",
        {
            "Input Directory": INBOUND_DIR,
            "File Filter":     "SRC_SALES_PIPELINE.*\\.csv",
            "Keep Source File": "false",
        },
    )

    pipeline_weight_cfg = _proc_config(
        "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_PipelineWeight",
        {
            "Record Reader":  "CSVReader_Pipeline",
            "Record Writer":  "CSVWriter_Sales",
            "/weighted_amount": "${amount:multiply(${probability}):divide(100)}",
        },
    )

    put_pipeline_cfg = _proc_config(
        "org.apache.nifi.processors.standard.PutFile",
        "PutFile_TGT_PIPELINE",
        {"Directory": f"{OUTBOUND_DIR}/TGT_PIPELINE"},
    )

    summary["processors"]["GetFile_SRC_SALES_PIPELINE"]  = get_pipeline_cfg
    summary["processors"]["UpdateRecord_PipelineWeight"] = pipeline_weight_cfg
    summary["processors"]["PutFile_TGT_PIPELINE"]        = put_pipeline_cfg

    # ------------------------------------------------------------------ #
    # Connection map                                                       #
    # ------------------------------------------------------------------ #
    summary["connections"] = [
        # Order load
        ("GetFile_SRC_SALES_ORDERS",    "UpdateRecord_SalesOrderMeta",      "success"),
        ("UpdateRecord_SalesOrderMeta", "PutFile_TGT_SALES_ORDER",          "success"),
        # Validation
        ("UpdateRecord_SalesOrderMeta", "RouteOnAttribute_SalesValidate",   "success"),
        ("RouteOnAttribute_SalesValidate","PutFile_TGT_SALES_VALIDATE",     "is_valid"),
        ("RouteOnAttribute_SalesValidate","PutFile_TGT_SALES_INVALID",      "unmatched"),
        # Discount + line items
        ("GetFile_SRC_SALES_LINE_ITEMS","UpdateRecord_DiscountCalc",        "success"),
        ("UpdateRecord_DiscountCalc",   "PutFile_TGT_LINE_ITEMS",           "success"),
        ("UpdateRecord_DiscountCalc",   "PutFile_TGT_DISCOUNT_CALC",        "success"),
        # Tax
        ("UpdateRecord_DiscountCalc",   "UpdateRecord_TaxCalc",             "success"),
        ("UpdateRecord_TaxCalc",        "PutFile_TGT_TAX_CALC",             "success"),
        # Commission
        ("UpdateRecord_SalesOrderMeta", "UpdateRecord_Commission",          "success"),
        ("UpdateRecord_Commission",     "PutFile_TGT_COMMISSION",           "success"),
        # Returns
        ("GetFile_SRC_SALES_RETURNS",   "UpdateRecord_Returns",             "success"),
        ("UpdateRecord_Returns",        "PutFile_TGT_RETURNS",              "success"),
        # Revenue aggregation
        ("UpdateRecord_SalesOrderMeta", "QueryRecord_RevenueAgg",           "success"),
        ("QueryRecord_RevenueAgg",      "PutFile_TGT_REVENUE_AGG",          "REVENUE_AGG"),
        # Forecast
        ("GetFile_SRC_SALES_FORECAST",  "PutFile_TGT_FORECAST",             "success"),
        # Pipeline
        ("GetFile_SRC_SALES_PIPELINE",  "UpdateRecord_PipelineWeight",      "success"),
        ("UpdateRecord_PipelineWeight", "PutFile_TGT_PIPELINE",             "success"),
    ]

    logger.info("[sales] Process Group definition built with %d processors.",
                len(summary["processors"]))
    return summary


def run() -> None:
    """Entry point called by main.py."""
    logger.info("=== Sales domain: starting ===")
    try:
        config.nifi_config.host = NIFI_HOST
        root_pg = canvas.get_process_group("root")
        parent_id = root_pg.id if root_pg else "root"
        summary = build_sales_process_group(parent_id)
        logger.info("Sales Process Group summary:\n%s",
                    json.dumps({"processor_count": len(summary["processors"]),
                                "connection_count": len(summary["connections"])}, indent=2))
    except Exception as exc:
        logger.error("Sales domain failed: %s", exc, exc_info=True)
        raise
    logger.info("=== Sales domain: complete ===")


if __name__ == "__main__":
    run()
