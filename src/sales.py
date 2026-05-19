"""
Sales Domain - Apache NiFi Process Group
Migrated from Informatica PowerCenter XClarity_ETL

Covers tickets:
  - Migrate Sales Module: order load, validation, discount, tax, commission,
    returns, revenue aggregation, pipeline
  - Implement Sales order load, validation, discount, tax, and commission sub-flows
  - Implement Sales returns processing, revenue aggregation, forecast load,
    and pipeline opportunity weighting

Sources : SRC_SALES_ORDERS, SRC_SALES_LINE_ITEMS, SRC_SALES_RETURNS,
          SRC_SALES_FORECAST, SRC_SALES_PIPELINE
Targets : TGT_SALES_ORDER, TGT_SALES_VALIDATE, TGT_LINE_ITEMS,
          TGT_DISCOUNT_CALC, TGT_TAX_CALC, TGT_COMMISSION, TGT_RETURNS,
          TGT_REVENUE_AGG, TGT_FORECAST, TGT_PIPELINE
"""

import logging

import nipyapi
from nipyapi import canvas

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# NiFi Expression Language snippets for tax calc (Informatica DECODE replacement)
# ---------------------------------------------------------------------------

# Regional tax: NE=8%, W=7.25%, MW=6.5%, default=7%
TAX_RATE_EXPR = (
    "${region:equals('Northeast'):ifElse('0.08',"
    "${region:equals('West'):ifElse('0.0725',"
    "${region:equals('Midwest'):ifElse('0.065','0.07')})}}"
)

TOTAL_WITH_TAX_EXPR = "${net_amount:toNumber():multiply(${tax_rate:toNumber():plus(1)})}"


def create_sales_process_group(parent_pg_id: str, nifi_url: str = "http://localhost:8080") -> None:
    """
    Build the full Sales NiFi Process Group.

    Covers Informatica mappings:
      m_sales_load, m_sales_validate, m_discount_calc, m_tax_calc,
      m_commission, m_returns_process, m_revenue_agg, m_forecast_load,
      m_pipeline_weight
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    try:
        root = canvas.get_root_pg_id()
        parent_id = parent_pg_id or root

        logger.info("Creating Sales Process Group under parent %s", parent_id)
        sales_pg = canvas.create_process_group(
            canvas.get_process_group(parent_id),
            "Sales",
            (600, 100),
        )
        pg_id = sales_pg.id
        logger.info("Sales PG created: %s", pg_id)

        _build_sales_load(pg_id)
        _build_sales_validate(pg_id)
        _build_discount_calc(pg_id)
        _build_tax_calc(pg_id)
        _build_commission(pg_id)
        _build_returns_process(pg_id)
        _build_revenue_agg(pg_id)
        _build_forecast_load(pg_id)
        _build_pipeline_weight(pg_id)

        logger.info("Sales Process Group fully built: %s", pg_id)

    except Exception as exc:
        logger.error("Failed to build Sales Process Group: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Sub-flow builders
# ---------------------------------------------------------------------------

def _build_sales_load(pg_id: str) -> None:
    """
    m_sales_load:
    GetFile(SRC_SALES_ORDERS) + GetFile(SRC_SALES_LINE_ITEMS)
    -> UpdateRecord (metadata stamp: load_date, batch_id, source_system)
    -> PutFile TGT_SALES_ORDER / TGT_LINE_ITEMS
    """
    logger.info("[m_sales_load] Building sub-flow in PG %s", pg_id)

    try:
        get_orders = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 200),
            "GetFile_SRC_SALES_ORDERS",
            {
                "Input Directory": "/data/inbound/sales_orders",
                "File Filter": "*.csv",
            },
        )

        meta_orders = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 200),
            "UpdateRecord_SalesOrderMetadata",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/load_date": "${now()}",
                "/batch_id": "${now():format('yyyyMMdd_HHmmss')}_SALES",
                "/source_system": "informatica_etl",
            },
        )

        put_orders = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 200),
            "PutFile_TGT_SALES_ORDER",
            {"Directory": "/data/outbound/TGT_SALES_ORDER"},
        )

        get_lines = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 380),
            "GetFile_SRC_SALES_LINE_ITEMS",
            {
                "Input Directory": "/data/inbound/sales_line_items",
                "File Filter": "*.csv",
            },
        )

        put_lines = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 380),
            "PutFile_TGT_LINE_ITEMS",
            {"Directory": "/data/outbound/TGT_LINE_ITEMS"},
        )

        canvas.create_connection(get_orders, meta_orders, ["success"])
        canvas.create_connection(meta_orders, put_orders, ["success"])
        canvas.create_connection(get_lines, put_lines, ["success"])

        logger.info("[m_sales_load] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_sales_load] Could not build via API (offline mode): %s", exc)


def _build_sales_validate(pg_id: str) -> None:
    """
    m_sales_validate:
    RouteOnAttribute (amount > 0 AND IS_DATE check on order_date)
    -> valid -> TGT_SALES_VALIDATE  / invalid -> error
    """
    logger.info("[m_sales_validate] Building sub-flow in PG %s", pg_id)

    try:
        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (100, 560),
            "RouteOnAttribute_SalesValidate",
            {
                "Routing Strategy": "Route to Property name",
                "valid": (
                    "${total_amount:toNumber():gt(0):and("
                    "${order_date:toDate('yyyy-MM-dd HH:mm:ss'):format('yyyy-MM-dd'):matches('\\\\d{4}-\\\\d{2}-\\\\d{2}')})"
                    "}"
                ),
            },
        )

        put_valid = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 520),
            "PutFile_TGT_SALES_VALIDATE",
            {"Directory": "/data/outbound/TGT_SALES_VALIDATE"},
        )

        put_invalid = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 640),
            "PutFile_TGT_SALES_INVALID",
            {"Directory": "/data/error/TGT_SALES_VALIDATE"},
        )

        canvas.create_connection(route, put_valid, ["valid"])
        canvas.create_connection(route, put_invalid, ["unmatched"])

        logger.info("[m_sales_validate] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_sales_validate] Could not build via API (offline mode): %s", exc)


def _build_discount_calc(pg_id: str) -> None:
    """
    m_discount_calc:
    UpdateRecord:
      gross_amount  = quantity * unit_price
      discount_amount = gross_amount * discount_pct / 100
      net_amount    = gross_amount - discount_amount
    -> TGT_DISCOUNT_CALC
    """
    logger.info("[m_discount_calc] Building sub-flow in PG %s", pg_id)

    try:
        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (100, 760),
            "UpdateRecord_DiscountCalc",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/gross_amount": "${quantity:toNumber():multiply(${unit_price:toNumber()})}",
                "/discount_amount": "${gross_amount:toNumber():multiply(${discount_pct:toNumber()}):divide(100)}",
                "/net_amount": "${gross_amount:toNumber():minus(${discount_amount:toNumber()})}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 760),
            "PutFile_TGT_DISCOUNT_CALC",
            {"Directory": "/data/outbound/TGT_DISCOUNT_CALC"},
        )

        canvas.create_connection(update, put, ["success"])

        logger.info("[m_discount_calc] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_discount_calc] Could not build via API (offline mode): %s", exc)


def _build_tax_calc(pg_id: str) -> None:
    """
    m_tax_calc:
    UpdateRecord:
      tax_rate       = DECODE(region, NE=0.08, W=0.0725, MW=0.065, default=0.07)
      total_with_tax = net_amount * (1 + tax_rate)
    -> TGT_TAX_CALC
    """
    logger.info("[m_tax_calc] Building sub-flow in PG %s", pg_id)

    try:
        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (100, 940),
            "UpdateRecord_TaxCalc",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/tax_rate": TAX_RATE_EXPR,
                "/total_with_tax": "${net_amount:toNumber():multiply(${tax_rate:toNumber():plus(1)})}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 940),
            "PutFile_TGT_TAX_CALC",
            {"Directory": "/data/outbound/TGT_TAX_CALC"},
        )

        canvas.create_connection(update, put, ["success"])

        logger.info("[m_tax_calc] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_tax_calc] Could not build via API (offline mode): %s", exc)


def _build_commission(pg_id: str) -> None:
    """
    m_commission:
    UpdateRecord: commission = total_amount * 0.08 -> TGT_COMMISSION
    """
    logger.info("[m_commission] Building sub-flow in PG %s", pg_id)

    try:
        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (100, 1120),
            "UpdateRecord_Commission",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/commission": "${total_amount:toNumber():multiply(0.08)}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1120),
            "PutFile_TGT_COMMISSION",
            {"Directory": "/data/outbound/TGT_COMMISSION"},
        )

        canvas.create_connection(update, put, ["success"])

        logger.info("[m_commission] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_commission] Could not build via API (offline mode): %s", exc)


def _build_returns_process(pg_id: str) -> None:
    """
    m_returns_process:
    GetFile(SRC_SALES_RETURNS)
    -> UpdateRecord: processing_date=${now()}, is_processed='Y'
    -> TGT_RETURNS
    """
    logger.info("[m_returns_process] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1300),
            "GetFile_SRC_SALES_RETURNS",
            {
                "Input Directory": "/data/inbound/sales_returns",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1300),
            "UpdateRecord_ReturnsProcess",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/processing_date": "${now()}",
                "/is_processed": "Y",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1300),
            "PutFile_TGT_RETURNS",
            {"Directory": "/data/outbound/TGT_RETURNS"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_returns_process] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_returns_process] Could not build via API (offline mode): %s", exc)


def _build_revenue_agg(pg_id: str) -> None:
    """
    m_revenue_agg:
    QueryRecord:
      SUM(total_amount) AS revenue, COUNT(order_id) AS order_count
      GROUP BY region, period
    -> TGT_REVENUE_AGG
    """
    logger.info("[m_revenue_agg] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1480),
            "QueryRecord_RevenueAgg",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "revenue_agg": (
                    "SELECT region, "
                    "SUBSTRING(order_date, 1, 7) AS period, "
                    "SUM(total_amount) AS revenue, "
                    "COUNT(order_id) AS order_count "
                    "FROM FLOWFILE GROUP BY region, SUBSTRING(order_date, 1, 7)"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1480),
            "PutFile_TGT_REVENUE_AGG",
            {"Directory": "/data/outbound/TGT_REVENUE_AGG"},
        )

        canvas.create_connection(query, put, ["revenue_agg"])

        logger.info("[m_revenue_agg] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_revenue_agg] Could not build via API (offline mode): %s", exc)


def _build_forecast_load(pg_id: str) -> None:
    """
    m_forecast_load:
    GetFile(SRC_SALES_FORECAST) -> PutFile TGT_FORECAST
    """
    logger.info("[m_forecast_load] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1660),
            "GetFile_SRC_SALES_FORECAST",
            {
                "Input Directory": "/data/inbound/sales_forecast",
                "File Filter": "*.csv",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1660),
            "PutFile_TGT_FORECAST",
            {"Directory": "/data/outbound/TGT_FORECAST"},
        )

        canvas.create_connection(get, put, ["success"])

        logger.info("[m_forecast_load] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_forecast_load] Could not build via API (offline mode): %s", exc)


def _build_pipeline_weight(pg_id: str) -> None:
    """
    m_pipeline_weight:
    GetFile(SRC_SALES_PIPELINE)
    -> UpdateRecord: weighted_amount = amount * probability / 100
    -> TGT_PIPELINE
    """
    logger.info("[m_pipeline_weight] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1840),
            "GetFile_SRC_SALES_PIPELINE",
            {
                "Input Directory": "/data/inbound/sales_pipeline",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1840),
            "UpdateRecord_PipelineWeight",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/weighted_amount": "${amount:toNumber():multiply(${probability:toNumber()}):divide(100)}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1840),
            "PutFile_TGT_PIPELINE",
            {"Directory": "/data/outbound/TGT_PIPELINE"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_pipeline_weight] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_pipeline_weight] Could not build via API (offline mode): %s", exc)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = "http://localhost:8080", parent_pg_id: str = "") -> None:
    """Entry-point called by main.py to deploy the Sales Process Group."""
    logger.info("=== Sales domain deployment starting ===")
    create_sales_process_group(parent_pg_id=parent_pg_id, nifi_url=nifi_url)
    logger.info("=== Sales domain deployment complete ===")


if __name__ == "__main__":
    run()
