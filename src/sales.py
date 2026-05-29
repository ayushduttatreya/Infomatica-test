"""
src/sales.py
Sales domain — PG_Sales — 10 mappings migrated from Informatica to NiFi.

Mappings covered (ticket: Migrate Sales domain flows):
  1.  m_sales_order_load       → TGT_SALES_ORDER     (metadata stamp)
  2.  m_sales_order_validate   → TGT_SALES_VALIDATE  (amount>0, IS_DATE)
  3.  m_sales_line_item_load   → TGT_LINE_ITEMS       (gross/discount/net)
  4.  m_sales_discount_calc    → TGT_DISCOUNT_CALC    (discount amount)
  5.  m_sales_tax_calc         → TGT_TAX_CALC         (regional tax IIF cascade)
  6.  m_sales_commission_calc  → TGT_COMMISSION       (8% of total_amount)
  7.  m_sales_returns_load     → TGT_RETURNS          (completion flag)
  8.  m_sales_revenue_agg      → TGT_REVENUE_AGG      (QueryRecord GROUP BY region+period)
  9.  m_sales_forecast_load    → TGT_FORECAST         (passthrough)
  10. m_sales_pipeline_load    → TGT_PIPELINE         (weighted amount = amount * prob/100)
"""

import logging
from datetime import datetime
from typing import Optional

import nipyapi
import nipyapi.nifi as nifi_api

from src.utils import (
    connect_nifi,
    generate_batch_id,
    get_or_create_pg,
    get_root_pg_id,
    tax_rate_for_region,
)

logger = logging.getLogger("xclarity_etl.sales")

SOURCE_DATA_PATH = "/opt/nifi/source_data/sales"
OUTPUT_DATA_PATH = "/opt/nifi/output_data/sales"


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
        proc = nifi_api.ProcessGroupsApi().create_processor(pg_id, body)
        logger.debug("Added processor '%s'.", name)
        return proc
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

def transform_sales_order_load(row: dict, batch_id: str) -> dict:
    """m_sales_order_load: stamp load_date and batch_id."""
    return {
        "order_id": row.get("order_id"),
        "customer_id": row.get("customer_id"),
        "order_date": row.get("order_date"),
        "total_amount": row.get("total_amount"),
        "region": row.get("region"),
        "load_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "batch_id": batch_id,
    }


def transform_sales_validate(row: dict) -> dict:
    """m_sales_order_validate: amount > 0 and parseable order_date."""
    total = float(row.get("total_amount") or 0)
    amount_ok = total > 0
    date_ok = False
    try:
        datetime.strptime(str(row.get("order_date") or "")[:10], "%Y-%m-%d")
        date_ok = True
    except ValueError:
        date_ok = False
    return {
        "order_id": row.get("order_id"),
        "amount_valid": amount_ok,
        "date_valid": date_ok,
        "is_valid": amount_ok and date_ok,
    }


def transform_line_items(row: dict) -> dict:
    """m_sales_line_item_load: gross/discount/net per line item."""
    qty = float(row.get("quantity") or 0)
    price = float(row.get("unit_price") or 0)
    disc_pct = float(row.get("discount_pct") or 0)
    gross = round(qty * price, 2)
    discount = round(qty * price * disc_pct / 100, 2)
    net = round(qty * price * (1 - disc_pct / 100), 2)
    return {
        "line_item_id": row.get("line_item_id"),
        "order_id": row.get("order_id"),
        "product_id": row.get("product_id"),
        "gross_amount": gross,
        "discount_amount": discount,
        "net_amount": net,
    }


def transform_discount_calc(row: dict) -> dict:
    """m_sales_discount_calc: extract discount fields."""
    qty = float(row.get("quantity") or 0)
    price = float(row.get("unit_price") or 0)
    disc_pct = float(row.get("discount_pct") or 0)
    return {
        "line_item_id": row.get("line_item_id"),
        "discount_pct": disc_pct,
        "discount_amount": round(qty * price * disc_pct / 100, 2),
    }


def transform_tax_calc(row: dict) -> dict:
    """
    m_sales_tax_calc: apply regional tax.
    Replicates Informatica IIF cascade: Northeast 8%, West 7.25%, Midwest 6.5%, default 7%.
    """
    region = row.get("region") or ""
    rate = tax_rate_for_region(region)
    total = float(row.get("total_amount") or 0)
    tax_amount = round(total * rate, 2)
    return {
        "order_id": row.get("order_id"),
        "region": region,
        "tax_rate": rate,
        "tax_amount": tax_amount,
        "total_with_tax": round(total + tax_amount, 2),
    }


def transform_commission(row: dict) -> dict:
    """m_sales_commission_calc: 8% flat commission per order."""
    total = float(row.get("total_amount") or 0)
    return {
        "order_id": row.get("order_id"),
        "sales_rep_id": row.get("sales_rep_id"),
        "total_amount": total,
        "commission_amount": round(total * 0.08, 2),
    }


def transform_returns(row: dict) -> dict:
    """m_sales_returns_load: add completion flag."""
    status = (row.get("status") or "").lower()
    return {
        "return_id": row.get("return_id"),
        "order_id": row.get("order_id"),
        "refund_amount": row.get("refund_amount"),
        "status": row.get("status"),
        "is_complete": status in ("completed", "processed", "refunded"),
    }


def transform_revenue_agg(rows: list[dict]) -> list[dict]:
    """m_sales_revenue_agg: SUM(total_amount), COUNT(order_id) GROUP BY region, period."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {"total_revenue": 0.0, "order_count": 0})
    for row in rows:
        region = row.get("region") or "Unknown"
        # Derive period from order_date as YYYY-MM
        order_date = row.get("order_date") or ""
        period = order_date[:7] if len(order_date) >= 7 else "Unknown"
        key = (region, period)
        agg[key]["total_revenue"] += float(row.get("total_amount") or 0)
        agg[key]["order_count"] += 1
    return [
        {"region": k[0], "period": k[1],
         "total_revenue": round(v["total_revenue"], 2),
         "order_count": v["order_count"]}
        for k, v in agg.items()
    ]


def transform_forecast(row: dict) -> dict:
    """m_sales_forecast_load: passthrough with load stamp."""
    return {
        "forecast_id": row.get("forecast_id"),
        "region": row.get("region"),
        "period": row.get("period"),
        "forecast_amount": row.get("forecast_amount"),
        "confidence_pct": row.get("confidence_pct"),
    }


def transform_pipeline(row: dict) -> dict:
    """m_sales_pipeline_load: weighted_amount = amount * probability / 100."""
    amount = float(row.get("amount") or 0)
    prob = float(row.get("probability") or 0)
    return {
        "opportunity_id": row.get("opportunity_id"),
        "stage": row.get("stage"),
        "amount": amount,
        "probability": int(prob),
        "weighted_amount": round(amount * prob / 100, 2),
    }


# ---------------------------------------------------------------------------
# NiFi flow builders
# ---------------------------------------------------------------------------

def build_m_sales_order_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_order_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_ORDERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_orders*.csv",
         "Keep Source File": "true"}, (0, 0))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_SalesOrderMetadata",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/load_date": "${now():format('yyyy-MM-dd HH:mm:ss')}",
         "/batch_id": "${batch.id}"}, (200, 0))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_SALES_ORDER",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_sales_order"}, (400, 0))
    _connect(pg_id, get_file.id, update.id)
    _connect(pg_id, update.id, put_db.id)


def build_m_sales_order_validate(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_order_validate sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_VAL_SRC_SALES_ORDERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_orders*.csv",
         "Keep Source File": "true"}, (0, 200))
    validate_script = r"""
import json
from datetime import datetime
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        amt = float(row.get('total_amount') or 0)
        amt_ok = amt > 0
        date_ok = False
        try:
            datetime.strptime(str(row.get('order_date',''))[:10], '%Y-%m-%d')
            date_ok = True
        except: pass
        out.append({'order_id': row.get('order_id'),
                    'amount_valid': str(amt_ok).lower(),
                    'date_valid': str(date_ok).lower(),
                    'is_valid': str(amt_ok and date_ok).lower()})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_SalesValidate",
        {"Script Engine": "python", "Script Body": validate_script}, (200, 200))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_SALES_VALIDATE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_sales_validate"}, (400, 200))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_sales_line_item_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_line_item_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_LINE_ITEMS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_line_items*.csv",
         "Keep Source File": "true"}, (0, 400))
    line_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        qty = float(row.get('quantity') or 0)
        price = float(row.get('unit_price') or 0)
        disc = float(row.get('discount_pct') or 0)
        out.append({
            'line_item_id': row.get('line_item_id'),
            'order_id': row.get('order_id'),
            'product_id': row.get('product_id'),
            'gross_amount': round(qty * price, 2),
            'discount_amount': round(qty * price * disc / 100, 2),
            'net_amount': round(qty * price * (1 - disc / 100), 2),
        })
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_LineItemCalc",
        {"Script Engine": "python", "Script Body": line_script}, (200, 400))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_LINE_ITEMS",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_line_items"}, (400, 400))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_sales_discount_calc(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_discount_calc sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_DISC_SRC_SALES_LINE_ITEMS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_line_items*.csv",
         "Keep Source File": "true"}, (0, 600))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_DiscountCalc",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/discount_amount":
             "${field.value:toDecimal():multiply(${quantity}):multiply(${unit_price})"
             ":divide(100):toDecimal()}"}, (200, 600))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_DISCOUNT_CALC",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_discount_calc"}, (400, 600))
    _connect(pg_id, get_file.id, update.id)
    _connect(pg_id, update.id, put_db.id)


def build_m_sales_tax_calc(pg_id, csv_reader_id, db_pool_id):
    """Regional tax via IIF cascade → RouteOnAttribute."""
    logger.info("Building m_sales_tax_calc sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_TAX_SRC_SALES_ORDERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_orders*.csv",
         "Keep Source File": "true"}, (0, 800))
    tax_script = r"""
import json
TAX = {'Northeast': 0.08, 'West': 0.0725, 'Midwest': 0.065}
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        region = row.get('region', '')
        rate = TAX.get(region, 0.07)
        total = float(row.get('total_amount') or 0)
        tax_amt = round(total * rate, 2)
        out.append({'order_id': row.get('order_id'), 'region': region,
                    'tax_rate': rate, 'tax_amount': tax_amt,
                    'total_with_tax': round(total + tax_amt, 2)})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_TaxCalc",
        {"Script Engine": "python", "Script Body": tax_script}, (200, 800))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_TAX_CALC",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_tax_calc"}, (400, 800))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_sales_commission_calc(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_commission_calc sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_COMM_SRC_SALES_ORDERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_orders*.csv",
         "Keep Source File": "true"}, (0, 1000))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_Commission",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/commission_amount": "${total_amount:toDecimal():multiply(0.08)}"}, (200, 1000))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_COMMISSION",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_commission"}, (400, 1000))
    _connect(pg_id, get_file.id, update.id)
    _connect(pg_id, update.id, put_db.id)


def build_m_sales_returns_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_returns_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_RETURNS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_returns*.csv",
         "Keep Source File": "true"}, (0, 1200))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_ReturnsComplete",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/is_complete": "${status:toLower():matches('completed|processed|refunded')}"}, (200, 1200))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_RETURNS",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_returns"}, (400, 1200))
    _connect(pg_id, get_file.id, update.id)
    _connect(pg_id, update.id, put_db.id)


def build_m_sales_revenue_agg(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_revenue_agg sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_AGG_SRC_SALES_ORDERS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_orders*.csv",
         "Keep Source File": "true"}, (0, 1400))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_RevenueAgg",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "revenue": (
             "SELECT region, SUBSTR(order_date,1,7) AS period, "
             "SUM(total_amount) AS total_revenue, COUNT(order_id) AS order_count "
             "FROM FLOWFILE GROUP BY region, SUBSTR(order_date,1,7)"
         )}, (200, 1400))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_REVENUE_AGG",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_revenue_agg"}, (400, 1400))
    _connect(pg_id, get_file.id, query.id)
    _connect(pg_id, query.id, put_db.id, "revenue")


def build_m_sales_forecast_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_forecast_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_FORECAST",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_forecast*.csv",
         "Keep Source File": "true"}, (0, 1600))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_FORECAST",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_forecast"}, (200, 1600))
    _connect(pg_id, get_file.id, put_db.id)


def build_m_sales_pipeline_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_sales_pipeline_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_SALES_PIPELINE",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "sales_pipeline*.csv",
         "Keep Source File": "true"}, (0, 1800))
    update = _add_processor(pg_id, "org.apache.nifi.processors.standard.UpdateRecord",
        "UpdateRecord_WeightedAmount",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "/weighted_amount": "${amount:toDecimal():multiply(${probability}):divide(100)}"}, (200, 1800))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PIPELINE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_pipeline"}, (400, 1800))
    _connect(pg_id, get_file.id, update.id)
    _connect(pg_id, update.id, put_db.id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    logger.info("=== sales.py: building PG_Sales (10 mappings) ===")
    connect_nifi()
    root_pg_id = get_root_pg_id()

    def _svc_id(name):
        svc = nipyapi.canvas.get_controller_service(name)
        return svc.id if svc else ""

    csv_reader_id = _svc_id("XClarity_CSVReader")
    db_pool_id = _svc_id("XClarity_DBCPConnectionPool")

    pg = get_or_create_pg("PG_Sales", parent_pg_id=root_pg_id)
    pg_id = pg.id if hasattr(pg, "id") else str(pg)

    for builder in [
        build_m_sales_order_load,
        build_m_sales_order_validate,
        build_m_sales_line_item_load,
        build_m_sales_discount_calc,
        build_m_sales_tax_calc,
        build_m_sales_commission_calc,
        build_m_sales_returns_load,
        build_m_sales_revenue_agg,
        build_m_sales_forecast_load,
        build_m_sales_pipeline_load,
    ]:
        try:
            builder(pg_id, csv_reader_id, db_pool_id)
        except Exception as exc:
            logger.error("%s failed: %s", builder.__name__, exc)

    logger.info("PG_Sales build complete.")
