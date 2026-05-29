"""
src/product.py
Product domain — PG_Product — 10 mappings migrated from Informatica to NiFi.

Mappings covered (ticket: Migrate Product domain flows):
  1.  m_product_load           → TGT_PRODUCTS          (passthrough)
  2.  m_product_category_hier  → TGT_CATEGORY_HIER     (LookupRecord parent + path)
  3.  m_product_price_hist     → TGT_PRICE_HIST        (price delta + pct)
  4.  m_product_inventory_load → TGT_INVENTORY         (passthrough)
  5.  m_product_inv_alert      → TGT_INV_ALERT         (critical/low/ok)
  6.  m_product_supplier_join  → TGT_PROD_SUPPLIER     (LookupRecord supplier name)
  7.  m_product_bundle_pricing → TGT_BUNDLES           (QueryRecord SUM + 0.85 discount)
  8.  m_product_review_sentim  → TGT_REVIEW_SENTIMENT  (QueryRecord AVG + classify)
  9.  m_product_lifecycle      → TGT_LIFECYCLE         (DATE_DIFF from launch_date)
  10. m_product_recommendation → TGT_RECOMMENDATIONS   (QueryRecord + SortRecord)
"""

import logging
from datetime import date

import nipyapi
import nipyapi.nifi as nifi_api

from src.utils import (
    connect_nifi,
    get_or_create_pg,
    get_root_pg_id,
    lifecycle_stage,
    review_sentiment,
    stock_alert,
)

logger = logging.getLogger("xclarity_etl.product")

SOURCE_DATA_PATH = "/opt/nifi/source_data/product"
OUTPUT_DATA_PATH = "/opt/nifi/output_data/product"


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

def transform_product_load(row: dict) -> dict:
    """m_product_load: passthrough product catalog."""
    return {
        "product_id": row.get("product_id"),
        "product_name": row.get("product_name"),
        "category": row.get("category"),
        "unit_price": row.get("unit_price"),
        "status": row.get("status"),
        "launch_date": row.get("launch_date"),
    }


def transform_category_hier(row: dict, parent_name: str) -> dict:
    """m_product_category_hier: build hierarchy path from parent lookup."""
    cat_name = row.get("category_name") or ""
    path = f"{parent_name}/{cat_name}" if parent_name else cat_name
    return {
        "category_id": row.get("category_id"),
        "category_name": cat_name,
        "parent_category_id": row.get("parent_category_id"),
        "parent_name": parent_name,
        "hierarchy_path": path,
        "level": row.get("level"),
    }


def transform_price_hist(row: dict, prev_price: float) -> dict:
    """m_product_price_hist: price change delta and percentage."""
    current = float(row.get("list_price") or 0)
    change = round(current - prev_price, 2)
    pct = round((change / prev_price * 100), 2) if prev_price else 0.0
    return {
        "price_id": row.get("price_id"),
        "product_id": row.get("product_id"),
        "effective_date": row.get("effective_date"),
        "list_price": current,
        "price_change": change,
        "price_change_pct": pct,
    }


def transform_inventory_load(row: dict) -> dict:
    """m_product_inventory_load: passthrough inventory."""
    return {
        "inventory_id": row.get("inventory_id"),
        "product_id": row.get("product_id"),
        "warehouse_id": row.get("warehouse_id"),
        "quantity_on_hand": row.get("quantity_on_hand"),
        "reorder_level": row.get("reorder_level"),
    }


def transform_inv_alert(row: dict) -> dict:
    """m_product_inv_alert: stock status classification."""
    qty = int(row.get("quantity_on_hand") or 0)
    reorder = int(row.get("reorder_level") or 0)
    return {
        "inventory_id": row.get("inventory_id"),
        "product_id": row.get("product_id"),
        "quantity_on_hand": qty,
        "reorder_level": reorder,
        "stock_status": stock_alert(qty, reorder),
    }


def transform_prod_supplier(product_row: dict, supplier_row: dict) -> dict:
    """m_product_supplier_join: join product + supplier data."""
    return {
        "product_id": product_row.get("product_id"),
        "supplier_id": product_row.get("supplier_id"),
        "supplier_name": supplier_row.get("supplier_name"),
        "country": supplier_row.get("country"),
    }


def transform_bundles(rows: list[dict]) -> list[dict]:
    """m_product_bundle_pricing: SUM unit_prices by category, apply 15% discount."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {"total_price": 0.0, "count": 0})
    for row in rows:
        cat = row.get("category") or "Unknown"
        agg[cat]["total_price"] += float(row.get("unit_price") or 0)
        agg[cat]["count"] += 1
    return [
        {"category": cat, "bundle_price": round(v["total_price"] * 0.85, 2),
         "product_count": v["count"]}
        for cat, v in agg.items()
    ]


def transform_review_sentiment_calc(rows: list[dict]) -> list[dict]:
    """m_product_review_sentiment: AVG(rating) + classify."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {"sum": 0, "count": 0})
    for row in rows:
        pid = row.get("product_id") or ""
        rating = int(row.get("rating") or 0)
        agg[pid]["sum"] += rating
        agg[pid]["count"] += 1
    result = []
    for pid, v in agg.items():
        avg = round(v["sum"] / v["count"], 2) if v["count"] else 0.0
        result.append({"product_id": pid, "avg_rating": avg,
                        "review_count": v["count"],
                        "sentiment": review_sentiment(avg)})
    return result


def transform_lifecycle(row: dict) -> dict:
    """m_product_lifecycle: classify by launch_date age."""
    return {
        "product_id": row.get("product_id"),
        "launch_date": row.get("launch_date"),
        "status": row.get("status"),
        "lifecycle_stage": lifecycle_stage(row.get("launch_date"), row.get("status") or ""),
    }


def transform_recommendations(rows: list[dict]) -> list[dict]:
    """m_product_recommendation: aggregate purchase frequency, rank by frequency."""
    from collections import defaultdict
    freq: dict = defaultdict(int)
    for row in rows:
        pid = row.get("product_id") or ""
        freq[pid] += int(row.get("quantity") or 1)
    sorted_items = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [
        {"product_id": pid, "purchase_frequency": count, "recommendation_rank": rank + 1}
        for rank, (pid, count) in enumerate(sorted_items)
    ]


# ---------------------------------------------------------------------------
# NiFi flow builders
# ---------------------------------------------------------------------------

def build_m_product_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PRODUCTS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "products*.csv",
         "Keep Source File": "true"}, (0, 0))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PRODUCTS",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_products"}, (200, 0))
    _connect(pg_id, get_file.id, put_db.id)


def build_m_product_category_hier(pg_id, csv_reader_id, db_pool_id, db_lookup_id):
    logger.info("Building m_product_category_hier sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PRODUCT_CATEGORIES",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "product_categories*.csv",
         "Keep Source File": "true"}, (0, 200))
    lookup = _add_processor(pg_id, "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_ParentCategoryName",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Lookup Service": db_lookup_id,
         "Result RecordPath": "/parent_name",
         "Routing Strategy": "Route to 'matched' or 'unmatched'",
         "parent_category_id": "/parent_category_id"}, (200, 200))
    path_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    for row in (rows if isinstance(rows, list) else [rows]):
        cat = row.get('category_name', '')
        parent = row.get('parent_name', '')
        row['hierarchy_path'] = f'{parent}/{cat}' if parent else cat
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(rows).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_BuildHierarchyPath",
        {"Script Engine": "python", "Script Body": path_script}, (400, 200))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_CATEGORY_HIER",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_category_hier"}, (600, 200))
    _connect(pg_id, get_file.id, lookup.id)
    _connect(pg_id, lookup.id, execute.id, "matched")
    _connect(pg_id, lookup.id, execute.id, "unmatched")
    _connect(pg_id, execute.id, put_db.id)


def build_m_product_price_hist(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_price_hist sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PRODUCT_PRICES",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "product_prices*.csv",
         "Keep Source File": "true"}, (0, 400))
    price_script = r"""
import json
from collections import defaultdict
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    prev = {}
    out = []
    for row in sorted((rows if isinstance(rows, list) else [rows]),
                       key=lambda r: (r.get('product_id',''), r.get('effective_date',''))):
        pid = row.get('product_id','')
        current = float(row.get('list_price') or 0)
        previous = prev.get(pid, current)
        change = round(current - previous, 2)
        pct = round(change / previous * 100, 2) if previous else 0.0
        out.append({'price_id': row.get('price_id'), 'product_id': pid,
                    'effective_date': row.get('effective_date'),
                    'list_price': current, 'price_change': change,
                    'price_change_pct': pct})
        prev[pid] = current
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_PriceHist",
        {"Script Engine": "python", "Script Body": price_script}, (200, 400))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PRICE_HIST",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_price_hist"}, (400, 400))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_product_inventory_load(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_inventory_load sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PRODUCT_INVENTORY",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "product_inventory*.csv",
         "Keep Source File": "true"}, (0, 600))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_INVENTORY",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_inventory"}, (200, 600))
    _connect(pg_id, get_file.id, put_db.id)


def build_m_product_inv_alert(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_inv_alert sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_ALERT_SRC_PRODUCT_INVENTORY",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "product_inventory*.csv",
         "Keep Source File": "true"}, (0, 800))
    alert_script = r"""
import json
def stock_alert(qty, reorder):
    if qty <= reorder * 0.5: return 'critical'
    if qty <= reorder: return 'low'
    return 'ok'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = []
    for row in (rows if isinstance(rows, list) else [rows]):
        qty = int(row.get('quantity_on_hand') or 0)
        reorder = int(row.get('reorder_level') or 0)
        status = stock_alert(qty, reorder)
        if status in ('critical', 'low'):
            out.append({**row, 'stock_status': status})
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_InvAlert",
        {"Script Engine": "python", "Script Body": alert_script}, (200, 800))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_INV_ALERT",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_inv_alert"}, (400, 800))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_product_supplier_join(pg_id, csv_reader_id, db_pool_id, db_lookup_id):
    logger.info("Building m_product_supplier_join sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_JOIN_SRC_PRODUCTS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "products*.csv",
         "Keep Source File": "true"}, (0, 1000))
    lookup = _add_processor(pg_id, "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_SupplierName",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Lookup Service": db_lookup_id,
         "Result RecordPath": "/supplier_name",
         "supplier_id": "/supplier_id"}, (200, 1000))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_PROD_SUPPLIER",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_prod_supplier"}, (400, 1000))
    _connect(pg_id, get_file.id, lookup.id)
    _connect(pg_id, lookup.id, put_db.id, "matched")


def build_m_product_bundle_pricing(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_bundle_pricing sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_BUNDLE_SRC_PRODUCTS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "products*.csv",
         "Keep Source File": "true"}, (0, 1200))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_BundlePrice",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "bundle": (
             "SELECT category, SUM(unit_price) * 0.85 AS bundle_price, "
             "COUNT(product_id) AS product_count "
             "FROM FLOWFILE GROUP BY category"
         )}, (200, 1200))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_BUNDLES",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_bundles"}, (400, 1200))
    _connect(pg_id, get_file.id, query.id)
    _connect(pg_id, query.id, put_db.id, "bundle")


def build_m_product_review_sentiment(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_review_sentiment sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_SRC_PRODUCT_REVIEWS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "product_reviews*.csv",
         "Keep Source File": "true"}, (0, 1400))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_AvgRating",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "sentiment": (
             "SELECT product_id, AVG(rating) AS avg_rating, COUNT(review_id) AS review_count "
             "FROM FLOWFILE GROUP BY product_id"
         )}, (200, 1400))
    sent_script = r"""
import json
def sentiment(avg):
    if avg >= 4.0: return 'positive'
    if avg >= 3.0: return 'neutral'
    return 'negative'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    for row in (rows if isinstance(rows, list) else [rows]):
        row['sentiment'] = sentiment(float(row.get('avg_rating') or 0))
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(rows).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_Sentiment",
        {"Script Engine": "python", "Script Body": sent_script}, (400, 1400))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_REVIEW_SENTIMENT",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_review_sentiment"}, (600, 1400))
    _connect(pg_id, get_file.id, query.id)
    _connect(pg_id, query.id, execute.id, "sentiment")
    _connect(pg_id, execute.id, put_db.id)


def build_m_product_lifecycle(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_lifecycle sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_LC_SRC_PRODUCTS",
        {"Input Directory": SOURCE_DATA_PATH, "File Filter": "products*.csv",
         "Keep Source File": "true"}, (0, 1600))
    lc_script = r"""
import json
from datetime import date, datetime
def lifecycle(launch, status):
    if (status or '').lower() == 'discontinued': return 'end_of_life'
    try:
        d = datetime.strptime(str(launch or '')[:10], '%Y-%m-%d').date()
        months = (date.today().year - d.year)*12 + (date.today().month - d.month)
    except: return 'unknown'
    if months < 6: return 'introduction'
    if months < 18: return 'growth'
    return 'maturity'
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    out = [{'product_id': r.get('product_id'), 'launch_date': r.get('launch_date'),
            'status': r.get('status'),
            'lifecycle_stage': lifecycle(r.get('launch_date'), r.get('status'))}
           for r in (rows if isinstance(rows, list) else [rows])]
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(out).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_Lifecycle",
        {"Script Engine": "python", "Script Body": lc_script}, (200, 1600))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_LIFECYCLE",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_lifecycle"}, (400, 1600))
    _connect(pg_id, get_file.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


def build_m_product_recommendation(pg_id, csv_reader_id, db_pool_id):
    logger.info("Building m_product_recommendation sub-flow…")
    get_file = _add_processor(pg_id, "org.apache.nifi.processors.standard.GetFile",
        "GetFile_REC_SRC_SALES_LINE_ITEMS",
        {"Input Directory": "/opt/nifi/source_data/sales",
         "File Filter": "sales_line_items*.csv",
         "Keep Source File": "true"}, (0, 1800))
    query = _add_processor(pg_id, "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_PurchaseFreq",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "freq": (
             "SELECT product_id, SUM(quantity) AS purchase_frequency "
             "FROM FLOWFILE GROUP BY product_id"
         )}, (200, 1800))
    sort = _add_processor(pg_id, "org.apache.nifi.processors.standard.SortRecord",
        "SortRecord_ByFrequency",
        {"Record Reader": csv_reader_id, "Record Writer": "",
         "Sort Attribute": "/purchase_frequency", "Sort Order": "Descending"}, (400, 1800))
    rank_script = r"""
import json
flowFile = session.get()
if flowFile is not None:
    rows = json.loads(session.read(flowFile).decode('utf-8'))
    for rank, row in enumerate(rows if isinstance(rows, list) else [rows]):
        row['recommendation_rank'] = rank + 1
    ff = session.create(flowFile)
    ff = session.write(ff, OutputStreamCallback(
        lambda os: os.write(json.dumps(rows).encode('utf-8'))))
    session.transfer(ff, REL_SUCCESS)
    session.remove(flowFile)
"""
    execute = _add_processor(pg_id, "org.apache.nifi.processors.script.ExecuteScript",
        "ExecuteScript_RecommendationRank",
        {"Script Engine": "python", "Script Body": rank_script}, (600, 1800))
    put_db = _add_processor(pg_id, "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDB_TGT_RECOMMENDATIONS",
        {"Record Reader": csv_reader_id,
         "Database Connection Pooling Service": db_pool_id,
         "Statement Type": "INSERT", "Table Name": "tgt_recommendations"}, (800, 1800))
    _connect(pg_id, get_file.id, query.id)
    _connect(pg_id, query.id, sort.id, "freq")
    _connect(pg_id, sort.id, execute.id)
    _connect(pg_id, execute.id, put_db.id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    logger.info("=== product.py: building PG_Product (10 mappings) ===")
    connect_nifi()
    root_pg_id = get_root_pg_id()

    def _svc_id(name):
        svc = nipyapi.canvas.get_controller_service(name)
        return svc.id if svc else ""

    csv_reader_id = _svc_id("XClarity_CSVReader")
    db_pool_id = _svc_id("XClarity_DBCPConnectionPool")
    db_lookup_id = _svc_id("XClarity_DatabaseRecordLookupService")

    pg = get_or_create_pg("PG_Product", parent_pg_id=root_pg_id)
    pg_id = pg.id if hasattr(pg, "id") else str(pg)

    builders_no_lookup = [
        build_m_product_load,
        build_m_product_price_hist,
        build_m_product_inventory_load,
        build_m_product_inv_alert,
        build_m_product_bundle_pricing,
        build_m_product_review_sentiment,
        build_m_product_lifecycle,
        build_m_product_recommendation,
    ]
    builders_with_lookup = [
        (build_m_product_category_hier, db_lookup_id),
        (build_m_product_supplier_join, db_lookup_id),
    ]

    for builder in builders_no_lookup:
        try:
            builder(pg_id, csv_reader_id, db_pool_id)
        except Exception as exc:
            logger.error("%s failed: %s", builder.__name__, exc)

    for builder, lookup_id in builders_with_lookup:
        try:
            builder(pg_id, csv_reader_id, db_pool_id, lookup_id)
        except Exception as exc:
            logger.error("%s failed: %s", builder.__name__, exc)

    logger.info("PG_Product build complete.")
