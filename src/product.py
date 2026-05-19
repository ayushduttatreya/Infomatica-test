"""
Product Domain - Apache NiFi Process Group
Migrated from Informatica PowerCenter XClarity_ETL

Covers tickets:
  - Migrate Product catalog load, category hierarchy, price history, and supplier mapping
  - Migrate Product inventory alerting, bundle pricing, review sentiment,
    lifecycle, and recommendations
  - Migrate Product Module: catalog, category hierarchy, price history,
    inventory alerting, supplier mapping, bundle pricing, sentiment,
    lifecycle, and recommendations

Sources : SRC_PRODUCTS, SRC_PRODUCT_CATEGORIES, SRC_PRODUCT_PRICES,
          SRC_PRODUCT_INVENTORY, SRC_PRODUCT_SUPPLIERS, SRC_PRODUCT_REVIEWS
Targets : TGT_PRODUCTS, TGT_CATEGORY_HIER, TGT_PRICE_HIST, TGT_INVENTORY,
          TGT_INV_ALERT, TGT_PROD_SUPPLIER, TGT_BUNDLES,
          TGT_REVIEW_SENTIMENT, TGT_LIFECYCLE, TGT_RECOMMENDATIONS
"""

import logging

import nipyapi
from nipyapi import canvas

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Groovy script for product lifecycle classification (DATE_DIFF)
# See customer.py for the shared GROOVY_LIFECYCLE snippet
# ---------------------------------------------------------------------------

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
            try {
                def launchDate = LocalDate.parse(launchStr[0..9])
                def months = ChronoUnit.MONTHS.between(launchDate, today)
                if (months < 6)      tier = 'introduction'
                else if (months < 18) tier = 'growth'
            } catch (Exception ignored) {
                tier = 'unknown'
            }
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


def create_product_process_group(parent_pg_id: str, nifi_url: str = "http://localhost:8080") -> None:
    """
    Build the full Product NiFi Process Group.

    Covers Informatica mappings:
      m_product_load, m_product_category_hier, m_product_price_hist,
      m_product_inventory, m_product_supplier_map, m_product_bundle_pricing,
      m_product_review_sentiment, m_product_lifecycle, m_product_recommendations
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    try:
        root = canvas.get_root_pg_id()
        parent_id = parent_pg_id or root

        logger.info("Creating Product Process Group under parent %s", parent_id)
        product_pg = canvas.create_process_group(
            canvas.get_process_group(parent_id),
            "Product",
            (1100, 100),
        )
        pg_id = product_pg.id
        logger.info("Product PG created: %s", pg_id)

        _build_product_load(pg_id)
        _build_category_hierarchy(pg_id)
        _build_price_history(pg_id)
        _build_inventory(pg_id)
        _build_supplier_map(pg_id)
        _build_bundle_pricing(pg_id)
        _build_review_sentiment(pg_id)
        _build_lifecycle(pg_id)
        _build_recommendations(pg_id)

        logger.info("Product Process Group fully built: %s", pg_id)

    except Exception as exc:
        logger.error("Failed to build Product Process Group: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Sub-flow builders
# ---------------------------------------------------------------------------

def _build_product_load(pg_id: str) -> None:
    """m_product_load: GetFile(SRC_PRODUCTS) -> CSVReader -> PutFile TGT_PRODUCTS"""
    logger.info("[m_product_load] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 200),
            "GetFile_SRC_PRODUCTS",
            {
                "Input Directory": "/data/inbound/products",
                "File Filter": "*.csv",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 200),
            "PutFile_TGT_PRODUCTS",
            {"Directory": "/data/outbound/TGT_PRODUCTS"},
        )

        canvas.create_connection(get, put, ["success"])

        logger.info("[m_product_load] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_load] Could not build via API (offline mode): %s", exc)


def _build_category_hierarchy(pg_id: str) -> None:
    """
    m_product_category_hier:
    GetFile(SRC_PRODUCT_CATEGORIES)
    -> LookupRecord (self-join on parent_category_id via JDBC)
    -> UpdateRecord (build path: parent_name > category_name)
    -> TGT_CATEGORY_HIER
    """
    logger.info("[m_product_category_hier] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 380),
            "GetFile_SRC_PRODUCT_CATEGORIES",
            {
                "Input Directory": "/data/inbound/product_categories",
                "File Filter": "*.csv",
            },
        )

        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (400, 380),
            "LookupRecord_CategoryParent",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "DatabaseRecordLookupService",
                "Result RecordPath": "/parent_category_name",
                "category_id": "/parent_category_id",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (700, 380),
            "UpdateRecord_CategoryPath",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/category_path": "${parent_category_name:isEmpty():ifElse(${category_name},'${parent_category_name} > ${category_name}')}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (1000, 380),
            "PutFile_TGT_CATEGORY_HIER",
            {"Directory": "/data/outbound/TGT_CATEGORY_HIER"},
        )

        canvas.create_connection(get, lookup, ["success"])
        canvas.create_connection(lookup, update, ["matched", "unmatched"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_product_category_hier] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_category_hier] Could not build via API (offline mode): %s", exc)


def _build_price_history(pg_id: str) -> None:
    """
    m_product_price_hist:
    GetFile(SRC_PRODUCT_PRICES)
    -> UpdateRecord:
         price_change     = sale_price - list_price
         price_change_pct = price_change / list_price * 100
    -> TGT_PRICE_HIST
    """
    logger.info("[m_product_price_hist] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 560),
            "GetFile_SRC_PRODUCT_PRICES",
            {
                "Input Directory": "/data/inbound/product_prices",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 560),
            "UpdateRecord_PriceHistory",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/price_change": "${sale_price:toNumber():minus(${list_price:toNumber()})}",
                "/price_change_pct": "${sale_price:toNumber():minus(${list_price:toNumber()}):divide(${list_price:toNumber()}):multiply(100)}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 560),
            "PutFile_TGT_PRICE_HIST",
            {"Directory": "/data/outbound/TGT_PRICE_HIST"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_product_price_hist] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_price_hist] Could not build via API (offline mode): %s", exc)


def _build_inventory(pg_id: str) -> None:
    """
    m_product_inventory:
    GetFile(SRC_PRODUCT_INVENTORY)
    -> UpdateRecord:
         stock_status = critical (qty <= 50% reorder_level),
                        low     (qty <= reorder_level),
                        ok
    -> RouteOnAttribute: low/critical -> TGT_INV_ALERT; all -> TGT_INVENTORY
    """
    logger.info("[m_product_inventory] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 740),
            "GetFile_SRC_PRODUCT_INVENTORY",
            {
                "Input Directory": "/data/inbound/product_inventory",
                "File Filter": "*.csv",
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 740),
            "UpdateRecord_StockStatus",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/stock_status": (
                    "${quantity_on_hand:toNumber():le(${reorder_level:toNumber():divide(2)}):ifElse('critical',"
                    "${quantity_on_hand:toNumber():le(${reorder_level:toNumber()}):ifElse('low','ok')})}"
                ),
            },
        )

        put_inventory = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 700),
            "PutFile_TGT_INVENTORY",
            {"Directory": "/data/outbound/TGT_INVENTORY"},
        )

        route = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.RouteOnAttribute"),
            (700, 820),
            "RouteOnAttribute_InvAlert",
            {
                "Routing Strategy": "Route to Property name",
                "alert": "${stock_status:equals('critical'):or(${stock_status:equals('low')})}",
            },
        )

        put_alert = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (1000, 820),
            "PutFile_TGT_INV_ALERT",
            {"Directory": "/data/outbound/TGT_INV_ALERT"},
        )

        canvas.create_connection(get, update, ["success"])
        canvas.create_connection(update, put_inventory, ["success"])
        canvas.create_connection(update, route, ["success"])
        canvas.create_connection(route, put_alert, ["alert"])

        logger.info("[m_product_inventory] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_inventory] Could not build via API (offline mode): %s", exc)


def _build_supplier_map(pg_id: str) -> None:
    """
    m_product_supplier_map:
    GetFile(SRC_PRODUCT_SUPPLIERS)
    -> LookupRecord (join products on supplier_id)
    -> TGT_PROD_SUPPLIER
    """
    logger.info("[m_product_supplier_map] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1020),
            "GetFile_SRC_PRODUCT_SUPPLIERS",
            {
                "Input Directory": "/data/inbound/product_suppliers",
                "File Filter": "*.csv",
            },
        )

        lookup = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.LookupRecord"),
            (400, 1020),
            "LookupRecord_SupplierMap",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "Lookup Service": "DatabaseRecordLookupService",
                "Result RecordPath": "/supplier_details",
                "supplier_id": "/supplier_id",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1020),
            "PutFile_TGT_PROD_SUPPLIER",
            {"Directory": "/data/outbound/TGT_PROD_SUPPLIER"},
        )

        canvas.create_connection(get, lookup, ["success"])
        canvas.create_connection(lookup, put, ["matched"])

        logger.info("[m_product_supplier_map] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_supplier_map] Could not build via API (offline mode): %s", exc)


def _build_bundle_pricing(pg_id: str) -> None:
    """
    m_product_bundle_pricing:
    QueryRecord (GROUP BY category, AVG price)
    -> UpdateRecord (bundle_price = avg_price * 0.85)
    -> TGT_BUNDLES
    """
    logger.info("[m_product_bundle_pricing] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1200),
            "QueryRecord_BundlePricing",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "bundle": (
                    "SELECT category, AVG(unit_price) AS avg_price "
                    "FROM FLOWFILE GROUP BY category"
                ),
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (400, 1200),
            "UpdateRecord_BundleDiscount",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/bundle_price": "${avg_price:toNumber():multiply(0.85)}",
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1200),
            "PutFile_TGT_BUNDLES",
            {"Directory": "/data/outbound/TGT_BUNDLES"},
        )

        canvas.create_connection(query, update, ["bundle"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_product_bundle_pricing] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_bundle_pricing] Could not build via API (offline mode): %s", exc)


def _build_review_sentiment(pg_id: str) -> None:
    """
    m_product_review_sentiment:
    GetFile(SRC_PRODUCT_REVIEWS)
    -> QueryRecord (AVG rating, COUNT reviews GROUP BY product_id)
    -> UpdateRecord (sentiment: positive>=4.0, neutral>=3.0, negative)
    -> TGT_REVIEW_SENTIMENT
    """
    logger.info("[m_product_review_sentiment] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1380),
            "GetFile_SRC_PRODUCT_REVIEWS",
            {
                "Input Directory": "/data/inbound/product_reviews",
                "File Filter": "*.csv",
            },
        )

        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (400, 1380),
            "QueryRecord_ReviewStats",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "review_stats": (
                    "SELECT product_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count "
                    "FROM FLOWFILE GROUP BY product_id"
                ),
            },
        )

        update = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.UpdateRecord"),
            (700, 1380),
            "UpdateRecord_Sentiment",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "/sentiment": (
                    "${avg_rating:toNumber():ge(4.0):ifElse('positive',"
                    "${avg_rating:toNumber():ge(3.0):ifElse('neutral','negative')})}"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (1000, 1380),
            "PutFile_TGT_REVIEW_SENTIMENT",
            {"Directory": "/data/outbound/TGT_REVIEW_SENTIMENT"},
        )

        canvas.create_connection(get, query, ["success"])
        canvas.create_connection(query, update, ["review_stats"])
        canvas.create_connection(update, put, ["success"])

        logger.info("[m_product_review_sentiment] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_review_sentiment] Could not build via API (offline mode): %s", exc)


def _build_lifecycle(pg_id: str) -> None:
    """
    m_product_lifecycle:
    GetFile(SRC_PRODUCTS)
    -> ExecuteScript (Groovy DATE_DIFF months since launch_date -> lifecycle tier)
    -> TGT_LIFECYCLE
    """
    logger.info("[m_product_lifecycle] Building sub-flow in PG %s", pg_id)

    try:
        get = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.GetFile"),
            (100, 1560),
            "GetFile_SRC_PRODUCTS_Lifecycle",
            {
                "Input Directory": "/data/inbound/products",
                "File Filter": "*.csv",
            },
        )

        execute = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.script.ExecuteScript"),
            (400, 1560),
            "ExecuteScript_Lifecycle",
            {
                "Script Engine": "Groovy",
                "Script Body": GROOVY_LIFECYCLE,
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (700, 1560),
            "PutFile_TGT_LIFECYCLE",
            {"Directory": "/data/outbound/TGT_LIFECYCLE"},
        )

        canvas.create_connection(get, execute, ["success"])
        canvas.create_connection(execute, put, ["success"])

        logger.info("[m_product_lifecycle] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_lifecycle] Could not build via API (offline mode): %s", exc)


def _build_recommendations(pg_id: str) -> None:
    """
    m_product_recommendations:
    QueryRecord (COUNT purchases GROUP BY product_id ORDER BY freq DESC)
    -> TGT_RECOMMENDATIONS
    """
    logger.info("[m_product_recommendations] Building sub-flow in PG %s", pg_id)

    try:
        query = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.QueryRecord"),
            (100, 1740),
            "QueryRecord_Recommendations",
            {
                "Record Reader": "CSVReader",
                "Record Writer": "CSVRecordSetWriter",
                "recommendations": (
                    "SELECT product_id, COUNT(*) AS purchase_frequency "
                    "FROM FLOWFILE GROUP BY product_id ORDER BY purchase_frequency DESC"
                ),
            },
        )

        put = canvas.create_processor(
            canvas.get_process_group(pg_id),
            canvas.get_processor_type("org.apache.nifi.processors.standard.PutFile"),
            (400, 1740),
            "PutFile_TGT_RECOMMENDATIONS",
            {"Directory": "/data/outbound/TGT_RECOMMENDATIONS"},
        )

        canvas.create_connection(query, put, ["recommendations"])

        logger.info("[m_product_recommendations] sub-flow complete")

    except Exception as exc:
        logger.warning("[m_product_recommendations] Could not build via API (offline mode): %s", exc)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = "http://localhost:8080", parent_pg_id: str = "") -> None:
    """Entry-point called by main.py to deploy the Product Process Group."""
    logger.info("=== Product domain deployment starting ===")
    create_product_process_group(parent_pg_id=parent_pg_id, nifi_url=nifi_url)
    logger.info("=== Product domain deployment complete ===")


if __name__ == "__main__":
    run()
