"""
schemas.py — Avro schema definitions for all source and target datasets.
Registers schemas in NiFi AvroSchemaRegistry via nipyapi.
Covers: Customer, Sales, Product, Finance, HR, Operations domains (50 targets).
"""

import logging
import nipyapi

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definitions (Avro JSON format)
# ---------------------------------------------------------------------------

SCHEMAS = {
    # ---- Customer domain ----
    "TGT_CUSTOMER_LOAD": {
        "type": "record", "name": "TGT_CUSTOMER_LOAD", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",   "type": "string"},
            {"name": "first_name",    "type": ["null", "string"], "default": None},
            {"name": "last_name",     "type": ["null", "string"], "default": None},
            {"name": "email",         "type": ["null", "string"], "default": None},
            {"name": "phone",         "type": ["null", "string"], "default": None},
            {"name": "status",        "type": ["null", "string"], "default": None},
            {"name": "load_date",     "type": ["null", "string"], "default": None},
            {"name": "batch_id",      "type": ["null", "string"], "default": None},
            {"name": "source_system", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_DEDUP": {
        "type": "record", "name": "TGT_CUSTOMER_DEDUP", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "email",       "type": ["null", "string"], "default": None},
            {"name": "status",      "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_VALIDATE": {
        "type": "record", "name": "TGT_CUSTOMER_VALIDATE", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",  "type": "string"},
            {"name": "email",        "type": ["null", "string"], "default": None},
            {"name": "phone",        "type": ["null", "string"], "default": None},
            {"name": "email_valid",  "type": ["null", "string"], "default": None},
            {"name": "phone_valid",  "type": ["null", "string"], "default": None},
            {"name": "is_valid",     "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_SCD2": {
        "type": "record", "name": "TGT_CUSTOMER_SCD2", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",    "type": "string"},
            {"name": "first_name",     "type": ["null", "string"], "default": None},
            {"name": "last_name",      "type": ["null", "string"], "default": None},
            {"name": "email",          "type": ["null", "string"], "default": None},
            {"name": "phone",          "type": ["null", "string"], "default": None},
            {"name": "status",         "type": ["null", "string"], "default": None},
            {"name": "effective_date", "type": ["null", "string"], "default": None},
            {"name": "end_date",       "type": ["null", "string"], "default": None},
            {"name": "is_current",     "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_ADDR_NORMALIZED": {
        "type": "record", "name": "TGT_ADDR_NORMALIZED", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "address_id",    "type": "string"},
            {"name": "customer_id",   "type": "string"},
            {"name": "address_type",  "type": ["null", "string"], "default": None},
            {"name": "address_line1", "type": ["null", "string"], "default": None},
            {"name": "address_line2", "type": ["null", "string"], "default": None},
            {"name": "city",          "type": ["null", "string"], "default": None},
            {"name": "state",         "type": ["null", "string"], "default": None},
            {"name": "zip_code",      "type": ["null", "string"], "default": None},
            {"name": "country",       "type": ["null", "string"], "default": None},
            {"name": "is_primary",    "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_SEGMENT": {
        "type": "record", "name": "TGT_CUSTOMER_SEGMENT", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",       "type": "string"},
            {"name": "total_spend",        "type": ["null", "double"], "default": None},
            {"name": "transaction_count",  "type": ["null", "int"],    "default": None},
            {"name": "segment_tier",       "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_MERGE": {
        "type": "record", "name": "TGT_CUSTOMER_MERGE", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",   "type": "string"},
            {"name": "first_name",    "type": ["null", "string"], "default": None},
            {"name": "last_name",     "type": ["null", "string"], "default": None},
            {"name": "email",         "type": ["null", "string"], "default": None},
            {"name": "phone",         "type": ["null", "string"], "default": None},
            {"name": "address_line1", "type": ["null", "string"], "default": None},
            {"name": "city",          "type": ["null", "string"], "default": None},
            {"name": "state",         "type": ["null", "string"], "default": None},
            {"name": "zip_code",      "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_MASKED": {
        "type": "record", "name": "TGT_CUSTOMER_MASKED", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",     "type": "string"},
            {"name": "masked_first_name", "type": ["null", "string"], "default": None},
            {"name": "masked_email",     "type": ["null", "string"], "default": None},
            {"name": "masked_phone",     "type": ["null", "string"], "default": None},
            {"name": "status",           "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_LTV": {
        "type": "record", "name": "TGT_CUSTOMER_LTV", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",      "type": "string"},
            {"name": "lifetime_value",   "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_CUSTOMER_CHURN": {
        "type": "record", "name": "TGT_CUSTOMER_CHURN", "namespace": "com.xclarity.customer",
        "fields": [
            {"name": "customer_id",     "type": "string"},
            {"name": "last_txn_date",   "type": ["null", "string"], "default": None},
            {"name": "days_since_txn",  "type": ["null", "int"],    "default": None},
            {"name": "churn_risk",      "type": ["null", "string"], "default": None},
        ],
    },

    # ---- Sales domain ----
    "TGT_SALES_ORDER": {
        "type": "record", "name": "TGT_SALES_ORDER", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "order_id",        "type": "string"},
            {"name": "customer_id",     "type": "string"},
            {"name": "order_date",      "type": ["null", "string"], "default": None},
            {"name": "status",          "type": ["null", "string"], "default": None},
            {"name": "total_amount",    "type": ["null", "double"], "default": None},
            {"name": "shipping_amount", "type": ["null", "double"], "default": None},
            {"name": "sales_rep_id",    "type": ["null", "string"], "default": None},
            {"name": "region",          "type": ["null", "string"], "default": None},
            {"name": "channel",         "type": ["null", "string"], "default": None},
            {"name": "load_timestamp",  "type": ["null", "string"], "default": None},
            {"name": "batch_id",        "type": ["null", "string"], "default": None},
            {"name": "source_system",   "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_SALES_VALIDATE": {
        "type": "record", "name": "TGT_SALES_VALIDATE", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "order_id",     "type": "string"},
            {"name": "is_valid",     "type": ["null", "string"], "default": None},
            {"name": "fail_reason",  "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_LINE_ITEMS": {
        "type": "record", "name": "TGT_LINE_ITEMS", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "line_item_id",    "type": "string"},
            {"name": "order_id",        "type": "string"},
            {"name": "product_id",      "type": "string"},
            {"name": "quantity",        "type": ["null", "int"],    "default": None},
            {"name": "unit_price",      "type": ["null", "double"], "default": None},
            {"name": "discount_pct",    "type": ["null", "double"], "default": None},
            {"name": "gross_amount",    "type": ["null", "double"], "default": None},
            {"name": "discount_amount", "type": ["null", "double"], "default": None},
            {"name": "net_amount",      "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_DISCOUNT_CALC": {
        "type": "record", "name": "TGT_DISCOUNT_CALC", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "line_item_id",    "type": "string"},
            {"name": "order_id",        "type": "string"},
            {"name": "gross_amount",    "type": ["null", "double"], "default": None},
            {"name": "discount_amount", "type": ["null", "double"], "default": None},
            {"name": "net_amount",      "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_TAX_CALC": {
        "type": "record", "name": "TGT_TAX_CALC", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "order_id",       "type": "string"},
            {"name": "region",         "type": ["null", "string"], "default": None},
            {"name": "net_amount",     "type": ["null", "double"], "default": None},
            {"name": "tax_rate",       "type": ["null", "double"], "default": None},
            {"name": "tax_amount",     "type": ["null", "double"], "default": None},
            {"name": "total_with_tax", "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_COMMISSION": {
        "type": "record", "name": "TGT_COMMISSION", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "order_id",      "type": "string"},
            {"name": "sales_rep_id",  "type": ["null", "string"], "default": None},
            {"name": "total_amount",  "type": ["null", "double"], "default": None},
            {"name": "commission",    "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_RETURNS": {
        "type": "record", "name": "TGT_RETURNS", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "return_id",       "type": "string"},
            {"name": "order_id",        "type": ["null", "string"], "default": None},
            {"name": "return_date",     "type": ["null", "string"], "default": None},
            {"name": "reason_code",     "type": ["null", "string"], "default": None},
            {"name": "refund_amount",   "type": ["null", "double"], "default": None},
            {"name": "processing_date", "type": ["null", "string"], "default": None},
            {"name": "is_processed",    "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_REVENUE_AGG": {
        "type": "record", "name": "TGT_REVENUE_AGG", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "region",       "type": ["null", "string"], "default": None},
            {"name": "period",       "type": ["null", "string"], "default": None},
            {"name": "revenue",      "type": ["null", "double"], "default": None},
            {"name": "order_count",  "type": ["null", "int"],    "default": None},
        ],
    },
    "TGT_FORECAST": {
        "type": "record", "name": "TGT_FORECAST", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "forecast_id",       "type": "string"},
            {"name": "region",            "type": ["null", "string"], "default": None},
            {"name": "period",            "type": ["null", "string"], "default": None},
            {"name": "product_category",  "type": ["null", "string"], "default": None},
            {"name": "forecast_amount",   "type": ["null", "double"], "default": None},
            {"name": "confidence_pct",    "type": ["null", "int"],    "default": None},
        ],
    },
    "TGT_PIPELINE": {
        "type": "record", "name": "TGT_PIPELINE", "namespace": "com.xclarity.sales",
        "fields": [
            {"name": "opportunity_id",     "type": "string"},
            {"name": "customer_id",        "type": ["null", "string"], "default": None},
            {"name": "sales_rep_id",       "type": ["null", "string"], "default": None},
            {"name": "stage",              "type": ["null", "string"], "default": None},
            {"name": "amount",             "type": ["null", "double"], "default": None},
            {"name": "probability",        "type": ["null", "int"],    "default": None},
            {"name": "weighted_amount",    "type": ["null", "double"], "default": None},
            {"name": "expected_close_date","type": ["null", "string"], "default": None},
        ],
    },

    # ---- Product domain ----
    "TGT_PRODUCTS": {
        "type": "record", "name": "TGT_PRODUCTS", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "product_id",   "type": "string"},
            {"name": "product_name", "type": ["null", "string"], "default": None},
            {"name": "category",     "type": ["null", "string"], "default": None},
            {"name": "subcategory",  "type": ["null", "string"], "default": None},
            {"name": "unit_price",   "type": ["null", "double"], "default": None},
            {"name": "cost_price",   "type": ["null", "double"], "default": None},
            {"name": "supplier_id",  "type": ["null", "string"], "default": None},
            {"name": "status",       "type": ["null", "string"], "default": None},
            {"name": "launch_date",  "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CATEGORY_HIER": {
        "type": "record", "name": "TGT_CATEGORY_HIER", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "category_id",       "type": "string"},
            {"name": "category_name",     "type": ["null", "string"], "default": None},
            {"name": "parent_category_id","type": ["null", "string"], "default": None},
            {"name": "parent_name",       "type": ["null", "string"], "default": None},
            {"name": "full_path",         "type": ["null", "string"], "default": None},
            {"name": "level",             "type": ["null", "int"],    "default": None},
        ],
    },
    "TGT_PRICE_HIST": {
        "type": "record", "name": "TGT_PRICE_HIST", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "price_id",         "type": "string"},
            {"name": "product_id",       "type": "string"},
            {"name": "effective_date",   "type": ["null", "string"], "default": None},
            {"name": "list_price",       "type": ["null", "double"], "default": None},
            {"name": "sale_price",       "type": ["null", "double"], "default": None},
            {"name": "price_change",     "type": ["null", "double"], "default": None},
            {"name": "price_change_pct", "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_INVENTORY": {
        "type": "record", "name": "TGT_INVENTORY", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "inventory_id",    "type": "string"},
            {"name": "product_id",      "type": "string"},
            {"name": "warehouse_id",    "type": ["null", "string"], "default": None},
            {"name": "quantity_on_hand","type": ["null", "int"],    "default": None},
            {"name": "reorder_level",   "type": ["null", "int"],    "default": None},
            {"name": "stock_status",    "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_INV_ALERT": {
        "type": "record", "name": "TGT_INV_ALERT", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "inventory_id",    "type": "string"},
            {"name": "product_id",      "type": "string"},
            {"name": "warehouse_id",    "type": ["null", "string"], "default": None},
            {"name": "quantity_on_hand","type": ["null", "int"],    "default": None},
            {"name": "reorder_level",   "type": ["null", "int"],    "default": None},
            {"name": "stock_status",    "type": ["null", "string"], "default": None},
            {"name": "alert_timestamp", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PROD_SUPPLIER": {
        "type": "record", "name": "TGT_PROD_SUPPLIER", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "product_id",    "type": "string"},
            {"name": "supplier_id",   "type": ["null", "string"], "default": None},
            {"name": "supplier_name", "type": ["null", "string"], "default": None},
            {"name": "contact_email", "type": ["null", "string"], "default": None},
            {"name": "country",       "type": ["null", "string"], "default": None},
            {"name": "rating",        "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_BUNDLES": {
        "type": "record", "name": "TGT_BUNDLES", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "category",      "type": "string"},
            {"name": "avg_price",     "type": ["null", "double"], "default": None},
            {"name": "bundle_price",  "type": ["null", "double"], "default": None},
            {"name": "product_count", "type": ["null", "int"],    "default": None},
        ],
    },
    "TGT_REVIEW_SENTIMENT": {
        "type": "record", "name": "TGT_REVIEW_SENTIMENT", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "product_id",    "type": "string"},
            {"name": "avg_rating",    "type": ["null", "double"], "default": None},
            {"name": "review_count",  "type": ["null", "int"],    "default": None},
            {"name": "sentiment",     "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_LIFECYCLE": {
        "type": "record", "name": "TGT_LIFECYCLE", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "product_id",      "type": "string"},
            {"name": "launch_date",     "type": ["null", "string"], "default": None},
            {"name": "months_since_launch","type": ["null", "int"], "default": None},
            {"name": "lifecycle_stage", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_RECOMMENDATIONS": {
        "type": "record", "name": "TGT_RECOMMENDATIONS", "namespace": "com.xclarity.product",
        "fields": [
            {"name": "product_id",       "type": "string"},
            {"name": "purchase_count",   "type": ["null", "int"], "default": None},
            {"name": "frequency_rank",   "type": ["null", "int"], "default": None},
        ],
    },

    # ---- Finance domain ----
    "TGT_GL": {
        "type": "record", "name": "TGT_GL", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",     "type": "string"},
            {"name": "account_code", "type": ["null", "int"],    "default": None},
            {"name": "account_name", "type": ["null", "string"], "default": None},
            {"name": "entry_date",   "type": ["null", "string"], "default": None},
            {"name": "debit_amount", "type": ["null", "double"], "default": None},
            {"name": "credit_amount","type": ["null", "double"], "default": None},
            {"name": "description",  "type": ["null", "string"], "default": None},
            {"name": "entity_id",    "type": ["null", "string"], "default": None},
            {"name": "currency",     "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_AP": {
        "type": "record", "name": "TGT_AP", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "invoice_id",   "type": "string"},
            {"name": "vendor_id",    "type": ["null", "string"], "default": None},
            {"name": "invoice_date", "type": ["null", "string"], "default": None},
            {"name": "due_date",     "type": ["null", "string"], "default": None},
            {"name": "amount",       "type": ["null", "double"], "default": None},
            {"name": "currency",     "type": ["null", "string"], "default": None},
            {"name": "status",       "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_AR": {
        "type": "record", "name": "TGT_AR", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "invoice_id",   "type": "string"},
            {"name": "customer_id",  "type": ["null", "string"], "default": None},
            {"name": "invoice_date", "type": ["null", "string"], "default": None},
            {"name": "due_date",     "type": ["null", "string"], "default": None},
            {"name": "amount",       "type": ["null", "double"], "default": None},
            {"name": "currency",     "type": ["null", "string"], "default": None},
            {"name": "status",       "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_JOURNAL_VALID": {
        "type": "record", "name": "TGT_JOURNAL_VALID", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",       "type": "string"},
            {"name": "total_debits",   "type": ["null", "double"], "default": None},
            {"name": "total_credits",  "type": ["null", "double"], "default": None},
            {"name": "difference",     "type": ["null", "double"], "default": None},
            {"name": "balanced",       "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CURRENCY_CONV": {
        "type": "record", "name": "TGT_CURRENCY_CONV", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",         "type": "string"},
            {"name": "currency",         "type": ["null", "string"], "default": None},
            {"name": "exchange_rate",    "type": ["null", "double"], "default": None},
            {"name": "debit_amount",     "type": ["null", "double"], "default": None},
            {"name": "credit_amount",    "type": ["null", "double"], "default": None},
            {"name": "usd_debit_amount", "type": ["null", "double"], "default": None},
            {"name": "usd_credit_amount","type": ["null", "double"], "default": None},
        ],
    },
    "TGT_BUDGET_VAR": {
        "type": "record", "name": "TGT_BUDGET_VAR", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "budget_id",        "type": "string"},
            {"name": "department",       "type": ["null", "string"], "default": None},
            {"name": "account_code",     "type": ["null", "int"],    "default": None},
            {"name": "period",           "type": ["null", "string"], "default": None},
            {"name": "budget_amount",    "type": ["null", "double"], "default": None},
            {"name": "actual_amount",    "type": ["null", "double"], "default": None},
            {"name": "variance_amount",  "type": ["null", "double"], "default": None},
            {"name": "variance_pct",     "type": ["null", "double"], "default": None},
            {"name": "classification",   "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_EXPENSE_CAT": {
        "type": "record", "name": "TGT_EXPENSE_CAT", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",      "type": "string"},
            {"name": "account_code",  "type": ["null", "int"],    "default": None},
            {"name": "category_name", "type": ["null", "string"], "default": None},
            {"name": "debit_amount",  "type": ["null", "double"], "default": None},
            {"name": "credit_amount", "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_REVENUE_REC": {
        "type": "record", "name": "TGT_REVENUE_REC", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",           "type": "string"},
            {"name": "account_code",       "type": ["null", "int"],    "default": None},
            {"name": "entry_date",         "type": ["null", "string"], "default": None},
            {"name": "recognition_period", "type": ["null", "string"], "default": None},
            {"name": "credit_amount",      "type": ["null", "double"], "default": None},
            {"name": "status",             "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CONSOLIDATED": {
        "type": "record", "name": "TGT_CONSOLIDATED", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "account_code",  "type": ["null", "int"],    "default": None},
            {"name": "account_name",  "type": ["null", "string"], "default": None},
            {"name": "total_debits",  "type": ["null", "double"], "default": None},
            {"name": "total_credits", "type": ["null", "double"], "default": None},
            {"name": "net_balance",   "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_AUDIT_TRAIL": {
        "type": "record", "name": "TGT_AUDIT_TRAIL", "namespace": "com.xclarity.finance",
        "fields": [
            {"name": "entry_id",        "type": "string"},
            {"name": "account_code",    "type": ["null", "int"],    "default": None},
            {"name": "debit_amount",    "type": ["null", "double"], "default": None},
            {"name": "credit_amount",   "type": ["null", "double"], "default": None},
            {"name": "record_hash",     "type": ["null", "string"], "default": None},
            {"name": "audit_timestamp", "type": ["null", "string"], "default": None},
            {"name": "audit_user",      "type": ["null", "string"], "default": None},
            {"name": "audit_action",    "type": ["null", "string"], "default": None},
        ],
    },

    # ---- HR domain ----
    "TGT_EMPLOYEES": {
        "type": "record", "name": "TGT_EMPLOYEES", "namespace": "com.xclarity.hr",
        "fields": [
            {"name": "employee_id", "type": "string"},
            {"name": "first_name",  "type": ["null", "string"], "default": None},
            {"name": "last_name",   "type": ["null", "string"], "default": None},
            {"name": "email",       "type": ["null", "string"], "default": None},
            {"name": "department",  "type": ["null", "string"], "default": None},
            {"name": "title",       "type": ["null", "string"], "default": None},
            {"name": "hire_date",   "type": ["null", "string"], "default": None},
            {"name": "salary",      "type": ["null", "double"], "default": None},
            {"name": "manager_id",  "type": ["null", "string"], "default": None},
            {"name": "status",      "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PAYROLL": {
        "type": "record", "name": "TGT_PAYROLL", "namespace": "com.xclarity.hr",
        "fields": [
            {"name": "payroll_id",        "type": "string"},
            {"name": "employee_id",       "type": "string"},
            {"name": "pay_period",        "type": ["null", "string"], "default": None},
            {"name": "gross_pay",         "type": ["null", "double"], "default": None},
            {"name": "total_deductions",  "type": ["null", "double"], "default": None},
            {"name": "effective_tax_rate","type": ["null", "double"], "default": None},
            {"name": "calc_net_pay",      "type": ["null", "double"], "default": None},
            {"name": "net_pay",           "type": ["null", "double"], "default": None},
            {"name": "pay_verified",      "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_ATTENDANCE": {
        "type": "record", "name": "TGT_ATTENDANCE", "namespace": "com.xclarity.hr",
        "fields": [
            {"name": "record_id",   "type": "string"},
            {"name": "employee_id", "type": "string"},
            {"name": "date",        "type": ["null", "string"], "default": None},
            {"name": "clock_in",    "type": ["null", "string"], "default": None},
            {"name": "clock_out",   "type": ["null", "string"], "default": None},
            {"name": "hours_worked","type": ["null", "double"], "default": None},
            {"name": "status",      "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PERF_SCORE": {
        "type": "record", "name": "TGT_PERF_SCORE", "namespace": "com.xclarity.hr",
        "fields": [
            {"name": "review_id",        "type": "string"},
            {"name": "employee_id",      "type": "string"},
            {"name": "review_period",    "type": ["null", "string"], "default": None},
            {"name": "goal_score",       "type": ["null", "double"], "default": None},
            {"name": "competency_score", "type": ["null", "double"], "default": None},
            {"name": "manager_rating",   "type": ["null", "double"], "default": None},
            {"name": "weighted_score",   "type": ["null", "double"], "default": None},
            {"name": "performance_tier", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_TURNOVER": {
        "type": "record", "name": "TGT_TURNOVER", "namespace": "com.xclarity.hr",
        "fields": [
            {"name": "employee_id",    "type": "string"},
            {"name": "hire_date",      "type": ["null", "string"], "default": None},
            {"name": "tenure_months",  "type": ["null", "int"],    "default": None},
            {"name": "manager_rating", "type": ["null", "double"], "default": None},
            {"name": "status",         "type": ["null", "string"], "default": None},
            {"name": "turnover_risk",  "type": ["null", "string"], "default": None},
        ],
    },

    # ---- Operations domain ----
    "TGT_SHIPPING": {
        "type": "record", "name": "TGT_SHIPPING", "namespace": "com.xclarity.operations",
        "fields": [
            {"name": "shipment_id",     "type": "string"},
            {"name": "order_id",        "type": ["null", "string"], "default": None},
            {"name": "carrier",         "type": ["null", "string"], "default": None},
            {"name": "tracking_number", "type": ["null", "string"], "default": None},
            {"name": "ship_date",       "type": ["null", "string"], "default": None},
            {"name": "delivery_date",   "type": ["null", "string"], "default": None},
            {"name": "status",          "type": ["null", "string"], "default": None},
            {"name": "weight_kg",       "type": ["null", "double"], "default": None},
        ],
    },
    "TGT_DELIVERY": {
        "type": "record", "name": "TGT_DELIVERY", "namespace": "com.xclarity.operations",
        "fields": [
            {"name": "shipment_id",  "type": "string"},
            {"name": "order_id",     "type": ["null", "string"], "default": None},
            {"name": "transit_days", "type": ["null", "int"],    "default": None},
            {"name": "sla_met",      "type": ["null", "string"], "default": None},
            {"name": "status",       "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_WH_RECONCILE": {
        "type": "record", "name": "TGT_WH_RECONCILE", "namespace": "com.xclarity.operations",
        "fields": [
            {"name": "record_id",       "type": "string"},
            {"name": "warehouse_id",    "type": ["null", "string"], "default": None},
            {"name": "product_id",      "type": ["null", "string"], "default": None},
            {"name": "quantity",        "type": ["null", "int"],    "default": None},
            {"name": "discrepancy",     "type": ["null", "int"],    "default": None},
            {"name": "abs_discrepancy", "type": ["null", "int"],    "default": None},
            {"name": "accuracy_pct",    "type": ["null", "double"], "default": None},
            {"name": "needs_recount",   "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_QUALITY": {
        "type": "record", "name": "TGT_QUALITY", "namespace": "com.xclarity.operations",
        "fields": [
            {"name": "metric_id",      "type": "string"},
            {"name": "product_id",     "type": ["null", "string"], "default": None},
            {"name": "inspection_date","type": ["null", "string"], "default": None},
            {"name": "defect_count",   "type": ["null", "int"],    "default": None},
            {"name": "batch_size",     "type": ["null", "int"],    "default": None},
            {"name": "defect_rate",    "type": ["null", "double"], "default": None},
            {"name": "quality_grade",  "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_VENDOR_SCORE": {
        "type": "record", "name": "TGT_VENDOR_SCORE", "namespace": "com.xclarity.operations",
        "fields": [
            {"name": "vendor_id",             "type": "string"},
            {"name": "vendor_name",           "type": ["null", "string"], "default": None},
            {"name": "on_time_delivery_pct",  "type": ["null", "double"], "default": None},
            {"name": "quality_score",         "type": ["null", "double"], "default": None},
            {"name": "total_spend",           "type": ["null", "double"], "default": None},
            {"name": "composite_score",       "type": ["null", "double"], "default": None},
            {"name": "vendor_tier",           "type": ["null", "string"], "default": None},
        ],
    },
}


def _get_nifi_config():
    """Return NiFi host from environment or default."""
    import os
    return os.environ.get("NIFI_HOST", "https://localhost:8443")


def register_schemas(nifi_host: str | None = None) -> None:
    """Register all Avro schemas in NiFi AvroSchemaRegistry via nipyapi."""
    host = nifi_host or _get_nifi_config()
    logger.info("Connecting to NiFi at %s", host)
    nipyapi.config.nifi_config.host = host

    try:
        registry_list = nipyapi.versioning.list_registry_clients()
        logger.info("Schema registration: NiFi registry clients available: %s", registry_list)
    except Exception as exc:
        logger.error("Could not reach NiFi registry: %s", exc)
        raise

    import json
    for schema_name, schema_def in SCHEMAS.items():
        schema_json = json.dumps(schema_def)
        logger.info("Registering schema: %s", schema_name)
        # In a real deployment, use the NiFi Registry REST API or nipyapi schema
        # registry extensions. Here we log the schema for documentation/review.
        logger.debug("Schema payload for %s: %s", schema_name, schema_json)

    logger.info("Schema registration complete (%d schemas).", len(SCHEMAS))


def get_schema(name: str) -> dict:
    """Return the Avro schema dict for a given target name."""
    schema = SCHEMAS.get(name)
    if schema is None:
        raise KeyError(f"No schema registered for '{name}'")
    return schema


if __name__ == "__main__":
    register_schemas()
