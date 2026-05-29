"""
src/schemas.py
Avro schema definitions for all 25 source tables and 40 target tables
derived from mappings/informatica_repository.xml.

Registers all schemas into NiFi's AvroSchemaRegistry controller service
via nipyapi. Schema files are also written to ./schemas/ for Git versioning.

Ticket: Register Avro schemas for all 25 source tables and 40 target tables
"""

import json
import logging
import os
from pathlib import Path

import nipyapi
from nipyapi import canvas, versioning

from src.utils import connect_nifi, get_root_pg_id

logger = logging.getLogger("xclarity_etl.schemas")

SCHEMAS_DIR = Path(os.environ.get("SCHEMAS_DIR", "./schemas"))
SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Source Avro schemas (25 tables)
# ---------------------------------------------------------------------------

SOURCE_SCHEMAS: dict[str, dict] = {
    "SRC_CUSTOMERS": {
        "type": "record",
        "name": "SRC_CUSTOMERS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "phone", "type": ["null", "string"], "default": None},
            {"name": "address_line1", "type": ["null", "string"], "default": None},
            {"name": "address_line2", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "state", "type": ["null", "string"], "default": None},
            {"name": "zip_code", "type": ["null", "string"], "default": None},
            {"name": "country", "type": ["null", "string"], "default": None},
            {"name": "registration_date", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_CUSTOMER_ADDRESSES": {
        "type": "record",
        "name": "SRC_CUSTOMER_ADDRESSES",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "address_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "address_type", "type": ["null", "string"], "default": None},
            {"name": "address_line1", "type": ["null", "string"], "default": None},
            {"name": "address_line2", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "state", "type": ["null", "string"], "default": None},
            {"name": "zip_code", "type": ["null", "string"], "default": None},
            {"name": "country", "type": ["null", "string"], "default": None},
            {"name": "is_primary", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_CUSTOMER_TRANSACTIONS": {
        "type": "record",
        "name": "SRC_CUSTOMER_TRANSACTIONS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "transaction_date", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "transaction_type", "type": ["null", "string"], "default": None},
            {"name": "channel", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_SALES_ORDERS": {
        "type": "record",
        "name": "SRC_SALES_ORDERS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "order_date", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "total_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "shipping_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "sales_rep_id", "type": ["null", "string"], "default": None},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "channel", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_SALES_LINE_ITEMS": {
        "type": "record",
        "name": "SRC_SALES_LINE_ITEMS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "line_item_id", "type": "string"},
            {"name": "order_id", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "quantity", "type": ["null", "int"], "default": None},
            {"name": "unit_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "discount_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}], "default": None},
            {"name": "line_total", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
        ],
    },
    "SRC_SALES_RETURNS": {
        "type": "record",
        "name": "SRC_SALES_RETURNS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "return_id", "type": "string"},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "line_item_id", "type": ["null", "string"], "default": None},
            {"name": "return_date", "type": ["null", "string"], "default": None},
            {"name": "reason_code", "type": ["null", "string"], "default": None},
            {"name": "refund_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_SALES_FORECAST": {
        "type": "record",
        "name": "SRC_SALES_FORECAST",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "forecast_id", "type": "string"},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "period", "type": ["null", "string"], "default": None},
            {"name": "product_category", "type": ["null", "string"], "default": None},
            {"name": "forecast_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "confidence_pct", "type": ["null", "int"], "default": None},
        ],
    },
    "SRC_SALES_PIPELINE": {
        "type": "record",
        "name": "SRC_SALES_PIPELINE",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "opportunity_id", "type": "string"},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "sales_rep_id", "type": ["null", "string"], "default": None},
            {"name": "stage", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "probability", "type": ["null", "int"], "default": None},
            {"name": "expected_close_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCTS": {
        "type": "record",
        "name": "SRC_PRODUCTS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "product_name", "type": ["null", "string"], "default": None},
            {"name": "category", "type": ["null", "string"], "default": None},
            {"name": "subcategory", "type": ["null", "string"], "default": None},
            {"name": "unit_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "cost_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "supplier_id", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "launch_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCT_CATEGORIES": {
        "type": "record",
        "name": "SRC_PRODUCT_CATEGORIES",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "category_id", "type": "string"},
            {"name": "category_name", "type": ["null", "string"], "default": None},
            {"name": "parent_category_id", "type": ["null", "string"], "default": None},
            {"name": "level", "type": ["null", "int"], "default": None},
            {"name": "description", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCT_INVENTORY": {
        "type": "record",
        "name": "SRC_PRODUCT_INVENTORY",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "inventory_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "warehouse_id", "type": ["null", "string"], "default": None},
            {"name": "quantity_on_hand", "type": ["null", "int"], "default": None},
            {"name": "reorder_level", "type": ["null", "int"], "default": None},
            {"name": "last_restock_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCT_SUPPLIERS": {
        "type": "record",
        "name": "SRC_PRODUCT_SUPPLIERS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "supplier_id", "type": "string"},
            {"name": "supplier_name", "type": ["null", "string"], "default": None},
            {"name": "contact_email", "type": ["null", "string"], "default": None},
            {"name": "contact_phone", "type": ["null", "string"], "default": None},
            {"name": "country", "type": ["null", "string"], "default": None},
            {"name": "rating", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
            {"name": "payment_terms", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCT_REVIEWS": {
        "type": "record",
        "name": "SRC_PRODUCT_REVIEWS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "review_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "rating", "type": ["null", "int"], "default": None},
            {"name": "review_text", "type": ["null", "string"], "default": None},
            {"name": "review_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PRODUCT_PRICES": {
        "type": "record",
        "name": "SRC_PRODUCT_PRICES",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "price_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "effective_date", "type": ["null", "string"], "default": None},
            {"name": "end_date", "type": ["null", "string"], "default": None},
            {"name": "list_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "sale_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
        ],
    },
    "SRC_GENERAL_LEDGER": {
        "type": "record",
        "name": "SRC_GENERAL_LEDGER",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "account_code", "type": ["null", "int"], "default": None},
            {"name": "account_name", "type": ["null", "string"], "default": None},
            {"name": "entry_date", "type": ["null", "string"], "default": None},
            {"name": "debit_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "credit_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "description", "type": ["null", "string"], "default": None},
            {"name": "entity_id", "type": ["null", "string"], "default": None},
            {"name": "currency", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_ACCOUNTS_PAYABLE": {
        "type": "record",
        "name": "SRC_ACCOUNTS_PAYABLE",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "invoice_id", "type": "string"},
            {"name": "vendor_id", "type": ["null", "string"], "default": None},
            {"name": "invoice_date", "type": ["null", "string"], "default": None},
            {"name": "due_date", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "currency", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "payment_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_ACCOUNTS_RECEIVABLE": {
        "type": "record",
        "name": "SRC_ACCOUNTS_RECEIVABLE",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "invoice_id", "type": "string"},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "invoice_date", "type": ["null", "string"], "default": None},
            {"name": "due_date", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "currency", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "payment_date", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_BUDGET": {
        "type": "record",
        "name": "SRC_BUDGET",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "budget_id", "type": "string"},
            {"name": "department", "type": ["null", "string"], "default": None},
            {"name": "account_code", "type": ["null", "int"], "default": None},
            {"name": "period", "type": ["null", "string"], "default": None},
            {"name": "budget_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "actual_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "SRC_EXCHANGE_RATES": {
        "type": "record",
        "name": "SRC_EXCHANGE_RATES",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "from_currency", "type": "string"},
            {"name": "to_currency", "type": ["null", "string"], "default": None},
            {"name": "rate_date", "type": ["null", "string"], "default": None},
            {"name": "exchange_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 4}], "default": None},
        ],
    },
    "SRC_EMPLOYEES": {
        "type": "record",
        "name": "SRC_EMPLOYEES",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "employee_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "department", "type": ["null", "string"], "default": None},
            {"name": "title", "type": ["null", "string"], "default": None},
            {"name": "hire_date", "type": ["null", "string"], "default": None},
            {"name": "salary", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "manager_id", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PAYROLL": {
        "type": "record",
        "name": "SRC_PAYROLL",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "payroll_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "pay_period", "type": ["null", "string"], "default": None},
            {"name": "gross_pay", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "federal_tax", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "state_tax", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "insurance", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "retirement_401k", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "net_pay", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
        ],
    },
    "SRC_ATTENDANCE": {
        "type": "record",
        "name": "SRC_ATTENDANCE",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "date", "type": ["null", "string"], "default": None},
            {"name": "clock_in", "type": ["null", "string"], "default": None},
            {"name": "clock_out", "type": ["null", "string"], "default": None},
            {"name": "hours_worked", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_PERFORMANCE": {
        "type": "record",
        "name": "SRC_PERFORMANCE",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "review_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "review_period", "type": ["null", "string"], "default": None},
            {"name": "goal_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
            {"name": "competency_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
            {"name": "manager_rating", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
            {"name": "self_rating", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
        ],
    },
    "SRC_SHIPPING": {
        "type": "record",
        "name": "SRC_SHIPPING",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "shipment_id", "type": "string"},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "carrier", "type": ["null", "string"], "default": None},
            {"name": "tracking_number", "type": ["null", "string"], "default": None},
            {"name": "ship_date", "type": ["null", "string"], "default": None},
            {"name": "delivery_date", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "weight_kg", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 1}], "default": None},
        ],
    },
    "SRC_WAREHOUSE_INVENTORY": {
        "type": "record",
        "name": "SRC_WAREHOUSE_INVENTORY",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "warehouse_id", "type": ["null", "string"], "default": None},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "quantity", "type": ["null", "int"], "default": None},
            {"name": "location_bin", "type": ["null", "string"], "default": None},
            {"name": "last_count_date", "type": ["null", "string"], "default": None},
            {"name": "discrepancy", "type": ["null", "int"], "default": None},
        ],
    },
    "SRC_QUALITY_METRICS": {
        "type": "record",
        "name": "SRC_QUALITY_METRICS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "metric_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "inspection_date", "type": ["null", "string"], "default": None},
            {"name": "defect_count", "type": ["null", "int"], "default": None},
            {"name": "batch_size", "type": ["null", "int"], "default": None},
            {"name": "pass_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}], "default": None},
            {"name": "inspector_id", "type": ["null", "string"], "default": None},
        ],
    },
    "SRC_VENDORS": {
        "type": "record",
        "name": "SRC_VENDORS",
        "namespace": "xclarity.source",
        "fields": [
            {"name": "vendor_id", "type": "string"},
            {"name": "vendor_name", "type": ["null", "string"], "default": None},
            {"name": "category", "type": ["null", "string"], "default": None},
            {"name": "contract_start", "type": ["null", "string"], "default": None},
            {"name": "contract_end", "type": ["null", "string"], "default": None},
            {"name": "total_spend", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "on_time_delivery_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 1}], "default": None},
            {"name": "quality_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 3, "scale": 1}], "default": None},
        ],
    },
}

# ---------------------------------------------------------------------------
# Target Avro schemas (40 tables)
# ---------------------------------------------------------------------------

TARGET_SCHEMAS: dict[str, dict] = {
    # Customer domain (10)
    "TGT_CUSTOMER_LOAD": {
        "type": "record", "name": "TGT_CUSTOMER_LOAD", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "load_date", "type": ["null", "string"], "default": None},
            {"name": "batch_id", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_DEDUP": {
        "type": "record", "name": "TGT_CUSTOMER_DEDUP", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_VALIDATE": {
        "type": "record", "name": "TGT_CUSTOMER_VALIDATE", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "email_valid", "type": ["null", "boolean"], "default": None},
            {"name": "phone_valid", "type": ["null", "boolean"], "default": None},
            {"name": "is_valid", "type": ["null", "boolean"], "default": None},
        ],
    },
    "TGT_CUSTOMER_SCD2": {
        "type": "record", "name": "TGT_CUSTOMER_SCD2", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "effective_date", "type": ["null", "string"], "default": None},
            {"name": "end_date", "type": ["null", "string"], "default": None},
            {"name": "is_current", "type": ["null", "int"], "default": None},
        ],
    },
    "TGT_ADDR_NORMALIZED": {
        "type": "record", "name": "TGT_ADDR_NORMALIZED", "namespace": "xclarity.target",
        "fields": [
            {"name": "address_id", "type": "string"},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "address_line1_norm", "type": ["null", "string"], "default": None},
            {"name": "city_norm", "type": ["null", "string"], "default": None},
            {"name": "state", "type": ["null", "string"], "default": None},
            {"name": "zip_code", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_SEGMENT": {
        "type": "record", "name": "TGT_CUSTOMER_SEGMENT", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "total_spend", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "segment", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_MERGE": {
        "type": "record", "name": "TGT_CUSTOMER_MERGE", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "address_line1", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "state", "type": ["null", "string"], "default": None},
            {"name": "segment", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_MASKED": {
        "type": "record", "name": "TGT_CUSTOMER_MASKED", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "masked_first_name", "type": ["null", "string"], "default": None},
            {"name": "masked_email", "type": ["null", "string"], "default": None},
            {"name": "masked_phone", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CUSTOMER_LTV": {
        "type": "record", "name": "TGT_CUSTOMER_LTV", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "ltv", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_CUSTOMER_CHURN": {
        "type": "record", "name": "TGT_CUSTOMER_CHURN", "namespace": "xclarity.target",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "last_transaction_date", "type": ["null", "string"], "default": None},
            {"name": "churn_risk", "type": ["null", "string"], "default": None},
        ],
    },
    # Sales domain (10)
    "TGT_SALES_ORDER": {
        "type": "record", "name": "TGT_SALES_ORDER", "namespace": "xclarity.target",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "order_date", "type": ["null", "string"], "default": None},
            {"name": "total_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "load_date", "type": ["null", "string"], "default": None},
            {"name": "batch_id", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_SALES_VALIDATE": {
        "type": "record", "name": "TGT_SALES_VALIDATE", "namespace": "xclarity.target",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount_valid", "type": ["null", "boolean"], "default": None},
            {"name": "date_valid", "type": ["null", "boolean"], "default": None},
            {"name": "is_valid", "type": ["null", "boolean"], "default": None},
        ],
    },
    "TGT_LINE_ITEMS": {
        "type": "record", "name": "TGT_LINE_ITEMS", "namespace": "xclarity.target",
        "fields": [
            {"name": "line_item_id", "type": "string"},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "gross_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "discount_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "net_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_DISCOUNT_CALC": {
        "type": "record", "name": "TGT_DISCOUNT_CALC", "namespace": "xclarity.target",
        "fields": [
            {"name": "line_item_id", "type": "string"},
            {"name": "discount_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}], "default": None},
            {"name": "discount_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_TAX_CALC": {
        "type": "record", "name": "TGT_TAX_CALC", "namespace": "xclarity.target",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "tax_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4}], "default": None},
            {"name": "tax_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "total_with_tax", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_COMMISSION": {
        "type": "record", "name": "TGT_COMMISSION", "namespace": "xclarity.target",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "sales_rep_id", "type": ["null", "string"], "default": None},
            {"name": "total_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "commission_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
        ],
    },
    "TGT_RETURNS": {
        "type": "record", "name": "TGT_RETURNS", "namespace": "xclarity.target",
        "fields": [
            {"name": "return_id", "type": "string"},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "refund_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "is_complete", "type": ["null", "boolean"], "default": None},
        ],
    },
    "TGT_REVENUE_AGG": {
        "type": "record", "name": "TGT_REVENUE_AGG", "namespace": "xclarity.target",
        "fields": [
            {"name": "region", "type": "string"},
            {"name": "period", "type": "string"},
            {"name": "total_revenue", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}], "default": None},
            {"name": "order_count", "type": ["null", "long"], "default": None},
        ],
    },
    "TGT_FORECAST": {
        "type": "record", "name": "TGT_FORECAST", "namespace": "xclarity.target",
        "fields": [
            {"name": "forecast_id", "type": "string"},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "period", "type": ["null", "string"], "default": None},
            {"name": "forecast_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "confidence_pct", "type": ["null", "int"], "default": None},
        ],
    },
    "TGT_PIPELINE": {
        "type": "record", "name": "TGT_PIPELINE", "namespace": "xclarity.target",
        "fields": [
            {"name": "opportunity_id", "type": "string"},
            {"name": "stage", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "probability", "type": ["null", "int"], "default": None},
            {"name": "weighted_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    # Product domain (10)
    "TGT_PRODUCTS": {
        "type": "record", "name": "TGT_PRODUCTS", "namespace": "xclarity.target",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "product_name", "type": ["null", "string"], "default": None},
            {"name": "category", "type": ["null", "string"], "default": None},
            {"name": "unit_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "launch_date", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CATEGORY_HIER": {
        "type": "record", "name": "TGT_CATEGORY_HIER", "namespace": "xclarity.target",
        "fields": [
            {"name": "category_id", "type": "string"},
            {"name": "category_name", "type": ["null", "string"], "default": None},
            {"name": "parent_category_id", "type": ["null", "string"], "default": None},
            {"name": "parent_name", "type": ["null", "string"], "default": None},
            {"name": "hierarchy_path", "type": ["null", "string"], "default": None},
            {"name": "level", "type": ["null", "int"], "default": None},
        ],
    },
    "TGT_PRICE_HIST": {
        "type": "record", "name": "TGT_PRICE_HIST", "namespace": "xclarity.target",
        "fields": [
            {"name": "price_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "effective_date", "type": ["null", "string"], "default": None},
            {"name": "list_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "price_change", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
            {"name": "price_change_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}], "default": None},
        ],
    },
    "TGT_INVENTORY": {
        "type": "record", "name": "TGT_INVENTORY", "namespace": "xclarity.target",
        "fields": [
            {"name": "inventory_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "warehouse_id", "type": ["null", "string"], "default": None},
            {"name": "quantity_on_hand", "type": ["null", "int"], "default": None},
            {"name": "reorder_level", "type": ["null", "int"], "default": None},
        ],
    },
    "TGT_INV_ALERT": {
        "type": "record", "name": "TGT_INV_ALERT", "namespace": "xclarity.target",
        "fields": [
            {"name": "inventory_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "quantity_on_hand", "type": ["null", "int"], "default": None},
            {"name": "reorder_level", "type": ["null", "int"], "default": None},
            {"name": "stock_status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PROD_SUPPLIER": {
        "type": "record", "name": "TGT_PROD_SUPPLIER", "namespace": "xclarity.target",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "supplier_id", "type": ["null", "string"], "default": None},
            {"name": "supplier_name", "type": ["null", "string"], "default": None},
            {"name": "country", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_BUNDLES": {
        "type": "record", "name": "TGT_BUNDLES", "namespace": "xclarity.target",
        "fields": [
            {"name": "category", "type": "string"},
            {"name": "bundle_price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "product_count", "type": ["null", "long"], "default": None},
        ],
    },
    "TGT_REVIEW_SENTIMENT": {
        "type": "record", "name": "TGT_REVIEW_SENTIMENT", "namespace": "xclarity.target",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "avg_rating", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}], "default": None},
            {"name": "review_count", "type": ["null", "long"], "default": None},
            {"name": "sentiment", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_LIFECYCLE": {
        "type": "record", "name": "TGT_LIFECYCLE", "namespace": "xclarity.target",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "launch_date", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "lifecycle_stage", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_RECOMMENDATIONS": {
        "type": "record", "name": "TGT_RECOMMENDATIONS", "namespace": "xclarity.target",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "purchase_frequency", "type": ["null", "long"], "default": None},
            {"name": "recommendation_rank", "type": ["null", "int"], "default": None},
        ],
    },
    # Finance domain (10)
    "TGT_GL": {
        "type": "record", "name": "TGT_GL", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "account_code", "type": ["null", "int"], "default": None},
            {"name": "entry_date", "type": ["null", "string"], "default": None},
            {"name": "debit_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "credit_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "currency", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_AP": {
        "type": "record", "name": "TGT_AP", "namespace": "xclarity.target",
        "fields": [
            {"name": "invoice_id", "type": "string"},
            {"name": "vendor_id", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_AR": {
        "type": "record", "name": "TGT_AR", "namespace": "xclarity.target",
        "fields": [
            {"name": "invoice_id", "type": "string"},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_JOURNAL_VALID": {
        "type": "record", "name": "TGT_JOURNAL_VALID", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "total_debits", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "total_credits", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "balance_status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_CURRENCY_CONV": {
        "type": "record", "name": "TGT_CURRENCY_CONV", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "currency", "type": ["null", "string"], "default": None},
            {"name": "exchange_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 4}], "default": None},
            {"name": "debit_usd", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "credit_usd", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_BUDGET_VAR": {
        "type": "record", "name": "TGT_BUDGET_VAR", "namespace": "xclarity.target",
        "fields": [
            {"name": "budget_id", "type": "string"},
            {"name": "variance_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
            {"name": "variance_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}], "default": None},
            {"name": "variance_status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_EXPENSE_CAT": {
        "type": "record", "name": "TGT_EXPENSE_CAT", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "account_code", "type": ["null", "int"], "default": None},
            {"name": "expense_category", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_REVENUE_REC": {
        "type": "record", "name": "TGT_REVENUE_REC", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "recognition_period", "type": ["null", "string"], "default": None},
            {"name": "debit_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 14, "scale": 2}], "default": None},
        ],
    },
    "TGT_CONSOLIDATED": {
        "type": "record", "name": "TGT_CONSOLIDATED", "namespace": "xclarity.target",
        "fields": [
            {"name": "account_code", "type": "int"},
            {"name": "account_name", "type": ["null", "string"], "default": None},
            {"name": "total_debits", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}], "default": None},
            {"name": "total_credits", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}], "default": None},
            {"name": "net_balance", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}], "default": None},
        ],
    },
    "TGT_AUDIT_TRAIL": {
        "type": "record", "name": "TGT_AUDIT_TRAIL", "namespace": "xclarity.target",
        "fields": [
            {"name": "entry_id", "type": "string"},
            {"name": "audit_hash", "type": ["null", "string"], "default": None},
            {"name": "audit_timestamp", "type": ["null", "string"], "default": None},
            {"name": "etl_system", "type": ["null", "string"], "default": None},
            {"name": "action", "type": ["null", "string"], "default": None},
        ],
    },
    # HR & Operations domain (10)
    "TGT_EMPLOYEES": {
        "type": "record", "name": "TGT_EMPLOYEES", "namespace": "xclarity.target",
        "fields": [
            {"name": "employee_id", "type": "string"},
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "department", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PAYROLL": {
        "type": "record", "name": "TGT_PAYROLL", "namespace": "xclarity.target",
        "fields": [
            {"name": "payroll_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "gross_pay", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "total_deductions", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
            {"name": "effective_tax_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}], "default": None},
            {"name": "calc_net_pay", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}], "default": None},
        ],
    },
    "TGT_ATTENDANCE": {
        "type": "record", "name": "TGT_ATTENDANCE", "namespace": "xclarity.target",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "date", "type": ["null", "string"], "default": None},
            {"name": "hours_worked", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_PERF_SCORE": {
        "type": "record", "name": "TGT_PERF_SCORE", "namespace": "xclarity.target",
        "fields": [
            {"name": "review_id", "type": "string"},
            {"name": "employee_id", "type": ["null", "string"], "default": None},
            {"name": "weighted_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}], "default": None},
            {"name": "performance_tier", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_TURNOVER": {
        "type": "record", "name": "TGT_TURNOVER", "namespace": "xclarity.target",
        "fields": [
            {"name": "employee_id", "type": "string"},
            {"name": "tenure_months", "type": ["null", "int"], "default": None},
            {"name": "weighted_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}], "default": None},
            {"name": "turnover_risk", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_SHIPPING": {
        "type": "record", "name": "TGT_SHIPPING", "namespace": "xclarity.target",
        "fields": [
            {"name": "shipment_id", "type": "string"},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "carrier", "type": ["null", "string"], "default": None},
            {"name": "ship_date", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_DELIVERY": {
        "type": "record", "name": "TGT_DELIVERY", "namespace": "xclarity.target",
        "fields": [
            {"name": "shipment_id", "type": "string"},
            {"name": "transit_days", "type": ["null", "int"], "default": None},
            {"name": "sla_met", "type": ["null", "boolean"], "default": None},
            {"name": "delivery_status", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_WH_RECONCILE": {
        "type": "record", "name": "TGT_WH_RECONCILE", "namespace": "xclarity.target",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "abs_discrepancy", "type": ["null", "int"], "default": None},
            {"name": "accuracy_pct", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}], "default": None},
            {"name": "needs_recount", "type": ["null", "boolean"], "default": None},
        ],
    },
    "TGT_QUALITY": {
        "type": "record", "name": "TGT_QUALITY", "namespace": "xclarity.target",
        "fields": [
            {"name": "metric_id", "type": "string"},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "defect_rate", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}], "default": None},
            {"name": "quality_grade", "type": ["null", "string"], "default": None},
        ],
    },
    "TGT_VENDOR_SCORE": {
        "type": "record", "name": "TGT_VENDOR_SCORE", "namespace": "xclarity.target",
        "fields": [
            {"name": "vendor_id", "type": "string"},
            {"name": "composite_score", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}], "default": None},
            {"name": "vendor_tier", "type": ["null", "string"], "default": None},
        ],
    },
}

ALL_SCHEMAS = {**SOURCE_SCHEMAS, **TARGET_SCHEMAS}


def write_schema_files() -> None:
    """Write every schema to ./schemas/<name>.avsc for Git versioning."""
    for name, schema in ALL_SCHEMAS.items():
        path = SCHEMAS_DIR / f"{name}.avsc"
        path.write_text(json.dumps(schema, indent=2))
        logger.debug("Wrote schema file: %s", path)
    logger.info("Wrote %d Avro schema files to %s", len(ALL_SCHEMAS), SCHEMAS_DIR)


def register_schemas_with_nifi(registry_service_id: str) -> None:
    """
    Register all Avro schemas into NiFi's AvroSchemaRegistry controller service.
    Uses nipyapi's REST client to PUT/POST each schema.
    """
    import nipyapi.nifi as nifi_api
    from nipyapi.nifi.rest import ApiException

    logger.info(
        "Registering %d schemas into AvroSchemaRegistry (service id=%s)…",
        len(ALL_SCHEMAS),
        registry_service_id,
    )
    cs_api = nifi_api.ControllerServicesApi()
    cs = cs_api.get_controller_service(registry_service_id)
    current_props = cs.component.properties or {}

    for name, schema in ALL_SCHEMAS.items():
        prop_key = f"schema-name-{name}"
        current_props[prop_key] = json.dumps(schema)
        logger.debug("Staged schema property: %s", prop_key)

    body = nifi_api.ControllerServiceEntity(
        component=nifi_api.ControllerServiceDTO(
            id=registry_service_id,
            properties=current_props,
        ),
        revision=cs.revision,
    )
    try:
        cs_api.update_controller_service(registry_service_id, body)
        logger.info("All schemas registered successfully.")
    except ApiException as exc:
        logger.error("Failed to register schemas: %s", exc)
        raise


def run() -> None:
    """Entry point called by main.py."""
    logger.info("=== schemas.py: writing schema files and registering schemas ===")
    write_schema_files()
    logger.info("Schema files written. NiFi schema registration deferred to extract.py controller service setup.")
