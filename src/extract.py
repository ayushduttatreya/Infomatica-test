"""
src/extract.py
NiFi infrastructure setup: controller services, parameter contexts, and
process-group scaffolding for all 5 domains.

Corresponds to tickets:
  - Provision NiFi cluster and configure ZooKeeper/NiFi Registry
  - Configure shared Controller Services (readers, writers, schema registry)
  - Create NiFi parameter contexts per domain and set up CI/CD pipeline
"""

import logging
import os
import time
from typing import Optional

import nipyapi
import nipyapi.nifi as nifi_api
from nipyapi import canvas, security

from src.utils import connect_nifi, get_root_pg_id, get_or_create_pg

logger = logging.getLogger("xclarity_etl.extract")

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
SOURCE_DATA_PATH = os.environ.get("SOURCE_DATA_PATH", "/opt/nifi/source_data")
OUTPUT_DATA_PATH = os.environ.get("OUTPUT_DATA_PATH", "/opt/nifi/output_data")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "xclarity_etl")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "xclarity")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "xclarity_pass")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_DRIVER_CLASS = "org.postgresql.Driver"

# Per-domain parameter values
DOMAIN_PARAMS = {
    "Customer": {
        "source_path": f"{SOURCE_DATA_PATH}/customer",
        "output_path": f"{OUTPUT_DATA_PATH}/customer",
        "db_url": JDBC_URL,
        "db_user": POSTGRES_USER,
        "db_password": POSTGRES_PASSWORD,
        "segment_platinum_threshold": "400",
        "segment_gold_threshold": "200",
        "segment_silver_threshold": "100",
        "churn_days_threshold": "90",
        "scd2_end_date": "9999-12-31",
    },
    "Sales": {
        "source_path": f"{SOURCE_DATA_PATH}/sales",
        "output_path": f"{OUTPUT_DATA_PATH}/sales",
        "db_url": JDBC_URL,
        "db_user": POSTGRES_USER,
        "db_password": POSTGRES_PASSWORD,
        "commission_rate": "0.08",
        "tax_northeast": "0.08",
        "tax_west": "0.0725",
        "tax_midwest": "0.065",
        "tax_default": "0.07",
    },
    "Product": {
        "source_path": f"{SOURCE_DATA_PATH}/product",
        "output_path": f"{OUTPUT_DATA_PATH}/product",
        "db_url": JDBC_URL,
        "db_user": POSTGRES_USER,
        "db_password": POSTGRES_PASSWORD,
        "bundle_discount": "0.15",
        "stock_critical_pct": "0.5",
        "lifecycle_intro_months": "6",
        "lifecycle_growth_months": "18",
    },
    "Finance": {
        "source_path": f"{SOURCE_DATA_PATH}/finance",
        "output_path": f"{OUTPUT_DATA_PATH}/finance",
        "db_url": JDBC_URL,
        "db_user": POSTGRES_USER,
        "db_password": POSTGRES_PASSWORD,
        "journal_balance_tolerance": "0.01",
        "budget_variance_threshold": "5.0",
    },
    "HR_Operations": {
        "source_path": f"{SOURCE_DATA_PATH}/hr",
        "output_path": f"{OUTPUT_DATA_PATH}/hr",
        "db_url": JDBC_URL,
        "db_user": POSTGRES_USER,
        "db_password": POSTGRES_PASSWORD,
        "delivery_sla_days": "5",
        "perf_exceptional_threshold": "4.5",
        "perf_strong_threshold": "3.8",
        "perf_meets_threshold": "3.0",
        "recount_discrepancy_threshold": "1",
    },
}


# ---------------------------------------------------------------------------
# Controller service helpers
# ---------------------------------------------------------------------------

def _get_or_create_controller_service(
    pg_id: str,
    cs_type: str,
    cs_name: str,
    properties: dict,
) -> object:
    """
    Retrieve a controller service by name under *pg_id*, or create it with
    *properties* if it does not exist.
    """
    cs_api = nifi_api.FlowApi()
    existing = nipyapi.canvas.get_controller_service(cs_name, pg_id=pg_id)
    if existing:
        logger.info("Controller service '%s' already exists.", cs_name)
        return existing

    body = nifi_api.CreateControllerServiceRequestEntity(
        revision=nifi_api.RevisionDTO(version=0),
        component=nifi_api.ControllerServiceDTO(
            type=cs_type,
            name=cs_name,
            properties=properties,
        ),
    )
    try:
        cs = nifi_api.ProcessGroupsApi().create_controller_service(pg_id, body)
        logger.info("Created controller service '%s' (id=%s).", cs_name, cs.id)
        return cs
    except Exception as exc:
        logger.error("Failed to create controller service '%s': %s", cs_name, exc)
        raise


def _enable_controller_service(cs_entity: object) -> None:
    """Enable a controller service (set state to ENABLED)."""
    try:
        nipyapi.canvas.schedule_controller_service(cs_entity, True)
        logger.info("Enabled controller service '%s'.", cs_entity.component.name)
    except Exception as exc:
        logger.warning(
            "Could not enable controller service '%s': %s",
            cs_entity.component.name,
            exc,
        )


def setup_controller_services(root_pg_id: str) -> dict[str, object]:
    """
    Create and enable all shared controller services on the root process group.
    Returns a dict mapping service logical name → service entity.

    Services created:
    - AvroSchemaRegistry
    - CSVReader
    - CSVWriter
    - JsonRecordSetWriter
    - DBCPConnectionPool (targets)
    - DatabaseRecordLookupService (exchange rates, SCD2, performance)
    - SimpleCsvFileLookupService (static lookups)
    """
    logger.info("Setting up shared controller services on root PG %s…", root_pg_id)
    services = {}

    # 1. AvroSchemaRegistry
    avro_registry = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.schemaregistry.services.AvroSchemaRegistry",
        cs_name="XClarity_AvroSchemaRegistry",
        properties={},
    )
    services["schema_registry"] = avro_registry

    # 2. CSVReader
    csv_reader = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.csv.CSVReader",
        cs_name="XClarity_CSVReader",
        properties={
            "schema-access-strategy": "infer-schema",
            "csv-format": "DEFAULT",
            "first-line-is-header": "true",
            "ignore-csv-header": "false",
            "date-format": "yyyy-MM-dd",
            "time-format": "HH:mm:ss",
            "timestamp-format": "yyyy-MM-dd HH:mm:ss",
        },
    )
    services["csv_reader"] = csv_reader

    # 3. CSVWriter
    csv_writer = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.csv.CSVRecordSetWriter",
        cs_name="XClarity_CSVWriter",
        properties={
            "schema-access-strategy": "inherit-record-schema",
            "csv-format": "DEFAULT",
            "include-zero-record-flowfiles": "true",
        },
    )
    services["csv_writer"] = csv_writer

    # 4. JsonRecordSetWriter
    json_writer = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.json.JsonRecordSetWriter",
        cs_name="XClarity_JsonRecordSetWriter",
        properties={
            "schema-access-strategy": "inherit-record-schema",
            "output-grouping": "output-array",
            "suppress-null-values": "never-suppress",
        },
    )
    services["json_writer"] = json_writer

    # 5. DBCPConnectionPool (PostgreSQL targets)
    db_pool = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.dbcp.DBCPConnectionPool",
        cs_name="XClarity_DBCPConnectionPool",
        properties={
            "Database Connection URL": JDBC_URL,
            "Database Driver Class Name": JDBC_DRIVER_CLASS,
            "database-driver-locations": "/opt/nifi/nifi-current/lib/postgresql.jar",
            "Database User": POSTGRES_USER,
            "Password": POSTGRES_PASSWORD,
            "Max Wait Time": "500 millis",
            "Max Total Connections": "20",
        },
    )
    services["db_pool"] = db_pool

    # 6. DatabaseRecordLookupService (for SCD2, exchange rates, performance)
    db_lookup = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.lookup.db.DatabaseRecordLookupService",
        cs_name="XClarity_DatabaseRecordLookupService",
        properties={
            "dbcp-connection-pool": db_pool.id if hasattr(db_pool, "id") else "",
            "Lookup Value Column": "exchange_rate",
            "Table Name": "src_exchange_rates",
            "Lookup Key Column": "lookup_key",
        },
    )
    services["db_lookup"] = db_lookup

    # 7. SimpleCsvFileLookupService (static reference data)
    csv_lookup = _get_or_create_controller_service(
        pg_id=root_pg_id,
        cs_type="org.apache.nifi.lookup.SimpleCsvFileLookupService",
        cs_name="XClarity_SimpleCsvFileLookupService",
        properties={
            "CSV File": f"{SOURCE_DATA_PATH}/lookup/exchange_rates.csv",
            "Lookup Key Column": "lookup_key",
            "Lookup Value Column": "exchange_rate",
            "CSV Format": "DEFAULT",
        },
    )
    services["csv_lookup"] = csv_lookup

    # Enable all services
    for name, svc in services.items():
        try:
            _enable_controller_service(svc)
        except Exception as exc:
            logger.warning("Non-fatal: could not enable %s: %s", name, exc)

    logger.info("Controller service setup complete (%d services).", len(services))
    return services


# ---------------------------------------------------------------------------
# Parameter contexts
# ---------------------------------------------------------------------------

def _create_parameter_context(domain: str, params: dict) -> object:
    """Create a NiFi parameter context for a domain if it does not exist."""
    ctx_name = f"PC_{domain}"
    try:
        existing = nipyapi.canvas.get_parameter_context(ctx_name)
        if existing:
            logger.info("Parameter context '%s' already exists.", ctx_name)
            return existing
    except Exception:
        pass

    parameters = [
        nifi_api.ParameterEntity(
            parameter=nifi_api.ParameterDTO(
                name=k,
                value=v,
                sensitive=("password" in k.lower()),
                description=f"{domain} parameter: {k}",
            )
        )
        for k, v in params.items()
    ]
    body = nifi_api.ParameterContextEntity(
        revision=nifi_api.RevisionDTO(version=0),
        component=nifi_api.ParameterContextDTO(
            name=ctx_name,
            description=f"Parameter context for {domain} domain",
            parameters=parameters,
        ),
    )
    try:
        ctx = nifi_api.ParameterContextsApi().create_parameter_context(body)
        logger.info("Created parameter context '%s'.", ctx_name)
        return ctx
    except Exception as exc:
        logger.error("Failed to create parameter context '%s': %s", ctx_name, exc)
        raise


def setup_parameter_contexts() -> dict[str, object]:
    """Create parameter contexts for all 5 domains."""
    logger.info("Setting up parameter contexts…")
    contexts = {}
    for domain, params in DOMAIN_PARAMS.items():
        ctx = _create_parameter_context(domain, params)
        contexts[domain] = ctx
    logger.info("Parameter contexts ready for %d domains.", len(contexts))
    return contexts


# ---------------------------------------------------------------------------
# Process group scaffolding
# ---------------------------------------------------------------------------

DOMAIN_PG_NAMES = [
    "PG_Customer",
    "PG_Sales",
    "PG_Product",
    "PG_Finance",
    "PG_HR_Operations",
]


def setup_process_groups(root_pg_id: str) -> dict[str, object]:
    """Create top-level domain process groups under the root PG."""
    logger.info("Scaffolding domain process groups…")
    pgs = {}
    for name in DOMAIN_PG_NAMES:
        pg = get_or_create_pg(name, parent_pg_id=root_pg_id)
        pgs[name] = pg
        logger.info("Process group ready: %s", name)
    return pgs


# ---------------------------------------------------------------------------
# NiFi Registry (flow versioning)
# ---------------------------------------------------------------------------

def setup_nifi_registry() -> None:
    """
    Configure NiFi Registry client on the NiFi instance so that flow
    definitions can be versioned in the registry.
    """
    registry_url = os.environ.get(
        "NIFI_REGISTRY_URL", "http://nifi-registry:18080"
    )
    try:
        existing = nipyapi.versioning.get_registry_client("XClarity_Registry")
        if existing:
            logger.info("NiFi Registry client already configured.")
            return
    except Exception:
        pass

    try:
        nipyapi.versioning.create_registry_client(
            name="XClarity_Registry",
            uri=registry_url,
            description="XClarity ETL NiFi Registry for version-controlled flow definitions",
        )
        logger.info("Registered NiFi Registry client at %s.", registry_url)
    except Exception as exc:
        logger.warning("Could not register NiFi Registry client: %s", exc)


# ---------------------------------------------------------------------------
# Monitoring / bulletin setup
# ---------------------------------------------------------------------------

def setup_reporting_tasks(root_pg_id: str) -> None:
    """
    Configure SiteToSiteProvenanceReportingTask for data provenance forwarding
    to a central log store (outputs to stdout/log in this reference setup).
    """
    logger.info("Configuring reporting tasks…")
    try:
        rt_api = nifi_api.ControllerApi()
        body = nifi_api.ReportingTaskEntity(
            revision=nifi_api.RevisionDTO(version=0),
            component=nifi_api.ReportingTaskDTO(
                type="org.apache.nifi.reporting.SiteToSiteProvenanceReportingTask",
                name="XClarity_ProvenanceReporter",
                properties={
                    "Destination URL": "http://localhost:8080/nifi",
                    "Input Port Name": "provenance",
                    "Compress Events": "true",
                    "Batch Size": "1000",
                    "Platform": "nifi",
                },
                scheduling_period="60 secs",
                scheduling_strategy="TIMER_DRIVEN",
            ),
        )
        rt_api.create_reporting_task(body)
        logger.info("Provenance reporting task created.")
    except Exception as exc:
        logger.warning("Non-fatal: reporting task setup skipped: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Entry point called by main.py.
    Sets up the full NiFi infrastructure before domain flows are built.
    """
    logger.info("=== extract.py: NiFi infrastructure setup ===")

    connect_nifi()
    root_pg_id = get_root_pg_id()

    # Step 1: Schema files (idempotent)
    from src.schemas import write_schema_files
    write_schema_files()

    # Step 2: Shared controller services
    services = setup_controller_services(root_pg_id)

    # Step 3: Parameter contexts
    setup_parameter_contexts()

    # Step 4: Domain process groups
    pgs = setup_process_groups(root_pg_id)

    # Step 5: NiFi Registry
    setup_nifi_registry()

    # Step 6: Reporting tasks
    setup_reporting_tasks(root_pg_id)

    logger.info(
        "Infrastructure setup complete. PGs: %s | Services: %s",
        list(pgs.keys()),
        list(services.keys()),
    )
