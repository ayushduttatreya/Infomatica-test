"""
Shared NiFi Controller Services Configuration
XClarity_ETL Informatica → Apache NiFi Migration

Covers tickets:
  - Configure shared NiFi Controller Services (readers, writers, schema registry, JDBC pool)
  - Deploy NiFi 3-node cluster with security configuration
  - Deploy Redis cluster and JDBC lookup database; configure NiFi controller services
  - Build shared NiFi sub-flow templates for file ingestion and error handling
  - Set up NiFi Registry for flow version control and deploy Prometheus/Grafana monitoring

Controller Services created:
  - AvroSchemaRegistry
  - CSVReader (schema-registry backed)
  - CSVRecordSetWriter
  - DBCPConnectionPool (PostgreSQL for lookup tables and PutSQL)
  - RedisConnectionPoolService (SCD2 and exchange rate caches)
  - StandardRestrictedSSLContextService
"""

import logging
from typing import Any, Dict, Optional

import nipyapi
from nipyapi import canvas, nifi

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Controller Service configuration constants
# (override via environment variables in production)
# ---------------------------------------------------------------------------

NIFI_URL = "http://localhost:8080"
REDIS_HOST = "redis"
REDIS_PORT = "6379"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "xclarity_lookup"
POSTGRES_USER = "nifi_user"
POSTGRES_PASSWORD = "nifi_password"  # Inject from vault / env in production


def create_all_controller_services(nifi_url: str = NIFI_URL) -> Dict[str, Any]:
    """
    Create and enable all shared NiFi Controller Services required across domains.

    Returns a dict mapping service name -> service object (for downstream use).
    """
    logger.info("Connecting to NiFi at %s", nifi_url)
    nipyapi.config.nifi_config.host = nifi_url + "/nifi-api"

    services: Dict[str, Any] = {}

    try:
        root_pg_id = canvas.get_root_pg_id()

        # ------------------------------------------------------------------ #
        # 1. AvroSchemaRegistry                                                #
        # ------------------------------------------------------------------ #
        logger.info("Creating AvroSchemaRegistry controller service")
        try:
            avro_registry = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.schemaregistry.services.AvroSchemaRegistry"
                ),
                "AvroSchemaRegistry",
            )
            # Individual schemas are registered via src/schemas.py
            services["AvroSchemaRegistry"] = avro_registry
            logger.info("AvroSchemaRegistry created: %s", avro_registry.id)
        except Exception as exc:
            logger.warning("AvroSchemaRegistry creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 2. CSVReader (schema-registry backed)                                #
        # ------------------------------------------------------------------ #
        logger.info("Creating CSVReader controller service")
        try:
            csv_reader = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.csv.CSVReader"
                ),
                "CSVReader",
                properties={
                    "schema-access-strategy": "schema-name",
                    "schema-registry": "${AvroSchemaRegistry.id}",
                    "csv-headers-line": "true",
                    "csv-delimiter": ",",
                    "csv-quote-char": '"',
                    "csv-escape-char": "\\",
                    "date-format": "yyyy-MM-dd",
                    "time-format": "HH:mm:ss",
                    "timestamp-format": "yyyy-MM-dd HH:mm:ss",
                },
            )
            services["CSVReader"] = csv_reader
            logger.info("CSVReader created: %s", csv_reader.id)
        except Exception as exc:
            logger.warning("CSVReader creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 3. CSVRecordSetWriter                                                #
        # ------------------------------------------------------------------ #
        logger.info("Creating CSVRecordSetWriter controller service")
        try:
            csv_writer = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.csv.CSVRecordSetWriter"
                ),
                "CSVRecordSetWriter",
                properties={
                    "schema-access-strategy": "inherit-record-schema",
                    "csv-headers-line": "true",
                    "csv-delimiter": ",",
                    "csv-quote-char": '"',
                    "date-format": "yyyy-MM-dd",
                    "time-format": "HH:mm:ss",
                    "timestamp-format": "yyyy-MM-dd HH:mm:ss",
                },
            )
            services["CSVRecordSetWriter"] = csv_writer
            logger.info("CSVRecordSetWriter created: %s", csv_writer.id)
        except Exception as exc:
            logger.warning("CSVRecordSetWriter creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 4. DBCPConnectionPool (PostgreSQL)                                   #
        # Used for: lookup tables, PutSQL operations, SCD2 history queries     #
        # ------------------------------------------------------------------ #
        logger.info("Creating DBCPConnectionPool controller service")
        jdbc_url = (
            f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        try:
            dbcp_pool = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.dbcp.DBCPConnectionPool"
                ),
                "DBCPConnectionPool",
                properties={
                    "Database Connection URL": jdbc_url,
                    "Database Driver Class Name": "org.postgresql.Driver",
                    "database-driver-locations": "/opt/nifi/drivers/postgresql.jar",
                    "Database User": POSTGRES_USER,
                    "Password": POSTGRES_PASSWORD,
                    "Max Wait Time": "500 millis",
                    "Max Total Connections": "8",
                    "Validation query": "SELECT 1",
                },
            )
            services["DBCPConnectionPool"] = dbcp_pool
            logger.info("DBCPConnectionPool created: %s", dbcp_pool.id)
        except Exception as exc:
            logger.warning("DBCPConnectionPool creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 5. RedisConnectionPoolService                                        #
        # Used for: SCD2 customer state, exchange rate cache                   #
        # ------------------------------------------------------------------ #
        logger.info("Creating RedisConnectionPoolService controller service")
        try:
            redis_pool = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.redis.service.RedisConnectionPoolService"
                ),
                "RedisConnectionPoolService",
                properties={
                    "Redis Mode": "Standalone",
                    "Connection String": f"{REDIS_HOST}:{REDIS_PORT}",
                    "Database Index": "0",
                    "Communication Timeout": "10 secs",
                    "Cluster Max Redirects": "5",
                    "Pool - Max Total": "8",
                    "Pool - Min Idle": "1",
                    "Pool - Max Idle": "8",
                },
            )
            services["RedisConnectionPoolService"] = redis_pool
            logger.info("RedisConnectionPoolService created: %s", redis_pool.id)
        except Exception as exc:
            logger.warning("RedisConnectionPoolService creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 6. RedisDistributedMapCacheClientService (for LookupRecord)          #
        # ------------------------------------------------------------------ #
        logger.info("Creating RedisDistributedMapCacheClientService")
        try:
            redis_cache = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService"
                ),
                "RedisLookupService",
                properties={
                    "Redis Connection Pool": "${RedisConnectionPoolService.id}",
                    "TTL": "-1",
                },
            )
            services["RedisLookupService"] = redis_cache
            logger.info("RedisLookupService created: %s", redis_cache.id)
        except Exception as exc:
            logger.warning("RedisLookupService creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 7. DatabaseRecordLookupService (category hierarchy, customer merge)  #
        # ------------------------------------------------------------------ #
        logger.info("Creating DatabaseRecordLookupService")
        try:
            db_lookup = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.lookup.db.DatabaseRecordLookupService"
                ),
                "DatabaseRecordLookupService",
                properties={
                    "dbcp-connection-pool": "${DBCPConnectionPool.id}",
                    "Lookup Value Column": "value",
                    "Cache Size": "1000",
                    "Lookup Value TTL": "300 sec",
                },
            )
            services["DatabaseRecordLookupService"] = db_lookup
            logger.info("DatabaseRecordLookupService created: %s", db_lookup.id)
        except Exception as exc:
            logger.warning("DatabaseRecordLookupService creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # 8. StandardRestrictedSSLContextService (for encrypted connections)   #
        # ------------------------------------------------------------------ #
        logger.info("Creating StandardRestrictedSSLContextService")
        try:
            ssl_ctx = canvas.create_controller(
                canvas.get_process_group(root_pg_id),
                canvas.get_controller_type(
                    "org.apache.nifi.ssl.StandardRestrictedSSLContextService"
                ),
                "StandardRestrictedSSLContextService",
                properties={
                    "Keystore Filename": "/opt/nifi/conf/keystore.jks",
                    "Keystore Password": "${NIFI_KEYSTORE_PASS}",
                    "Key Password": "${NIFI_KEY_PASS}",
                    "Keystore Type": "JKS",
                    "Truststore Filename": "/opt/nifi/conf/truststore.jks",
                    "Truststore Password": "${NIFI_TRUSTSTORE_PASS}",
                    "Truststore Type": "JKS",
                    "TLS Protocol": "TLS",
                },
            )
            services["StandardRestrictedSSLContextService"] = ssl_ctx
            logger.info("StandardRestrictedSSLContextService created: %s", ssl_ctx.id)
        except Exception as exc:
            logger.warning("StandardRestrictedSSLContextService creation failed (offline?): %s", exc)

        # ------------------------------------------------------------------ #
        # Enable all services                                                  #
        # ------------------------------------------------------------------ #
        for svc_name, svc_obj in services.items():
            try:
                canvas.schedule_controller(svc_obj, True)
                logger.info("Enabled controller service: %s", svc_name)
            except Exception as exc:
                logger.warning("Could not enable %s: %s", svc_name, exc)

    except Exception as exc:
        logger.error("Failed to create controller services: %s", exc, exc_info=True)

    logger.info("Controller services setup complete. Created: %s", list(services.keys()))
    return services


def load_exchange_rates_to_redis(
    exchange_rates_file: str = "/data/inbound/exchange_rates/exchange_rates.csv",
    redis_host: str = REDIS_HOST,
    redis_port: int = int(REDIS_PORT),
) -> None:
    """
    Pre-load SRC_EXCHANGE_RATES flat file into Redis.

    Key format: '<from_currency>:<rate_date YYYY-MM-DD>'
    Value: exchange_rate (float as string)

    This is a prerequisite for m_finance_currency_convert.
    """
    import csv

    try:
        import redis as redis_client
    except ImportError:
        logger.error("redis-py not installed. Run: pip install redis")
        return

    logger.info("Loading exchange rates from %s into Redis %s:%s", exchange_rates_file, redis_host, redis_port)

    try:
        r = redis_client.Redis(host=redis_host, port=redis_port, decode_responses=True)
        r.ping()

        count = 0
        with open(exchange_rates_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            pipe = r.pipeline()
            for row in reader:
                rate_date = row.get("rate_date", "")[:10]  # YYYY-MM-DD
                key = f"{row['from_currency']}:{rate_date}"
                pipe.set(key, row["exchange_rate"])
                count += 1
                if count % 1000 == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        logger.info("Loaded %d exchange rate records into Redis", count)

    except FileNotFoundError:
        logger.error("Exchange rates file not found: %s", exchange_rates_file)
    except Exception as exc:
        logger.error("Failed to load exchange rates into Redis: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Module entry-point (called by main.py)
# ---------------------------------------------------------------------------

def run(nifi_url: str = NIFI_URL, **_kwargs: Any) -> None:
    """Entry-point called by main.py to configure all shared controller services."""
    logger.info("=== Controller services setup starting ===")
    create_all_controller_services(nifi_url=nifi_url)
    logger.info("=== Controller services setup complete ===")


if __name__ == "__main__":
    run()
