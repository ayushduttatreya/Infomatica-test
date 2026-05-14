# Returns Reconciliation Process - NiFi Migration

===FILE: src/extract.py===
"""
Extract module for Returns Reconciliation Process.
Reads returns data and original orders from source systems.
"""

import logging
from typing import Dict, List, Any
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logger = logging.getLogger(__name__)


class ReturnsReconciliationExtractor:
    """Handles extraction of returns and orders data for reconciliation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor with configuration.
        
        Args:
            config: Configuration dictionary containing source settings
        """
        self.config = config
        self.canvas = nipyapi.canvas.get_root_pg_id()
        
    def create_extraction_flow(self, process_group: ProcessGroupEntity) -> Dict[str, Any]:
        """
        Create NiFi processors for data extraction.
        
        Args:
            process_group: Parent process group for extraction flow
            
        Returns:
            Dictionary containing created processor references
        """
        logger.info("Creating extraction flow for returns reconciliation")
        
        processors = {}
        
        try:
            # Create GetFile processor for returns data
            returns_source = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(100, 100),
                name='Get_Returns_Data',
                config=ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.config['sources']['returns']['directory'],
                        'File Filter': self.config['sources']['returns']['file_pattern'],
                        'Keep Source File': 'false',
                        'Recurse Subdirectories': 'false',
                        'Polling Interval': '10 sec',
                        'Batch Size': '10'
                    },
                    auto_terminated_relationships=['not.found'],
                    scheduling_period='30 sec',
                    scheduling_strategy='TIMER_DRIVEN'
                )
            )
            processors['returns_source'] = returns_source
            logger.info("Created returns data source processor")
            
            # Create GetFile processor for orders data
            orders_source = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(100, 300),
                name='Get_Orders_Data',
                config=ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.config['sources']['orders']['directory'],
                        'File Filter': self.config['sources']['orders']['file_pattern'],
                        'Keep Source File': 'true',
                        'Recurse Subdirectories': 'false',
                        'Polling Interval': '10 sec',
                        'Batch Size': '10'
                    },
                    auto_terminated_relationships=['not.found'],
                    scheduling_period='30 sec',
                    scheduling_strategy='TIMER_DRIVEN'
                )
            )
            processors['orders_source'] = orders_source
            logger.info("Created orders data source processor")
            
            # Create QueryRecord processor for returns validation
            validate_returns = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.QueryRecord'),
                location=(400, 100),
                name='Validate_Returns_Schema',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'CSVReader',
                        'record-writer': 'JSONRecordSetWriter',
                        'include-zero-record-flowfiles': 'false',
                        'valid_returns': """
                            SELECT 
                                return_id,
                                order_id,
                                customer_id,
                                product_id,
                                return_quantity,
                                return_reason,
                                return_date,
                                refund_amount,
                                return_status
                            FROM FLOWFILE
                            WHERE return_id IS NOT NULL
                                AND order_id IS NOT NULL
                                AND customer_id IS NOT NULL
                                AND return_quantity > 0
                        """
                    },
                    auto_terminated_relationships=['original']
                )
            )
            processors['validate_returns'] = validate_returns
            logger.info("Created returns validation processor")
            
            # Create QueryRecord processor for orders extraction
            extract_orders = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.QueryRecord'),
                location=(400, 300),
                name='Extract_Order_Details',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'CSVReader',
                        'record-writer': 'JSONRecordSetWriter',
                        'include-zero-record-flowfiles': 'false',
                        'order_details': """
                            SELECT 
                                order_id,
                                customer_id,
                                product_id,
                                order_quantity,
                                unit_price,
                                total_amount,
                                order_date,
                                order_status,
                                payment_method,
                                shipping_address
                            FROM FLOWFILE
                            WHERE order_status IN ('COMPLETED', 'SHIPPED', 'DELIVERED')
                        """
                    },
                    auto_terminated_relationships=['original']
                )
            )
            processors['extract_orders'] = extract_orders
            logger.info("Created orders extraction processor")
            
            # Create connections
            nipyapi.canvas.create_connection(
                returns_source,
                validate_returns,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                orders_source,
                extract_orders,
                relationships=['success']
            )
            
            logger.info("Extraction flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating extraction flow: {str(e)}")
            raise
    
    def create_error_handling(self, process_group: ProcessGroupEntity, 
                            source_processors: Dict[str, Any]) -> None:
        """
        Create error handling processors for extraction failures.
        
        Args:
            process_group: Parent process group
            source_processors: Dictionary of source processors to handle errors for
        """
        logger.info("Creating error handling for extraction")
        
        try:
            # Create LogAttribute processor for errors
            log_errors = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LogAttribute'),
                location=(700, 500),
                name='Log_Extraction_Errors',
                config=ProcessorConfigDTO(
                    properties={
                        'Log Level': 'error',
                        'Log Payload': 'true',
                        'Attributes to Log': 'filename,path,error.message',
                        'Attributes to Log by Regular Expression': '.*error.*'
                    },
                    auto_terminated_relationships=['success']
                )
            )
            
            # Create PutFile processor for failed files
            archive_failures = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
                location=(700, 650),
                name='Archive_Failed_Files',
                config=ProcessorConfigDTO(
                    properties={
                        'Directory': self.config['error_handling']['failed_files_directory'],
                        'Conflict Resolution Strategy': 'rename',
                        'Create Missing Directories': 'true'
                    },
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            
            # Connect error relationships
            for processor_name, processor in source_processors.items():
                if 'validate' in processor_name.lower() or 'extract' in processor_name.lower():
                    nipyapi.canvas.create_connection(
                        processor,
                        log_errors,
                        relationships=['failure']
                    )
                    
                    nipyapi.canvas.create_connection(
                        log_errors,
                        archive_failures,
                        relationships=['success']
                    )
            
            logger.info("Error handling created successfully")
            
        except Exception as e:
            logger.error(f"Error creating error handling: {str(e)}")
            raise


===FILE: src/transform.py===
"""
Transform module for Returns Reconciliation Process.
Implements reconciliation logic to match returns with orders and validate refund eligibility.
"""

import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity

logger = logging.getLogger(__name__)


class ReturnsReconciliationTransformer:
    """Handles transformation and reconciliation of returns with orders."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer with configuration.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.reconciliation_rules = config.get('reconciliation_rules', {})
        
    def create_transformation_flow(self, process_group: ProcessGroupEntity,
                                  input_processors: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create NiFi processors for returns reconciliation transformation.
        
        Args:
            process_group: Parent process group for transformation flow
            input_processors: Dictionary of input processors from extraction
            
        Returns:
            Dictionary containing created processor references
        """
        logger.info("Creating transformation flow for returns reconciliation")
        
        processors = {}
        
        try:
            # Create JoinEnrichment processor to match returns with orders
            join_returns_orders = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoinEnrichment'),
                location=(700, 200),
                name='Match_Returns_With_Orders',
                config=ProcessorConfigDTO(
                    properties={
                        'enrichment-strategy': 'JOIN',
                        'join-strategy': 'INNER_JOIN',
                        'original-record-path': '/order_id',
                        'enrichment-record-path': '/order_id',
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter'
                    },
                    auto_terminated_relationships=['original', 'unmatched']
                )
            )
            processors['join_returns_orders'] = join_returns_orders
            logger.info("Created returns-orders join processor")
            
            # Create UpdateRecord processor for reconciliation logic
            reconcile_returns = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(1000, 200),
                name='Apply_Reconciliation_Logic',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'replacement-value-strategy': 'RECORD_PATH_VALUES',
                        # Calculate refund eligibility
                        '/refund_eligible': """
                            ${field.value:equals('${return_status}', 'PENDING'):and(
                                ${field.value:toNumber('${return_quantity}'):le(${field.value:toNumber('${order_quantity}')})}
                            ):and(
                                ${field.value:toDate('${return_date}', 'yyyy-MM-dd'):ge(
                                    ${field.value:toDate('${order_date}', 'yyyy-MM-dd'):plus(0, 'DAYS')}
                                )}
                            ):and(
                                ${field.value:toDate('${return_date}', 'yyyy-MM-dd'):le(
                                    ${field.value:toDate('${order_date}', 'yyyy-MM-dd'):plus(""" + 
                                    str(self.reconciliation_rules.get('return_window_days', 30)) + 
                                    """, 'DAYS')}
                                )}
                            )}
                        """,
                        # Calculate refund amount
                        '/calculated_refund_amount': """
                            ${field.value:toNumber('${return_quantity}'):multiply(
                                ${field.value:toNumber('${unit_price}')}
                            )}
                        """,
                        # Validate refund amount
                        '/refund_amount_valid': """
                            ${field.value:toNumber('${refund_amount}'):le(
                                ${field.value:toNumber('${calculated_refund_amount}')}
                            )}
                        """,
                        # Set reconciliation status
                        '/reconciliation_status': """
                            ${field.value:equals('${refund_eligible}', 'true'):and(
                                ${field.value:equals('${refund_amount_valid}', 'true')}
                            ):ifElse('APPROVED', 'REJECTED')}
                        """,
                        # Add reconciliation timestamp
                        '/reconciliation_timestamp': "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        # Calculate days since order
                        '/days_since_order': """
                            ${field.value:toDate('${return_date}', 'yyyy-MM-dd'):toNumber():minus(
                                ${field.value:toDate('${order_date}', 'yyyy-MM-dd'):toNumber()}
                            ):divide(86400000)}
                        """
                    }
                )
            )
            processors['reconcile_returns'] = reconcile_returns
            logger.info("Created reconciliation logic processor")
            
            # Create RouteOnAttribute processor to separate approved/rejected returns
            route_reconciliation = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(1300, 200),
                name='Route_By_Reconciliation_Status',
                config=ProcessorConfigDTO(
                    properties={
                        'Routing Strategy': 'Route to Property name',
                        'approved': "${reconciliation_status:equals('APPROVED')}",
                        'rejected': "${reconciliation_status:equals('REJECTED')}",
                        'requires_review': """
                            ${refund_amount:toNumber():gt(""" + 
                            str(self.reconciliation_rules.get('manual_review_threshold', 1000)) + 
                            """)}
                        """
                    },
                    auto_terminated_relationships=['unmatched']
                )
            )
            processors['route_reconciliation'] = route_reconciliation
            logger.info("Created reconciliation routing processor")
            
            # Create UpdateRecord processor for approved returns enrichment
            enrich_approved = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(1600, 100),
                name='Enrich_Approved_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'replacement-value-strategy': 'RECORD_PATH_VALUES',
                        '/approval_date': "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        '/approved_by': 'SYSTEM_AUTO',
                        '/refund_method': '${payment_method}',
                        '/processing_priority': """
                            ${days_since_order:toNumber():lt(7):ifElse('HIGH', 
                                ${days_since_order:toNumber():lt(14):ifElse('MEDIUM', 'NORMAL')}
                            )}
                        """,
                        '/estimated_refund_date': """
                            ${now():plus(""" + 
                            str(self.reconciliation_rules.get('refund_processing_days', 5)) + 
                            """, 'DAYS'):format('yyyy-MM-dd')}
                        """
                    }
                )
            )
            processors['enrich_approved'] = enrich_approved
            logger.info("Created approved returns enrichment processor")
            
            # Create UpdateRecord processor for rejected returns
            enrich_rejected = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(1600, 300),
                name='Enrich_Rejected_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'replacement-value-strategy': 'RECORD_PATH_VALUES',
                        '/rejection_date': "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        '/rejected_by': 'SYSTEM_AUTO',
                        '/rejection_reason': """
                            ${refund_eligible:equals('false'):ifElse('OUTSIDE_RETURN_WINDOW',
                                ${refund_amount_valid:equals('false'):ifElse('INVALID_REFUND_AMOUNT',
                                    ${return_quantity:toNumber():gt(${order_quantity:toNumber()}):ifElse('QUANTITY_MISMATCH', 'OTHER')}
                                )}
                            )}
                        """,
                        '/customer_notification_required': 'true'
                    }
                )
            )
            processors['enrich_rejected'] = enrich_rejected
            logger.info("Created rejected returns enrichment processor")
            
            # Create UpdateRecord processor for manual review cases
            flag_manual_review = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(1600, 500),
                name='Flag_For_Manual_Review',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'replacement-value-strategy': 'RECORD_PATH_VALUES',
                        '/review_required': 'true',
                        '/review_reason': 'HIGH_VALUE_REFUND',
                        '/review_priority': 'HIGH',
                        '/flagged_date': "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        '/assigned_to': 'REVIEW_TEAM'
                    }
                )
            )
            processors['flag_manual_review'] = flag_manual_review
            logger.info("Created manual review flagging processor")
            
            # Create connections
            nipyapi.canvas.create_connection(
                input_processors['validate_returns'],
                join_returns_orders,
                relationships=['valid_returns']
            )
            
            nipyapi.canvas.create_connection(
                input_processors['extract_orders'],
                join_returns_orders,
                relationships=['order_details']
            )
            
            nipyapi.canvas.create_connection(
                join_returns_orders,
                reconcile_returns,
                relationships=['joined']
            )
            
            nipyapi.canvas.create_connection(
                reconcile_returns,
                route_reconciliation,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                route_reconciliation,
                enrich_approved,
                relationships=['approved']
            )
            
            nipyapi.canvas.create_connection(
                route_reconciliation,
                enrich_rejected,
                relationships=['rejected']
            )
            
            nipyapi.canvas.create_connection(
                route_reconciliation,
                flag_manual_review,
                relationships=['requires_review']
            )
            
            logger.info("Transformation flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating transformation flow: {str(e)}")
            raise
    
    def create_validation_processors(self, process_group: ProcessGroupEntity,
                                    transform_processors: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create validation processors for reconciliation results.
        
        Args:
            process_group: Parent process group
            transform_processors: Dictionary of transformation processors
            
        Returns:
            Dictionary containing validation processor references
        """
        logger.info("Creating validation processors")
        
        processors = {}
        
        try:
            # Create ValidateRecord processor for approved returns
            validate_approved = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(1900, 100),
                name='Validate_Approved_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'schema-access-strategy': 'schema-text-property',
                        'schema-text': """
                        {
                            "type": "record",
                            "name": "ApprovedReturn",
                            "fields": [
                                {"name": "return_id", "type": "string"},
                                {"name": "order_id", "type": "string"},
                                {"name": "customer_id", "type": "string"},
                                {"name": "calculated_refund_amount", "type": "double"},
                                {"name": "reconciliation_status", "type": "string"},
                                {"name": "approval_date", "type": "string"},
                                {"name": "estimated_refund_date", "type": "string"}
                            ]
                        }
                        """
                    },
                    auto_terminated_relationships=['invalid']
                )
            )
            processors['validate_approved'] = validate_approved
            
            # Create ValidateRecord processor for rejected returns
            validate_rejected = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(1900, 300),
                name='Validate_Rejected_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'record-writer': 'JsonRecordSetWriter',
                        'schema-access-strategy': 'schema-text-property',
                        'schema-text': """
                        {
                            "type": "record",
                            "name": "RejectedReturn",
                            "fields": [
                                {"name": "return_id", "type": "string"},
                                {"name": "order_id", "type": "string"},
                                {"name": "customer_id", "type": "string"},
                                {"name": "reconciliation_status", "type": "string"},
                                {"name": "rejection_date", "type": "string"},
                                {"name": "rejection_reason", "type": "string"}
                            ]
                        }
                        """
                    },
                    auto_terminated_relationships=['invalid']
                )
            )
            processors['validate_rejected'] = validate_rejected
            
            # Create connections
            nipyapi.canvas.create_connection(
                transform_processors['enrich_approved'],
                validate_approved,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                transform_processors['enrich_rejected'],
                validate_rejected,
                relationships=['success']
            )
            
            logger.info("Validation processors created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating validation processors: {str(e)}")
            raise


===FILE: src/load.py===
"""
Load module for Returns Reconciliation Process.
Handles loading of reconciled returns data to target systems.
"""

import logging
from typing import Dict, List, Any
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity

logger = logging.getLogger(__name__)


class ReturnsReconciliationLoader:
    """Handles loading of reconciled returns data to target destinations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the loader with configuration.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.targets = config.get('targets', {})
        
    def create_load_flow(self, process_group: ProcessGroupEntity,
                        input_processors: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create NiFi processors for loading reconciled data.
        
        Args:
            process_group: Parent process group for load flow
            input_processors: Dictionary of input processors from transformation
            
        Returns:
            Dictionary containing created processor references
        """
        logger.info("Creating load flow for reconciled returns")
        
        processors = {}
        
        try:
            # Create PutDatabaseRecord processor for approved returns
            load_approved_returns = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
                location=(2200, 100),
                name='Load_Approved_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'statement-type': 'INSERT',
                        'database-connection-pooling-service': self.targets['database']['connection_pool'],
                        'catalog-name': self.targets['database']['catalog'],
                        'schema-name': self.targets['database']['schema'],
                        'table-name': self.targets['database']['approved_returns_table'],
                        'translate-field-names': 'true',
                        'unmatched-field-behavior': 'Ignore Unmatched Fields',
                        'unmatched-column-behavior': 'Fail on Unmatched Columns',
                        'update-keys': 'return_id',
                        'field-containing-sql': '',
                        'allow-multiple-statements': 'false',
                        'quote-identifiers': 'true',
                        'quote-table-identifier': 'true',
                        'query-timeout': '30 seconds',
                        'rollback-on-failure': 'true',
                        'batch-size': '100'
                    },
                    auto_terminated_relationships=['success']
                )
            )
            processors['load_approved_returns'] = load_approved_returns
            logger.info("Created approved returns database loader")
            
            # Create PutDatabaseRecord processor for rejected returns
            load_rejected_returns = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
                location=(2200, 300),
                name='Load_Rejected_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'statement-type': 'INSERT',
                        'database-connection-pooling-service': self.targets['database']['connection_pool'],
                        'catalog-name': self.targets['database']['catalog'],
                        'schema-name': self.targets['database']['schema'],
                        'table-name': self.targets['database']['rejected_returns_table'],
                        'translate-field-names': 'true',
                        'unmatched-field-behavior': 'Ignore Unmatched Fields',
                        'unmatched-column-behavior': 'Fail on Unmatched Columns',
                        'update-keys': 'return_id',
                        'quote-identifiers': 'true',
                        'rollback-on-failure': 'true',
                        'batch-size': '100'
                    },
                    auto_terminated_relationships=['success']
                )
            )
            processors['load_rejected_returns'] = load_rejected_returns
            logger.info("Created rejected returns database loader")
            
            # Create PutDatabaseRecord processor for manual review queue
            load_review_queue = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
                location=(2200, 500),
                name='Load_Manual_Review_Queue',
                config=ProcessorConfigDTO(
                    properties={
                        'record-reader': 'JsonTreeReader',
                        'statement-type': 'INSERT',
                        'database-connection-pooling-service': self.targets['database']['connection_pool'],
                        'catalog-name': self.targets['database']['catalog'],
                        'schema-name': self.targets['database']['schema'],
                        'table-name': self.targets['database']['review_queue_table'],
                        'translate-field-names': 'true',
                        'update-keys': 'return_id',
                        'quote-identifiers': 'true',
                        'rollback-on-failure': 'true',
                        'batch-size': '50'
                    },
                    auto_terminated_relationships=['success']
                )
            )
            processors['load_review_queue'] = load_review_queue
            logger.info("Created manual review queue loader")
            
            # Create PutFile processor for approved returns archive
            archive_approved = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
                location=(2500, 100),
                name='Archive_Approved_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'Directory': self.targets['archive']['approved_directory'],
                        'Conflict Resolution Strategy': 'rename',
                        'Create Missing Directories': 'true',
                        'Maximum File Count': '-1',
                        'Last Modified Time': '${now():format("yyyy-MM-dd\'T\'HH:mm:ss")}',
                        'Permissions': '0644',
                        'Owner': '',
                        'Group': '',
                        'Directory Permissions': '0755'
                    },
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            processors['archive_approved'] = archive_approved
            logger.info("Created approved returns archive processor")
            
            # Create PutFile processor for rejected returns archive
            archive_rejected = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
                location=(2500, 300),
                name='Archive_Rejected_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'Directory': self.targets['archive']['rejected_directory'],
                        'Conflict Resolution Strategy': 'rename',
                        'Create Missing Directories': 'true',
                        'Maximum File Count': '-1',
                        'Last Modified Time': '${now():format("yyyy-MM-dd\'T\'HH:mm:ss")}',
                        'Permissions': '0644',
                        'Directory Permissions': '0755'
                    },
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            processors['archive_rejected'] = archive_rejected
            logger.info("Created rejected returns archive processor")
            
            # Create connections
            nipyapi.canvas