# Sales Returns Processing Migration - NiFi Implementation

===FILE: src/extract.py===
"""
Extract module for Sales Returns Processing
Handles extraction from source systems with error handling and logging
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logger = logging.getLogger(__name__)


class SalesReturnsExtractor:
    """Extracts sales returns data from source systems"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize extractor with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.extract_config = self.config['extraction']
        self.nifi_config = self.config['nifi']
        
    def create_extraction_flow(self, canvas: ProcessGroupEntity) -> Dict:
        """
        Create NiFi extraction flow for sales returns data
        
        Args:
            canvas: NiFi process group to create flow in
            
        Returns:
            Dictionary containing processor references
        """
        try:
            logger.info("Creating sales returns extraction flow")
            
            # Create process group for extraction
            extract_pg = nipyapi.canvas.create_process_group(
                parent_pg=canvas,
                new_pg_name='Sales_Returns_Extract',
                location=(0, 0)
            )
            
            processors = {}
            
            # GetFile processor for returns data
            processors['get_returns'] = self._create_get_file_processor(
                extract_pg,
                name='Get_Returns_Data',
                directory=self.extract_config['returns_source_dir'],
                file_filter=self.extract_config['returns_file_pattern'],
                position=(100, 100)
            )
            
            # GetFile processor for customer reference data
            processors['get_customers'] = self._create_get_file_processor(
                extract_pg,
                name='Get_Customer_Data',
                directory=self.extract_config['customer_source_dir'],
                file_filter=self.extract_config['customer_file_pattern'],
                position=(100, 300)
            )
            
            # ValidateRecord processor for returns data
            processors['validate_returns'] = self._create_validate_processor(
                extract_pg,
                name='Validate_Returns_Schema',
                schema_name='returns_schema',
                position=(400, 100)
            )
            
            # ValidateRecord processor for customer data
            processors['validate_customers'] = self._create_validate_processor(
                extract_pg,
                name='Validate_Customer_Schema',
                schema_name='customer_schema',
                position=(400, 300)
            )
            
            # RouteOnAttribute for error handling
            processors['route_valid'] = self._create_route_processor(
                extract_pg,
                name='Route_Valid_Records',
                position=(700, 100)
            )
            
            # LogAttribute for errors
            processors['log_errors'] = self._create_log_processor(
                extract_pg,
                name='Log_Extraction_Errors',
                position=(700, 400)
            )
            
            # Create connections
            self._create_connections(processors)
            
            logger.info("Extraction flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating extraction flow: {str(e)}")
            raise
    
    def _create_get_file_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        directory: str,
        file_filter: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create GetFile processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('GetFile'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Input Directory': directory,
            'File Filter': file_filter,
            'Keep Source File': 'false',
            'Recurse Subdirectories': 'true',
            'Polling Interval': self.extract_config['polling_interval'],
            'Batch Size': str(self.extract_config['batch_size'])
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_validate_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        schema_name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create ValidateRecord processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('ValidateRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'CSVReader',
            'Record Writer': 'CSVRecordSetWriter',
            'Schema Access Strategy': 'Use Schema Name Property',
            'Schema Registry': 'AvroSchemaRegistry',
            'Schema Name': schema_name,
            'Allow Extra Fields': 'true',
            'Strict Type Checking': 'true'
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_route_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create RouteOnAttribute processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Routing Strategy': 'Route to Property name',
            'valid_records': "${record.count:gt(0)}"
        }
        config.auto_terminated_relationships = ['unmatched']
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_log_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create LogAttribute processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('LogAttribute'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Log Level': 'error',
            'Log Payload': 'true',
            'Attributes to Log': 'filename,path,error.message',
            'Attributes to Log by Comma Separated List': 'true'
        }
        config.auto_terminated_relationships = ['success']
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_connections(self, processors: Dict) -> None:
        """Create connections between processors"""
        
        # Returns validation flow
        nipyapi.canvas.create_connection(
            source=processors['get_returns'],
            target=processors['validate_returns'],
            relationships=['success']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['validate_returns'],
            target=processors['route_valid'],
            relationships=['valid']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['validate_returns'],
            target=processors['log_errors'],
            relationships=['invalid']
        )
        
        # Customer validation flow
        nipyapi.canvas.create_connection(
            source=processors['get_customers'],
            target=processors['validate_customers'],
            relationships=['success']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['validate_customers'],
            target=processors['route_valid'],
            relationships=['valid']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['validate_customers'],
            target=processors['log_errors'],
            relationships=['invalid']
        )


===FILE: src/transform.py===
"""
Transform module for Sales Returns Processing
Handles validation, reason code mapping, and date processing
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logger = logging.getLogger(__name__)


class SalesReturnsTransformer:
    """Transforms sales returns data with validations and mappings"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize transformer with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.transform_config = self.config['transformation']
        self.validation_config = self.config['validation']
        
    def create_transformation_flow(self, canvas: ProcessGroupEntity) -> Dict:
        """
        Create NiFi transformation flow for sales returns
        
        Args:
            canvas: NiFi process group to create flow in
            
        Returns:
            Dictionary containing processor references
        """
        try:
            logger.info("Creating sales returns transformation flow")
            
            # Create process group for transformation
            transform_pg = nipyapi.canvas.create_process_group(
                parent_pg=canvas,
                new_pg_name='Sales_Returns_Transform',
                location=(1000, 0)
            )
            
            processors = {}
            
            # QueryRecord for data enrichment
            processors['enrich_returns'] = self._create_query_processor(
                transform_pg,
                name='Enrich_Returns_With_Customer',
                position=(100, 100)
            )
            
            # UpdateRecord for refund amount validation
            processors['validate_refund'] = self._create_update_processor(
                transform_pg,
                name='Validate_Refund_Amount',
                record_path='/refund_amount',
                position=(400, 100)
            )
            
            # LookupRecord for reason code mapping
            processors['map_reason_codes'] = self._create_lookup_processor(
                transform_pg,
                name='Map_Return_Reason_Codes',
                position=(700, 100)
            )
            
            # UpdateRecord for date processing
            processors['process_dates'] = self._create_date_processor(
                transform_pg,
                name='Process_Return_Dates',
                position=(1000, 100)
            )
            
            # PartitionRecord for business rules
            processors['partition_returns'] = self._create_partition_processor(
                transform_pg,
                name='Partition_By_Return_Type',
                position=(1300, 100)
            )
            
            # RouteOnAttribute for validation results
            processors['route_validated'] = self._create_validation_router(
                transform_pg,
                name='Route_Validated_Returns',
                position=(1600, 100)
            )
            
            # UpdateRecord for error flagging
            processors['flag_errors'] = self._create_error_flag_processor(
                transform_pg,
                name='Flag_Validation_Errors',
                position=(1600, 400)
            )
            
            # Create connections
            self._create_connections(processors)
            
            logger.info("Transformation flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating transformation flow: {str(e)}")
            raise
    
    def _create_query_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create QueryRecord processor for enrichment"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('QueryRecord'),
            location=position,
            name=name
        )
        
        # SQL query to enrich returns with customer data
        enrichment_query = """
        SELECT 
            r.return_id,
            r.order_id,
            r.customer_id,
            r.return_date,
            r.return_reason_code,
            r.refund_amount,
            r.return_quantity,
            r.product_id,
            c.first_name,
            c.last_name,
            c.email,
            c.customer_status,
            CONCAT(c.first_name, ' ', c.last_name) as customer_name
        FROM FLOWFILE r
        LEFT JOIN LOOKUP c ON r.customer_id = c.customer_id
        """
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'CSVReader',
            'Record Writer': 'JSONRecordSetWriter',
            'enriched': enrichment_query,
            'Include Zero Record FlowFiles': 'false',
            'Cache Schema': 'true'
        }
        config.auto_terminated_relationships = ['original']
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_update_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        record_path: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateRecord processor for refund validation"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('UpdateRecord'),
            location=position,
            name=name
        )
        
        min_refund = self.validation_config['refund_amount']['min']
        max_refund = self.validation_config['refund_amount']['max']
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'JSONRecordSetWriter',
            'Replacement Value Strategy': 'Record Path Value',
            # Validate refund amount is within acceptable range
            '/refund_amount_valid': f"""
                coalesce(
                    toDecimal(/refund_amount) >= {min_refund} 
                    AND toDecimal(/refund_amount) <= {max_refund},
                    false
                )
            """,
            # Flag if refund exceeds threshold
            '/high_value_return': f"""
                coalesce(
                    toDecimal(/refund_amount) > {self.validation_config['high_value_threshold']},
                    false
                )
            """,
            # Calculate refund per unit
            '/refund_per_unit': """
                coalesce(
                    toDecimal(/refund_amount) / toDecimal(/return_quantity),
                    0
                )
            """
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_lookup_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create LookupRecord processor for reason code mapping"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('LookupRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'JSONRecordSetWriter',
            'Lookup Service': 'DistributedMapCacheLookupService',
            'Result RecordPath': '/reason_code_details',
            'Routing Strategy': 'Route to success',
            # Lookup mappings
            'reason_code_key': '/return_reason_code',
            'reason_description': '/reason_description',
            'reason_category': '/reason_category',
            'requires_inspection': '/requires_inspection',
            'restocking_fee_pct': '/restocking_fee_pct'
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_date_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateRecord processor for date processing"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('UpdateRecord'),
            location=position,
            name=name
        )
        
        date_format = self.transform_config['date_format']
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'JSONRecordSetWriter',
            'Replacement Value Strategy': 'Record Path Value',
            # Parse and standardize return date
            '/return_date_parsed': f"""
                format(
                    toDate(/return_date, '{date_format}'),
                    'yyyy-MM-dd HH:mm:ss'
                )
            """,
            # Extract date components
            '/return_year': """
                format(toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'), 'yyyy')
            """,
            '/return_month': """
                format(toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'), 'MM')
            """,
            '/return_day': """
                format(toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'), 'dd')
            """,
            '/return_quarter': """
                concat('Q', toString((toNumber(format(toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'), 'MM')) - 1) / 3 + 1))
            """,
            # Calculate days since return
            '/days_since_return': """
                daysDiff(
                    toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'),
                    now()
                )
            """,
            # Validate return date is not in future
            '/return_date_valid': """
                not(isAfter(
                    toDate(/return_date_parsed, 'yyyy-MM-dd HH:mm:ss'),
                    now()
                ))
            """,
            # Add processing timestamp
            '/processed_timestamp': """
                format(now(), 'yyyy-MM-dd HH:mm:ss')
            """
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_partition_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PartitionRecord processor for business rules"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('PartitionRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'JSONRecordSetWriter',
            'Partition by': """
                ${field.value:equals('high_value_return', 'true'):ifElse('high_value', 
                ${field.value:equals('requires_inspection', 'true'):ifElse('inspection_required',
                'standard')})}
            """
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_validation_router(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create RouteOnAttribute for validation routing"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Routing Strategy': 'Route to Property name',
            'all_validations_passed': """
                ${refund_amount_valid:equals('true'):and(
                ${return_date_valid:equals('true')})}
            """,
            'validation_failed': """
                ${refund_amount_valid:equals('false'):or(
                ${return_date_valid:equals('false')})}
            """
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_error_flag_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateRecord processor for error flagging"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('UpdateRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'JSONRecordSetWriter',
            'Replacement Value Strategy': 'Literal Value',
            '/error_flag': 'true',
            '/error_timestamp': "${now():format('yyyy-MM-dd HH:mm:ss')}",
            '/error_details': """
                ${refund_amount_valid:equals('false'):ifElse('Invalid refund amount', '')}
                ${return_date_valid:equals('false'):ifElse('Invalid return date', '')}
            """
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_connections(self, processors: Dict) -> None:
        """Create connections between processors"""
        
        # Main transformation flow
        nipyapi.canvas.create_connection(
            source=processors['enrich_returns'],
            target=processors['validate_refund'],
            relationships=['enriched']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['validate_refund'],
            target=processors['map_reason_codes'],
            relationships=['success']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['map_reason_codes'],
            target=processors['process_dates'],
            relationships=['matched', 'unmatched']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['process_dates'],
            target=processors['partition_returns'],
            relationships=['success']
        )
        
        nipyapi.canvas.create_connection(
            source=processors['partition_returns'],
            target=processors['route_validated'],
            relationships=['success']
        )
        
        # Validation routing
        nipyapi.canvas.create_connection(
            source=processors['route_validated'],
            target=processors['flag_errors'],
            relationships=['validation_failed']
        )


===FILE: src/load.py===
"""
Load module for Sales Returns Processing
Handles loading to target systems with error handling
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logger = logging.getLogger(__name__)


class SalesReturnsLoader:
    """Loads processed sales returns data to target systems"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize loader with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.load_config = self.config['loading']
        
    def create_loading_flow(self, canvas: ProcessGroupEntity) -> Dict:
        """
        Create NiFi loading flow for sales returns
        
        Args:
            canvas: NiFi process group to create flow in
            
        Returns:
            Dictionary containing processor references
        """
        try:
            logger.info("Creating sales returns loading flow")
            
            # Create process group for loading
            load_pg = nipyapi.canvas.create_process_group(
                parent_pg=canvas,
                new_pg_name='Sales_Returns_Load',
                location=(2000, 0)
            )
            
            processors = {}
            
            # ConvertRecord to target format
            processors['convert_to_target'] = self._create_convert_processor(
                load_pg,
                name='Convert_To_Target_Format',
                position=(100, 100)
            )
            
            # PutDatabaseRecord for main returns table
            processors['load_returns'] = self._create_database_processor(
                load_pg,
                name='Load_Returns_Table',
                table_name=self.load_config['returns_table'],
                position=(400, 100)
            )
            
            # PutDatabaseRecord for returns audit table
            processors['load_audit'] = self._create_database_processor(
                load_pg,
                name='Load_Returns_Audit',
                table_name=self.load_config['audit_table'],
                position=(400, 300)
            )
            
            # PutFile for error records
            processors['write_errors'] = self._create_putfile_processor(
                load_pg,
                name='Write_Error_Records',
                directory=self.load_config['error_directory'],
                position=(700, 500)
            )
            
            # PutFile for archive
            processors['archive_processed'] = self._create_putfile_processor(
                load_pg,
                name='Archive_Processed_Records',
                directory=self.load_config['archive_directory'],
                position=(700, 100)
            )
            
            # UpdateAttribute for success metrics
            processors['update_metrics'] = self._create_metrics_processor(
                load_pg,
                name='Update_Load_Metrics',
                position=(1000, 100)
            )
            
            # PutEmail for notifications
            processors['send_notification'] = self._create_email_processor(
                load_pg,
                name='Send_Load_Notification',
                position=(1300, 100)
            )
            
            # Create connections
            self._create_connections(processors)
            
            logger.info("Loading flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating loading flow: {str(e)}")
            raise
    
    def _create_convert_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create ConvertRecord processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('ConvertRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'JSONReader',
            'Record Writer': 'AvroRecordSetWriter',
            'Include Zero Record FlowFiles': 'false'
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_database_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        table_name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutDatabaseRecord processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('PutDatabaseRecord'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Record Reader': 'AvroReader',
            'Database Connection Pooling Service': self.load_config['db_connection_pool'],
            'Statement Type': 'INSERT',
            'Table Name': table_name,
            'Translate Field Names': 'true',
            'Unmatched Field Behavior': 'Ignore Unmatched Fields',
            'Unmatched Column Behavior': 'Fail on Unmatched Columns',
            'Update Keys': 'return_id',
            'Field Containing SQL': '',
            'Allow Multiple Statements': 'false',
            'Maximum Batch Size': str(self.load_config['batch_size']),
            'Rollback On Failure': 'true'
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_putfile_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        directory: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutFile processor"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('PutFile'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'Directory': directory,
            'Conflict Resolution Strategy': 'replace',
            'Create Missing Directories': 'true',
            'Maximum File Count': '-1',
            'Last Modified Time': '',
            'Permissions': '',
            'Owner': '',
            'Group': ''
        }
        config.auto_terminated_relationships = ['success', 'failure']
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_metrics_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateAttribute processor for metrics"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('UpdateAttribute'),
            location=position,
            name=name
        )
        
        config = ProcessorConfigDTO()
        config.properties = {
            'load.timestamp': "${now():format('yyyy-MM-dd HH:mm:ss')}",
            'load.status': 'SUCCESS',
            'load.record.count': '${record.count}',
            'load.table': '${database.table.name}',
            'load.duration.ms': '${processing.duration.ms}'
        }
        config.auto_terminated_relationships = []
        
        nipyapi.canvas.update_processor(processor, config)
        return processor
    
    def _create_email_processor(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutEmail processor for notifications"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('PutEmail'),
            location=position,
            name=name
        )
        
        notification_config = self.load_config['notification']
        
        config = ProcessorConfigDTO()