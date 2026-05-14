===FILE: src/extract.py===
"""
Sales Order Processing - Extract Module
Extracts sales order data from source systems using NiFi processors.
"""

import logging
from typing import Dict, Any, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessorEntity

logger = logging.getLogger(__name__)


class SalesOrderExtractor:
    """Handles extraction of sales order data from source systems."""
    
    def __init__(self, canvas_id: str, config: Dict[str, Any]):
        """
        Initialize the extractor.
        
        Args:
            canvas_id: NiFi canvas/process group ID
            config: Configuration dictionary
        """
        self.canvas_id = canvas_id
        self.config = config
        self.processor_ids = {}
        
    def create_extraction_flow(self) -> Dict[str, str]:
        """
        Create NiFi processors for data extraction.
        
        Returns:
            Dictionary mapping processor names to their IDs
        """
        try:
            logger.info("Creating sales order extraction flow")
            
            # Create GetFile processor for customers
            customers_processor = self._create_getfile_processor(
                name="Extract_Customers",
                directory=self.config['sources']['customers']['directory'],
                file_filter=self.config['sources']['customers']['file_pattern']
            )
            self.processor_ids['extract_customers'] = customers_processor.id
            
            # Create GetFile processor for customer addresses
            addresses_processor = self._create_getfile_processor(
                name="Extract_Customer_Addresses",
                directory=self.config['sources']['customer_addresses']['directory'],
                file_filter=self.config['sources']['customer_addresses']['file_pattern']
            )
            self.processor_ids['extract_addresses'] = addresses_processor.id
            
            # Create GetFile processor for sales orders
            orders_processor = self._create_getfile_processor(
                name="Extract_Sales_Orders",
                directory=self.config['sources']['sales_orders']['directory'],
                file_filter=self.config['sources']['sales_orders']['file_pattern']
            )
            self.processor_ids['extract_orders'] = orders_processor.id
            
            # Create GetFile processor for order line items
            line_items_processor = self._create_getfile_processor(
                name="Extract_Order_Line_Items",
                directory=self.config['sources']['order_line_items']['directory'],
                file_filter=self.config['sources']['order_line_items']['file_pattern']
            )
            self.processor_ids['extract_line_items'] = line_items_processor.id
            
            # Create RouteOnAttribute for error handling
            error_router = self._create_error_router("Extract_Error_Router")
            self.processor_ids['error_router'] = error_router.id
            
            logger.info(f"Created {len(self.processor_ids)} extraction processors")
            return self.processor_ids
            
        except Exception as e:
            logger.error(f"Failed to create extraction flow: {str(e)}")
            raise
    
    def _create_getfile_processor(
        self, 
        name: str, 
        directory: str, 
        file_filter: str
    ) -> ProcessorEntity:
        """
        Create a GetFile processor.
        
        Args:
            name: Processor name
            directory: Source directory path
            file_filter: File pattern filter
            
        Returns:
            Created processor entity
        """
        processor_config = ProcessorConfigDTO(
            properties={
                'Input Directory': directory,
                'File Filter': file_filter,
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'false',
                'Polling Interval': self.config['extraction']['polling_interval'],
                'Batch Size': str(self.config['extraction']['batch_size']),
                'Ignore Hidden Files': 'true'
            },
            auto_terminated_relationships=['not.found'],
            scheduling_period=self.config['extraction']['scheduling_period'],
            scheduling_strategy='TIMER_DRIVEN',
            execution_node='ALL'
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.GetFile',
                    name=name,
                    config=processor_config
                )
            ),
            location=(100.0, 100.0)
        )
        
        logger.info(f"Created GetFile processor: {name}")
        return processor
    
    def _create_error_router(self, name: str) -> ProcessorEntity:
        """
        Create RouteOnAttribute processor for error handling.
        
        Args:
            name: Processor name
            
        Returns:
            Created processor entity
        """
        processor_config = ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'error': "${file.size:lt(1)}"
            },
            auto_terminated_relationships=['unmatched']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.RouteOnAttribute',
                    name=name,
                    config=processor_config
                )
            ),
            location=(100.0, 300.0)
        )
        
        logger.info(f"Created RouteOnAttribute processor: {name}")
        return processor
    
    def connect_processors(self) -> None:
        """Create connections between extraction processors."""
        try:
            # Connect each extractor to error router
            for extractor_name in ['extract_customers', 'extract_addresses', 
                                   'extract_orders', 'extract_line_items']:
                if extractor_name in self.processor_ids:
                    nipyapi.canvas.create_connection(
                        source=nipyapi.canvas.get_processor(
                            self.processor_ids[extractor_name], 'id'
                        ),
                        target=nipyapi.canvas.get_processor(
                            self.processor_ids['error_router'], 'id'
                        ),
                        relationships=['success']
                    )
            
            logger.info("Connected extraction processors")
            
        except Exception as e:
            logger.error(f"Failed to connect processors: {str(e)}")
            raise


===FILE: src/transform.py===
"""
Sales Order Processing - Transform Module
Transforms sales order data including aggregations, validations, and mappings.
"""

import logging
from typing import Dict, Any, List, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessorEntity

logger = logging.getLogger(__name__)


class SalesOrderTransformer:
    """Handles transformation of sales order data."""
    
    def __init__(self, canvas_id: str, config: Dict[str, Any]):
        """
        Initialize the transformer.
        
        Args:
            canvas_id: NiFi canvas/process group ID
            config: Configuration dictionary
        """
        self.canvas_id = canvas_id
        self.config = config
        self.processor_ids = {}
        
    def create_transformation_flow(self) -> Dict[str, str]:
        """
        Create NiFi processors for data transformation.
        
        Returns:
            Dictionary mapping processor names to their IDs
        """
        try:
            logger.info("Creating sales order transformation flow")
            
            # Parse CSV files
            csv_parser = self._create_csv_parser("Parse_CSV_Records")
            self.processor_ids['csv_parser'] = csv_parser.id
            
            # Validate order status
            status_validator = self._create_status_validator("Validate_Order_Status")
            self.processor_ids['status_validator'] = status_validator.id
            
            # Map region codes
            region_mapper = self._create_region_mapper("Map_Region_Codes")
            self.processor_ids['region_mapper'] = region_mapper.id
            
            # Aggregate order amounts
            amount_aggregator = self._create_amount_aggregator("Aggregate_Order_Amounts")
            self.processor_ids['amount_aggregator'] = amount_aggregator.id
            
            # Join customer data
            customer_joiner = self._create_join_processor("Join_Customer_Data")
            self.processor_ids['customer_joiner'] = customer_joiner.id
            
            # Calculate derived fields
            field_calculator = self._create_field_calculator("Calculate_Derived_Fields")
            self.processor_ids['field_calculator'] = field_calculator.id
            
            # Validate data quality
            quality_validator = self._create_quality_validator("Validate_Data_Quality")
            self.processor_ids['quality_validator'] = quality_validator.id
            
            # Format output
            output_formatter = self._create_output_formatter("Format_Output")
            self.processor_ids['output_formatter'] = output_formatter.id
            
            logger.info(f"Created {len(self.processor_ids)} transformation processors")
            return self.processor_ids
            
        except Exception as e:
            logger.error(f"Failed to create transformation flow: {str(e)}")
            raise
    
    def _create_csv_parser(self, name: str) -> ProcessorEntity:
        """Create CSV parser processor."""
        processor_config = ProcessorConfigDTO(
            properties={
                'CSV Format': 'Custom Format',
                'Value Separator': ',',
                'Quote Character': '"',
                'Escape Character': '\\',
                'Comment Marker': '#',
                'Null String': '',
                'Trim Fields': 'true',
                'Schema Access Strategy': 'Use String Fields From Header',
                'Treat First Line as Header': 'true'
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.ConvertRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 100.0)
        )
        
        logger.info(f"Created CSV parser: {name}")
        return processor
    
    def _create_status_validator(self, name: str) -> ProcessorEntity:
        """Create order status validator using UpdateRecord."""
        valid_statuses = self.config['validations']['valid_order_statuses']
        status_list = "','".join(valid_statuses)
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value',
                '/status_valid': f"${{fieldValue('/status'):in('{status_list}')}}",
                '/validation_timestamp': "${now():format('yyyy-MM-dd HH:mm:ss')}"
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.UpdateRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 200.0)
        )
        
        logger.info(f"Created status validator: {name}")
        return processor
    
    def _create_region_mapper(self, name: str) -> ProcessorEntity:
        """Create region code mapper using LookupRecord."""
        region_mappings = self.config['mappings']['region_codes']
        
        # Build lookup properties
        lookup_props = {
            'Record Reader': 'JsonTreeReader',
            'Record Writer': 'JsonRecordSetWriter',
            'Lookup Service': 'SimpleCsvFileLookupService',
            'Result RecordPath': '/region_name',
            'Routing Strategy': 'Route to Success',
            'Record Result Contents': 'Insert Entire Record'
        }
        
        # Add region mapping logic
        for region_code, region_name in region_mappings.items():
            lookup_props[f'region_{region_code}'] = region_name
        
        processor_config = ProcessorConfigDTO(
            properties=lookup_props,
            auto_terminated_relationships=['failure', 'unmatched']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.LookupRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 300.0)
        )
        
        logger.info(f"Created region mapper: {name}")
        return processor
    
    def _create_amount_aggregator(self, name: str) -> ProcessorEntity:
        """Create amount aggregator using PartitionRecord and UpdateRecord."""
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value',
                '/total_amount': "${fieldValue('/unit_price'):multiply(${fieldValue('/quantity')})}",
                '/tax_amount': "${fieldValue('/total_amount'):multiply(${tax_rate:toDecimal()})}",
                '/grand_total': "${fieldValue('/total_amount'):plus(${fieldValue('/tax_amount')})}",
                '/discount_amount': "${fieldValue('/total_amount'):multiply(${fieldValue('/discount_percent'):divide(100)})}",
                '/net_amount': "${fieldValue('/grand_total'):minus(${fieldValue('/discount_amount')})}"
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.UpdateRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 400.0)
        )
        
        logger.info(f"Created amount aggregator: {name}")
        return processor
    
    def _create_join_processor(self, name: str) -> ProcessorEntity:
        """Create processor to join customer data."""
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Join Strategy': 'Inner Join',
                'Join Key': '/customer_id',
                'Cache Size': '10000',
                'Cache Expiration': '1 hour'
            },
            auto_terminated_relationships=['failure', 'unmatched']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.JoinEnrichment',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 500.0)
        )
        
        logger.info(f"Created join processor: {name}")
        return processor
    
    def _create_field_calculator(self, name: str) -> ProcessorEntity:
        """Create processor to calculate derived fields."""
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value',
                '/customer_full_name': "${fieldValue('/first_name'):append(' '):append(${fieldValue('/last_name')})}",
                '/order_age_days': "${now():toNumber():minus(${fieldValue('/order_date'):toDate('yyyy-MM-dd'):toNumber()}):divide(86400000)}",
                '/is_priority': "${fieldValue('/net_amount'):ge(${priority_threshold:toDecimal()})}",
                '/processing_timestamp': "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
                '/record_hash': "${fieldValue('/order_id'):append(${fieldValue('/customer_id')}):hash('SHA-256')}"
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.UpdateRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 600.0)
        )
        
        logger.info(f"Created field calculator: {name}")
        return processor
    
    def _create_quality_validator(self, name: str) -> ProcessorEntity:
        """Create data quality validator using ValidateRecord."""
        validations = self.config['validations']
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Schema Access Strategy': 'Use Schema Name Property',
                'Allow Extra Fields': 'true',
                'Strict Type Checking': 'true',
                'Max Validation Details': '100',
                'customer_id_not_null': "/customer_id != null",
                'order_id_not_null': "/order_id != null",
                'amount_positive': "/net_amount > 0",
                'email_format': "/email matches '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'",
                'phone_format': "/phone matches '^[0-9]{3}-[0-9]{3}-[0-9]{4}$'",
                'zip_code_format': "/zip_code matches '^[0-9]{5}(-[0-9]{4})?$'"
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.ValidateRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 700.0)
        )
        
        logger.info(f"Created quality validator: {name}")
        return processor
    
    def _create_output_formatter(self, name: str) -> ProcessorEntity:
        """Create output formatter processor."""
        output_format = self.config['output']['format']
        
        writer_type = {
            'json': 'JsonRecordSetWriter',
            'csv': 'CSVRecordSetWriter',
            'avro': 'AvroRecordSetWriter'
        }.get(output_format, 'JsonRecordSetWriter')
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Record Writer': writer_type,
                'Include Zero Record FlowFiles': 'false',
                'Pretty Print JSON': 'true' if output_format == 'json' else 'false',
                'Suppress Null Values': 'Never Suppress',
                'Output Grouping': 'One FlowFile Per Batch'
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.ConvertRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(300.0, 800.0)
        )
        
        logger.info(f"Created output formatter: {name}")
        return processor
    
    def connect_processors(self, extract_processor_id: str) -> None:
        """
        Create connections between transformation processors.
        
        Args:
            extract_processor_id: ID of the extraction processor to connect from
        """
        try:
            # Define processor chain
            processor_chain = [
                'csv_parser',
                'status_validator',
                'region_mapper',
                'amount_aggregator',
                'customer_joiner',
                'field_calculator',
                'quality_validator',
                'output_formatter'
            ]
            
            # Connect extraction to first transformer
            nipyapi.canvas.create_connection(
                source=nipyapi.canvas.get_processor(extract_processor_id, 'id'),
                target=nipyapi.canvas.get_processor(
                    self.processor_ids[processor_chain[0]], 'id'
                ),
                relationships=['success']
            )
            
            # Connect transformers in sequence
            for i in range(len(processor_chain) - 1):
                current_proc = self.processor_ids[processor_chain[i]]
                next_proc = self.processor_ids[processor_chain[i + 1]]
                
                nipyapi.canvas.create_connection(
                    source=nipyapi.canvas.get_processor(current_proc, 'id'),
                    target=nipyapi.canvas.get_processor(next_proc, 'id'),
                    relationships=['success', 'matched', 'valid']
                )
            
            logger.info("Connected transformation processors")
            
        except Exception as e:
            logger.error(f"Failed to connect processors: {str(e)}")
            raise


===FILE: src/load.py===
"""
Sales Order Processing - Load Module
Loads transformed sales order data to target systems.
"""

import logging
from typing import Dict, Any, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessorEntity

logger = logging.getLogger(__name__)


class SalesOrderLoader:
    """Handles loading of transformed sales order data."""
    
    def __init__(self, canvas_id: str, config: Dict[str, Any]):
        """
        Initialize the loader.
        
        Args:
            canvas_id: NiFi canvas/process group ID
            config: Configuration dictionary
        """
        self.canvas_id = canvas_id
        self.config = config
        self.processor_ids = {}
        
    def create_load_flow(self) -> Dict[str, str]:
        """
        Create NiFi processors for data loading.
        
        Returns:
            Dictionary mapping processor names to their IDs
        """
        try:
            logger.info("Creating sales order load flow")
            
            # Route records by destination
            destination_router = self._create_destination_router("Route_By_Destination")
            self.processor_ids['destination_router'] = destination_router.id
            
            # Load to database
            db_loader = self._create_database_loader("Load_To_Database")
            self.processor_ids['db_loader'] = db_loader.id
            
            # Load to file system
            file_loader = self._create_file_loader("Load_To_FileSystem")
            self.processor_ids['file_loader'] = file_loader.id
            
            # Load to S3
            s3_loader = self._create_s3_loader("Load_To_S3")
            self.processor_ids['s3_loader'] = s3_loader.id
            
            # Archive successful loads
            archiver = self._create_archiver("Archive_Successful_Loads")
            self.processor_ids['archiver'] = archiver.id
            
            # Handle load failures
            failure_handler = self._create_failure_handler("Handle_Load_Failures")
            self.processor_ids['failure_handler'] = failure_handler.id
            
            # Update load statistics
            stats_updater = self._create_stats_updater("Update_Load_Statistics")
            self.processor_ids['stats_updater'] = stats_updater.id
            
            logger.info(f"Created {len(self.processor_ids)} load processors")
            return self.processor_ids
            
        except Exception as e:
            logger.error(f"Failed to create load flow: {str(e)}")
            raise
    
    def _create_destination_router(self, name: str) -> ProcessorEntity:
        """Create router to direct records to appropriate destinations."""
        processor_config = ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'database': "${destination:equals('database')}",
                'filesystem': "${destination:equals('filesystem')}",
                's3': "${destination:equals('s3')}",
                'archive': "${destination:equals('archive')}"
            },
            auto_terminated_relationships=['unmatched']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.RouteOnAttribute',
                    name=name,
                    config=processor_config
                )
            ),
            location=(500.0, 100.0)
        )
        
        logger.info(f"Created destination router: {name}")
        return processor
    
    def _create_database_loader(self, name: str) -> ProcessorEntity:
        """Create database loader using PutDatabaseRecord."""
        db_config = self.config['targets']['database']
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Record Reader': 'JsonTreeReader',
                'Statement Type': 'INSERT',
                'Database Connection Pooling Service': 'DBCPConnectionPool',
                'Schema Name': db_config.get('schema', 'public'),
                'Table Name': db_config['table'],
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields',
                'Unmatched Column Behavior': 'Fail on Unmatched Columns',
                'Update Keys': db_config.get('update_keys', 'order_id'),
                'Field Containing SQL': '',
                'Allow Multiple Statements': 'false',
                'Quote Column Identifiers': 'true',
                'Quote Table Identifiers': 'true',
                'Max Wait Time': '30 seconds',
                'Rollback On Failure': 'true',
                'Batch Size': str(self.config['load']['batch_size'])
            },
            auto_terminated_relationships=['retry']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.PutDatabaseRecord',
                    name=name,
                    config=processor_config
                )
            ),
            location=(500.0, 200.0)
        )
        
        logger.info(f"Created database loader: {name}")
        return processor
    
    def _create_file_loader(self, name: str) -> ProcessorEntity:
        """Create file system loader using PutFile."""
        file_config = self.config['targets']['filesystem']
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Directory': file_config['directory'],
                'Conflict Resolution Strategy': file_config.get('conflict_resolution', 'replace'),
                'Create Missing Directories': 'true',
                'Maximum File Count': '-1',
                'Last Modified Time': '',
                'Permissions': file_config.get('permissions', ''),
                'Owner': file_config.get('owner', ''),
                'Group': file_config.get('group', ''),
                'Batch Size': str(self.config['load']['batch_size'])
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.standard.PutFile',
                    name=name,
                    config=processor_config
                )
            ),
            location=(500.0, 300.0)
        )
        
        logger.info(f"Created file loader: {name}")
        return processor
    
    def _create_s3_loader(self, name: str) -> ProcessorEntity:
        """Create S3 loader using PutS3Object."""
        s3_config = self.config['targets']['s3']
        
        processor_config = ProcessorConfigDTO(
            properties={
                'Bucket': s3_config['bucket'],
                'Object Key': s3_config.get('key_prefix', 'sales_orders/') + '${filename}',
                'Region': s3_config.get('region', 'us-east-1'),
                'Access Key ID': s3_config.get('access_key_id', ''),
                'Secret Access Key': s3_config.get('secret_access_key', ''),
                'Credentials File': s3_config.get('credentials_file', ''),
                'AWS Credentials Provider service': s3_config.get('credentials_provider', ''),
                'Storage Class': s3_config.get('storage_class', 'Standard'),
                'Server Side Encryption': s3_config.get('encryption', 'None'),
                'Content Type': 'application/json',
                'Content Disposition': '',
                'Cache Control': '',
                'Expiration Time Rule': '',
                'Multipart Threshold': '5 GB',
                'Multipart Part Size': '5 GB',
                'Multipart Upload AgeOff Interval': '7 days',
                'Multipart Upload Max Age Threshold': '7 days',
                'Use Path Style Access': 'false'
            },
            auto_terminated_relationships=['failure']
        )
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(self.canvas_id, 'id'),
            processor=nipyapi.nifi.ProcessorEntity(
                component=nipyapi.nifi.ProcessorDTO(
                    type='org.apache.nifi.processors.aws.s3.PutS3Object',
                    name=name,
                    config=processor_config
                )
            ),
            location=(500.0, 400.0)
        )
        
        logger.info(f"Created S3 loader: {name}")
        return processor