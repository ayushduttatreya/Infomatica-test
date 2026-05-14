# NiFi Migration: Project Scaffold and CI/CD Pipeline Setup

===FILE: src/extract.py===
"""
Extract module for customer data ingestion from flat files.
Handles reading customer and address data with error handling and validation.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import csv
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class CustomerDataExtractor:
    """Extracts customer data from flat file sources."""
    
    def __init__(self, config: Dict):
        """
        Initialize extractor with configuration.
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('sources', {})
        self.error_handling = config.get('error_handling', {})
        
    def create_extract_processor_group(
        self, 
        canvas_id: str, 
        group_name: str = "Customer_Data_Extract"
    ) -> nipyapi.nifi.ProcessGroupEntity:
        """
        Create NiFi processor group for data extraction.
        
        Args:
            canvas_id: Parent canvas ID
            group_name: Name for the processor group
            
        Returns:
            Created processor group entity
        """
        try:
            # Create processor group
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name=group_name,
                location=(100, 100)
            )
            
            logger.info(f"Created processor group: {group_name}")
            
            # Create GetFile processor for customers
            customer_processor = self._create_getfile_processor(
                pg.id,
                "Get_Customer_Files",
                self.source_config['customers']['directory'],
                self.source_config['customers']['file_filter'],
                (200, 100)
            )
            
            # Create GetFile processor for addresses
            address_processor = self._create_getfile_processor(
                pg.id,
                "Get_Address_Files",
                self.source_config['addresses']['directory'],
                self.source_config['addresses']['file_filter'],
                (200, 300)
            )
            
            # Create validation processor
            validation_processor = self._create_validation_processor(
                pg.id,
                "Validate_Customer_Data",
                (500, 100)
            )
            
            # Create error handling funnel
            error_funnel = nipyapi.canvas.create_funnel(
                pg.id,
                location=(800, 400)
            )
            
            # Connect processors
            self._connect_processors(
                customer_processor,
                validation_processor,
                ['success']
            )
            
            self._connect_processors(
                validation_processor,
                error_funnel,
                ['failure', 'invalid']
            )
            
            logger.info("Extract processor group configured successfully")
            return pg
            
        except Exception as e:
            logger.error(f"Failed to create extract processor group: {str(e)}")
            raise
    
    def _create_getfile_processor(
        self,
        pg_id: str,
        name: str,
        directory: str,
        file_filter: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create and configure GetFile processor."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('GetFile'),
                location=location,
                name=name
            )
            
            # Configure processor properties
            config = ProcessorConfigDTO()
            config.properties = {
                'Input Directory': directory,
                'File Filter': file_filter,
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'true',
                'Polling Interval': '10 sec',
                'Batch Size': str(self.source_config.get('batch_size', 100)),
                'Ignore Hidden Files': 'true'
            }
            config.auto_terminated_relationships = ['not.found']
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created GetFile processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create GetFile processor: {str(e)}")
            raise
    
    def _create_validation_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create ValidateRecord processor for data validation."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('ValidateRecord'),
                location=location,
                name=name
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Schema Access Strategy': 'Use String Fields From Header',
                'Allow Extra Fields': 'true',
                'Max Validation Details': '100'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created validation processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create validation processor: {str(e)}")
            raise
    
    def _connect_processors(
        self,
        source: nipyapi.nifi.ProcessorEntity,
        destination: nipyapi.nifi.ProcessorEntity,
        relationships: List[str]
    ):
        """Connect two processors with specified relationships."""
        try:
            for relationship in relationships:
                nipyapi.canvas.create_connection(
                    source=source,
                    destination=destination,
                    relationships=[relationship]
                )
            logger.debug(f"Connected {source.component.name} to {destination.component.name}")
        except Exception as e:
            logger.error(f"Failed to connect processors: {str(e)}")
            raise
    
    def validate_source_files(self) -> Dict[str, bool]:
        """
        Validate that source files exist and are readable.
        
        Returns:
            Dictionary with validation results for each source
        """
        results = {}
        
        for source_name, source_config in self.source_config.items():
            if source_name in ['customers', 'addresses']:
                directory = Path(source_config['directory'])
                results[source_name] = directory.exists() and directory.is_dir()
                
                if not results[source_name]:
                    logger.warning(f"Source directory not found: {directory}")
        
        return results


===FILE: src/transform.py===
"""
Transform module for customer data processing.
Handles data cleansing, enrichment, and business rule application.
"""

import logging
from typing import Dict, List, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class CustomerDataTransformer:
    """Transforms customer data according to business rules."""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transformations', {})
        self.business_rules = config.get('business_rules', {})
        
    def create_transform_processor_group(
        self,
        canvas_id: str,
        group_name: str = "Customer_Data_Transform"
    ) -> nipyapi.nifi.ProcessGroupEntity:
        """
        Create NiFi processor group for data transformation.
        
        Args:
            canvas_id: Parent canvas ID
            group_name: Name for the processor group
            
        Returns:
            Created processor group entity
        """
        try:
            # Create processor group
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name=group_name,
                location=(100, 500)
            )
            
            logger.info(f"Created processor group: {group_name}")
            
            # Create UpdateRecord processor for data cleansing
            cleanse_processor = self._create_cleanse_processor(
                pg.id,
                "Cleanse_Customer_Data",
                (200, 100)
            )
            
            # Create JoltTransformJSON for data enrichment
            enrich_processor = self._create_enrichment_processor(
                pg.id,
                "Enrich_Customer_Data",
                (500, 100)
            )
            
            # Create RouteOnAttribute for business rules
            route_processor = self._create_routing_processor(
                pg.id,
                "Apply_Business_Rules",
                (800, 100)
            )
            
            # Create UpdateAttribute for metadata
            metadata_processor = self._create_metadata_processor(
                pg.id,
                "Add_Metadata",
                (1100, 100)
            )
            
            # Connect processors
            self._connect_processors(cleanse_processor, enrich_processor, ['success'])
            self._connect_processors(enrich_processor, route_processor, ['success'])
            self._connect_processors(route_processor, metadata_processor, ['matched'])
            
            # Create error handling
            error_funnel = nipyapi.canvas.create_funnel(pg.id, location=(1100, 400))
            self._connect_processors(cleanse_processor, error_funnel, ['failure'])
            self._connect_processors(enrich_processor, error_funnel, ['failure'])
            self._connect_processors(route_processor, error_funnel, ['unmatched'])
            
            logger.info("Transform processor group configured successfully")
            return pg
            
        except Exception as e:
            logger.error(f"Failed to create transform processor group: {str(e)}")
            raise
    
    def _create_cleanse_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateRecord processor for data cleansing."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('UpdateRecord'),
                location=location,
                name=name
            )
            
            # Build cleansing rules from config
            cleansing_rules = self._build_cleansing_rules()
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value',
                **cleansing_rules
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created cleanse processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create cleanse processor: {str(e)}")
            raise
    
    def _create_enrichment_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create JoltTransformJSON processor for data enrichment."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('JoltTransformJSON'),
                location=location,
                name=name
            )
            
            # Build Jolt specification from config
            jolt_spec = self._build_jolt_specification()
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Jolt Transformation DSL': 'Chain',
                'Jolt Specification': jolt_spec,
                'Transform Cache Size': '1'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created enrichment processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create enrichment processor: {str(e)}")
            raise
    
    def _create_routing_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create RouteOnAttribute processor for business rules."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
                location=location,
                name=name
            )
            
            # Build routing rules from business rules config
            routing_rules = self._build_routing_rules()
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Routing Strategy': 'Route to Property name',
                **routing_rules
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created routing processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create routing processor: {str(e)}")
            raise
    
    def _create_metadata_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create UpdateAttribute processor for metadata."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('UpdateAttribute'),
                location=location,
                name=name
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'processing_timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                'source_system': 'informatica_migration',
                'data_version': self.config.get('version', '1.0'),
                'environment': self.config.get('environment', 'production')
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created metadata processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create metadata processor: {str(e)}")
            raise
    
    def _build_cleansing_rules(self) -> Dict[str, str]:
        """Build cleansing rules from configuration."""
        rules = {}
        
        cleansing_config = self.transform_config.get('cleansing', {})
        
        # Trim whitespace
        if cleansing_config.get('trim_whitespace', True):
            rules['/first_name'] = 'trim(${field.value})'
            rules['/last_name'] = 'trim(${field.value})'
            rules['/email'] = 'trim(${field.value})'
        
        # Uppercase state codes
        if cleansing_config.get('uppercase_state', True):
            rules['/state'] = 'toUpper(${field.value})'
        
        # Standardize phone format
        if cleansing_config.get('standardize_phone', True):
            rules['/phone'] = 'replaceAll(${field.value}, "[^0-9]", "")'
        
        # Lowercase email
        if cleansing_config.get('lowercase_email', True):
            rules['/email'] = 'toLower(${field.value})'
        
        return rules
    
    def _build_jolt_specification(self) -> str:
        """Build Jolt transformation specification."""
        enrichment_config = self.transform_config.get('enrichment', {})
        
        jolt_spec = [
            {
                "operation": "shift",
                "spec": {
                    "customer_id": "customer_id",
                    "first_name": "customer.first_name",
                    "last_name": "customer.last_name",
                    "email": "customer.email",
                    "phone": "customer.phone",
                    "address_line1": "address.line1",
                    "address_line2": "address.line2",
                    "city": "address.city",
                    "state": "address.state",
                    "zip_code": "address.zip_code",
                    "country": "address.country",
                    "registration_date": "metadata.registration_date",
                    "status": "metadata.status"
                }
            },
            {
                "operation": "default",
                "spec": {
                    "customer.full_name": "${customer.first_name} ${customer.last_name}",
                    "metadata.record_type": "customer",
                    "metadata.source": "flat_file"
                }
            }
        ]
        
        import json
        return json.dumps(jolt_spec)
    
    def _build_routing_rules(self) -> Dict[str, str]:
        """Build routing rules from business rules configuration."""
        rules = {}
        
        business_rules = self.business_rules
        
        # Active customers
        if business_rules.get('route_active_customers', True):
            rules['active_customer'] = "${status:equals('ACTIVE')}"
        
        # Inactive customers
        if business_rules.get('route_inactive_customers', True):
            rules['inactive_customer'] = "${status:equals('INACTIVE')}"
        
        # VIP customers (example rule)
        if business_rules.get('route_vip_customers', False):
            rules['vip_customer'] = "${customer_type:equals('VIP')}"
        
        # International customers
        if business_rules.get('route_international', True):
            rules['international'] = "${country:notEquals('US')}"
        
        return rules
    
    def _connect_processors(
        self,
        source: nipyapi.nifi.ProcessorEntity,
        destination: nipyapi.nifi.ProcessorEntity,
        relationships: List[str]
    ):
        """Connect two processors with specified relationships."""
        try:
            for relationship in relationships:
                nipyapi.canvas.create_connection(
                    source=source,
                    destination=destination,
                    relationships=[relationship]
                )
            logger.debug(f"Connected {source.component.name} to {destination.component.name}")
        except Exception as e:
            logger.error(f"Failed to connect processors: {str(e)}")
            raise


===FILE: src/load.py===
"""
Load module for customer data persistence.
Handles writing transformed data to target systems with error handling.
"""

import logging
from typing import Dict, List, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class CustomerDataLoader:
    """Loads customer data to target systems."""
    
    def __init__(self, config: Dict):
        """
        Initialize loader with configuration.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.target_config = config.get('targets', {})
        self.error_handling = config.get('error_handling', {})
        
    def create_load_processor_group(
        self,
        canvas_id: str,
        group_name: str = "Customer_Data_Load"
    ) -> nipyapi.nifi.ProcessGroupEntity:
        """
        Create NiFi processor group for data loading.
        
        Args:
            canvas_id: Parent canvas ID
            group_name: Name for the processor group
            
        Returns:
            Created processor group entity
        """
        try:
            # Create processor group
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name=group_name,
                location=(100, 900)
            )
            
            logger.info(f"Created processor group: {group_name}")
            
            # Create PutDatabaseRecord processor for primary target
            primary_load_processor = self._create_database_load_processor(
                pg.id,
                "Load_To_Primary_Database",
                self.target_config.get('primary_database', {}),
                (200, 100)
            )
            
            # Create PutFile processor for archive
            archive_processor = self._create_archive_processor(
                pg.id,
                "Archive_Processed_Files",
                (500, 100)
            )
            
            # Create PutFile processor for error handling
            error_processor = self._create_error_processor(
                pg.id,
                "Write_Error_Files",
                (500, 300)
            )
            
            # Create success notification processor
            notify_processor = self._create_notification_processor(
                pg.id,
                "Send_Success_Notification",
                (800, 100)
            )
            
            # Connect processors
            self._connect_processors(
                primary_load_processor,
                archive_processor,
                ['success']
            )
            
            self._connect_processors(
                archive_processor,
                notify_processor,
                ['success']
            )
            
            self._connect_processors(
                primary_load_processor,
                error_processor,
                ['failure', 'retry']
            )
            
            logger.info("Load processor group configured successfully")
            return pg
            
        except Exception as e:
            logger.error(f"Failed to create load processor group: {str(e)}")
            raise
    
    def _create_database_load_processor(
        self,
        pg_id: str,
        name: str,
        db_config: Dict,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutDatabaseRecord processor for database loading."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('PutDatabaseRecord'),
                location=location,
                name=name
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Statement Type': db_config.get('statement_type', 'INSERT'),
                'Database Connection Pooling Service': db_config.get('connection_pool', 'DBCPConnectionPool'),
                'Schema Name': db_config.get('schema_name', 'public'),
                'Table Name': db_config.get('table_name', 'customers'),
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields',
                'Unmatched Column Behavior': 'Fail on Unmatched Columns',
                'Update Keys': db_config.get('update_keys', 'customer_id'),
                'Field Containing SQL': '',
                'Allow Multiple Statements': 'false',
                'Maximum Batch Size': str(db_config.get('batch_size', 1000)),
                'Obtain Generated Keys': 'false'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created database load processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create database load processor: {str(e)}")
            raise
    
    def _create_archive_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutFile processor for archiving processed files."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('PutFile'),
                location=location,
                name=name
            )
            
            archive_config = self.target_config.get('archive', {})
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Directory': archive_config.get('directory', '/data/archive'),
                'Conflict Resolution Strategy': 'replace',
                'Create Missing Directories': 'true',
                'Maximum File Count': '-1',
                'Last Modified Time': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                'Permissions': archive_config.get('permissions', '644'),
                'Owner': archive_config.get('owner', ''),
                'Group': archive_config.get('group', ''),
                'Directory Permissions': archive_config.get('directory_permissions', '755')
            }
            config.auto_terminated_relationships = ['success', 'failure']
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created archive processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create archive processor: {str(e)}")
            raise
    
    def _create_error_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutFile processor for error handling."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('PutFile'),
                location=location,
                name=name
            )
            
            error_config = self.error_handling
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Directory': error_config.get('error_directory', '/data/errors'),
                'Conflict Resolution Strategy': 'replace',
                'Create Missing Directories': 'true',
                'Maximum File Count': '-1',
                'Last Modified Time': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                'Permissions': '644',
                'Directory Permissions': '755'
            }
            config.auto_terminated_relationships = ['success', 'failure']
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created error processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create error processor: {str(e)}")
            raise
    
    def _create_notification_processor(
        self,
        pg_id: str,
        name: str,
        location: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create PutEmail processor for success notifications."""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('PutEmail'),
                location=location,
                name=name
            )
            
            notification_config = self.config.get('notifications', {})
            
            config = ProcessorConfigDTO()
            config.properties = {
                'SMTP Hostname': notification_config.get('smtp_host', 'localhost'),
                'SMTP Port': str(notification_config.get('smtp_port', 25)),
                'SMTP Username': notification_config.get('smtp_username', ''),
                'SMTP Password': notification_config.get('smtp_password', ''),
                'SMTP TLS': str(notification_config.get('smtp_tls', False)),
                'From': notification_config.get('from_email', 'nifi@example.com'),
                'To': notification_config.get('to_email', 'admin@example.com'),
                'Subject': 'Customer Data Load Completed - ${now():format("yyyy-MM-dd HH:mm:ss")}',
                'Message': 'Customer data load completed successfully. Records processed: ${record.count}',
                'Attach File': 'false',
                'Include All Attributes In Message': 'true'
            }
            config.auto_terminated_relationships = ['success', 'failure']
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created notification processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create notification processor: {str(e)}")
            raise
    
    def _connect_processors(
        self,
        source: nipyapi.nifi.ProcessorEntity,
        destination: nipyapi.nifi.ProcessorEntity,
        relationships: List[str]
    ):
        """Connect two processors with specified relationships."""
        try:
            for relationship in relationships:
                nipyapi.canvas.create_connection(
                    source=source,
                    destination=destination,
                    relationships=[relationship]
                )
            logger.debug(f"Connected {source.component.name} to {destination.component.name}")
        except Exception as e:
            logger.error(f"Failed to connect processors: {str(e)}")
            raise
    
    def validate_target_connectivity(self) -> Dict[str, bool]:
        """
        Validate connectivity to target systems.
        
        Returns:
            Dictionary with validation results for each target
        """
        results = {}
        
        # Validate primary database
        primary_db = self.target_config.get('primary_database', {})
        if primary_db:
            results['primary_database'] = self._test_database_connection(primary_db)
        
        # Validate archive directory
        archive_config = self.target_config.get('archive', {})
        if archive_config:
            from pathlib import Path
            archive_dir = Path(archive_config.get('directory', '/data/archive'))
            results['archive'] = archive_dir.exists() or archive_config.get('create_if_missing', True)
        
        return results
    
    def _test_database_connection(self, db_config: Dict) -> bool:
        """Test database connectivity."""
        try:
            # This would use actual database connection testing
            # For now, return True if config exists
            return bool(db_config.get('connection_pool'))
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False


===FILE: src/pipeline_orchestrator.py===
"""
Pipeline orchestrator for end-to-end customer data migration.
Coordinates extract, transform, and load operations.
"""

import logging
from typing import Dict, Optional
import nipyapi
from nipyapi.nifi import ProcessGroupEntity

from src.extract import CustomerDataExtractor
from src.transform import CustomerDataTransformer
from src.load import CustomerDataLoader

logger = logging.getLogger(__name__)


class CustomerDataPipeline:
    """Orchestrates the complete customer data migration pipeline."""
    
    def __init__(self, config: Dict):
        """
        Initialize pipeline with configuration.
        
        Args:
            config: Configuration dictionary for the entire pipeline
        """
        self.config = config
        self.nifi_config = config.get('nifi', {})
        
        # Initialize components
        self.extractor = CustomerDataExtractor(config)
        self.transformer = CustomerDataTransformer(config)
        self.loader = CustomerDataLoader(config)
        
        # Pipeline state
        self.root_pg = None
        self.extract_pg = None
        self