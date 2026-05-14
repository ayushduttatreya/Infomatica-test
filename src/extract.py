"""
Extract module for Line Item Aggregation Logic
Extracts order line items from source systems
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LineItemExtractor:
    """Extracts line item data from source systems"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the extractor with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.nifi_config = self.config['nifi']
        self.source_config = self.config['source']
        self.canvas = None
        
    def connect_to_nifi(self) -> bool:
        """
        Establish connection to NiFi instance
        
        Returns:
            bool: True if connection successful
        """
        try:
            nipyapi.config.nifi_config.host = self.nifi_config['host']
            nipyapi.config.nifi_config.port = self.nifi_config['port']
            
            # Test connection
            nipyapi.canvas.get_root_pg_id()
            logger.info(f"Successfully connected to NiFi at {self.nifi_config['host']}:{self.nifi_config['port']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to NiFi: {str(e)}")
            raise
    
    def create_process_group(self, parent_pg_id: str, group_name: str) -> ProcessGroupEntity:
        """
        Create a process group for line item extraction
        
        Args:
            parent_pg_id: Parent process group ID
            group_name: Name for the new process group
            
        Returns:
            ProcessGroupEntity: Created process group
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
                new_pg_name=group_name,
                location=(400.0, 400.0)
            )
            logger.info(f"Created process group: {group_name}")
            return pg
            
        except Exception as e:
            logger.error(f"Failed to create process group: {str(e)}")
            raise
    
    def create_list_file_processor(self, pg_id: str) -> Any:
        """
        Create ListFile processor to monitor source directory
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ListFile'),
                location=(200.0, 200.0),
                name='List Line Item Files'
            )
            
            # Configure processor
            config = ProcessorConfigDTO()
            config.properties = {
                'Input Directory': self.source_config['line_items']['directory'],
                'File Filter': self.source_config['line_items']['file_pattern'],
                'Recurse Subdirectories': 'false',
                'Minimum File Age': '0 sec',
                'Maximum File Age': self.source_config['line_items'].get('max_file_age', '30 days')
            }
            config.scheduling_period = self.source_config['line_items'].get('polling_interval', '30 sec')
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured ListFile processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create ListFile processor: {str(e)}")
            raise
    
    def create_fetch_file_processor(self, pg_id: str) -> Any:
        """
        Create FetchFile processor to read file content
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.FetchFile'),
                location=(200.0, 400.0),
                name='Fetch Line Item File'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'File to Fetch': '${absolute.path}/${filename}',
                'Completion Strategy': 'Move File',
                'Move Destination Directory': self.source_config['line_items'].get('archive_directory', '/archive'),
                'Move Conflict Strategy': 'Rename'
            }
            config.auto_terminated_relationships = ['not.found', 'permission.denied']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured FetchFile processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create FetchFile processor: {str(e)}")
            raise
    
    def create_split_record_processor(self, pg_id: str) -> Any:
        """
        Create SplitRecord processor to split line items
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.SplitRecord'),
                location=(200.0, 600.0),
                name='Split Line Items'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Records Per Split': self.source_config['line_items'].get('records_per_split', '1000')
            }
            config.auto_terminated_relationships = ['failure']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured SplitRecord processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create SplitRecord processor: {str(e)}")
            raise
    
    def create_validate_record_processor(self, pg_id: str) -> Any:
        """
        Create ValidateRecord processor to validate line item schema
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(200.0, 800.0),
                name='Validate Line Items'
            )
            
            validation_rules = self.source_config['line_items'].get('validation_rules', {})
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Schema Access Strategy': 'Use String Property',
                'Allow Extra Fields': 'true',
                'Max Validation Details': '100'
            }
            
            # Add validation rules
            for field, rule in validation_rules.items():
                config.properties[f'validate.{field}'] = rule
            
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured ValidateRecord processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create ValidateRecord processor: {str(e)}")
            raise
    
    def create_route_on_attribute_processor(self, pg_id: str) -> Any:
        """
        Create RouteOnAttribute processor to route valid/invalid records
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(200.0, 1000.0),
                name='Route Valid Line Items'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Routing Strategy': 'Route to Property name',
                'valid': "${record.error.message:isEmpty()}",
                'invalid': "${record.error.message:isEmpty():not()}"
            }
            config.auto_terminated_relationships = ['unmatched']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured RouteOnAttribute processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create RouteOnAttribute processor: {str(e)}")
            raise
    
    def create_extraction_flow(self, parent_pg_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create complete extraction flow for line items
        
        Args:
            parent_pg_id: Parent process group ID (uses root if None)
            
        Returns:
            Dict containing created processors and connections
        """
        try:
            if parent_pg_id is None:
                parent_pg_id = nipyapi.canvas.get_root_pg_id()
            
            # Create process group
            pg = self.create_process_group(parent_pg_id, 'Line_Item_Extraction')
            pg_id = pg.id
            
            # Create processors
            list_file = self.create_list_file_processor(pg_id)
            fetch_file = self.create_fetch_file_processor(pg_id)
            split_record = self.create_split_record_processor(pg_id)
            validate_record = self.create_validate_record_processor(pg_id)
            route_attribute = self.create_route_on_attribute_processor(pg_id)
            
            # Create connections
            connections = []
            
            # ListFile -> FetchFile
            conn1 = nipyapi.canvas.create_connection(
                source=list_file,
                target=fetch_file,
                relationships=['success']
            )
            connections.append(conn1)
            
            # FetchFile -> SplitRecord
            conn2 = nipyapi.canvas.create_connection(
                source=fetch_file,
                target=split_record,
                relationships=['success']
            )
            connections.append(conn2)
            
            # SplitRecord -> ValidateRecord
            conn3 = nipyapi.canvas.create_connection(
                source=split_record,
                target=validate_record,
                relationships=['splits']
            )
            connections.append(conn3)
            
            # ValidateRecord -> RouteOnAttribute
            conn4 = nipyapi.canvas.create_connection(
                source=validate_record,
                target=route_attribute,
                relationships=['valid', 'invalid']
            )
            connections.append(conn4)
            
            logger.info("Successfully created extraction flow")
            
            return {
                'process_group': pg,
                'processors': {
                    'list_file': list_file,
                    'fetch_file': fetch_file,
                    'split_record': split_record,
                    'validate_record': validate_record,
                    'route_attribute': route_attribute
                },
                'connections': connections
            }
            
        except Exception as e:
            logger.error(f"Failed to create extraction flow: {str(e)}")
            raise
    
    def start_extraction_flow(self, pg_id: str) -> bool:
        """
        Start all processors in the extraction flow
        
        Args:
            pg_id: Process group ID
            
        Returns:
            bool: True if successful
        """
        try:
            pg = nipyapi.canvas.get_process_group(pg_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, True)
            logger.info(f"Started extraction flow in process group: {pg_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start extraction flow: {str(e)}")
            raise
    
    def stop_extraction_flow(self, pg_id: str) -> bool:
        """
        Stop all processors in the extraction flow
        
        Args:
            pg_id: Process group ID
            
        Returns:
            bool: True if successful
        """
        try:
            pg = nipyapi.canvas.get_process_group(pg_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, False)
            logger.info(f"Stopped extraction flow in process group: {pg_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop extraction flow: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        extractor = LineItemExtractor()
        extractor.connect_to_nifi()
        
        # Create extraction flow
        flow = extractor.create_extraction_flow()
        
        logger.info("Line item extraction flow created successfully")
        logger.info(f"Process Group ID: {flow['process_group'].id}")
        
    except Exception as e:
        logger.error(f"Failed to create extraction flow: {str(e)}")
        raise


if __name__ == "__main__":
    main()