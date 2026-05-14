"""
Sales Forecast Data Extraction Module
Extracts customer and sales forecast data from source systems
"""

import logging
from typing import Dict, Any, List
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesForecastExtractor:
    """Handles extraction of sales forecast data from source systems"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the extractor with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.nifi_config = self.config['nifi']
        self.source_config = self.config['sources']
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
    
    def create_extraction_process_group(self, parent_pg_id: str = None) -> ProcessGroupEntity:
        """
        Create process group for extraction workflow
        
        Args:
            parent_pg_id: Parent process group ID, defaults to root
            
        Returns:
            ProcessGroupEntity: Created process group
        """
        try:
            if parent_pg_id is None:
                parent_pg_id = nipyapi.canvas.get_root_pg_id()
            
            pg_name = self.config['process_groups']['extraction']['name']
            
            # Check if process group already exists
            existing_pg = nipyapi.canvas.get_process_group(pg_name, 'name')
            if existing_pg:
                logger.info(f"Process group '{pg_name}' already exists")
                return existing_pg
            
            # Create new process group
            process_group = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
                name=pg_name,
                location=(100, 100)
            )
            
            logger.info(f"Created extraction process group: {pg_name}")
            return process_group
            
        except Exception as e:
            logger.error(f"Failed to create extraction process group: {str(e)}")
            raise
    
    def create_customer_source_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to read customer source data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            customer_config = self.source_config['customers']
            
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(200, 200),
                name='Extract_Customer_Data'
            )
            
            # Configure processor properties
            config = ProcessorConfigDTO()
            config.properties = {
                'Input Directory': customer_config['input_directory'],
                'File Filter': customer_config['file_pattern'],
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'false',
                'Polling Interval': '10 sec',
                'Batch Size': '10'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created customer source processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create customer source processor: {str(e)}")
            raise
    
    def create_sales_forecast_source_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to read sales forecast source data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            forecast_config = self.source_config['sales_forecast']
            
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(200, 400),
                name='Extract_Sales_Forecast_Data'
            )
            
            # Configure processor properties
            config = ProcessorConfigDTO()
            config.properties = {
                'Input Directory': forecast_config['input_directory'],
                'File Filter': forecast_config['file_pattern'],
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'false',
                'Polling Interval': '10 sec',
                'Batch Size': '10'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created sales forecast source processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create sales forecast source processor: {str(e)}")
            raise
    
    def create_schema_validation_processor(self, process_group: ProcessGroupEntity, 
                                          location: tuple, name: str) -> Any:
        """
        Create processor to validate data schema
        
        Args:
            process_group: Parent process group
            location: Processor location coordinates
            name: Processor name
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=location,
                name=name
            )
            
            # Configure processor properties
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Schema Access Strategy': 'Use String Fields From Header',
                'Allow Extra Fields': 'true'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created schema validation processor: {name}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create schema validation processor: {str(e)}")
            raise
    
    def create_error_handling_funnel(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create funnel for error handling
        
        Args:
            process_group: Parent process group
            
        Returns:
            Funnel entity
        """
        try:
            funnel = nipyapi.canvas.create_funnel(
                parent_pg=process_group,
                location=(800, 300)
            )
            
            logger.info(f"Created error handling funnel: {funnel.id}")
            return funnel
            
        except Exception as e:
            logger.error(f"Failed to create error handling funnel: {str(e)}")
            raise
    
    def build_extraction_pipeline(self) -> Dict[str, Any]:
        """
        Build complete extraction pipeline
        
        Returns:
            Dictionary containing all created components
        """
        try:
            logger.info("Building extraction pipeline...")
            
            # Connect to NiFi
            self.connect_to_nifi()
            
            # Create process group
            extraction_pg = self.create_extraction_process_group()
            
            # Create source processors
            customer_processor = self.create_customer_source_processor(extraction_pg)
            forecast_processor = self.create_sales_forecast_source_processor(extraction_pg)
            
            # Create validation processors
            customer_validator = self.create_schema_validation_processor(
                extraction_pg, 
                (500, 200), 
                'Validate_Customer_Schema'
            )
            forecast_validator = self.create_schema_validation_processor(
                extraction_pg, 
                (500, 400), 
                'Validate_Forecast_Schema'
            )
            
            # Create error handling funnel
            error_funnel = self.create_error_handling_funnel(extraction_pg)
            
            # Create connections
            self._create_connections(
                extraction_pg,
                customer_processor,
                forecast_processor,
                customer_validator,
                forecast_validator,
                error_funnel
            )
            
            components = {
                'process_group': extraction_pg,
                'customer_processor': customer_processor,
                'forecast_processor': forecast_processor,
                'customer_validator': customer_validator,
                'forecast_validator': forecast_validator,
                'error_funnel': error_funnel
            }
            
            logger.info("Extraction pipeline built successfully")
            return components
            
        except Exception as e:
            logger.error(f"Failed to build extraction pipeline: {str(e)}")
            raise
    
    def _create_connections(self, process_group: ProcessGroupEntity, 
                           customer_proc: Any, forecast_proc: Any,
                           customer_val: Any, forecast_val: Any,
                           error_funnel: Any) -> None:
        """
        Create connections between processors
        
        Args:
            process_group: Parent process group
            customer_proc: Customer source processor
            forecast_proc: Forecast source processor
            customer_val: Customer validator
            forecast_val: Forecast validator
            error_funnel: Error handling funnel
        """
        try:
            # Customer source to validator
            nipyapi.canvas.create_connection(
                source=customer_proc,
                target=customer_val,
                relationships=['success']
            )
            
            # Forecast source to validator
            nipyapi.canvas.create_connection(
                source=forecast_proc,
                target=forecast_val,
                relationships=['success']
            )
            
            # Validation failures to error funnel
            nipyapi.canvas.create_connection(
                source=customer_val,
                target=error_funnel,
                relationships=['invalid']
            )
            
            nipyapi.canvas.create_connection(
                source=forecast_val,
                target=error_funnel,
                relationships=['invalid']
            )
            
            logger.info("Created all connections in extraction pipeline")
            
        except Exception as e:
            logger.error(f"Failed to create connections: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        extractor = SalesForecastExtractor()
        components = extractor.build_extraction_pipeline()
        logger.info(f"Extraction pipeline deployed with {len(components)} components")
        
    except Exception as e:
        logger.error(f"Extraction pipeline deployment failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()