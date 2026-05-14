"""
Sales Forecast Data Transformation Module
Transforms and enriches sales forecast data
"""

import logging
from typing import Dict, Any, List
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesForecastTransformer:
    """Handles transformation of sales forecast data"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the transformer with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.nifi_config = self.config['nifi']
        self.transform_config = self.config['transformations']
        
    def connect_to_nifi(self) -> bool:
        """
        Establish connection to NiFi instance
        
        Returns:
            bool: True if connection successful
        """
        try:
            nipyapi.config.nifi_config.host = self.nifi_config['host']
            nipyapi.config.nifi_config.port = self.nifi_config['port']
            
            nipyapi.canvas.get_root_pg_id()
            logger.info(f"Successfully connected to NiFi at {self.nifi_config['host']}:{self.nifi_config['port']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to NiFi: {str(e)}")
            raise
    
    def create_transformation_process_group(self, parent_pg_id: str = None) -> ProcessGroupEntity:
        """
        Create process group for transformation workflow
        
        Args:
            parent_pg_id: Parent process group ID, defaults to root
            
        Returns:
            ProcessGroupEntity: Created process group
        """
        try:
            if parent_pg_id is None:
                parent_pg_id = nipyapi.canvas.get_root_pg_id()
            
            pg_name = self.config['process_groups']['transformation']['name']
            
            existing_pg = nipyapi.canvas.get_process_group(pg_name, 'name')
            if existing_pg:
                logger.info(f"Process group '{pg_name}' already exists")
                return existing_pg
            
            process_group = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
                name=pg_name,
                location=(100, 300)
            )
            
            logger.info(f"Created transformation process group: {pg_name}")
            return process_group
            
        except Exception as e:
            logger.error(f"Failed to create transformation process group: {str(e)}")
            raise
    
    def create_record_enrichment_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to enrich records with additional data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(200, 200),
                name='Enrich_Customer_Records'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created record enrichment processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create record enrichment processor: {str(e)}")
            raise
    
    def create_data_cleansing_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to cleanse and standardize data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(500, 200),
                name='Cleanse_Data_Fields'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Replacement Value Strategy': 'Record Path Value',
                '/email': "trim(${field.value})",
                '/phone': "replace(${field.value}, '[^0-9]', '')",
                '/zip_code': "padLeft(${field.value}, 5, '0')"
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created data cleansing processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create data cleansing processor: {str(e)}")
            raise
    
    def create_aggregation_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to aggregate sales forecast data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.QueryRecord'),
                location=(200, 400),
                name='Aggregate_Sales_Forecast'
            )
            
            aggregation_query = self.transform_config['aggregation']['query']
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'aggregated': aggregation_query,
                'Include Zero Record FlowFiles': 'false'
            }
            config.auto_terminated_relationships = ['original']
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created aggregation processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create aggregation processor: {str(e)}")
            raise
    
    def create_join_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to join customer and forecast data
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoinEnrichment'),
                location=(800, 300),
                name='Join_Customer_Forecast'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'CSVRecordSetWriter',
                'Join Strategy': 'Inner Join',
                'Enrichment Record Path': '/customer_id',
                'Original Record Path': '/customer_id'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created join processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create join processor: {str(e)}")
            raise
    
    def create_format_conversion_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to convert data format
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertRecord'),
                location=(1100, 300),
                name='Convert_To_JSON'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'CSVReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Include Zero Record FlowFiles': 'false'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created format conversion processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create format conversion processor: {str(e)}")
            raise
    
    def create_quality_check_processor(self, process_group: ProcessGroupEntity) -> Any:
        """
        Create processor to perform data quality checks
        
        Args:
            process_group: Parent process group
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(1100, 500),
                name='Quality_Check_Router'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Routing Strategy': 'Route to Property name',
                'valid_record': "${record.count:gt(0):and(${email:isEmpty():not()})}",
                'invalid_record': "${record.count:equals(0):or(${email:isEmpty()})}"
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            
            logger.info(f"Created quality check processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create quality check processor: {str(e)}")
            raise
    
    def build_transformation_pipeline(self) -> Dict[str, Any]:
        """
        Build complete transformation pipeline
        
        Returns:
            Dictionary containing all created components
        """
        try:
            logger.info("Building transformation pipeline...")
            
            self.connect_to_nifi()
            
            transform_pg = self.create_transformation_process_group()
            
            enrichment_proc = self.create_record_enrichment_processor(transform_pg)
            cleansing_proc = self.create_data_cleansing_processor(transform_pg)
            aggregation_proc = self.create_aggregation_processor(transform_pg)
            join_proc = self.create_join_processor(transform_pg)
            conversion_proc = self.create_format_conversion_processor(transform_pg)
            quality_proc = self.create_quality_check_processor(transform_pg)
            
            self._create_connections(
                transform_pg,
                enrichment_proc,
                cleansing_proc,
                aggregation_proc,
                join_proc,
                conversion_proc,
                quality_proc
            )
            
            components = {
                'process_group': transform_pg,
                'enrichment_processor': enrichment_proc,
                'cleansing_processor': cleansing_proc,
                'aggregation_processor': aggregation_proc,
                'join_processor': join_proc,
                'conversion_processor': conversion_proc,
                'quality_processor': quality_proc
            }
            
            logger.info("Transformation pipeline built successfully")
            return components
            
        except Exception as e:
            logger.error(f"Failed to build transformation pipeline: {str(e)}")
            raise
    
    def _create_connections(self, process_group: ProcessGroupEntity,
                           enrichment: Any, cleansing: Any, aggregation: Any,
                           join: Any, conversion: Any, quality: Any) -> None:
        """
        Create connections between transformation processors
        
        Args:
            process_group: Parent process group
            enrichment: Enrichment processor
            cleansing: Cleansing processor
            aggregation: Aggregation processor
            join: Join processor
            conversion: Conversion processor
            quality: Quality check processor
        """
        try:
            nipyapi.canvas.create_connection(
                source=enrichment,
                target=cleansing,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                source=cleansing,
                target=join,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                source=aggregation,
                target=join,
                relationships=['aggregated']
            )
            
            nipyapi.canvas.create_connection(
                source=join,
                target=conversion,
                relationships=['joined']
            )
            
            nipyapi.canvas.create_connection(
                source=conversion,
                target=quality,
                relationships=['success']
            )
            
            logger.info("Created all connections in transformation pipeline")
            
        except Exception as e:
            logger.error(f"Failed to create connections: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        transformer = SalesForecastTransformer()
        components = transformer.build_transformation_pipeline()
        logger.info(f"Transformation pipeline deployed with {len(components)} components")
        
    except Exception as e:
        logger.error(f"Transformation pipeline deployment failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()