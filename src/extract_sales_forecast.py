"""
Extract module for Sales Forecast Processing
Extracts sales forecast data from source systems
"""
import logging
from typing import Dict, Any, List
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class SalesForecastExtractor:
    """Handles extraction of sales forecast data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_config = config['sources']['sales_forecast']
        
    def create_extraction_flow(self, canvas_id: str) -> str:
        """
        Create NiFi flow for extracting sales forecasts
        
        Args:
            canvas_id: Parent process group ID
            
        Returns:
            Process group ID
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name='Sales_Forecast_Extract',
                location=(100, 500)
            )
            
            # GetFile processor
            get_file = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(200, 200),
                name='Get_Sales_Forecasts',
                config=ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.source_config['input_directory'],
                        'File Filter': self.source_config['file_pattern'],
                        'Keep Source File': 'false',
                        'Recurse Subdirectories': 'false',
                        'Polling Interval': '1 hour',
                        'Batch Size': '1'
                    },
                    auto_terminated_relationships=['not.found']
                )
            )
            
            # ValidateRecord processor
            validate_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(200, 400),
                name='Validate_Forecasts',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'CSVReader',
                        'Record Writer': 'CSVRecordSetWriter',
                        'Schema Access Strategy': 'Use String Fields From Header',
                        'Allow Extra Fields': 'true'
                    },
                    auto_terminated_relationships=['invalid']
                )
            )
            
            # RouteOnAttribute for forecast validation
            route_on_attr = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(200, 600),
                name='Route_Valid_Forecasts',
                config=ProcessorConfigDTO(
                    properties={
                        'Routing Strategy': 'Route to Property name',
                        'valid_forecast': "${forecast_id:isEmpty():not()}",
                        'valid_period': "${forecast_period:isEmpty():not()}",
                        'valid_amount': "${forecast_amount:ge(0)}",
                        'future_date': "${forecast_date:toDate('yyyy-MM-dd'):ge(${now():format('yyyy-MM-dd')})}"
                    },
                    auto_terminated_relationships=['unmatched']
                )
            )
            
            # Connect processors
            nipyapi.canvas.create_connection(
                source=get_file,
                target=validate_record,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                source=validate_record,
                target=route_on_attr,
                relationships=['valid']
            )
            
            logger.info(f"Created sales forecast extraction flow in process group: {pg.id}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create forecast extraction flow: {str(e)}")
            raise