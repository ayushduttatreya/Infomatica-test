"""
Extract module for Sales Returns Processing
Extracts sales return data from source systems
"""
import logging
from typing import Dict, Any, List
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class SalesReturnsExtractor:
    """Handles extraction of sales returns data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_config = config['sources']['sales_returns']
        
    def create_extraction_flow(self, canvas_id: str) -> str:
        """
        Create NiFi flow for extracting sales returns
        
        Args:
            canvas_id: Parent process group ID
            
        Returns:
            Process group ID
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name='Sales_Returns_Extract',
                location=(100, 300)
            )
            
            # GetFile processor
            get_file = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(200, 200),
                name='Get_Sales_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.source_config['input_directory'],
                        'File Filter': self.source_config['file_pattern'],
                        'Keep Source File': 'false',
                        'Recurse Subdirectories': 'false',
                        'Polling Interval': '15 sec',
                        'Batch Size': '5'
                    },
                    auto_terminated_relationships=['not.found']
                )
            )
            
            # ValidateRecord processor
            validate_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(200, 400),
                name='Validate_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'CSVReader',
                        'Record Writer': 'CSVRecordSetWriter',
                        'Schema Access Strategy': 'Use String Fields From Header',
                        'Allow Extra Fields': 'false'
                    },
                    auto_terminated_relationships=['invalid']
                )
            )
            
            # RouteOnAttribute for return reason validation
            route_on_attr = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(200, 600),
                name='Route_Valid_Returns',
                config=ProcessorConfigDTO(
                    properties={
                        'Routing Strategy': 'Route to Property name',
                        'valid_return': "${return_id:isEmpty():not()}",
                        'valid_reason': "${return_reason:isEmpty():not()}",
                        'valid_amount': "${return_amount:ge(0)}",
                        'within_window': "${return_date:toDate('yyyy-MM-dd'):format('yyyy-MM-dd'):ge(${order_date:toDate('yyyy-MM-dd'):plus(30, 'days'):format('yyyy-MM-dd')}):not()}"
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
            
            logger.info(f"Created sales returns extraction flow in process group: {pg.id}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create returns extraction flow: {str(e)}")
            raise