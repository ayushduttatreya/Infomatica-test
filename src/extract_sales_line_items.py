"""
Extract module for Sales Line Items Processing
Extracts sales line item data from source systems
"""
import logging
from typing import Dict, Any, List
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class SalesLineItemExtractor:
    """Handles extraction of sales line item data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_config = config['sources']['sales_line_items']
        
    def create_extraction_flow(self, canvas_id: str) -> str:
        """
        Create NiFi flow for extracting sales line items
        
        Args:
            canvas_id: Parent process group ID
            
        Returns:
            Process group ID
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name='Sales_Line_Items_Extract',
                location=(100, 100)
            )
            
            # GetFile processor for source data
            get_file = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(200, 200),
                name='Get_Sales_Line_Items',
                config=ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.source_config['input_directory'],
                        'File Filter': self.source_config['file_pattern'],
                        'Keep Source File': 'false',
                        'Recurse Subdirectories': 'false',
                        'Polling Interval': '10 sec',
                        'Batch Size': '10'
                    },
                    auto_terminated_relationships=['not.found']
                )
            )
            
            # ValidateRecord processor
            validate_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
                location=(200, 400),
                name='Validate_Line_Items',
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
            
            # RouteOnAttribute for business rule validation
            route_on_attr = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(200, 600),
                name='Route_Valid_Line_Items',
                config=ProcessorConfigDTO(
                    properties={
                        'Routing Strategy': 'Route to Property name',
                        'valid_quantity': "${line_item_quantity:gt(0)}",
                        'valid_price': "${line_item_price:ge(0)}",
                        'valid_order': "${order_id:isEmpty():not()}"
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
            
            logger.info(f"Created sales line items extraction flow in process group: {pg.id}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create extraction flow: {str(e)}")
            raise
    
    def extract_line_items(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract and validate line items from file
        
        Args:
            file_path: Path to source file
            
        Returns:
            List of validated line item records
        """
        line_items = []
        
        try:
            # Implementation would read from actual file
            # This is a placeholder for the extraction logic
            logger.info(f"Extracting line items from: {file_path}")
            
            # Validation rules
            required_fields = ['order_id', 'line_item_id', 'product_id', 
                             'quantity', 'unit_price', 'line_total']
            
            # Return extracted items
            return line_items
            
        except Exception as e:
            logger.error(f"Failed to extract line items: {str(e)}")
            raise