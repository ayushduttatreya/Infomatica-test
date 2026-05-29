"""
Transform module for Sales Line Items Processing
Applies business rules and transformations to sales line items
"""
import logging
from typing import Dict, Any, List
from decimal import Decimal
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class SalesLineItemTransformer:
    """Handles transformation of sales line item data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.transform_config = config['transformations']['sales_line_items']
        
    def create_transformation_flow(self, canvas_id: str) -> str:
        """
        Create NiFi flow for transforming sales line items
        
        Args:
            canvas_id: Parent process group ID
            
        Returns:
            Process group ID
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name='Sales_Line_Items_Transform',
                location=(400, 100)
            )
            
            # UpdateRecord processor for calculations
            update_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(200, 200),
                name='Calculate_Line_Totals',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'CSVReader',
                        'Record Writer': 'CSVRecordSetWriter',
                        '/line_total': "concat(${quantity}, ' * ', ${unit_price})",
                        '/discount_amount': "multiply(${line_total}, divide(${discount_percent}, 100))",
                        '/tax_amount': "multiply(subtract(${line_total}, ${discount_amount}), divide(${tax_rate}, 100))",
                        '/final_amount': "add(subtract(${line_total}, ${discount_amount}), ${tax_amount})"
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            # JoltTransformRecord for structure transformation
            jolt_transform = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoltTransformRecord'),
                location=(200, 400),
                name='Transform_Line_Item_Structure',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'CSVReader',
                        'Record Writer': 'JSONRecordSetWriter',
                        'Jolt Transformation DSL': 'Chain',
                        'Jolt Specification': self._get_line_item_jolt_spec()
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            # LookupRecord for product enrichment
            lookup_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LookupRecord'),
                location=(200, 600),
                name='Enrich_Product_Data',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'JSONReader',
                        'Record Writer': 'JSONRecordSetWriter',
                        'Lookup Service': 'DatabaseRecordLookupService',
                        'Result RecordPath': '/product_details',
                        'Routing Strategy': 'Route to Success',
                        'product_name': '/product_id',
                        'product_category': '/product_id',
                        'product_cost': '/product_id'
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            # Connect processors
            nipyapi.canvas.create_connection(
                source=update_record,
                target=jolt_transform,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                source=jolt_transform,
                target=lookup_record,
                relationships=['success']
            )
            
            logger.info(f"Created sales line items transformation flow in process group: {pg.id}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create transformation flow: {str(e)}")
            raise
    
    def _get_line_item_jolt_spec(self) -> str:
        """Get JOLT specification for line item transformation"""
        return '''[
          {
            "operation": "shift",
            "spec": {
              "order_id": "order_id",
              "line_item_id": "line_item_id",
              "product_id": "product.product_id",
              "quantity": "quantity",
              "unit_price": "pricing.unit_price",
              "line_total": "pricing.line_total",
              "discount_amount": "pricing.discount_amount",
              "tax_amount": "pricing.tax_amount",
              "final_amount": "pricing.final_amount"
            }
          },
          {
            "operation": "default",
            "spec": {
              "metadata": {
                "processed_date": "${now():format('yyyy-MM-dd HH:mm:ss')}",
                "source_system": "sales_system"
              }
            }
          }
        ]'''
    
    def transform_line_item(self, line_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply business rules and transformations to a line item
        
        Args:
            line_item: Raw line item data
            
        Returns:
            Transformed line item
        """
        try:
            transformed = line_item.copy()
            
            # Calculate line total
            quantity = Decimal(str(line_item.get('quantity', 0)))
            unit_price = Decimal(str(line_item.get('unit_price', 0)))
            transformed['line_total'] = float(quantity * unit_price)
            
            # Apply discount
            discount_percent = Decimal(str(line_item.get('discount_percent', 0)))
            discount_amount = transformed['line_total'] * (discount_percent / 100)
            transformed['discount_amount'] = float(discount_amount)
            
            # Calculate tax
            tax_rate = Decimal(str(self.transform_config.get('default_tax_rate', 0)))
            taxable_amount = Decimal(str(transformed['line_total'])) - discount_amount
            tax_amount = taxable_amount * (tax_rate / 100)
            transformed['tax_amount'] = float(tax_amount)
            
            # Calculate final amount
            transformed['final_amount'] = float(taxable_amount + tax_amount)
            
            # Add metadata
            transformed['processed_timestamp'] = datetime.now().isoformat()
            transformed['transformation_version'] = self.transform_config.get('version', '1.0')
            
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform line item: {str(e)}")
            raise