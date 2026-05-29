"""
Transform module for Sales Returns Processing
Applies business rules and transformations to sales returns
"""
import logging
from typing import Dict, Any, List
from decimal import Decimal
from datetime import datetime, timedelta
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class SalesReturnsTransformer:
    """Handles transformation of sales returns data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.transform_config = config['transformations']['sales_returns']
        
    def create_transformation_flow(self, canvas_id: str) -> str:
        """
        Create NiFi flow for transforming sales returns
        
        Args:
            canvas_id: Parent process group ID
            
        Returns:
            Process group ID
        """
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(canvas_id, 'id'),
                new_pg_name='Sales_Returns_Transform',
                location=(400, 300)
            )
            
            # UpdateRecord for return calculations
            update_record = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(200, 200),
                name='Calculate_Return_Amounts',
                config=ProcessorConfigDTO(
                    properties={
                        'Record Reader': 'CSVReader',
                        'Record Writer': 'CSVRecordSetWriter',
                        '/restocking_fee': "multiply(${return_amount}, 0.15)",
                        '/refund_amount': "subtract(${return_amount}, ${restocking_fee})",
                        '/return_days': "dateDiff(${return_date}, ${order_date})"
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            # RouteOnAttribute for return policy validation
            route_policy = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
                location=(200, 400),
                name='Validate_Return_Policy',
                config=ProcessorConfigDTO(
                    properties={
                        'Routing Strategy': 'Route to Property name',
                        'within_30_days': "${return_days:lt(30)}",
                        'within_60_days': "${return_days:ge(30):and(${return_days:lt(60)})}",
                        'within_90_days': "${return_days:ge(60):and(${return_days:lt(90)})}",
                        'expired': "${return_days:ge(90)}"
                    }
                )
            )
            
            # UpdateAttribute for policy-based adjustments
            update_attr_30 = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
                location=(400, 300),
                name='Apply_30Day_Policy',
                config=ProcessorConfigDTO(
                    properties={
                        'restocking_fee_percent': '0',
                        'return_policy': 'FULL_REFUND'
                    }
                )
            )
            
            update_attr_60 = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
                location=(400, 450),
                name='Apply_60Day_Policy',
                config=ProcessorConfigDTO(
                    properties={
                        'restocking_fee_percent': '15',
                        'return_policy': 'PARTIAL_REFUND'
                    }
                )
            )
            
            update_attr_90 = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
                location=(400, 600),
                name='Apply_90Day_Policy',
                config=ProcessorConfigDTO(
                    properties={
                        'restocking_fee_percent': '25',
                        'return_policy': 'STORE_CREDIT_ONLY'
                    }
                )
            )
            
            # MergeContent to combine policy branches
            merge_content = nipyapi.canvas.create_processor(
                parent_pg=pg,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.MergeContent'),
                location=(600, 450),
                name='Merge_Policy_Results',
                config=ProcessorConfigDTO(
                    properties={
                        'Merge Strategy': 'Defragment',
                        'Merge Format': 'Binary Concatenation',
                        'Attribute Strategy': 'Keep Common Attributes'
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            # Connect processors
            nipyapi.canvas.create_connection(
                source=update_record,
                target=route_policy,
                relationships=['success']
            )
            
            nipyapi.canvas.create_connection(
                source=route_policy,
                target=update_attr_30,
                relationships=['within_30_days']
            )
            
            nipyapi.canvas.create_connection(
                source=route_policy,
                target=update_attr_60,
                relationships=['within_60_days']
            )
            
            nipyapi.canvas.create_connection(
                source=route_policy,
                target=update_attr_90,
                relationships=['within_90_days']
            )
            
            for processor in [update_attr_30, update_attr_60, update_attr_90]:
                nipyapi.canvas.create_connection(
                    source=processor,
                    target=merge_content,
                    relationships=['success']
                )
            
            logger.info(f"Created sales returns transformation flow in process group: {pg.id}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create returns transformation flow: {str(e)}")
            raise
    
    def transform_return(self, return_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply business rules and transformations to a return
        
        Args:
            return_data: Raw return data
            
        Returns:
            Transformed return data
        """
        try:
            transformed = return_data.copy()
            
            # Calculate return days
            order_date = datetime.fromisoformat(return_data['order_date'])
            return_date = datetime.fromisoformat(return_data['return_date'])
            return_days = (return_date - order_date).days
            transformed['return_days'] = return_days
            
            # Apply return policy
            return_amount = Decimal(str(return_data.get('return_amount', 0)))
            
            if return_days <= 30:
                restocking_fee_percent = Decimal('0')
                policy = 'FULL_REFUND'
            elif return_days <= 60:
                restocking_fee_percent = Decimal('0.15')
                policy = 'PARTIAL_REFUND'
            elif return_days <= 90:
                restocking_fee_percent = Decimal('0.25')
                policy = 'STORE_CREDIT_ONLY'
            else:
                restocking_fee_percent = Decimal('1.0')
                policy = 'NO_REFUND'
            
            restocking_fee = return_amount * restocking_fee_percent
            refund_amount = return_amount - restocking_fee
            
            transformed['restocking_fee'] = float(restocking_fee)
            transformed['refund_amount'] = float(refund_amount)
            transformed['return_policy'] = policy
            
            # Validate return reason
            valid_reasons = self.transform_config.get('valid_return_reasons', [])
            if return_data.get('return_reason') not in valid_reasons:
                transformed['return_reason_valid'] = False
            else:
                transformed['return_reason_valid'] = True
            
            # Add metadata
            transformed['processed_timestamp'] = datetime.now().isoformat()
            transformed['transformation_version'] = self.transform_config.get('version', '1.0')
            
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform return: {str(e)}")
            raise