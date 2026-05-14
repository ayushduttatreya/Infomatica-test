"""
Transform module for Line Item Aggregation Logic
Aggregates line items to order level with grouping and summation
"""

import logging
from typing import Dict, List, Any, Optional
from decimal import Decimal
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LineItemAggregator:
    """Aggregates line items to order level"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the aggregator with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.nifi_config = self.config['nifi']
        self.aggregation_config = self.config['aggregation']
        
    def create_process_group(self, parent_pg_id: str, group_name: str) -> Any:
        """
        Create a process group for line item aggregation
        
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
                location=(800.0, 400.0)
            )
            logger.info(f"Created process group: {group_name}")
            return pg
            
        except Exception as e:
            logger.error(f"Failed to create process group: {str(e)}")
            raise
    
    def create_partition_record_processor(self, pg_id: str) -> Any:
        """
        Create PartitionRecord processor to group by order_id
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PartitionRecord'),
                location=(200.0, 200.0),
                name='Partition By Order ID'
            )
            
            partition_fields = self.aggregation_config.get('group_by_fields', ['order_id'])
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Partition by': ', '.join(partition_fields)
            }
            config.auto_terminated_relationships = ['failure']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured PartitionRecord processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create PartitionRecord processor: {str(e)}")
            raise
    
    def create_query_record_processor(self, pg_id: str) -> Any:
        """
        Create QueryRecord processor to perform aggregation
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.QueryRecord'),
                location=(200.0, 400.0),
                name='Aggregate Line Items'
            )
            
            # Build aggregation SQL
            agg_fields = self.aggregation_config.get('aggregation_fields', {})
            group_by = self.aggregation_config.get('group_by_fields', ['order_id'])
            
            select_clauses = []
            for field in group_by:
                select_clauses.append(field)
            
            for field, agg_type in agg_fields.items():
                if agg_type == 'SUM':
                    select_clauses.append(f"SUM({field}) as total_{field}")
                elif agg_type == 'COUNT':
                    select_clauses.append(f"COUNT({field}) as count_{field}")
                elif agg_type == 'AVG':
                    select_clauses.append(f"AVG({field}) as avg_{field}")
                elif agg_type == 'MIN':
                    select_clauses.append(f"MIN({field}) as min_{field}")
                elif agg_type == 'MAX':
                    select_clauses.append(f"MAX({field}) as max_{field}")
            
            # Add line item count
            select_clauses.append("COUNT(*) as line_item_count")
            
            sql_query = f"""
            SELECT 
                {', '.join(select_clauses)}
            FROM FLOWFILE
            GROUP BY {', '.join(group_by)}
            """
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Record Reader': 'JsonTreeReader',
                'Record Writer': 'JsonRecordSetWriter',
                'Include Zero Record FlowFiles': 'false',
                'aggregated': sql_query.strip()
            }
            config.auto_terminated_relationships = ['failure']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured QueryRecord processor")
            logger.info(f"Aggregation SQL: {sql_query.strip()}")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create QueryRecord processor: {str(e)}")
            raise
    
    def create_update_attribute_processor(self, pg_id: str) -> Any:
        """
        Create UpdateAttribute processor to add metadata
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
                location=(200.0, 600.0),
                name='Add Aggregation Metadata'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'aggregation.timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                'aggregation.type': 'order_level',
                'aggregation.version': self.aggregation_config.get('version', '1.0'),
                'record.type': 'aggregated_order'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured UpdateAttribute processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create UpdateAttribute processor: {str(e)}")
            raise
    
    def create_evaluate_json_path_processor(self, pg_id: str) -> Any:
        """
        Create EvaluateJsonPath processor to extract aggregated values
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.EvaluateJsonPath'),
                location=(200.0, 800.0),
                name='Extract Aggregated Values'
            )
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Destination': 'flowfile-attribute',
                'Return Type': 'json',
                'order_id': '$.order_id',
                'total_amount': '$.total_amount',
                'total_quantity': '$.total_quantity',
                'line_item_count': '$.line_item_count'
            }
            config.auto_terminated_relationships = ['failure', 'unmatched']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured EvaluateJsonPath processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create EvaluateJsonPath processor: {str(e)}")
            raise
    
    def create_route_on_content_processor(self, pg_id: str) -> Any:
        """
        Create RouteOnContent processor to validate aggregation results
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnContent'),
                location=(200.0, 1000.0),
                name='Validate Aggregation'
            )
            
            min_amount = self.aggregation_config.get('validation', {}).get('min_total_amount', 0)
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Match Requirement': 'content must contain match',
                'Character Set': 'UTF-8',
                'Content Buffer Size': '1 MB',
                'valid_aggregation': f'"total_amount"\\s*:\\s*[0-9]+\\.?[0-9]*'
            }
            config.auto_terminated_relationships = []
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured RouteOnContent processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create RouteOnContent processor: {str(e)}")
            raise
    
    def create_jolt_transform_processor(self, pg_id: str) -> Any:
        """
        Create JoltTransformJSON processor to reshape aggregated data
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Processor entity
        """
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoltTransformJSON'),
                location=(200.0, 1200.0),
                name='Reshape Aggregated Data'
            )
            
            jolt_spec = {
                "operation": "shift",
                "spec": {
                    "order_id": "order.id",
                    "customer_id": "order.customer_id",
                    "total_*": "order.totals.&(0,1)",
                    "count_*": "order.counts.&(0,1)",
                    "avg_*": "order.averages.&(0,1)",
                    "min_*": "order.minimums.&(0,1)",
                    "max_*": "order.maximums.&(0,1)",
                    "line_item_count": "order.line_item_count",
                    "*": "order.&"
                }
            }
            
            config = ProcessorConfigDTO()
            config.properties = {
                'Jolt Transformation DSL': 'jolt-transform-shift',
                'Jolt Specification': str(jolt_spec)
            }
            config.auto_terminated_relationships = ['failure']
            
            nipyapi.canvas.update_processor(processor, config)
            logger.info("Created and configured JoltTransformJSON processor")
            
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create JoltTransformJSON processor: {str(e)}")
            raise
    
    def create_aggregation_flow(self, parent_pg_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create complete aggregation flow for line items
        
        Args:
            parent_pg_id: Parent process group ID (uses root if None)
            
        Returns:
            Dict containing created processors and connections
        """
        try:
            if parent_pg_id is None:
                parent_pg_id = nipyapi.canvas.get_root_pg_id()
            
            # Create process group
            pg = self.create_process_group(parent_pg_id, 'Line_Item_Aggregation')
            pg_id = pg.id
            
            # Create processors
            partition_record = self.create_partition_record_processor(pg_id)
            query_record = self.create_query_record_processor(pg_id)
            update_attribute = self.create_update_attribute_processor(pg_id)
            evaluate_json = self.create_evaluate_json_path_processor(pg_id)
            route_content = self.create_route_on_content_processor(pg_id)
            jolt_transform = self.create_jolt_transform_processor(pg_id)
            
            # Create connections
            connections = []
            
            # PartitionRecord -> QueryRecord
            conn1 = nipyapi.canvas.create_connection(
                source=partition_record,
                target=query_record,
                relationships=['success']
            )
            connections.append(conn1)
            
            # QueryRecord -> UpdateAttribute
            conn2 = nipyapi.canvas.create_connection(
                source=query_record,
                target=update_attribute,
                relationships=['aggregated']
            )
            connections.append(conn2)
            
            # UpdateAttribute -> EvaluateJsonPath
            conn3 = nipyapi.canvas.create_connection(
                source=update_attribute,
                target=evaluate_json,
                relationships=['success']
            )
            connections.append(conn3)
            
            # EvaluateJsonPath -> RouteOnContent
            conn4 = nipyapi.canvas.create_connection(
                source=evaluate_json,
                target=route_content,
                relationships=['matched']
            )
            connections.append(conn4)
            
            # RouteOnContent -> JoltTransform
            conn5 = nipyapi.canvas.create_connection(
                source=route_content,
                target=jolt_transform,
                relationships=['valid_aggregation']
            )
            connections.append(conn5)
            
            logger.info("Successfully created aggregation flow")
            
            return {
                'process_group': pg,
                'processors': {
                    'partition_record': partition_record,
                    'query_record': query_record,
                    'update_attribute': update_attribute,
                    'evaluate_json': evaluate_json,
                    'route_content': route_content,
                    'jolt_transform': jolt_transform
                },
                'connections': connections
            }
            
        except Exception as e:
            logger.error(f"Failed to create aggregation flow: {str(e)}")
            raise
    
    def start_aggregation_flow(self, pg_id: str) -> bool:
        """
        Start all processors in the aggregation flow
        
        Args:
            pg_id: Process group ID
            
        Returns:
            bool: True if successful
        """
        try:
            pg = nipyapi.canvas.get_process_group(pg_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, True)
            logger.info(f"Started aggregation flow in process group: {pg_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start aggregation flow: {str(e)}")
            raise
    
    def stop_aggregation_flow(self, pg_id: str) -> bool:
        """
        Stop all processors in the aggregation flow
        
        Args:
            pg_id: Process group ID
            
        Returns:
            bool: True if successful
        """
        try:
            pg = nipyapi.canvas.get_process_group(pg_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, False)
            logger.info(f"Stopped aggregation flow in process group: {pg_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop aggregation flow: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        aggregator = LineItemAggregator()
        
        # Create aggregation flow
        flow = aggregator.create_aggregation_flow()
        
        logger.info("Line item aggregation flow created successfully")
        logger.info(f"Process Group ID: {flow['process_group'].id}")
        
    except Exception as e:
        logger.error(f"Failed to create aggregation flow: {str(e)}")
        raise


if __name__ == "__main__":
    main()