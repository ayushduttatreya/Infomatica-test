"""
NiFi Processor Integration for Forecast ID Validation

This module provides NiFi-specific integration using nipyapi to deploy
and manage the validation transform as a NiFi processor group.
"""

import json
import logging
from typing import Dict, List, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessorDTO

from src.transform import ForecastIDValidator, split_valid_invalid_records

logger = logging.getLogger(__name__)


class NiFiForecastValidationProcessor:
    """
    NiFi processor wrapper for forecast ID validation.
    
    This class manages the deployment and execution of validation logic
    within a NiFi processor group.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize NiFi processor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.nifi_config = config.get('nifi', {})
        self.process_group_name = self.nifi_config.get(
            'process_group_name',
            'Forecast_ID_Validation'
        )
        self.validator = ForecastIDValidator(config.get('validation', {}))
        
        # NiFi connection details
        self.nifi_url = self.nifi_config.get('url', 'http://localhost:8080/nifi-api')
        self.process_group_id = None
        
        logger.info(f"Initialized NiFi processor: {self.process_group_name}")
    
    def connect_nifi(self):
        """Establish connection to NiFi instance"""
        try:
            nipyapi.config.nifi_config.host = self.nifi_url
            
            # Test connection
            canvas = nipyapi.canvas.get_root_pg_id()
            logger.info(f"Connected to NiFi at {self.nifi_url}, root canvas: {canvas}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to NiFi: {e}")
            raise
    
    def create_process_group(self, parent_id: Optional[str] = None) -> str:
        """
        Create NiFi process group for validation flow.
        
        Args:
            parent_id: Parent process group ID (uses root if None)
            
        Returns:
            Process group ID
        """
        try:
            if parent_id is None:
                parent_id = nipyapi.canvas.get_root_pg_id()
            
            # Check if process group already exists
            existing_pg = nipyapi.canvas.get_process_group(
                self.process_group_name,
                'name'
            )
            
            if existing_pg:
                logger.info(f"Process group '{self.process_group_name}' already exists")
                self.process_group_id = existing_pg.id
                return existing_pg.id
            
            # Create new process group
            pg = nipyapi.canvas.create_process_group(
                parent_id,
                self.process_group_name,
                location=(400.0, 400.0)
            )
            
            self.process_group_id = pg.id
            logger.info(f"Created process group: {self.process_group_name} ({pg.id})")
            
            return pg.id
            
        except Exception as e:
            logger.error(f"Failed to create process group: {e}")
            raise
    
    def create_validation_flow(self):
        """
        Create complete validation flow in NiFi process group.
        
        Flow structure:
        1. Input Port (receive_forecast_data)
        2. ExecuteScript (validate_forecast_ids) - runs validation logic
        3. RouteOnAttribute (route_by_validation) - splits valid/invalid
        4. Output Port (valid_records)
        5. Output Port (invalid_records)
        """
        try:
            if not self.process_group_id:
                raise ValueError("Process group not created. Call create_process_group first.")
            
            pg_id = self.process_group_id
            
            # Create input port
            input_port = nipyapi.canvas.create_port(
                pg_id,
                'INPUT_PORT',
                'receive_forecast_data',
                'RUNNING',
                position=(200.0, 100.0)
            )
            logger.info(f"Created input port: {input_port.id}")
            
            # Create ExecuteScript processor for validation
            validation_processor = self._create_validation_processor(pg_id)
            logger.info(f"Created validation processor: {validation_processor.id}")
            
            # Create RouteOnAttribute processor
            route_processor = self._create_route_processor(pg_id)
            logger.info(f"Created route processor: {route_processor.id}")
            
            # Create output ports
            valid_output = nipyapi.canvas.create_port(
                pg_id,
                'OUTPUT_PORT',
                'valid_records',
                'RUNNING',
                position=(200.0, 700.0)
            )
            
            invalid_output = nipyapi.canvas.create_port(
                pg_id,
                'OUTPUT_PORT',
                'invalid_records',
                'RUNNING',
                position=(500.0, 700.0)
            )
            
            logger.info(f"Created output ports: valid={valid_output.id}, invalid={invalid_output.id}")
            
            # Create connections
            self._create_connections(
                pg_id,
                input_port,
                validation_processor,
                route_processor,
                valid_output,
                invalid_output
            )
            
            logger.info("Validation flow created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create validation flow: {e}")
            raise
    
    def _create_validation_processor(self, pg_id: str) -> ProcessorDTO:
        """Create ExecuteScript processor for validation logic"""
        
        # Python script that will run in NiFi
        validation_script = '''
import json
import sys
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

# Import validation logic (would be packaged with NiFi)
# For production, this would reference the actual module
# from src.transform import ForecastIDValidator

class ValidationCallback(StreamCallback):
    def __init__(self, validator_config):
        self.validator_config = validator_config
    
    def process(self, inputStream, outputStream):
        # Read input
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        records = json.loads(text)
        
        # Validate (simplified for NiFi script)
        results = []
        for record in records:
            forecast_id = record.get('forecast_id', '')
            is_valid = self.validate_format(forecast_id)
            
            record['validation_result'] = {
                'is_valid': is_valid,
                'forecast_id': forecast_id
            }
            results.append(record)
        
        # Write output
        outputStream.write(json.dumps(results).encode('utf-8'))
    
    def validate_format(self, forecast_id):
        import re
        pattern = r'^[A-Z]{3}-[0-9]{6}-[A-Z]{2}$'
        return bool(re.match(pattern, forecast_id))

# Get flowfile
flowFile = session.get()
if flowFile is not None:
    validator_config = {}  # Load from attributes
    callback = ValidationCallback(validator_config)
    flowFile = session.write(flowFile, callback)
    session.transfer(flowFile, REL_SUCCESS)
'''
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('ExecuteScript'),
            location=(200.0, 300.0),
            name='validate_forecast_ids',
            config=ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': validation_script,
                    'Module Directory': '/opt/nifi/python_modules'
                },
                auto_terminated_relationships=['failure']
            )
        )
        
        return processor
    
    def _create_route_processor(self, pg_id: str) -> ProcessorDTO:
        """Create RouteOnAttribute processor to split valid/invalid records"""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
            location=(200.0, 500.0),
            name='route_by_validation',
            config=ProcessorConfigDTO(
                properties={
                    'Routing Strategy': 'Route to Property name',
                    'valid': "${validation_result.is_valid:equals('true')}",
                    'invalid': "${validation_result.is_valid:equals('false')}"
                }
            )
        )
        
        return processor
    
    def _create_connections(
        self,
        pg_id: str,
        input_port,
        validation_processor,
        route_processor,
        valid_output,
        invalid_output
    ):
        """Create connections between processors"""
        
        # Input -> Validation
        nipyapi.canvas.create_connection(
            source=input_port,
            target=validation_processor,
            relationships=['']
        )
        
        # Validation -> Route
        nipyapi.canvas.create_connection(
            source=validation_processor,
            target=route_processor,
            relationships=['success']
        )
        
        # Route -> Valid Output
        nipyapi.canvas.create_connection(
            source=route_processor,
            target=valid_output,
            relationships=['valid']
        )
        
        # Route -> Invalid Output
        nipyapi.canvas.create_connection(
            source=route_processor,
            target=invalid_output,
            relationships=['invalid']
        )
        
        logger.info("Created all processor connections")
    
    def deploy(self):
        """Deploy complete validation flow to NiFi"""
        try:
            logger.info("Starting deployment to NiFi")
            
            # Connect to NiFi
            self.connect_nifi()
            
            # Create process group
            self.create_process_group()
            
            # Create validation flow
            self.create_validation_flow()
            
            logger.info("Deployment completed successfully")
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise
    
    def start_flow(self):
        """Start the validation flow"""
        try:
            if not self.process_group_id:
                raise ValueError("Process group not deployed")
            
            pg = nipyapi.canvas.get_process_group(self.process_group_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, True)
            
            logger.info(f"Started process group: {self.process_group_name}")
            
        except Exception as e:
            logger.error(f"Failed to start flow: {e}")
            raise
    
    def stop_flow(self):
        """Stop the validation flow"""
        try:
            if not self.process_group_id:
                raise ValueError("Process group not deployed")
            
            pg = nipyapi.canvas.get_process_group(self.process_group_id, 'id')
            nipyapi.canvas.schedule_process_group(pg.id, False)
            
            logger.info(f"Stopped process group: {self.process_group_name}")
            
        except Exception as e:
            logger.error(f"Failed to stop flow: {e}")
            raise
    
    def delete_flow(self):
        """Delete the validation flow from NiFi"""
        try:
            if not self.process_group_id:
                logger.warning("No process group to delete")
                return
            
            # Stop first
            self.stop_flow()
            
            # Delete process group
            pg = nipyapi.canvas.get_process_group(self.process_group_id, 'id')
            nipyapi.canvas.delete_process_group(pg, force=True)
            
            logger.info(f"Deleted process group: {self.process_group_name}")
            self.process_group_id = None
            
        except Exception as e:
            logger.error(f"Failed to delete flow: {e}")
            raise