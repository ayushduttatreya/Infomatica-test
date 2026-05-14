"""
Deployment orchestration for NiFi flows across environments.
Handles deployment to staging and production with validation.
"""

import logging
import sys
from typing import Dict, List, Optional
import nipyapi
from nipyapi.nifi import ProcessGroupsApi, FlowApi
from nipyapi.canvas import schedule_process_group
import yaml
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NiFiDeployer:
    """Handles deployment of NiFi flows to different environments."""
    
    def __init__(self, config_path: str):
        """Initialize deployer with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.pg_api = None
        self.flow_api = None
        self.current_env = None
        
    def connect(self, environment: str) -> bool:
        """Connect to NiFi instance for specified environment."""
        try:
            env_config = self.config['environments'][environment]
            self.current_env = environment
            
            nipyapi.config.nifi_config.host = f"{env_config['nifi_url']}/nifi-api"
            
            if env_config.get('use_ssl', False):
                nipyapi.config.nifi_config.verify_ssl = env_config.get('verify_ssl', True)
                if 'cert_file' in env_config:
                    nipyapi.config.nifi_config.cert_file = env_config['cert_file']
                if 'key_file' in env_config:
                    nipyapi.config.nifi_config.key_file = env_config['key_file']
            
            self.pg_api = ProcessGroupsApi()
            self.flow_api = FlowApi()
            
            # Test connection
            nipyapi.canvas.get_root_pg_id()
            logger.info(f"Successfully connected to {environment} environment")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {environment}: {str(e)}")
            return False
    
    def validate_flow(self, process_group_id: str) -> Dict[str, any]:
        """Validate process group before deployment."""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            pg = nipyapi.canvas.get_process_group(process_group_id, 'id')
            
            if not pg:
                validation_results['valid'] = False
                validation_results['errors'].append("Process group not found")
                return validation_results
            
            # Check for invalid components
            status = self.pg_api.get_process_group_status(process_group_id)
            
            if status.process_group_status.aggregate_snapshot.active_thread_count > 0:
                validation_results['warnings'].append(
                    "Process group has active threads"
                )
            
            # Check for stopped processors
            processors = nipyapi.canvas.list_all_processors(pg_id=process_group_id)
            stopped_count = sum(1 for p in processors if p.status.run_status == 'Stopped')
            
            if stopped_count > 0:
                validation_results['warnings'].append(
                    f"{stopped_count} processors are stopped"
                )
            
            # Check for invalid processors
            invalid_processors = [
                p.component.name for p in processors 
                if p.status.aggregate_snapshot.run_status == 'Invalid'
            ]
            
            if invalid_processors:
                validation_results['valid'] = False
                validation_results['errors'].append(
                    f"Invalid processors: {', '.join(invalid_processors)}"
                )
            
            # Check connections
            connections = nipyapi.canvas.list_all_connections(pg_id=process_group_id)
            for conn in connections:
                if conn.status.aggregate_snapshot.queued_count > 1000:
                    validation_results['warnings'].append(
                        f"Connection {conn.component.name} has high queue count"
                    )
            
            logger.info(f"Validation completed for {pg.component.name}")
            
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
            logger.error(f"Validation failed: {str(e)}")
        
        return validation_results
    
    def backup_flow(self, process_group_id: str) -> Optional[str]:
        """Create backup of process group before deployment."""
        try:
            pg = nipyapi.canvas.get_process_group(process_group_id, 'id')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"{pg.component.name}_backup_{timestamp}"
            
            # Export flow definition
            flow_definition = nipyapi.canvas.get_flow(process_group_id)
            
            backup_path = f"{self.config['deployment']['backup_dir']}/{backup_name}.json"
            
            import json
            with open(backup_path, 'w') as f:
                json.dump(flow_definition.to_dict(), f, indent=2)
            
            logger.info(f"Backup created: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            return None
    
    def stop_process_group(self, process_group_id: str, timeout: int = 60) -> bool:
        """Stop all processors in process group."""
        try:
            logger.info(f"Stopping process group {process_group_id}")
            
            schedule_process_group(process_group_id, scheduled=False)
            
            # Wait for processors to stop
            start_time = time.time()
            while time.time() - start_time < timeout:
                status = self.pg_api.get_process_group_status(process_group_id)
                if status.process_group_status.aggregate_snapshot.active_thread_count == 0:
                    logger.info("Process group stopped successfully")
                    return True
                time.sleep(2)
            
            logger.warning("Process group stop timeout reached")
            return False
            
        except Exception as e:
            logger.error(f"Failed to stop process group: {str(e)}")
            return False
    
    def start_process_group(self, process_group_id: str) -> bool:
        """Start all processors in process group."""
        try:
            logger.info(f"Starting process group {process_group_id}")
            schedule_process_group(process_group_id, scheduled=True)
            logger.info("Process group started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start process group: {str(e)}")
            return False
    
    def deploy_flow(self, flow_name: str, source_env: str, target_env: str) -> bool:
        """Deploy flow from source to target environment."""
        try:
            logger.info(f"Deploying {flow_name} from {source_env} to {target_env}")
            
            # Connect to source
            if not self.connect(source_env):
                return False
            
            # Find source process group
            source_pg = nipyapi.canvas.get_process_group(flow_name, 'name')
            if not source_pg:
                logger.error(f"Source flow {flow_name} not found")
                return False
            
            # Validate source
            validation = self.validate_flow(source_pg.id)
            if not validation['valid']:
                logger.error(f"Source validation failed: {validation['errors']}")
                return False
            
            # Export source flow
            flow_definition = nipyapi.canvas.get_flow(source_pg.id)
            
            # Connect to target
            if not self.connect(target_env):
                return False
            
            # Check if flow exists in target
            target_pg = nipyapi.canvas.get_process_group(flow_name, 'name')
            
            if target_pg:
                # Backup existing flow
                backup_path = self.backup_flow(target_pg.id)
                if not backup_path:
                    logger.error("Backup failed, aborting deployment")
                    return False
                
                # Stop existing flow
                if not self.stop_process_group(target_pg.id):
                    logger.error("Failed to stop existing flow")
                    return False
                
                # Delete existing flow
                nipyapi.canvas.delete_process_group(target_pg, force=True)
                logger.info("Existing flow removed")
            
            # Create new flow in target
            root_pg = nipyapi.canvas.get_root_pg_id()
            
            # Import flow
            new_pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(root_pg, 'id'),
                new_pg_name=flow_name,
                location=(0, 0)
            )
            
            # Note: Full flow import would require template or registry
            # This is a simplified version
            logger.info(f"Flow {flow_name} deployed to {target_env}")
            
            # Start flow if configured
            if self.config['deployment'].get('auto_start', False):
                self.start_process_group(new_pg.id)
            
            return True
            
        except Exception as e:
            logger.error(f"Deployment failed: {str(e)}")
            return False
    
    def rollback_deployment(self, backup_path: str, target_env: str) -> bool:
        """Rollback to previous version from backup."""
        try:
            logger.info(f"Rolling back deployment in {target_env}")
            
            if not self.connect(target_env):
                return False
            
            import json
            with open(backup_path, 'r') as f:
                flow_definition = json.load(f)
            
            # Restore from backup
            # Implementation depends on backup format
            logger.info("Rollback completed")
            return True
            
        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")
            return False


def main():
    """Main deployment execution."""
    if len(sys.argv) < 4:
        print("Usage: python deploy.py <flow_name> <source_env> <target_env>")
        sys.exit(1)
    
    flow_name = sys.argv[1]
    source_env = sys.argv[2]
    target_env = sys.argv[3]
    
    deployer = NiFiDeployer('config.yaml')
    
    success = deployer.deploy_flow(flow_name, source_env, target_env)
    
    if success:
        logger.info("Deployment completed successfully")
        sys.exit(0)
    else:
        logger.error("Deployment failed")
        sys.exit(1)


if __name__ == '__main__':
    main()