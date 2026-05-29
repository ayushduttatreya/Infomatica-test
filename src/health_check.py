"""
Standalone health check script for NiFi.
Can be used by load balancers and orchestration tools.
"""

import sys
import logging
from typing import Dict
import nipyapi
import yaml
import json

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class HealthChecker:
    """Performs health checks on NiFi instance."""
    
    def __init__(self, config_path: str):
        """Initialize health checker."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def check_connectivity(self, environment: str = 'production') -> bool:
        """Check if NiFi is reachable."""
        try:
            env_config = self.config['environments'][environment]
            nipyapi.config.nifi_config.host = f"{env_config['nifi_url']}/nifi-api"
            
            if env_config.get('use_ssl', False):
                nipyapi.config.nifi_config.verify_ssl = env_config.get('verify_ssl', True)
            
            nipyapi.canvas.get_root_pg_id()
            return True
        except Exception as e:
            logger.error(f"Connectivity check failed: {str(e)}")
            return False
    
    def check_cluster_status(self) -> Dict[str, any]:
        """Check cluster health if clustered."""
        try:
            from nipyapi.nifi import ControllerApi
            controller_api = ControllerApi()
            
            cluster = controller_api.get_cluster()
            
            if not cluster.cluster:
                return {'clustered': False, 'healthy': True}
            
            nodes = cluster.cluster.nodes
            connected_nodes = [n for n in nodes if n.status == 'CONNECTED']
            
            return {
                'clustered': True,
                'healthy': len(connected_nodes) == len(nodes),
                'total_nodes': len(nodes),
                'connected_nodes': len(connected_nodes),
                'disconnected_nodes': len(nodes) - len(connected_nodes)
            }
        except Exception as e:
            logger.error(f"Cluster check failed: {str(e)}")
            return {'clustered': False, 'healthy': False, 'error': str(e)}
    
    def check_critical_flows(self) -> Dict[str, bool]:
        """Check status of critical flows."""
        flow_status = {}
        
        try:
            critical_flows = self.config['monitoring'].get('critical_flows', [])
            
            for flow_name in critical_flows:
                try:
                    pg = nipyapi.canvas.get_process_group(flow_name, 'name')
                    if not pg:
                        flow_status[flow_name] = False
                        continue
                    
                    from nipyapi.nifi import ProcessGroupsApi
                    pg_api = ProcessGroupsApi()
                    status = pg_api.get_process_group_status(pg.id)
                    
                    snapshot = status.process_group_status.aggregate_snapshot
                    
                    # Flow is healthy if no invalid processors and at least some running
                    is_healthy = (
                        snapshot.invalid_count == 0 and
                        snapshot.running_count > 0
                    )
                    
                    flow_status[flow_name] = is_healthy
                    
                except Exception as e:
                    logger.error(f"Failed to check {flow_name}: {str(e)}")
                    flow_status[flow_name] = False
            
        except Exception as e:
            logger.error(f"Critical flows check failed: {str(e)}")
        
        return flow_status
    
    def perform_health_check(self, environment: str = 'production') -> Dict[str, any]:
        """Perform complete health check."""
        health_report = {
            'timestamp': str(nipyapi.utils.get_nifi_timestamp()),
            'environment': environment,
            'overall_healthy': True,
            'checks': {}
        }
        
        # Connectivity
        connectivity = self.check_connectivity(environment)
        health_report['checks']['connectivity'] = connectivity
        if not connectivity:
            health_report['overall_healthy'] = False
            return health_report
        
        # Cluster status
        cluster_status = self.check_cluster_status()
        health_report['checks']['cluster'] = cluster_status
        if not cluster_status.get('healthy', False):
            health_report['overall_healthy'] = False
        
        # Critical flows
        flow_status = self.check_critical_flows()
        health_report['checks']['critical_flows'] = flow_status
        if flow_status and not all(flow_status.values()):
            health_report['overall_healthy'] = False
        
        return health_report


def main():
    """Main health check execution."""
    if len(sys.argv) < 2:
        environment = 'production'
    else:
        environment = sys.argv[1]
    
    checker = HealthChecker('config.yaml')
    health_report = checker.perform_health_check(environment)
    
    # Output JSON for easy parsing
    print(json.dumps(health_report, indent=2))
    
    # Exit with appropriate code
    if health_report['overall_healthy']:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()