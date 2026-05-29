"""
NiFi monitoring and health check system.
Monitors flow health, performance metrics, and sends alerts.
"""

import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import nipyapi
from nipyapi.nifi import ProcessGroupsApi, SystemDiagnosticsApi, CountersApi
import yaml
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """Health check status."""
    healthy: bool
    component: str
    message: str
    timestamp: datetime
    severity: str  # INFO, WARNING, CRITICAL


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot."""
    timestamp: datetime
    active_threads: int
    queued_flowfiles: int
    bytes_queued: int
    bytes_read: int
    bytes_written: int
    flowfiles_in: int
    flowfiles_out: int
    cpu_usage: float
    memory_usage: float
    heap_usage: float


class NiFiMonitor:
    """Monitors NiFi instance health and performance."""
    
    def __init__(self, config_path: str):
        """Initialize monitor with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.pg_api = None
        self.system_api = None
        self.counters_api = None
        self.alert_history: List[HealthStatus] = []
        self.metrics_history: List[PerformanceMetrics] = []
        
    def connect(self, environment: str = 'production') -> bool:
        """Connect to NiFi instance."""
        try:
            env_config = self.config['environments'][environment]
            
            nipyapi.config.nifi_config.host = f"{env_config['nifi_url']}/nifi-api"
            
            if env_config.get('use_ssl', False):
                nipyapi.config.nifi_config.verify_ssl = env_config.get('verify_ssl', True)
            
            self.pg_api = ProcessGroupsApi()
            self.system_api = SystemDiagnosticsApi()
            self.counters_api = CountersApi()
            
            # Test connection
            nipyapi.canvas.get_root_pg_id()
            logger.info(f"Connected to {environment} for monitoring")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect: {str(e)}")
            return False
    
    def check_system_health(self) -> List[HealthStatus]:
        """Check overall system health."""
        health_checks = []
        
        try:
            # System diagnostics
            diagnostics = self.system_api.get_system_diagnostics()
            
            # Check heap usage
            heap_used = diagnostics.system_diagnostics.aggregate_snapshot.heap_utilization
            heap_threshold = self.config['monitoring']['thresholds']['heap_usage_percent']
            
            if heap_used > heap_threshold:
                health_checks.append(HealthStatus(
                    healthy=False,
                    component='System',
                    message=f"High heap usage: {heap_used}%",
                    timestamp=datetime.now(),
                    severity='CRITICAL' if heap_used > 90 else 'WARNING'
                ))
            
            # Check CPU usage
            cpu_usage = diagnostics.system_diagnostics.aggregate_snapshot.processor_load_average
            cpu_threshold = self.config['monitoring']['thresholds']['cpu_usage_percent']
            
            if cpu_usage > cpu_threshold:
                health_checks.append(HealthStatus(
                    healthy=False,
                    component='System',
                    message=f"High CPU usage: {cpu_usage}%",
                    timestamp=datetime.now(),
                    severity='WARNING'
                ))
            
            # Check available storage
            storage = diagnostics.system_diagnostics.aggregate_snapshot.content_repository_storage_usage
            for repo in storage:
                if repo.utilization > 80:
                    health_checks.append(HealthStatus(
                        healthy=False,
                        component='Storage',
                        message=f"High storage usage in {repo.identifier}: {repo.utilization}%",
                        timestamp=datetime.now(),
                        severity='CRITICAL' if repo.utilization > 90 else 'WARNING'
                    ))
            
            if not health_checks:
                health_checks.append(HealthStatus(
                    healthy=True,
                    component='System',
                    message='System health OK',
                    timestamp=datetime.now(),
                    severity='INFO'
                ))
            
        except Exception as e:
            logger.error(f"System health check failed: {str(e)}")
            health_checks.append(HealthStatus(
                healthy=False,
                component='System',
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                severity='CRITICAL'
            ))
        
        return health_checks
    
    def check_process_group_health(self, process_group_id: str) -> List[HealthStatus]:
        """Check health of specific process group."""
        health_checks = []
        
        try:
            pg = nipyapi.canvas.get_process_group(process_group_id, 'id')
            status = self.pg_api.get_process_group_status(process_group_id)
            
            snapshot = status.process_group_status.aggregate_snapshot
            
            # Check for invalid processors
            if snapshot.invalid_count > 0:
                health_checks.append(HealthStatus(
                    healthy=False,
                    component=pg.component.name,
                    message=f"{snapshot.invalid_count} invalid processors",
                    timestamp=datetime.now(),
                    severity='CRITICAL'
                ))
            
            # Check for stopped processors
            if snapshot.stopped_count > 0:
                health_checks.append(HealthStatus(
                    healthy=False,
                    component=pg.component.name,
                    message=f"{snapshot.stopped_count} stopped processors",
                    timestamp=datetime.now(),
                    severity='WARNING'
                ))
            
            # Check queue depth
            queue_threshold = self.config['monitoring']['thresholds']['queue_count']
            if snapshot.queued_count > queue_threshold:
                health_checks.append(HealthStatus(
                    healthy=False,
                    component=pg.component.name,
                    message=f"High queue count: {snapshot.queued_count}",
                    timestamp=datetime.now(),
                    severity='WARNING'
                ))
            
            # Check for errors
            processors = nipyapi.canvas.list_all_processors(pg_id=process_group_id)
            for processor in processors:
                if processor.status.aggregate_snapshot.tasks_duration_nanos > 0:
                    error_count = processor.status.aggregate_snapshot.output.get('error', 0)
                    if error_count > 0:
                        health_checks.append(HealthStatus(
                            healthy=False,
                            component=processor.component.name,
                            message=f"Processor has {error_count} errors",
                            timestamp=datetime.now(),
                            severity='WARNING'
                        ))
            
            if not health_checks:
                health_checks.append(HealthStatus(
                    healthy=True,
                    component=pg.component.name,
                    message='Process group health OK',
                    timestamp=datetime.now(),
                    severity='INFO'
                ))
            
        except Exception as e:
            logger.error(f"Process group health check failed: {str(e)}")
            health_checks.append(HealthStatus(
                healthy=False,
                component='Unknown',
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                severity='CRITICAL'
            ))
        
        return health_checks
    
    def collect_metrics(self) -> PerformanceMetrics:
        """Collect current performance metrics."""
        try:
            root_pg_id = nipyapi.canvas.get_root_pg_id()
            status = self.pg_api.get_process_group_status(root_pg_id)
            diagnostics = self.system_api.get_system_diagnostics()
            
            snapshot = status.process_group_status.aggregate_snapshot
            sys_snapshot = diagnostics.system_diagnostics.aggregate_snapshot
            
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                active_threads=snapshot.active_thread_count,
                queued_flowfiles=snapshot.queued_count,
                bytes_queued=int(snapshot.queued_size.split()[0]),
                bytes_read=snapshot.bytes_read,
                bytes_written=snapshot.bytes_written,
                flowfiles_in=snapshot.flow_files_in,
                flowfiles_out=snapshot.flow_files_out,
                cpu_usage=sys_snapshot.processor_load_average,
                memory_usage=sys_snapshot.total_non_heap_bytes / sys_snapshot.max_non_heap_bytes * 100,
                heap_usage=sys_snapshot.heap_utilization
            )
            
            self.metrics_history.append(metrics)
            
            # Keep only recent history
            max_history = self.config['monitoring'].get('metrics_history_size', 1000)
            if len(self.metrics_history) > max_history:
                self.metrics_history = self.metrics_history[-max_history:]
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect metrics: {str(e)}")
            return None
    
    def send_email_alert(self, health_status: HealthStatus):
        """Send email alert for health issue."""
        try:
            email_config = self.config['monitoring']['alerting']['email']
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from']
            msg['To'] = ', '.join(email_config['to'])
            msg['Subject'] = f"[{health_status.severity}] NiFi Alert: {health_status.component}"
            
            body = f"""
NiFi Monitoring Alert

Severity: {health_status.severity}
Component: {health_status.component}
Message: {health_status.message}
Timestamp: {health_status.timestamp}
Environment: {self.config.get('environment', 'Unknown')}

This is an automated alert from NiFi monitoring system.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(email_config['smtp_host'], email_config['smtp_port'])
            if email_config.get('use_tls', True):
                server.starttls()
            
            if 'username' in email_config:
                server.login(email_config['username'], email_config['password'])
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent for {health_status.component}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
    
    def send_webhook_alert(self, health_status: HealthStatus):
        """Send webhook alert for health issue."""
        try:
            webhook_config = self.config['monitoring']['alerting']['webhook']
            
            payload = {
                'severity': health_status.severity,
                'component': health_status.component,
                'message': health_status.message,
                'timestamp': health_status.timestamp.isoformat(),
                'healthy': health_status.healthy
            }
            
            response = requests.post(
                webhook_config['url'],
                json=payload,
                headers=webhook_config.get('headers', {}),
                timeout=10
            )
            
            response.raise_for_status()
            logger.info(f"Webhook alert sent for {health_status.component}")
            
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {str(e)}")
    
    def process_alerts(self, health_checks: List[HealthStatus]):
        """Process health checks and send alerts if needed."""
        for check in health_checks:
            if not check.healthy:
                # Check if we should alert based on severity
                alert_config = self.config['monitoring']['alerting']
                
                if check.severity in alert_config.get('severity_levels', ['WARNING', 'CRITICAL']):
                    # Check if we've alerted recently (avoid spam)
                    recent_alerts = [
                        a for a in self.alert_history
                        if a.component == check.component
                        and a.timestamp > datetime.now() - timedelta(minutes=15)
                    ]
                    
                    if not recent_alerts:
                        if alert_config.get('email', {}).get('enabled', False):
                            self.send_email_alert(check)
                        
                        if alert_config.get('webhook', {}).get('enabled', False):
                            self.send_webhook_alert(check)
                        
                        self.alert_history.append(check)
    
    def run_monitoring_cycle(self):
        """Run one complete monitoring cycle."""
        logger.info("Starting monitoring cycle")
        
        # System health
        system_health = self.check_system_health()
        self.process_alerts(system_health)
        
        # Process group health
        monitored_flows = self.config['monitoring'].get('monitored_flows', [])
        for flow_name in monitored_flows:
            try:
                pg = nipyapi.canvas.get_process_group(flow_name, 'name')
                if pg:
                    pg_health = self.check_process_group_health(pg.id)
                    self.process_alerts(pg_health)
            except Exception as e:
                logger.error(f"Failed to check {flow_name}: {str(e)}")
        
        # Collect metrics
        metrics = self.collect_metrics()
        if metrics:
            logger.info(f"Metrics: Active threads={metrics.active_threads}, "
                       f"Queue={metrics.queued_flowfiles}, "
                       f"Heap={metrics.heap_usage:.1f}%")
    
    def start_monitoring(self, interval: int = None):
        """Start continuous monitoring."""
        if interval is None:
            interval = self.config['monitoring'].get('check_interval_seconds', 60)
        
        logger.info(f"Starting continuous monitoring (interval: {interval}s)")
        
        while True:
            try:
                self.run_monitoring_cycle()
            except Exception as e:
                logger.error(f"Monitoring cycle failed: {str(e)}")
            
            time.sleep(interval)


def main():
    """Main monitoring execution."""
    monitor = NiFiMonitor('config.yaml')
    
    if not monitor.connect():
        logger.error("Failed to connect to NiFi")
        return
    
    # Run continuous monitoring
    monitor.start_monitoring()


if __name__ == '__main__':
    main()