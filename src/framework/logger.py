"""
Centralized Logging Module
Provides structured logging with multiple handlers, log levels, and formatting.
"""
import logging
import sys
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Optional
from datetime import datetime


class LoggerFactory:
    """
    Factory class for creating and managing loggers with consistent configuration.
    """
    
    _loggers = {}
    _default_level = logging.INFO
    _log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    _date_format = '%Y-%m-%d %H:%M:%S'
    
    @classmethod
    def setup_logging(cls, 
                     log_level: str = 'INFO',
                     log_dir: Optional[str] = None,
                     log_to_console: bool = True,
                     log_to_file: bool = True,
                     max_bytes: int = 10485760,  # 10MB
                     backup_count: int = 5) -> None:
        """
        Setup global logging configuration.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files
            log_to_console: Enable console logging
            log_to_file: Enable file logging
            max_bytes: Maximum size of log file before rotation
            backup_count: Number of backup files to keep
        """
        cls._default_level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Create log directory if needed
        if log_to_file and log_dir:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        cls._log_dir = log_dir
        cls._log_to_console = log_to_console
        cls._log_to_file = log_to_file
        cls._max_bytes = max_bytes
        cls._backup_count = backup_count
    
    @classmethod
    def get_logger(cls, name: str, log_level: Optional[str] = None) -> logging.Logger:
        """
        Get or create a logger with the specified name.
        
        Args:
            name: Logger name (typically module name)
            log_level: Optional specific log level for this logger
            
        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        level = getattr(logging, log_level.upper(), cls._default_level) if log_level else cls._default_level
        logger.setLevel(level)
        
        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()
        
        formatter = logging.Formatter(cls._log_format, cls._date_format)
        
        # Console handler
        if cls._log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler with rotation
        if cls._log_to_file and cls._log_dir:
            log_file = Path(cls._log_dir) / f"{name}.log"
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=cls._max_bytes,
                backupCount=cls._backup_count
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # Separate error log file
            error_log_file = Path(cls._log_dir) / f"{name}_error.log"
            error_handler = RotatingFileHandler(
                error_log_file,
                maxBytes=cls._max_bytes,
                backupCount=cls._backup_count
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            logger.addHandler(error_handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def get_audit_logger(cls, name: str = 'audit') -> logging.Logger:
        """
        Get audit logger for tracking data lineage and operations.
        
        Args:
            name: Audit logger name
            
        Returns:
            Configured audit logger
        """
        audit_logger = logging.getLogger(f"{name}_audit")
        audit_logger.setLevel(logging.INFO)
        audit_logger.handlers.clear()
        
        formatter = logging.Formatter(
            '%(asctime)s - AUDIT - %(message)s',
            cls._date_format
        )
        
        if cls._log_dir:
            audit_file = Path(cls._log_dir) / f"{name}_audit.log"
            # Use TimedRotatingFileHandler for daily rotation
            audit_handler = TimedRotatingFileHandler(
                audit_file,
                when='midnight',
                interval=1,
                backupCount=30
            )
            audit_handler.setLevel(logging.INFO)
            audit_handler.setFormatter(formatter)
            audit_logger.addHandler(audit_handler)
        
        audit_logger.propagate = False
        return audit_logger
    
    @classmethod
    def shutdown(cls) -> None:
        """Shutdown all loggers and handlers."""
        for logger in cls._loggers.values():
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
        cls._loggers.clear()
        logging.shutdown()


class AuditLogger:
    """
    Specialized logger for audit trail and data lineage tracking.
    """
    
    def __init__(self, logger_name: str = 'audit'):
        """
        Initialize audit logger.
        
        Args:
            logger_name: Name for the audit logger
        """
        self.logger = LoggerFactory.get_audit_logger(logger_name)
    
    def log_operation(self, 
                     operation: str,
                     entity_type: str,
                     entity_id: str,
                     details: Optional[dict] = None,
                     user: Optional[str] = None) -> None:
        """
        Log an audit operation.
        
        Args:
            operation: Operation type (CREATE, UPDATE, DELETE, READ)
            entity_type: Type of entity (CUSTOMER, ORDER, etc.)
            entity_id: Unique identifier of entity
            details: Additional operation details
            user: User performing operation
        """
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'operation': operation,
            'entity_type': entity_type,
            'entity_id': entity_id,
            'user': user or 'system',
            'details': details or {}
        }
        
        self.logger.info(str(audit_entry))
    
    def log_data_flow(self,
                     source: str,
                     destination: str,
                     record_count: int,
                     status: str,
                     duration_ms: Optional[float] = None) -> None:
        """
        Log data flow operation.
        
        Args:
            source: Source system/table
            destination: Destination system/table
            record_count: Number of records processed
            status: Operation status (SUCCESS, FAILED, PARTIAL)
            duration_ms: Operation duration in milliseconds
        """
        flow_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'type': 'DATA_FLOW',
            'source': source,
            'destination': destination,
            'record_count': record_count,
            'status': status,
            'duration_ms': duration_ms
        }
        
        self.logger.info(str(flow_entry))