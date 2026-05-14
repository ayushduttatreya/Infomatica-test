"""
Load module for customer address management data.
Handles loading transformed data to target systems with error handling.
"""

import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerAddressLoader:
    """Loads transformed customer and address data to target systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize loader with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.target_config = config.get('target', {})
        self.error_config = config.get('error_handling', {})
        
    def _ensure_directory(self, file_path: str) -> None:
        """Ensure target directory exists."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    
    def load_to_csv(
        self, 
        df: pd.DataFrame, 
        file_path: str,
        include_index: bool = False
    ) -> int:
        """
        Load DataFrame to CSV file.
        
        Args:
            df: DataFrame to load
            file_path: Target file path
            include_index: Whether to include index in output
            
        Returns:
            Number of records loaded
        """
        try:
            self._ensure_directory(file_path)
            
            df.to_csv(file_path, index=include_index)
            logger.info(f"Successfully loaded {len(df)} records to {file_path}")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load data to {file_path}: {e}")
            raise
    
    def load_to_parquet(
        self, 
        df: pd.DataFrame, 
        file_path: str,
        compression: str = 'snappy'
    ) -> int:
        """
        Load DataFrame to Parquet file.
        
        Args:
            df: DataFrame to load
            file_path: Target file path
            compression: Compression algorithm
            
        Returns:
            Number of records loaded
        """
        try:
            self._ensure_directory(file_path)
            
            df.to_parquet(file_path, compression=compression, index=False)
            logger.info(f"Successfully loaded {len(df)} records to {file_path}")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load data to {file_path}: {e}")
            raise
    
    def load_valid_addresses(self, df: pd.DataFrame) -> int:
        """
        Load valid address records to target.
        
        Args:
            df: DataFrame with valid addresses
            
        Returns:
            Number of records loaded
        """
        target_path = self.target_config.get('valid_addresses_path')
        target_format = self.target_config.get('format', 'csv')
        
        if not target_path:
            raise ValueError("Target path for valid addresses not configured")
        
        logger.info(f"Loading {len(df)} valid address records")
        
        if target_format == 'parquet':
            return self.load_to_parquet(df, target_path)
        else:
            return self.load_to_csv(df, target_path)
    
    def load_error_records(self, df: pd.DataFrame) -> int:
        """
        Load error records to error handling location.
        
        Args:
            df: DataFrame with error records
            
        Returns:
            Number of records loaded
        """
        error_path = self.error_config.get('error_records_path')
        
        if not error_path:
            logger.warning("Error records path not configured, skipping error load")
            return 0
        
        if len(df) == 0:
            logger.info("No error records to load")
            return 0
        
        logger.info(f"Loading {len(df)} error records")
        
        # Always use CSV for error records for easy inspection
        return self.load_to_csv(df, error_path)
    
    def load_audit_summary(self, summary: Dict[str, Any]) -> None:
        """
        Load audit summary information.
        
        Args:
            summary: Dictionary containing audit information
        """
        audit_path = self.target_config.get('audit_summary_path')
        
        if not audit_path:
            logger.warning("Audit summary path not configured, skipping audit load")
            return
        
        try:
            self._ensure_directory(audit_path)
            
            # Convert summary to DataFrame for consistent handling
            summary_df = pd.DataFrame([summary])
            summary_df.to_csv(audit_path, index=False)
            
            logger.info(f"Audit summary saved to {audit_path}")
            
        except Exception as e:
            logger.error(f"Failed to save audit summary: {e}")
            # Don't raise - audit failure shouldn't stop the pipeline
    
    def load_all(
        self, 
        valid_df: pd.DataFrame, 
        error_df: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, int]:
        """
        Load all data (valid records, errors, and audit information).
        
        Args:
            valid_df: DataFrame with valid records
            error_df: DataFrame with error records
            metadata: Optional metadata dictionary
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("Starting load process for all data")
        
        stats = {
            'valid_records_loaded': 0,
            'error_records_loaded': 0,
            'load_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Load valid records
            stats['valid_records_loaded'] = self.load_valid_addresses(valid_df)
            
            # Load error records
            stats['error_records_loaded'] = self.load_error_records(error_df)
            
            # Create and load audit summary
            audit_summary = {
                'total_valid_records': stats['valid_records_loaded'],
                'total_error_records': stats['error_records_loaded'],
                'load_timestamp': stats['load_timestamp'],
                'success': True
            }
            
            if metadata:
                audit_summary.update(metadata)
            
            self.load_audit_summary(audit_summary)
            
            logger.info(f"Load complete: {stats['valid_records_loaded']} valid, "
                       f"{stats['error_records_loaded']} error records")
            
            return stats
            
        except Exception as e:
            logger.error(f"Load process failed: {e}")
            raise