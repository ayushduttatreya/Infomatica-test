"""
Extract module for customer address management data from source systems.
Handles extraction of customer and address data with error handling and validation.
"""

import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class CustomerAddressExtractor:
    """Extracts customer and address data from source files."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the extractor with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.source_config = self.config.get('source', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def extract_customers(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer master data from source file.
        
        Args:
            file_path: Optional override for source file path
            
        Returns:
            DataFrame containing customer data
        """
        source_path = file_path or self.source_config.get('customers_file')
        
        if not source_path:
            raise ValueError("Customer source file path not configured")
        
        try:
            logger.info(f"Extracting customer data from {source_path}")
            
            # Define expected schema based on Informatica source definition
            dtype_mapping = {
                'customer_id': str,
                'first_name': str,
                'last_name': str,
                'email': str,
                'phone': str,
                'address_line1': str,
                'address_line2': str,
                'city': str,
                'state': str,
                'zip_code': str,
                'country': str,
                'status': str
            }
            
            df = pd.read_csv(
                source_path,
                dtype=dtype_mapping,
                parse_dates=['registration_date'],
                na_values=['', 'NULL', 'null']
            )
            
            # Validate required fields
            if df['customer_id'].isnull().any():
                raise ValueError("customer_id cannot be null (NOT NULL constraint)")
            
            logger.info(f"Successfully extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract customer data: {e}")
            raise
    
    def extract_addresses(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer address data from source file.
        
        Args:
            file_path: Optional override for source file path
            
        Returns:
            DataFrame containing address data
        """
        source_path = file_path or self.source_config.get('addresses_file')
        
        if not source_path:
            raise ValueError("Address source file path not configured")
        
        try:
            logger.info(f"Extracting address data from {source_path}")
            
            # Define expected schema
            dtype_mapping = {
                'address_id': str,
                'customer_id': str,
                'address_type': str,
                'address_line1': str,
                'address_line2': str,
                'city': str,
                'state': str,
                'zip_code': str,
                'country': str,
                'is_primary': str,
                'is_active': str
            }
            
            df = pd.read_csv(
                source_path,
                dtype=dtype_mapping,
                na_values=['', 'NULL', 'null']
            )
            
            # Validate required fields
            required_fields = ['address_id', 'customer_id']
            for field in required_fields:
                if df[field].isnull().any():
                    raise ValueError(f"{field} cannot be null (NOT NULL constraint)")
            
            logger.info(f"Successfully extracted {len(df)} address records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract address data: {e}")
            raise
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all source data.
        
        Returns:
            Dictionary containing all extracted DataFrames
        """
        return {
            'customers': self.extract_customers(),
            'addresses': self.extract_addresses()
        }