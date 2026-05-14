"""
Extract module for customer address pipeline.
Handles reading customer and address data from source files.
"""
import logging
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class CustomerAddressExtractor:
    """Extracts customer and address data from source files."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration.
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('source', {})
        
    def extract_customers(self) -> pd.DataFrame:
        """
        Extract customer data from source file.
        
        Returns:
            DataFrame containing customer records
            
        Raises:
            FileNotFoundError: If source file does not exist
            ValueError: If required columns are missing
        """
        source_path = Path(self.source_config.get('customers_path'))
        
        if not source_path.exists():
            raise FileNotFoundError(f"Customer source file not found: {source_path}")
            
        logger.info(f"Extracting customers from {source_path}")
        
        df = pd.read_csv(
            source_path,
            dtype={
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
            },
            parse_dates=['registration_date']
        )
        
        required_columns = ['customer_id', 'first_name', 'last_name', 'email']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        logger.info(f"Extracted {len(df)} customer records")
        return df
        
    def extract_addresses(self) -> pd.DataFrame:
        """
        Extract address data from source file.
        
        Returns:
            DataFrame containing address records
            
        Raises:
            FileNotFoundError: If source file does not exist
            ValueError: If required columns are missing
        """
        source_path = Path(self.source_config.get('addresses_path'))
        
        if not source_path.exists():
            raise FileNotFoundError(f"Address source file not found: {source_path}")
            
        logger.info(f"Extracting addresses from {source_path}")
        
        df = pd.read_csv(
            source_path,
            dtype={
                'address_id': str,
                'customer_id': str,
                'address_type': str,
                'street_address': str,
                'street_address2': str,
                'city': str,
                'state_province': str,
                'postal_code': str,
                'country_code': str,
                'is_primary': str,
                'is_active': str
            },
            parse_dates=['created_date', 'modified_date']
        )
        
        required_columns = ['address_id', 'customer_id', 'street_address', 'city']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        logger.info(f"Extracted {len(df)} address records")
        return df
        
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all source data.
        
        Returns:
            Dictionary containing customers and addresses DataFrames
        """
        return {
            'customers': self.extract_customers(),
            'addresses': self.extract_addresses()
        }