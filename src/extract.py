"""
Extract module for customer transaction loading pipeline.
Handles data extraction from source systems with error handling and validation.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts customer and transaction data from source files."""
    
    def __init__(self, config: Dict):
        """
        Initialize the data extractor.
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('source', {})
        self.batch_size = config.get('processing', {}).get('batch_size', 10000)
        
    def extract_customers(self, file_path: str) -> pd.DataFrame:
        """
        Extract customer data from source file.
        
        Args:
            file_path: Path to customer source file
            
        Returns:
            DataFrame containing customer data
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If data validation fails
        """
        try:
            logger.info(f"Extracting customer data from {file_path}")
            
            # Read customer data
            df = pd.read_csv(
                file_path,
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
            
            # Validate required fields
            required_fields = ['customer_id', 'first_name', 'last_name', 'email']
            missing_fields = [f for f in required_fields if f not in df.columns]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            
            # Add extraction metadata
            df['extract_timestamp'] = datetime.now()
            df['source_file'] = Path(file_path).name
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except FileNotFoundError:
            logger.error(f"Customer source file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_addresses(self, file_path: str) -> pd.DataFrame:
        """
        Extract customer address data from source file.
        
        Args:
            file_path: Path to address source file
            
        Returns:
            DataFrame containing address data
        """
        try:
            logger.info(f"Extracting address data from {file_path}")
            
            df = pd.read_csv(
                file_path,
                dtype={
                    'address_id': str,
                    'customer_id': str,
                    'address_type': str
                }
            )
            
            # Validate required fields
            required_fields = ['address_id', 'customer_id', 'address_type']
            missing_fields = [f for f in required_fields if f not in df.columns]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            
            df['extract_timestamp'] = datetime.now()
            df['source_file'] = Path(file_path).name
            
            logger.info(f"Extracted {len(df)} address records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting address data: {str(e)}")
            raise
    
    def extract_transactions(self, file_path: str) -> pd.DataFrame:
        """
        Extract transaction data from source file with batch processing support.
        
        Args:
            file_path: Path to transaction source file
            
        Returns:
            DataFrame containing transaction data
        """
        try:
            logger.info(f"Extracting transaction data from {file_path}")
            
            # Read in chunks for large files
            chunks = []
            for chunk in pd.read_csv(
                file_path,
                dtype={
                    'transaction_id': str,
                    'customer_id': str,
                    'transaction_type': str,
                    'status': str,
                    'payment_method': str,
                    'currency': str
                },
                parse_dates=['transaction_date'],
                chunksize=self.batch_size
            ):
                chunk['extract_timestamp'] = datetime.now()
                chunk['source_file'] = Path(file_path).name
                chunks.append(chunk)
            
            df = pd.concat(chunks, ignore_index=True)
            
            logger.info(f"Extracted {len(df)} transaction records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting transaction data: {str(e)}")
            raise
    
    def validate_data_quality(self, df: pd.DataFrame, entity_type: str) -> Dict:
        """
        Validate data quality metrics.
        
        Args:
            df: DataFrame to validate
            entity_type: Type of entity (customer, address, transaction)
            
        Returns:
            Dictionary containing validation metrics
        """
        metrics = {
            'total_records': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_count': df.duplicated().sum(),
            'entity_type': entity_type,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Data quality metrics for {entity_type}: {metrics}")
        return metrics