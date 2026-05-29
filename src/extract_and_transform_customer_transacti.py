# Customer Transaction Processing Migration

===FILE: src/extract.py===
"""
Extract module for customer transaction processing.
Handles data extraction from flat files and databases.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts customer and transaction data from various sources."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize extractor with configuration."""
        self.config = self._load_config(config_path)
        self.source_config = self.config.get('sources', {})
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def extract_customers(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer data from flat file.
        
        Args:
            file_path: Path to customer data file. Uses config default if None.
            
        Returns:
            DataFrame containing customer records.
        """
        if file_path is None:
            file_path = self.source_config.get('customers', {}).get('path')
        
        if not file_path:
            raise ValueError("Customer file path not provided")
        
        logger.info(f"Extracting customer data from {file_path}")
        
        try:
            # Define schema based on Informatica source definition
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
            
            # Read with proper encoding and delimiter
            df = pd.read_csv(
                file_path,
                dtype=dtype_mapping,
                parse_dates=['registration_date'],
                encoding=self.source_config.get('customers', {}).get('encoding', 'utf-8'),
                delimiter=self.source_config.get('customers', {}).get('delimiter', ',')
            )
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract customer data: {e}")
            raise
    
    def extract_transactions(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract transaction data from flat file.
        
        Args:
            file_path: Path to transaction data file. Uses config default if None.
            
        Returns:
            DataFrame containing transaction records.
        """
        if file_path is None:
            file_path = self.source_config.get('transactions', {}).get('path')
        
        if not file_path:
            raise ValueError("Transaction file path not provided")
        
        logger.info(f"Extracting transaction data from {file_path}")
        
        try:
            # Define schema for transactions
            dtype_mapping = {
                'transaction_id': str,
                'customer_id': str,
                'transaction_type': str,
                'transaction_status': str,
                'currency_code': str,
                'merchant_id': str,
                'payment_method': str
            }
            
            # Read with decimal handling for amounts
            df = pd.read_csv(
                file_path,
                dtype=dtype_mapping,
                parse_dates=['transaction_date', 'settlement_date'],
                encoding=self.source_config.get('transactions', {}).get('encoding', 'utf-8'),
                delimiter=self.source_config.get('transactions', {}).get('delimiter', ',')
            )
            
            # Convert amount fields to string initially to preserve precision
            amount_fields = ['transaction_amount', 'fee_amount', 'net_amount']
            for field in amount_fields:
                if field in df.columns:
                    df[field] = df[field].astype(str)
            
            logger.info(f"Extracted {len(df)} transaction records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract transaction data: {e}")
            raise
    
    def extract_customer_addresses(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer address data from flat file.
        
        Args:
            file_path: Path to address data file. Uses config default if None.
            
        Returns:
            DataFrame containing address records.
        """
        if file_path is None:
            file_path = self.source_config.get('addresses', {}).get('path')
        
        if not file_path:
            raise ValueError("Address file path not provided")
        
        logger.info(f"Extracting address data from {file_path}")
        
        try:
            dtype_mapping = {
                'address_id': str,
                'customer_id': str,
                'address_type': str,
                'address_line1': str,
                'address_line2': str,
                'city': str,
                'state': str,
                'zip_code': str,
                'country': str
            }
            
            df = pd.read_csv(
                file_path,
                dtype=dtype_mapping,
                encoding=self.source_config.get('addresses', {}).get('encoding', 'utf-8'),
                delimiter=self.source_config.get('addresses', {}).get('delimiter', ',')
            )
            
            logger.info(f"Extracted {len(df)} address records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract address data: {e}")
            raise
    
    def validate_extraction(self, df: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """
        Validate extracted data quality.
        
        Args:
            df: DataFrame to validate
            source_name: Name of the source for logging
            
        Returns:
            Dictionary containing validation metrics
        """
        validation_results = {
            'source': source_name,
            'record_count': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_count': df.duplicated().sum(),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Validation results for {source_name}: {validation_results}")
        return validation_results


===FILE: src/transform.py===
"""
Transform module for customer transaction processing.
Implements decimal precision handling, date/time conversion, and validation.
"""

import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
import yaml
import re

logger = logging.getLogger(__name__)


class TransactionTransformer:
    """Transforms customer transaction data with precision handling."""
    
    # Valid transaction types based on business rules
    VALID_TRANSACTION_TYPES = {
        'PURCHASE', 'REFUND', 'ADJUSTMENT', 'CHARGEBACK', 
        'REVERSAL', 'AUTHORIZATION', 'SETTLEMENT', 'VOID'
    }
    
    # Valid transaction statuses
    VALID_STATUSES = {
        'PENDING', 'APPROVED', 'DECLINED', 'CANCELLED', 
        'SETTLED', 'FAILED', 'PROCESSING'
    }
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize transformer with configuration."""
        self.config = self._load_config(config_path)
        self.transform_config = self.config.get('transformations', {})
        self.decimal_precision = self.transform_config.get('decimal_precision', 2)
        self.date_format = self.transform_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def transform_decimal_precision(
        self, 
        value: Any, 
        precision: Optional[int] = None
    ) -> Optional[Decimal]:
        """
        Transform numeric value to Decimal with specified precision.
        
        Args:
            value: Input value (string, float, int, or Decimal)
            precision: Number of decimal places (uses config default if None)
            
        Returns:
            Decimal value with proper precision or None if invalid
        """
        if pd.isna(value) or value is None or value == '':
            return None
        
        if precision is None:
            precision = self.decimal_precision
        
        try:
            # Convert to Decimal for precision handling
            if isinstance(value, str):
                # Remove currency symbols and commas
                cleaned = re.sub(r'[,$€£¥]', '', value.strip())
                decimal_value = Decimal(cleaned)
            else:
                decimal_value = Decimal(str(value))
            
            # Round to specified precision
            quantizer = Decimal('0.1') ** precision
            rounded_value = decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
            
            return rounded_value
            
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Failed to convert value '{value}' to Decimal: {e}")
            return None
    
    def transform_datetime(
        self, 
        value: Any, 
        input_format: Optional[str] = None,
        output_format: Optional[str] = None
    ) -> Optional[str]:
        """
        Transform datetime value to standardized format.
        
        Args:
            value: Input datetime value
            input_format: Expected input format (auto-detect if None)
            output_format: Desired output format (uses config default if None)
            
        Returns:
            Formatted datetime string or None if invalid
        """
        if pd.isna(value) or value is None or value == '':
            return None
        
        if output_format is None:
            output_format = self.date_format
        
        try:
            # Handle pandas Timestamp
            if isinstance(value, pd.Timestamp):
                dt = value.to_pydatetime()
            # Handle datetime object
            elif isinstance(value, datetime):
                dt = value
            # Handle string
            elif isinstance(value, str):
                if input_format:
                    dt = datetime.strptime(value, input_format)
                else:
                    # Try common formats
                    dt = pd.to_datetime(value)
                    if isinstance(dt, pd.Timestamp):
                        dt = dt.to_pydatetime()
            else:
                logger.warning(f"Unsupported datetime type: {type(value)}")
                return None
            
            # Ensure timezone awareness if configured
            if self.transform_config.get('use_utc', True):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            
            return dt.strftime(output_format)
            
        except Exception as e:
            logger.warning(f"Failed to convert datetime '{value}': {e}")
            return None
    
    def validate_transaction_type(self, transaction_type: str) -> bool:
        """
        Validate transaction type against allowed values.
        
        Args:
            transaction_type: Transaction type to validate
            
        Returns:
            True if valid, False otherwise
        """
        if pd.isna(transaction_type) or not transaction_type:
            return False
        
        normalized = str(transaction_type).strip().upper()
        return normalized in self.VALID_TRANSACTION_TYPES
    
    def validate_transaction_status(self, status: str) -> bool:
        """
        Validate transaction status against allowed values.
        
        Args:
            status: Status to validate
            
        Returns:
            True if valid, False otherwise
        """
        if pd.isna(status) or not status:
            return False
        
        normalized = str(status).strip().upper()
        return normalized in self.VALID_STATUSES
    
    def transform_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to transaction data.
        
        Args:
            df: Input DataFrame with transaction data
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} transaction records")
        
        # Create a copy to avoid modifying original
        transformed_df = df.copy()
        
        # Transform decimal amounts
        amount_fields = ['transaction_amount', 'fee_amount', 'net_amount']
        for field in amount_fields:
            if field in transformed_df.columns:
                logger.info(f"Transforming decimal field: {field}")
                transformed_df[f'{field}_decimal'] = transformed_df[field].apply(
                    self.transform_decimal_precision
                )
        
        # Transform datetime fields
        datetime_fields = ['transaction_date', 'settlement_date']
        for field in datetime_fields:
            if field in transformed_df.columns:
                logger.info(f"Transforming datetime field: {field}")
                transformed_df[f'{field}_formatted'] = transformed_df[field].apply(
                    self.transform_datetime
                )
        
        # Validate transaction types
        if 'transaction_type' in transformed_df.columns:
            logger.info("Validating transaction types")
            transformed_df['transaction_type_valid'] = transformed_df['transaction_type'].apply(
                self.validate_transaction_type
            )
            transformed_df['transaction_type_normalized'] = transformed_df['transaction_type'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else None
            )
        
        # Validate transaction statuses
        if 'transaction_status' in transformed_df.columns:
            logger.info("Validating transaction statuses")
            transformed_df['transaction_status_valid'] = transformed_df['transaction_status'].apply(
                self.validate_transaction_status
            )
            transformed_df['transaction_status_normalized'] = transformed_df['transaction_status'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else None
            )
        
        # Calculate derived fields
        if all(f in transformed_df.columns for f in ['transaction_amount_decimal', 'fee_amount_decimal']):
            logger.info("Calculating net amounts")
            transformed_df['calculated_net_amount'] = transformed_df.apply(
                lambda row: (row['transaction_amount_decimal'] - row['fee_amount_decimal'])
                if row['transaction_amount_decimal'] is not None and row['fee_amount_decimal'] is not None
                else None,
                axis=1
            )
        
        # Add audit fields
        transformed_df['transform_timestamp'] = datetime.now(timezone.utc).isoformat()
        transformed_df['transform_version'] = self.config.get('version', '1.0.0')
        
        logger.info(f"Transformation complete: {len(transformed_df)} records")
        return transformed_df
    
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to customer data.
        
        Args:
            df: Input DataFrame with customer data
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} customer records")
        
        transformed_df = df.copy()
        
        # Standardize email addresses
        if 'email' in transformed_df.columns:
            transformed_df['email_normalized'] = transformed_df['email'].apply(
                lambda x: str(x).strip().lower() if pd.notna(x) else None
            )
        
        # Standardize phone numbers (remove non-numeric characters)
        if 'phone' in transformed_df.columns:
            transformed_df['phone_normalized'] = transformed_df['phone'].apply(
                lambda x: re.sub(r'\D', '', str(x)) if pd.notna(x) else None
            )
        
        # Transform registration date
        if 'registration_date' in transformed_df.columns:
            transformed_df['registration_date_formatted'] = transformed_df['registration_date'].apply(
                self.transform_datetime
            )
        
        # Standardize status
        if 'status' in transformed_df.columns:
            transformed_df['status_normalized'] = transformed_df['status'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else None
            )
        
        # Create full name
        if 'first_name' in transformed_df.columns and 'last_name' in transformed_df.columns:
            transformed_df['full_name'] = transformed_df.apply(
                lambda row: f"{row['first_name']} {row['last_name']}".strip()
                if pd.notna(row['first_name']) and pd.notna(row['last_name'])
                else None,
                axis=1
            )
        
        # Add audit fields
        transformed_df['transform_timestamp'] = datetime.now(timezone.utc).isoformat()
        transformed_df['transform_version'] = self.config.get('version', '1.0.0')
        
        logger.info(f"Customer transformation complete: {len(transformed_df)} records")
        return transformed_df
    
    def get_transformation_metrics(self, original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate transformation quality metrics.
        
        Args:
            original_df: Original DataFrame before transformation
            transformed_df: Transformed DataFrame
            
        Returns:
            Dictionary containing transformation metrics
        """
        metrics = {
            'original_record_count': len(original_df),
            'transformed_record_count': len(transformed_df),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Count validation failures
        validation_columns = [col for col in transformed_df.columns if col.endswith('_valid')]
        for col in validation_columns:
            invalid_count = (~transformed_df[col]).sum()
            metrics[f'{col}_failures'] = int(invalid_count)
        
        # Count null transformations
        decimal_columns = [col for col in transformed_df.columns if col.endswith('_decimal')]
        for col in decimal_columns:
            null_count = transformed_df[col].isnull().sum()
            metrics[f'{col}_nulls'] = int(null_count)
        
        logger.info(f"Transformation metrics: {metrics}")
        return metrics


===FILE: src/load.py===
"""
Load module for customer transaction processing.
Handles data loading to target systems with error handling.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
import yaml
import json

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data to target destinations."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize loader with configuration."""
        self.config = self._load_config(config_path)
        self.target_config = self.config.get('targets', {})
        self.error_handling = self.config.get('error_handling', {})
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def load_to_csv(
        self, 
        df: pd.DataFrame, 
        target_name: str,
        file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Load DataFrame to CSV file.
        
        Args:
            df: DataFrame to load
            target_name: Name of target for configuration lookup
            file_path: Override path (uses config if None)
            
        Returns:
            Dictionary containing load results
        """
        if file_path is None:
            file_path = self.target_config.get(target_name, {}).get('path')
        
        if not file_path:
            raise ValueError(f"Target path not configured for {target_name}")
        
        logger.info(f"Loading {len(df)} records to {file_path}")
        
        try:
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Convert Decimal columns to string for CSV output
            df_output = df.copy()
            for col in df_output.columns:
                if df_output[col].dtype == object:
                    # Check if column contains Decimal objects
                    if df_output[col].apply(lambda x: isinstance(x, type(x)) and 'Decimal' in str(type(x))).any():
                        df_output[col] = df_output[col].apply(
                            lambda x: str(x) if x is not None else None
                        )
            
            # Write to CSV
            df_output.to_csv(
                file_path,
                index=False,
                encoding=self.target_config.get(target_name, {}).get('encoding', 'utf-8'),
                date_format=self.target_config.get(target_name, {}).get('date_format', '%Y-%m-%d %H:%M:%S')
            )
            
            result = {
                'status': 'success',
                'target': target_name,
                'file_path': file_path,
                'record_count': len(df),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Successfully loaded {len(df)} records to {file_path}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to load data to {file_path}: {e}")
            return {
                'status': 'error',
                'target': target_name,
                'file_path': file_path,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def load_to_parquet(
        self, 
        df: pd.DataFrame, 
        target_name: str,
        file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Load DataFrame to Parquet file.
        
        Args:
            df: DataFrame to load
            target_name: Name of target for configuration lookup
            file_path: Override path (uses config if None)
            
        Returns:
            Dictionary containing load results
        """
        if file_path is None:
            file_path = self.target_config.get(target_name, {}).get('path')
        
        if not file_path:
            raise ValueError(f"Target path not configured for {target_name}")
        
        logger.info(f"Loading {len(df)} records to {file_path}")
        
        try:
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Convert Decimal to float for Parquet
            df_output = df.copy()
            for col in df_output.columns:
                if df_output[col].dtype == object:
                    if df_output[col].apply(lambda x: isinstance(x, type(x)) and 'Decimal' in str(type(x))).any():
                        df_output[col] = df_output[col].apply(
                            lambda x: float(x) if x is not None else None
                        )
            
            # Write to Parquet
            df_output.to_parquet(
                file_path,
                index=False,
                compression=self.target_config.get(target_name, {}).get('compression', 'snappy')
            )
            
            result = {
                'status': 'success',
                'target': target_name,
                'file_path': file_path,
                'record_count': len(df),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Successfully loaded {len(df)} records to {file_path}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to load data to {file_path}: {e}")
            return {
                'status': 'error',
                'target': target_name,
                'file_path': file_path,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def load_error_records(
        self, 
        df: pd.DataFrame, 
        error_type: str,
        file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Load error records to error handling destination.
        
        Args:
            df: DataFrame containing error records
            error_type: Type of error for categorization
            file_path: Override path (uses config if None)
            
        Returns:
            Dictionary containing load results
        """
        if file_path is None:
            error_dir = self.error_handling.get('error_directory', 'data/errors')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = f"{error_dir}/{error_type}_{timestamp}.csv"
        
        logger.info(f"Loading {len(df)} error records to {file_path}")
        
        try:
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Add error metadata
            df_error = df.copy()
            df_error['error_type'] = error_type
            df_error['error_timestamp'] = datetime.now().isoformat()
            
            # Write to CSV
            df_error.to_csv(file_path, index=False, encoding='utf-8')
            
            result = {
                'status': 'success',
                'error_type': error_type,
                'file_path': file_path,
                'record_count': len(df),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Successfully loaded {len(df)} error records to {file_path}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to load error records to {file_path}: {e}")
            return {
                'status': 'error',
                'error_type': error_type,
                'file_path': file_path,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def load_audit_log(self, audit_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load audit log entry.
        
        Args:
            audit_data: Dictionary containing audit information
            
        Returns:
            Dictionary containing load results
        """
        audit_file = self.error_handling.get('audit_log', 'data/audit/audit.jsonl')
        
        logger.info(f"Writing audit log entry to {audit_file}")
        
        try:
            # Ensure directory exists
            Path(audit_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Append to JSONL file
            with open(audit_file, 'a') as f:
                json.dump(audit_data, f)
                f.write('\n')
            
            result = {
                'status': 'success',
                'audit_file': audit_file,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Successfully wrote audit log entry")
            return result
            
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
            return {
                'status': 'error',
                'audit_file': audit_file,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def validate_load(self, df: pd.DataFrame, load_result: Dict[str, Any]) -> bool:
        """
        Validate that load completed successfully.
        
        Args:
            df: Original DataFrame that was loaded
            load_result: Result dictionary from load operation
            
        Returns:
            True if validation passes, False otherwise
        """
        if load_result.get('status') != 'success':
            logger.error(f"Load failed: {load_result.get('error')}")
            return False
        
        expected_count = len(df)
        actual_count = load_result.get('record_count', 0)
        
        if expected_count != actual_count:
            logger.error(f"Record count mismatch: expected {expected_count}, got {actual_count}")
            return False
        
        logger.info(f"Load validation passed: {actual_count} records")
        return True


===FILE: src/nifi_integration.py===
"""
NiFi integration module for customer transaction processing.
Manages NiFi processor groups and flow deployment.
"""

import logging
from typing import Dict, List, Optional, Any
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessorDTO
import yaml
import time

logger = logging.getLogger(__name__)


class NiFiFlowManager:
    """Manages NiFi flow deployment and configuration."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize NiFi manager with configuration."""
        self.config = self._load_config(config_path)
        self.nifi_config = self.config.get('nifi', {})
        self._setup_nifi_connection()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def _setup_nifi_connection(self):
        """Setup connection to NiFi instance."""
        try:
            nifi_url = self.nifi_config.get('url', 'http://localhost:8080/nifi-api')
            nipyapi.config.nifi_config.host = nifi_url
            
            # Test connection
            nipyapi.canvas.get_root_pg_id()
            logger.info(f"Successfully connected to NiFi at {nifi_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to NiFi: {e}")
            raise
    
    def create_processor_group(
        self, 
        group_name: str, 
        parent_id: Optional[str] = None
    ) -> Any:
        """
        Create a processor group for transaction processing.
        
        Args:
            group_name: Name of the processor group
            parent_id: Parent process group ID (uses root if None)
            
        Returns:
            Created processor group object
        """
        if parent_id is None:
            parent_id = nipyapi.canvas.get_root_pg_id()
        
        logger.info(f"Creating processor group: {group_name}")
        
        try:
            # Check if group already exists
            existing_pg = nipyapi.canvas.get_process_group(group_name, 'name')
            if existing_pg:
                logger.info(f"Processor group '{group_name}' already exists")
                return existing_pg
            
            # Create new processor group
            pg = nipyapi.canvas.create_process_group(
                parent_id=parent_id,