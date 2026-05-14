"""
Extract module for Sales Order Data Quality Checks
Reads sales order data from source systems
"""
import logging
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class SalesOrderExtractor:
    """Extracts sales order data from source files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration
        
        Args:
            config: Configuration dictionary containing source settings
        """
        self.config = config
        self.source_config = config.get('source', {})
        
    def extract_customers(self, file_path: str) -> pd.DataFrame:
        """
        Extract customer data from source file
        
        Args:
            file_path: Path to customer source file
            
        Returns:
            DataFrame containing customer data
        """
        try:
            logger.info(f"Extracting customer data from {file_path}")
            
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
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_sales_orders(self, file_path: str) -> pd.DataFrame:
        """
        Extract sales order data from source file
        
        Args:
            file_path: Path to sales order source file
            
        Returns:
            DataFrame containing sales order data
        """
        try:
            logger.info(f"Extracting sales order data from {file_path}")
            
            df = pd.read_csv(
                file_path,
                dtype={
                    'order_id': str,
                    'customer_id': str,
                    'order_status': str,
                    'payment_method': str,
                    'shipping_method': str,
                    'currency': str
                },
                parse_dates=['order_date', 'ship_date', 'delivery_date']
            )
            
            # Convert numeric fields
            numeric_fields = ['order_amount', 'tax_amount', 'shipping_cost', 'discount_amount', 'total_amount']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            logger.info(f"Extracted {len(df)} sales order records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting sales order data: {str(e)}")
            raise
    
    def extract_order_line_items(self, file_path: str) -> pd.DataFrame:
        """
        Extract order line item data from source file
        
        Args:
            file_path: Path to order line items source file
            
        Returns:
            DataFrame containing order line item data
        """
        try:
            logger.info(f"Extracting order line items from {file_path}")
            
            df = pd.read_csv(
                file_path,
                dtype={
                    'line_item_id': str,
                    'order_id': str,
                    'product_id': str,
                    'product_name': str,
                    'sku': str
                }
            )
            
            # Convert numeric fields
            numeric_fields = ['quantity', 'unit_price', 'discount_percent', 'line_total']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            logger.info(f"Extracted {len(df)} order line item records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting order line items: {str(e)}")
            raise