"""
Extract module for Customer and Sales Order data from source systems.
Handles data extraction with error handling and logging.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import csv
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts data from various source systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the DataExtractor.
        
        Args:
            config: Configuration dictionary containing source settings
        """
        self.config = config
        self.source_config = config.get('sources', {})
        
    def extract_customers(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Extract customer master data from source file.
        
        Args:
            source_path: Path to the source customer data file
            
        Returns:
            List of customer records as dictionaries
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If data format is invalid
        """
        logger.info(f"Extracting customer data from {source_path}")
        
        try:
            customers = []
            with open(source_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    customer = self._validate_customer_record(row)
                    if customer:
                        customers.append(customer)
                        
            logger.info(f"Successfully extracted {len(customers)} customer records")
            return customers
            
        except FileNotFoundError:
            logger.error(f"Source file not found: {source_path}")
            raise
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
            
    def extract_customer_addresses(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Extract customer address data from source file.
        
        Args:
            source_path: Path to the source address data file
            
        Returns:
            List of address records as dictionaries
        """
        logger.info(f"Extracting customer address data from {source_path}")
        
        try:
            addresses = []
            with open(source_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    address = self._validate_address_record(row)
                    if address:
                        addresses.append(address)
                        
            logger.info(f"Successfully extracted {len(addresses)} address records")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting address data: {str(e)}")
            raise
            
    def extract_customer_transactions(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Extract customer transaction data from source file.
        
        Args:
            source_path: Path to the source transaction data file
            
        Returns:
            List of transaction records as dictionaries
        """
        logger.info(f"Extracting customer transaction data from {source_path}")
        
        try:
            transactions = []
            with open(source_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    transaction = self._validate_transaction_record(row)
                    if transaction:
                        transactions.append(transaction)
                        
            logger.info(f"Successfully extracted {len(transactions)} transaction records")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transaction data: {str(e)}")
            raise
            
    def extract_sales_orders(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Extract sales order data from source file.
        
        Args:
            source_path: Path to the source sales order data file
            
        Returns:
            List of sales order records as dictionaries
        """
        logger.info(f"Extracting sales order data from {source_path}")
        
        try:
            orders = []
            with open(source_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    order = self._validate_sales_order_record(row)
                    if order:
                        orders.append(order)
                        
            logger.info(f"Successfully extracted {len(orders)} sales order records")
            return orders
            
        except Exception as e:
            logger.error(f"Error extracting sales order data: {str(e)}")
            raise
            
    def _validate_customer_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and clean customer record."""
        try:
            required_fields = ['customer_id', 'first_name', 'last_name']
            for field in required_fields:
                if not record.get(field):
                    logger.warning(f"Missing required field {field} in customer record")
                    return None
                    
            return {
                'customer_id': record['customer_id'].strip(),
                'first_name': record['first_name'].strip(),
                'last_name': record['last_name'].strip(),
                'email': record.get('email', '').strip(),
                'phone': record.get('phone', '').strip(),
                'address_line1': record.get('address_line1', '').strip(),
                'address_line2': record.get('address_line2', '').strip(),
                'city': record.get('city', '').strip(),
                'state': record.get('state', '').strip(),
                'zip_code': record.get('zip_code', '').strip(),
                'country': record.get('country', '').strip(),
                'registration_date': record.get('registration_date', ''),
                'status': record.get('status', 'ACTIVE').strip()
            }
        except Exception as e:
            logger.error(f"Error validating customer record: {str(e)}")
            return None
            
    def _validate_address_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and clean address record."""
        try:
            required_fields = ['address_id', 'customer_id']
            for field in required_fields:
                if not record.get(field):
                    logger.warning(f"Missing required field {field} in address record")
                    return None
                    
            return {
                'address_id': record['address_id'].strip(),
                'customer_id': record['customer_id'].strip(),
                'address_type': record.get('address_type', 'PRIMARY').strip(),
                'address_line1': record.get('address_line1', '').strip(),
                'address_line2': record.get('address_line2', '').strip(),
                'city': record.get('city', '').strip(),
                'state': record.get('state', '').strip(),
                'zip_code': record.get('zip_code', '').strip(),
                'country': record.get('country', '').strip()
            }
        except Exception as e:
            logger.error(f"Error validating address record: {str(e)}")
            return None
            
    def _validate_transaction_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and clean transaction record."""
        try:
            required_fields = ['transaction_id', 'customer_id', 'amount']
            for field in required_fields:
                if not record.get(field):
                    logger.warning(f"Missing required field {field} in transaction record")
                    return None
                    
            return {
                'transaction_id': record['transaction_id'].strip(),
                'customer_id': record['customer_id'].strip(),
                'transaction_date': record.get('transaction_date', ''),
                'amount': float(record['amount']),
                'transaction_type': record.get('transaction_type', '').strip(),
                'status': record.get('status', 'COMPLETED').strip()
            }
        except Exception as e:
            logger.error(f"Error validating transaction record: {str(e)}")
            return None
            
    def _validate_sales_order_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and clean sales order record."""
        try:
            required_fields = ['order_id', 'customer_id', 'order_total']
            for field in required_fields:
                if not record.get(field):
                    logger.warning(f"Missing required field {field} in sales order record")
                    return None
                    
            return {
                'order_id': record['order_id'].strip(),
                'customer_id': record['customer_id'].strip(),
                'order_date': record.get('order_date', ''),
                'order_total': float(record['order_total']),
                'order_status': record.get('order_status', 'PENDING').strip(),
                'shipping_address_id': record.get('shipping_address_id', '').strip(),
                'billing_address_id': record.get('billing_address_id', '').strip()
            }
        except Exception as e:
            logger.error(f"Error validating sales order record: {str(e)}")
            return None