"""
Load module for Customer and Sales Order data to target systems.
Handles data loading with error handling and transaction management.
"""

import logging
from typing import Dict, List, Any, Optional
import csv
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data to target systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the DataLoader.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.target_config = config.get('targets', {})
        
    def load_customers(self, customers: List[Dict[str, Any]], target_path: str) -> int:
        """
        Load customer data to target system.
        
        Args:
            customers: List of transformed customer records
            target_path: Path to target file or database
            
        Returns:
            Number of records successfully loaded
        """
        logger.info(f"Loading {len(customers)} customer records to {target_path}")
        
        try:
            loaded_count = 0
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(target_path, 'w', newline='', encoding='utf-8') as f:
                if customers:
                    fieldnames = customers[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for customer in customers:
                        try:
                            writer.writerow(customer)
                            loaded_count += 1
                        except Exception as e:
                            logger.error(f"Error loading customer {customer.get('customer_id')}: {str(e)}")
                            
            logger.info(f"Successfully loaded {loaded_count} customer records")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading customer data: {str(e)}")
            raise
            
    def load_customer_addresses(self, addresses: List[Dict[str, Any]], target_path: str) -> int:
        """
        Load customer address data to target system.
        
        Args:
            addresses: List of transformed address records
            target_path: Path to target file or database
            
        Returns:
            Number of records successfully loaded
        """
        logger.info(f"Loading {len(addresses)} address records to {target_path}")
        
        try:
            loaded_count = 0
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(target_path, 'w', newline='', encoding='utf-8') as f:
                if addresses:
                    fieldnames = addresses[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for address in addresses:
                        try:
                            writer.writerow(address)
                            loaded_count += 1
                        except Exception as e:
                            logger.error(f"Error loading address {address.get('address_id')}: {str(e)}")
                            
            logger.info(f"Successfully loaded {loaded_count} address records")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading address data: {str(e)}")
            raise
            
    def load_customer_transactions(self, transactions: List[Dict[str, Any]], target_path: str) -> int:
        """
        Load customer transaction data to target system.
        
        Args:
            transactions: List of transformed transaction records
            target_path: Path to target file or database
            
        Returns:
            Number of records successfully loaded
        """
        logger.info(f"Loading {len(transactions)} transaction records to {target_path}")
        
        try:
            loaded_count = 0
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(target_path, 'w', newline='', encoding='utf-8') as f:
                if transactions:
                    fieldnames = transactions[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for transaction in transactions:
                        try:
                            writer.writerow(transaction)
                            loaded_count += 1
                        except Exception as e:
                            logger.error(f"Error loading transaction {transaction.get('transaction_id')}: {str(e)}")
                            
            logger.info(f"Successfully loaded {loaded_count} transaction records")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading transaction data: {str(e)}")
            raise
            
    def load_sales_orders(self, orders: List[Dict[str, Any]], target_path: str) -> int:
        """
        Load sales order data to target system.
        
        Args:
            orders: List of transformed sales order records
            target_path: Path to target file or database
            
        Returns:
            Number of records successfully loaded
        """
        logger.info(f"Loading {len(orders)} sales order records to {target_path}")
        
        try:
            loaded_count = 0
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(target_path, 'w', newline='', encoding='utf-8') as f:
                if orders:
                    fieldnames = orders[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for order in orders:
                        try:
                            writer.writerow(order)
                            loaded_count += 1
                        except Exception as e:
                            logger.error(f"Error loading order {order.get('order_id')}: {str(e)}")
                            
            logger.info(f"Successfully loaded {loaded_count} sales order records")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading sales order data: {str(e)}")
            raise
            
    def load_error_records(self, errors: List[Dict[str, Any]], error_path: str) -> int:
        """
        Load error records to error file.
        
        Args:
            errors: List of error records
            error_path: Path to error file
            
        Returns:
            Number of error records logged
        """
        logger.info(f"Logging {len(errors)} error records to {error_path}")
        
        try:
            Path(error_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(error_path, 'w', encoding='utf-8') as f:
                json.dump(errors, f, indent=2)
                
            logger.info(f"Successfully logged {len(errors)} error records")
            return len(errors)
            
        except Exception as e:
            logger.error(f"Error logging error records: {str(e)}")
            raise