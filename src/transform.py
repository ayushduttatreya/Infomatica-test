"""
Transform module for Customer and Sales Order data processing.
Applies business rules, data quality checks, and transformations.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms and enriches customer and sales order data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the DataTransformer.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transformations', {})
        
    def transform_customers(self, customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform customer master data.
        
        Args:
            customers: List of raw customer records
            
        Returns:
            List of transformed customer records
        """
        logger.info(f"Transforming {len(customers)} customer records")
        
        transformed = []
        for customer in customers:
            try:
                transformed_customer = self._transform_customer_record(customer)
                if transformed_customer:
                    transformed.append(transformed_customer)
            except Exception as e:
                logger.error(f"Error transforming customer {customer.get('customer_id')}: {str(e)}")
                
        logger.info(f"Successfully transformed {len(transformed)} customer records")
        return transformed
        
    def transform_customer_addresses(self, addresses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform customer address data.
        
        Args:
            addresses: List of raw address records
            
        Returns:
            List of transformed address records
        """
        logger.info(f"Transforming {len(addresses)} address records")
        
        transformed = []
        for address in addresses:
            try:
                transformed_address = self._transform_address_record(address)
                if transformed_address:
                    transformed.append(transformed_address)
            except Exception as e:
                logger.error(f"Error transforming address {address.get('address_id')}: {str(e)}")
                
        logger.info(f"Successfully transformed {len(transformed)} address records")
        return transformed
        
    def transform_customer_transactions(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform customer transaction data.
        
        Args:
            transactions: List of raw transaction records
            
        Returns:
            List of transformed transaction records
        """
        logger.info(f"Transforming {len(transactions)} transaction records")
        
        transformed = []
        for transaction in transactions:
            try:
                transformed_transaction = self._transform_transaction_record(transaction)
                if transformed_transaction:
                    transformed.append(transformed_transaction)
            except Exception as e:
                logger.error(f"Error transforming transaction {transaction.get('transaction_id')}: {str(e)}")
                
        logger.info(f"Successfully transformed {len(transformed)} transaction records")
        return transformed
        
    def transform_sales_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform sales order data.
        
        Args:
            orders: List of raw sales order records
            
        Returns:
            List of transformed sales order records
        """
        logger.info(f"Transforming {len(orders)} sales order records")
        
        transformed = []
        for order in orders:
            try:
                transformed_order = self._transform_sales_order_record(order)
                if transformed_order:
                    transformed.append(transformed_order)
            except Exception as e:
                logger.error(f"Error transforming order {order.get('order_id')}: {str(e)}")
                
        logger.info(f"Successfully transformed {len(transformed)} sales order records")
        return transformed
        
    def _transform_customer_record(self, customer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply transformations to customer record."""
        transformed = customer.copy()
        
        # Standardize name fields
        transformed['first_name'] = self._standardize_name(customer['first_name'])
        transformed['last_name'] = self._standardize_name(customer['last_name'])
        transformed['full_name'] = f"{transformed['first_name']} {transformed['last_name']}"
        
        # Validate and standardize email
        if customer.get('email'):
            transformed['email'] = self._standardize_email(customer['email'])
            transformed['email_valid'] = self._validate_email(transformed['email'])
        else:
            transformed['email_valid'] = False
            
        # Standardize phone
        if customer.get('phone'):
            transformed['phone'] = self._standardize_phone(customer['phone'])
            
        # Standardize address
        transformed['address_line1'] = self._standardize_address(customer.get('address_line1', ''))
        transformed['address_line2'] = self._standardize_address(customer.get('address_line2', ''))
        transformed['city'] = self._standardize_name(customer.get('city', ''))
        transformed['state'] = customer.get('state', '').upper()
        transformed['zip_code'] = self._standardize_zip(customer.get('zip_code', ''))
        transformed['country'] = customer.get('country', 'US').upper()
        
        # Parse registration date
        if customer.get('registration_date'):
            transformed['registration_date'] = self._parse_date(customer['registration_date'])
            
        # Add metadata
        transformed['processed_timestamp'] = datetime.now().isoformat()
        transformed['data_quality_score'] = self._calculate_customer_quality_score(transformed)
        
        return transformed
        
    def _transform_address_record(self, address: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply transformations to address record."""
        transformed = address.copy()
        
        # Standardize address fields
        transformed['address_line1'] = self._standardize_address(address.get('address_line1', ''))
        transformed['address_line2'] = self._standardize_address(address.get('address_line2', ''))
        transformed['city'] = self._standardize_name(address.get('city', ''))
        transformed['state'] = address.get('state', '').upper()
        transformed['zip_code'] = self._standardize_zip(address.get('zip_code', ''))
        transformed['country'] = address.get('country', 'US').upper()
        
        # Validate address type
        valid_types = ['PRIMARY', 'BILLING', 'SHIPPING', 'OTHER']
        address_type = address.get('address_type', 'PRIMARY').upper()
        transformed['address_type'] = address_type if address_type in valid_types else 'OTHER'
        
        # Add metadata
        transformed['processed_timestamp'] = datetime.now().isoformat()
        transformed['address_complete'] = self._is_address_complete(transformed)
        
        return transformed
        
    def _transform_transaction_record(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply transformations to transaction record."""
        transformed = transaction.copy()
        
        # Parse transaction date
        if transaction.get('transaction_date'):
            transformed['transaction_date'] = self._parse_date(transaction['transaction_date'])
            
        # Standardize amount
        transformed['amount'] = round(float(transaction['amount']), 2)
        
        # Categorize transaction
        transformed['transaction_category'] = self._categorize_transaction(
            transaction.get('transaction_type', ''),
            transformed['amount']
        )
        
        # Add metadata
        transformed['processed_timestamp'] = datetime.now().isoformat()
        
        return transformed
        
    def _transform_sales_order_record(self, order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply transformations to sales order record."""
        transformed = order.copy()
        
        # Parse order date
        if order.get('order_date'):
            transformed['order_date'] = self._parse_date(order['order_date'])
            
        # Standardize order total
        transformed['order_total'] = round(float(order['order_total']), 2)
        
        # Validate order status
        valid_statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
        order_status = order.get('order_status', 'PENDING').upper()
        transformed['order_status'] = order_status if order_status in valid_statuses else 'PENDING'
        
        # Calculate order priority
        transformed['order_priority'] = self._calculate_order_priority(transformed)
        
        # Add metadata
        transformed['processed_timestamp'] = datetime.now().isoformat()
        
        return transformed
        
    def _standardize_name(self, name: str) -> str:
        """Standardize name field."""
        if not name:
            return ''
        return ' '.join(word.capitalize() for word in name.strip().split())
        
    def _standardize_email(self, email: str) -> str:
        """Standardize email address."""
        return email.strip().lower()
        
    def _validate_email(self, email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
        
    def _standardize_phone(self, phone: str) -> str:
        """Standardize phone number."""
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        return phone
        
    def _standardize_address(self, address: str) -> str:
        """Standardize address field."""
        if not address:
            return ''
        return ' '.join(address.strip().split())
        
    def _standardize_zip(self, zip_code: str) -> str:
        """Standardize ZIP code."""
        digits = re.sub(r'\D', '', zip_code)
        if len(digits) == 5:
            return digits
        elif len(digits) == 9:
            return f"{digits[:5]}-{digits[5:]}"
        return zip_code
        
    def _parse_date(self, date_str: str) -> str:
        """Parse and standardize date string."""
        try:
            formats = ['%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return date_str
        except Exception:
            return date_str
            
    def _calculate_customer_quality_score(self, customer: Dict[str, Any]) -> float:
        """Calculate data quality score for customer record."""
        score = 0.0
        max_score = 10.0
        
        if customer.get('email_valid'):
            score += 2.0
        if customer.get('phone'):
            score += 1.5
        if customer.get('address_line1'):
            score += 2.0
        if customer.get('city') and customer.get('state'):
            score += 2.0
        if customer.get('zip_code'):
            score += 1.5
        if customer.get('registration_date'):
            score += 1.0
            
        return round(score / max_score * 100, 2)
        
    def _is_address_complete(self, address: Dict[str, Any]) -> bool:
        """Check if address has all required fields."""
        required = ['address_line1', 'city', 'state', 'zip_code', 'country']
        return all(address.get(field) for field in required)
        
    def _categorize_transaction(self, transaction_type: str, amount: float) -> str:
        """Categorize transaction based on type and amount."""
        if amount < 0:
            return 'REFUND'
        elif amount > 1000:
            return 'HIGH_VALUE'
        elif transaction_type.upper() in ['PURCHASE', 'SALE']:
            return 'STANDARD_PURCHASE'
        else:
            return 'OTHER'
            
    def _calculate_order_priority(self, order: Dict[str, Any]) -> str:
        """Calculate order priority based on total and status."""
        total = order.get('order_total', 0)
        status = order.get('order_status', '')
        
        if status == 'CANCELLED':
            return 'LOW'
        elif total > 5000:
            return 'HIGH'
        elif total > 1000:
            return 'MEDIUM'
        else:
            return 'LOW'