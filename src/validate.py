"""
Validation module for Sales Order Data Quality Checks
Implements comprehensive data quality validation framework
"""
import logging
from typing import Dict, Any, List, Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results"""
    
    def __init__(self):
        self.passed = []
        self.failed = []
        self.warnings = []
        self.error_records = []
        
    def add_pass(self, rule: str, message: str):
        """Add passed validation"""
        self.passed.append({'rule': rule, 'message': message})
        
    def add_fail(self, rule: str, message: str, record_ids: List[str] = None):
        """Add failed validation"""
        self.failed.append({
            'rule': rule,
            'message': message,
            'record_ids': record_ids or []
        })
        
    def add_warning(self, rule: str, message: str):
        """Add validation warning"""
        self.warnings.append({'rule': rule, 'message': message})
        
    def is_valid(self) -> bool:
        """Check if all validations passed"""
        return len(self.failed) == 0
    
    def get_summary(self) -> Dict[str, Any]:
        """Get validation summary"""
        return {
            'total_rules': len(self.passed) + len(self.failed),
            'passed': len(self.passed),
            'failed': len(self.failed),
            'warnings': len(self.warnings),
            'is_valid': self.is_valid()
        }


class SalesOrderValidator:
    """Validates sales order data quality"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize validator with configuration
        
        Args:
            config: Configuration dictionary containing validation rules
        """
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
        
    def validate_sales_orders(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, ValidationResult]:
        """
        Validate sales order data
        
        Args:
            df: DataFrame containing sales order data
            
        Returns:
            Tuple of (valid_records, invalid_records, validation_result)
        """
        result = ValidationResult()
        df_copy = df.copy()
        df_copy['validation_errors'] = ''
        df_copy['is_valid'] = True
        
        logger.info(f"Starting validation for {len(df)} sales order records")
        
        # Run all validation checks
        self._validate_null_checks(df_copy, result)
        self._validate_data_types(df_copy, result)
        self._validate_business_rules(df_copy, result)
        self._validate_referential_integrity(df_copy, result)
        self._validate_date_logic(df_copy, result)
        self._validate_amount_calculations(df_copy, result)
        self._validate_status_transitions(df_copy, result)
        
        # Split into valid and invalid records
        valid_df = df_copy[df_copy['is_valid']].copy()
        invalid_df = df_copy[~df_copy['is_valid']].copy()
        
        logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")
        
        return valid_df, invalid_df, result
    
    def _validate_null_checks(self, df: pd.DataFrame, result: ValidationResult):
        """Validate required fields are not null"""
        required_fields = self.validation_rules.get('required_fields', [])
        
        for field in required_fields:
            if field not in df.columns:
                result.add_fail(
                    'NULL_CHECK',
                    f"Required field '{field}' is missing from dataset"
                )
                continue
                
            null_mask = df[field].isna()
            null_count = null_mask.sum()
            
            if null_count > 0:
                null_ids = df[null_mask]['order_id'].tolist() if 'order_id' in df.columns else []
                df.loc[null_mask, 'validation_errors'] += f"NULL_{field};"
                df.loc[null_mask, 'is_valid'] = False
                
                result.add_fail(
                    'NULL_CHECK',
                    f"Field '{field}' has {null_count} null values",
                    null_ids[:100]  # Limit to first 100 IDs
                )
            else:
                result.add_pass(
                    'NULL_CHECK',
                    f"Field '{field}' has no null values"
                )
    
    def _validate_data_types(self, df: pd.DataFrame, result: ValidationResult):
        """Validate data types and formats"""
        
        # Validate email format
        if 'email' in df.columns:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_email_mask = ~df['email'].isna() & ~df['email'].str.match(email_pattern)
            invalid_count = invalid_email_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_email_mask, 'validation_errors'] += 'INVALID_EMAIL;'
                df.loc[invalid_email_mask, 'is_valid'] = False
                result.add_fail(
                    'DATA_TYPE',
                    f"Found {invalid_count} invalid email formats"
                )
            else:
                result.add_pass('DATA_TYPE', 'All email formats are valid')
        
        # Validate phone format
        if 'phone' in df.columns:
            phone_pattern = r'^\+?1?\d{10,15}$'
            invalid_phone_mask = ~df['phone'].isna() & ~df['phone'].str.replace(r'[\s\-\(\)]', '', regex=True).str.match(phone_pattern)
            invalid_count = invalid_phone_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_phone_mask, 'validation_errors'] += 'INVALID_PHONE;'
                df.loc[invalid_phone_mask, 'is_valid'] = False
                result.add_fail(
                    'DATA_TYPE',
                    f"Found {invalid_count} invalid phone formats"
                )
            else:
                result.add_pass('DATA_TYPE', 'All phone formats are valid')
        
        # Validate numeric fields
        numeric_fields = ['order_amount', 'tax_amount', 'shipping_cost', 'discount_amount', 'total_amount']
        for field in numeric_fields:
            if field in df.columns:
                non_numeric_mask = ~df[field].isna() & ~pd.to_numeric(df[field], errors='coerce').notna()
                invalid_count = non_numeric_mask.sum()
                
                if invalid_count > 0:
                    df.loc[non_numeric_mask, 'validation_errors'] += f'INVALID_NUMERIC_{field};'
                    df.loc[non_numeric_mask, 'is_valid'] = False
                    result.add_fail(
                        'DATA_TYPE',
                        f"Field '{field}' has {invalid_count} non-numeric values"
                    )
                else:
                    result.add_pass('DATA_TYPE', f"Field '{field}' has valid numeric values")
    
    def _validate_business_rules(self, df: pd.DataFrame, result: ValidationResult):
        """Validate business rules"""
        business_rules = self.validation_rules.get('business_rules', {})
        
        # Validate order amount is positive
        if 'order_amount' in df.columns:
            min_amount = business_rules.get('min_order_amount', 0)
            invalid_amount_mask = ~df['order_amount'].isna() & (df['order_amount'] < min_amount)
            invalid_count = invalid_amount_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_amount_mask, 'validation_errors'] += 'INVALID_ORDER_AMOUNT;'
                df.loc[invalid_amount_mask, 'is_valid'] = False
                result.add_fail(
                    'BUSINESS_RULE',
                    f"Found {invalid_count} orders with amount < {min_amount}"
                )
            else:
                result.add_pass('BUSINESS_RULE', f'All order amounts >= {min_amount}')
        
        # Validate order status
        if 'order_status' in df.columns:
            valid_statuses = business_rules.get('valid_order_statuses', [])
            if valid_statuses:
                invalid_status_mask = ~df['order_status'].isna() & ~df['order_status'].isin(valid_statuses)
                invalid_count = invalid_status_mask.sum()
                
                if invalid_count > 0:
                    df.loc[invalid_status_mask, 'validation_errors'] += 'INVALID_STATUS;'
                    df.loc[invalid_status_mask, 'is_valid'] = False
                    result.add_fail(
                        'BUSINESS_RULE',
                        f"Found {invalid_count} orders with invalid status"
                    )
                else:
                    result.add_pass('BUSINESS_RULE', 'All order statuses are valid')
        
        # Validate currency codes
        if 'currency' in df.columns:
            valid_currencies = business_rules.get('valid_currencies', [])
            if valid_currencies:
                invalid_currency_mask = ~df['currency'].isna() & ~df['currency'].isin(valid_currencies)
                invalid_count = invalid_currency_mask.sum()
                
                if invalid_count > 0:
                    df.loc[invalid_currency_mask, 'validation_errors'] += 'INVALID_CURRENCY;'
                    df.loc[invalid_currency_mask, 'is_valid'] = False
                    result.add_fail(
                        'BUSINESS_RULE',
                        f"Found {invalid_count} orders with invalid currency"
                    )
                else:
                    result.add_pass('BUSINESS_RULE', 'All currency codes are valid')
        
        # Validate discount percentage
        if 'discount_percent' in df.columns:
            max_discount = business_rules.get('max_discount_percent', 100)
            invalid_discount_mask = ~df['discount_percent'].isna() & (
                (df['discount_percent'] < 0) | (df['discount_percent'] > max_discount)
            )
            invalid_count = invalid_discount_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_discount_mask, 'validation_errors'] += 'INVALID_DISCOUNT;'
                df.loc[invalid_discount_mask, 'is_valid'] = False
                result.add_fail(
                    'BUSINESS_RULE',
                    f"Found {invalid_count} orders with invalid discount percentage"
                )
            else:
                result.add_pass('BUSINESS_RULE', 'All discount percentages are valid')
    
    def _validate_referential_integrity(self, df: pd.DataFrame, result: ValidationResult):
        """Validate referential integrity"""
        
        # Check for duplicate order IDs
        if 'order_id' in df.columns:
            duplicate_mask = df.duplicated(subset=['order_id'], keep=False)
            duplicate_count = duplicate_mask.sum()
            
            if duplicate_count > 0:
                df.loc[duplicate_mask, 'validation_errors'] += 'DUPLICATE_ORDER_ID;'
                df.loc[duplicate_mask, 'is_valid'] = False
                result.add_fail(
                    'REFERENTIAL_INTEGRITY',
                    f"Found {duplicate_count} duplicate order IDs"
                )
            else:
                result.add_pass('REFERENTIAL_INTEGRITY', 'No duplicate order IDs found')
        
        # Validate customer_id format
        if 'customer_id' in df.columns:
            invalid_customer_mask = ~df['customer_id'].isna() & (df['customer_id'].str.len() == 0)
            invalid_count = invalid_customer_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_customer_mask, 'validation_errors'] += 'INVALID_CUSTOMER_ID;'
                df.loc[invalid_customer_mask, 'is_valid'] = False
                result.add_fail(
                    'REFERENTIAL_INTEGRITY',
                    f"Found {invalid_count} empty customer IDs"
                )
            else:
                result.add_pass('REFERENTIAL_INTEGRITY', 'All customer IDs are valid')
    
    def _validate_date_logic(self, df: pd.DataFrame, result: ValidationResult):
        """Validate date logic and relationships"""
        
        # Validate order_date is not in future
        if 'order_date' in df.columns:
            future_date_mask = ~df['order_date'].isna() & (df['order_date'] > pd.Timestamp.now())
            invalid_count = future_date_mask.sum()
            
            if invalid_count > 0:
                df.loc[future_date_mask, 'validation_errors'] += 'FUTURE_ORDER_DATE;'
                df.loc[future_date_mask, 'is_valid'] = False
                result.add_fail(
                    'DATE_LOGIC',
                    f"Found {invalid_count} orders with future order dates"
                )
            else:
                result.add_pass('DATE_LOGIC', 'No future order dates found')
        
        # Validate ship_date >= order_date
        if 'order_date' in df.columns and 'ship_date' in df.columns:
            invalid_ship_mask = (
                ~df['order_date'].isna() & 
                ~df['ship_date'].isna() & 
                (df['ship_date'] < df['order_date'])
            )
            invalid_count = invalid_ship_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_ship_mask, 'validation_errors'] += 'INVALID_SHIP_DATE;'
                df.loc[invalid_ship_mask, 'is_valid'] = False
                result.add_fail(
                    'DATE_LOGIC',
                    f"Found {invalid_count} orders with ship_date before order_date"
                )
            else:
                result.add_pass('DATE_LOGIC', 'All ship dates are valid')
        
        # Validate delivery_date >= ship_date
        if 'ship_date' in df.columns and 'delivery_date' in df.columns:
            invalid_delivery_mask = (
                ~df['ship_date'].isna() & 
                ~df['delivery_date'].isna() & 
                (df['delivery_date'] < df['ship_date'])
            )
            invalid_count = invalid_delivery_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_delivery_mask, 'validation_errors'] += 'INVALID_DELIVERY_DATE;'
                df.loc[invalid_delivery_mask, 'is_valid'] = False
                result.add_fail(
                    'DATE_LOGIC',
                    f"Found {invalid_count} orders with delivery_date before ship_date"
                )
            else:
                result.add_pass('DATE_LOGIC', 'All delivery dates are valid')
    
    def _validate_amount_calculations(self, df: pd.DataFrame, result: ValidationResult):
        """Validate amount calculations"""
        
        # Validate total_amount = order_amount + tax_amount + shipping_cost - discount_amount
        required_fields = ['order_amount', 'tax_amount', 'shipping_cost', 'discount_amount', 'total_amount']
        if all(field in df.columns for field in required_fields):
            calculated_total = (
                df['order_amount'].fillna(0) + 
                df['tax_amount'].fillna(0) + 
                df['shipping_cost'].fillna(0) - 
                df['discount_amount'].fillna(0)
            )
            
            tolerance = self.validation_rules.get('amount_tolerance', 0.01)
            invalid_total_mask = (
                ~df['total_amount'].isna() & 
                (abs(df['total_amount'] - calculated_total) > tolerance)
            )
            invalid_count = invalid_total_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_total_mask, 'validation_errors'] += 'INVALID_TOTAL_AMOUNT;'
                df.loc[invalid_total_mask, 'is_valid'] = False
                result.add_fail(
                    'AMOUNT_CALCULATION',
                    f"Found {invalid_count} orders with incorrect total amount calculation"
                )
            else:
                result.add_pass('AMOUNT_CALCULATION', 'All total amounts are correctly calculated')
        
        # Validate tax_amount is reasonable percentage of order_amount
        if 'order_amount' in df.columns and 'tax_amount' in df.columns:
            max_tax_rate = self.validation_rules.get('max_tax_rate', 0.20)
            invalid_tax_mask = (
                ~df['order_amount'].isna() & 
                ~df['tax_amount'].isna() & 
                (df['order_amount'] > 0) &
                (df['tax_amount'] / df['order_amount'] > max_tax_rate)
            )
            invalid_count = invalid_tax_mask.sum()
            
            if invalid_count > 0:
                result.add_warning(
                    'AMOUNT_CALCULATION',
                    f"Found {invalid_count} orders with tax rate > {max_tax_rate*100}%"
                )
    
    def _validate_status_transitions(self, df: pd.DataFrame, result: ValidationResult):
        """Validate status transitions are logical"""
        
        if 'order_status' in df.columns:
            # Validate cancelled orders don't have ship/delivery dates
            cancelled_mask = df['order_status'] == 'CANCELLED'
            
            if 'ship_date' in df.columns:
                invalid_cancelled_mask = cancelled_mask & ~df['ship_date'].isna()
                invalid_count = invalid_cancelled_mask.sum()
                
                if invalid_count > 0:
                    result.add_warning(
                        'STATUS_TRANSITION',
                        f"Found {invalid_count} cancelled orders with ship dates"
                    )
            
            # Validate delivered orders have all required dates
            if 'delivery_date' in df.columns:
                delivered_mask = df['order_status'] == 'DELIVERED'
                missing_delivery_mask = delivered_mask & df['delivery_date'].isna()
                invalid_count = missing_delivery_mask.sum()
                
                if invalid_count > 0:
                    df.loc[missing_delivery_mask, 'validation_errors'] += 'MISSING_DELIVERY_DATE;'
                    df.loc[missing_delivery_mask, 'is_valid'] = False
                    result.add_fail(
                        'STATUS_TRANSITION',
                        f"Found {invalid_count} delivered orders without delivery dates"
                    )
                else:
                    result.add_pass('STATUS_TRANSITION', 'All delivered orders have delivery dates')


class OrderLineItemValidator:
    """Validates order line item data quality"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize validator with configuration"""
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
    
    def validate_line_items(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, ValidationResult]:
        """
        Validate order line item data
        
        Args:
            df: DataFrame containing order line item data
            
        Returns:
            Tuple of (valid_records, invalid_records, validation_result)
        """
        result = ValidationResult()
        df_copy = df.copy()
        df_copy['validation_errors'] = ''
        df_copy['is_valid'] = True
        
        logger.info(f"Starting validation for {len(df)} order line items")
        
        # Run validation checks
        self._validate_required_fields(df_copy, result)
        self._validate_quantities(df_copy, result)
        self._validate_prices(df_copy, result)
        self._validate_line_totals(df_copy, result)
        
        # Split into valid and invalid records
        valid_df = df_copy[df_copy['is_valid']].copy()
        invalid_df = df_copy[~df_copy['is_valid']].copy()
        
        logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")
        
        return valid_df, invalid_df, result
    
    def _validate_required_fields(self, df: pd.DataFrame, result: ValidationResult):
        """Validate required fields"""
        required_fields = ['line_item_id', 'order_id', 'product_id', 'quantity', 'unit_price']
        
        for field in required_fields:
            if field not in df.columns:
                result.add_fail('REQUIRED_FIELD', f"Missing required field: {field}")
                continue
            
            null_mask = df[field].isna()
            null_count = null_mask.sum()
            
            if null_count > 0:
                df.loc[null_mask, 'validation_errors'] += f'NULL_{field};'
                df.loc[null_mask, 'is_valid'] = False
                result.add_fail('REQUIRED_FIELD', f"Field '{field}' has {null_count} null values")
            else:
                result.add_pass('REQUIRED_FIELD', f"Field '{field}' has no null values")
    
    def _validate_quantities(self, df: pd.DataFrame, result: ValidationResult):
        """Validate quantity values"""
        if 'quantity' in df.columns:
            invalid_qty_mask = ~df['quantity'].isna() & (df['quantity'] <= 0)
            invalid_count = invalid_qty_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_qty_mask, 'validation_errors'] += 'INVALID_QUANTITY;'
                df.loc[invalid_qty_mask, 'is_valid'] = False
                result.add_fail('QUANTITY', f"Found {invalid_count} line items with quantity <= 0")
            else:
                result.add_pass('QUANTITY', 'All quantities are positive')
    
    def _validate_prices(self, df: pd.DataFrame, result: ValidationResult):
        """Validate price values"""
        if 'unit_price' in df.columns:
            invalid_price_mask = ~df['unit_price'].isna() & (df['unit_price'] < 0)
            invalid_count = invalid_price_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_price_mask, 'validation_errors'] += 'INVALID_PRICE;'
                df.loc[invalid_price_mask, 'is_valid'] = False
                result.add_fail('PRICE', f"Found {invalid_count} line items with negative price")
            else:
                result.add_pass('PRICE', 'All prices are non-negative')
    
    def _validate_line_totals(self, df: pd.DataFrame, result: ValidationResult):
        """Validate line total calculations"""
        required_fields = ['quantity', 'unit_price', 'line_total']
        
        if all(field in df.columns for field in required_fields):
            calculated_total = df['quantity'] * df['unit_price']
            
            if 'discount_percent' in df.columns:
                calculated_total = calculated_total * (1 - df['discount_percent'].fillna(0) / 100)
            
            tolerance = self.validation_rules.get('amount_tolerance', 0.01)
            invalid_total_mask = (
                ~df['line_total'].isna() & 
                (abs(df['line_total'] - calculated_total) > tolerance)
            )
            invalid_count = invalid_total_mask.sum()
            
            if invalid_count > 0:
                df.loc[invalid_total_mask, 'validation_errors'] += 'INVALID_LINE_TOTAL;'
                df.loc[invalid_total_mask, 'is_valid'] = False
                result.add_fail('LINE_TOTAL', f"Found {invalid_count} line items with incorrect total")
            else:
                result.add_pass('LINE_TOTAL', 'All line totals are correctly calculated')