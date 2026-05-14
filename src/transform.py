"""
Transform module for customer transaction loading pipeline.
Handles data transformation, enrichment, and business rule application.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from hashlib import sha256

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms customer and transaction data according to business rules."""
    
    def __init__(self, config: Dict):
        """
        Initialize the data transformer.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transformation', {})
        self.business_rules = self.transform_config.get('business_rules', {})
        
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform customer data with cleansing and enrichment.
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Transformed customer DataFrame
        """
        try:
            logger.info(f"Transforming {len(df)} customer records")
            
            df_transformed = df.copy()
            
            # Standardize names
            df_transformed['first_name'] = df_transformed['first_name'].str.strip().str.title()
            df_transformed['last_name'] = df_transformed['last_name'].str.strip().str.title()
            df_transformed['full_name'] = (
                df_transformed['first_name'] + ' ' + df_transformed['last_name']
            )
            
            # Standardize email
            df_transformed['email'] = df_transformed['email'].str.lower().str.strip()
            df_transformed['email_domain'] = df_transformed['email'].str.split('@').str[1]
            
            # Format phone numbers
            df_transformed['phone'] = df_transformed['phone'].apply(self._format_phone)
            
            # Standardize addresses
            df_transformed['address_line1'] = df_transformed['address_line1'].str.strip().str.title()
            df_transformed['city'] = df_transformed['city'].str.strip().str.title()
            df_transformed['state'] = df_transformed['state'].str.upper().str.strip()
            df_transformed['country'] = df_transformed['country'].str.upper().str.strip()
            df_transformed['zip_code'] = df_transformed['zip_code'].str.strip()
            
            # Create composite address
            df_transformed['full_address'] = self._create_full_address(df_transformed)
            
            # Generate customer hash for change detection
            df_transformed['customer_hash'] = df_transformed.apply(
                lambda row: self._generate_hash(row, ['customer_id', 'email', 'phone']),
                axis=1
            )
            
            # Calculate customer tenure
            df_transformed['customer_tenure_days'] = (
                datetime.now() - pd.to_datetime(df_transformed['registration_date'])
            ).dt.days
            
            # Categorize customer by tenure
            df_transformed['customer_segment'] = df_transformed['customer_tenure_days'].apply(
                self._categorize_customer_tenure
            )
            
            # Validate status
            valid_statuses = self.business_rules.get('valid_customer_statuses', 
                                                     ['ACTIVE', 'INACTIVE', 'SUSPENDED'])
            df_transformed['status'] = df_transformed['status'].str.upper()
            df_transformed['status_valid'] = df_transformed['status'].isin(valid_statuses)
            
            # Add transformation metadata
            df_transformed['transform_timestamp'] = datetime.now()
            df_transformed['record_version'] = 1
            
            logger.info(f"Successfully transformed {len(df_transformed)} customer records")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transforming customer data: {str(e)}")
            raise
    
    def transform_transactions(self, df: pd.DataFrame, 
                               customer_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Transform transaction data with enrichment and calculations.
        
        Args:
            df: Raw transaction DataFrame
            customer_df: Optional customer DataFrame for enrichment
            
        Returns:
            Transformed transaction DataFrame
        """
        try:
            logger.info(f"Transforming {len(df)} transaction records")
            
            df_transformed = df.copy()
            
            # Ensure numeric fields
            numeric_fields = ['amount', 'tax_amount', 'discount_amount', 'shipping_amount']
            for field in numeric_fields:
                if field in df_transformed.columns:
                    df_transformed[field] = pd.to_numeric(
                        df_transformed[field], errors='coerce'
                    ).fillna(0)
            
            # Calculate total amount
            df_transformed['total_amount'] = (
                df_transformed.get('amount', 0) +
                df_transformed.get('tax_amount', 0) +
                df_transformed.get('shipping_amount', 0) -
                df_transformed.get('discount_amount', 0)
            )
            
            # Standardize transaction type and status
            df_transformed['transaction_type'] = df_transformed['transaction_type'].str.upper()
            df_transformed['status'] = df_transformed['status'].str.upper()
            
            # Categorize transaction amount
            df_transformed['amount_category'] = df_transformed['total_amount'].apply(
                self._categorize_transaction_amount
            )
            
            # Extract date components
            df_transformed['transaction_year'] = df_transformed['transaction_date'].dt.year
            df_transformed['transaction_month'] = df_transformed['transaction_date'].dt.month
            df_transformed['transaction_day'] = df_transformed['transaction_date'].dt.day
            df_transformed['transaction_quarter'] = df_transformed['transaction_date'].dt.quarter
            df_transformed['transaction_day_of_week'] = df_transformed['transaction_date'].dt.dayofweek
            df_transformed['transaction_week_of_year'] = df_transformed['transaction_date'].dt.isocalendar().week
            
            # Flag weekend transactions
            df_transformed['is_weekend'] = df_transformed['transaction_day_of_week'].isin([5, 6])
            
            # Enrich with customer data if provided
            if customer_df is not None:
                df_transformed = self._enrich_with_customer_data(df_transformed, customer_df)
            
            # Generate transaction hash
            df_transformed['transaction_hash'] = df_transformed.apply(
                lambda row: self._generate_hash(
                    row, ['transaction_id', 'customer_id', 'total_amount']
                ),
                axis=1
            )
            
            # Validate business rules
            df_transformed['amount_valid'] = df_transformed['total_amount'] >= 0
            df_transformed['date_valid'] = df_transformed['transaction_date'] <= datetime.now()
            
            # Add transformation metadata
            df_transformed['transform_timestamp'] = datetime.now()
            df_transformed['record_version'] = 1
            
            logger.info(f"Successfully transformed {len(df_transformed)} transaction records")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transforming transaction data: {str(e)}")
            raise
    
    def aggregate_customer_metrics(self, transactions_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate transaction metrics by customer.
        
        Args:
            transactions_df: Transformed transaction DataFrame
            
        Returns:
            DataFrame with customer-level aggregated metrics
        """
        try:
            logger.info("Aggregating customer transaction metrics")
            
            metrics = transactions_df.groupby('customer_id').agg({
                'transaction_id': 'count',
                'total_amount': ['sum', 'mean', 'max', 'min'],
                'transaction_date': ['min', 'max']
            }).reset_index()
            
            # Flatten column names
            metrics.columns = [
                'customer_id',
                'total_transactions',
                'total_spent',
                'avg_transaction_amount',
                'max_transaction_amount',
                'min_transaction_amount',
                'first_transaction_date',
                'last_transaction_date'
            ]
            
            # Calculate additional metrics
            metrics['customer_lifetime_days'] = (
                metrics['last_transaction_date'] - metrics['first_transaction_date']
            ).dt.days
            
            metrics['avg_days_between_transactions'] = (
                metrics['customer_lifetime_days'] / metrics['total_transactions']
            )
            
            # Categorize customer value
            metrics['customer_value_segment'] = pd.qcut(
                metrics['total_spent'],
                q=4,
                labels=['Low', 'Medium', 'High', 'Premium']
            )
            
            metrics['aggregation_timestamp'] = datetime.now()
            
            logger.info(f"Aggregated metrics for {len(metrics)} customers")
            return metrics
            
        except Exception as e:
            logger.error(f"Error aggregating customer metrics: {str(e)}")
            raise
    
    def _format_phone(self, phone: str) -> str:
        """Format phone number to standard format."""
        if pd.isna(phone):
            return None
        
        # Remove non-numeric characters
        digits = ''.join(filter(str.isdigit, str(phone)))
        
        # Format as (XXX) XXX-XXXX for 10-digit numbers
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        
        return phone
    
    def _create_full_address(self, df: pd.DataFrame) -> pd.Series:
        """Create full address string from components."""
        address_parts = []
        
        for _, row in df.iterrows():
            parts = [
                row.get('address_line1', ''),
                row.get('address_line2', ''),
                row.get('city', ''),
                row.get('state', ''),
                row.get('zip_code', ''),
                row.get('country', '')
            ]
            
            # Filter out empty parts
            parts = [str(p).strip() for p in parts if pd.notna(p) and str(p).strip()]
            address_parts.append(', '.join(parts))
        
        return pd.Series(address_parts, index=df.index)
    
    def _generate_hash(self, row: pd.Series, fields: List[str]) -> str:
        """Generate SHA256 hash from specified fields."""
        values = '|'.join([str(row.get(f, '')) for f in fields])
        return sha256(values.encode()).hexdigest()
    
    def _categorize_customer_tenure(self, days: int) -> str:
        """Categorize customer by tenure."""
        if days < 30:
            return 'NEW'
        elif days < 180:
            return 'RECENT'
        elif days < 365:
            return 'ESTABLISHED'
        else:
            return 'LOYAL'
    
    def _categorize_transaction_amount(self, amount: float) -> str:
        """Categorize transaction by amount."""
        thresholds = self.business_rules.get('amount_thresholds', {
            'small': 50,
            'medium': 200,
            'large': 1000
        })
        
        if amount < thresholds['small']:
            return 'SMALL'
        elif amount < thresholds['medium']:
            return 'MEDIUM'
        elif amount < thresholds['large']:
            return 'LARGE'
        else:
            return 'EXTRA_LARGE'
    
    def _enrich_with_customer_data(self, transactions_df: pd.DataFrame,
                                   customer_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich transactions with customer information."""
        customer_fields = ['customer_id', 'full_name', 'email', 'customer_segment', 'state']
        customer_subset = customer_df[customer_fields].copy()
        
        enriched = transactions_df.merge(
            customer_subset,
            on='customer_id',
            how='left',
            suffixes=('', '_customer')
        )
        
        return enriched