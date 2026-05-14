"""
Transform module for customer address pipeline.
Handles validation, cleansing, and enrichment of address data.
"""
import logging
import re
from typing import Dict, Any, Tuple
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerAddressTransformer:
    """Transforms and validates customer address data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary containing validation rules
        """
        self.config = config
        self.validation_config = config.get('validation', {})
        self.transform_config = config.get('transform', {})
        
        # Validation patterns
        self.zip_pattern = re.compile(r'^\d{5}(-\d{4})?$')
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.phone_pattern = re.compile(r'^\+?1?\d{10,15}$')
        
        # Valid state codes
        self.valid_states = set([
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        ])
        
        # Valid country codes
        self.valid_countries = set(['US', 'USA', 'CA', 'MX'])
        
    def validate_address(self, row: pd.Series) -> Tuple[bool, str]:
        """
        Validate a single address record.
        
        Args:
            row: Address record as pandas Series
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        errors = []
        
        # Required field validation
        if pd.isna(row.get('street_address')) or not str(row.get('street_address')).strip():
            errors.append("Missing street address")
            
        if pd.isna(row.get('city')) or not str(row.get('city')).strip():
            errors.append("Missing city")
            
        # State validation
        state = str(row.get('state_province', '')).strip().upper()
        if state and state not in self.valid_states:
            errors.append(f"Invalid state code: {state}")
            
        # Postal code validation
        postal_code = str(row.get('postal_code', '')).strip()
        if postal_code and not self.zip_pattern.match(postal_code):
            errors.append(f"Invalid postal code format: {postal_code}")
            
        # Country validation
        country = str(row.get('country_code', '')).strip().upper()
        if country and country not in self.valid_countries:
            errors.append(f"Invalid country code: {country}")
            
        is_valid = len(errors) == 0
        error_message = "; ".join(errors) if errors else ""
        
        return is_valid, error_message
        
    def validate_customer(self, row: pd.Series) -> Tuple[bool, str]:
        """
        Validate a single customer record.
        
        Args:
            row: Customer record as pandas Series
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        errors = []
        
        # Email validation
        email = str(row.get('email', '')).strip()
        if email and not self.email_pattern.match(email):
            errors.append(f"Invalid email format: {email}")
            
        # Phone validation
        phone = str(row.get('phone', '')).strip().replace('-', '').replace(' ', '')
        if phone and not self.phone_pattern.match(phone):
            errors.append(f"Invalid phone format: {phone}")
            
        # Name validation
        if pd.isna(row.get('first_name')) or not str(row.get('first_name')).strip():
            errors.append("Missing first name")
            
        if pd.isna(row.get('last_name')) or not str(row.get('last_name')).strip():
            errors.append("Missing last name")
            
        is_valid = len(errors) == 0
        error_message = "; ".join(errors) if errors else ""
        
        return is_valid, error_message
        
    def cleanse_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleanse and standardize address data.
        
        Args:
            df: Address DataFrame
            
        Returns:
            Cleansed DataFrame
        """
        df = df.copy()
        
        # Standardize state codes
        df['state_province'] = df['state_province'].str.strip().str.upper()
        
        # Standardize country codes
        df['country_code'] = df['country_code'].str.strip().str.upper()
        df['country_code'] = df['country_code'].replace({'USA': 'US'})
        
        # Standardize postal codes
        df['postal_code'] = df['postal_code'].str.strip().str.upper()
        
        # Trim whitespace from text fields
        text_fields = ['street_address', 'street_address2', 'city', 'address_type']
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].str.strip()
                
        # Standardize boolean fields
        if 'is_primary' in df.columns:
            df['is_primary'] = df['is_primary'].map({
                'Y': True, 'N': False, 'YES': True, 'NO': False,
                '1': True, '0': False, 1: True, 0: False,
                True: True, False: False
            })
            
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].map({
                'Y': True, 'N': False, 'YES': True, 'NO': False,
                '1': True, '0': False, 1: True, 0: False,
                True: True, False: False
            })
            
        return df
        
    def enrich_addresses(self, addresses: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich address data with customer information.
        
        Args:
            addresses: Address DataFrame
            customers: Customer DataFrame
            
        Returns:
            Enriched address DataFrame
        """
        # Merge customer data
        enriched = addresses.merge(
            customers[['customer_id', 'first_name', 'last_name', 'email', 'status']],
            on='customer_id',
            how='left'
        )
        
        # Add processing metadata
        enriched['processed_date'] = datetime.now()
        enriched['record_hash'] = enriched.apply(
            lambda row: hash(f"{row['address_id']}_{row['customer_id']}_{row['street_address']}"),
            axis=1
        )
        
        return enriched
        
    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Execute full transformation pipeline.
        
        Args:
            data: Dictionary containing customers and addresses DataFrames
            
        Returns:
            Dictionary containing transformed data and validation results
        """
        customers = data['customers'].copy()
        addresses = data['addresses'].copy()
        
        logger.info("Starting transformation pipeline")
        
        # Validate customers
        logger.info("Validating customer records")
        customer_validation = customers.apply(self.validate_customer, axis=1)
        customers['is_valid'] = customer_validation.apply(lambda x: x[0])
        customers['validation_errors'] = customer_validation.apply(lambda x: x[1])
        
        valid_customers = customers[customers['is_valid']].copy()
        invalid_customers = customers[~customers['is_valid']].copy()
        
        logger.info(f"Valid customers: {len(valid_customers)}, Invalid: {len(invalid_customers)}")
        
        # Cleanse addresses
        logger.info("Cleansing address records")
        addresses = self.cleanse_address(addresses)
        
        # Validate addresses
        logger.info("Validating address records")
        address_validation = addresses.apply(self.validate_address, axis=1)
        addresses['is_valid'] = address_validation.apply(lambda x: x[0])
        addresses['validation_errors'] = address_validation.apply(lambda x: x[1])
        
        valid_addresses = addresses[addresses['is_valid']].copy()
        invalid_addresses = addresses[~addresses['is_valid']].copy()
        
        logger.info(f"Valid addresses: {len(valid_addresses)}, Invalid: {len(invalid_addresses)}")
        
        # Check for orphaned addresses (no matching customer)
        valid_customer_ids = set(valid_customers['customer_id'])
        valid_addresses['has_customer'] = valid_addresses['customer_id'].isin(valid_customer_ids)
        
        orphaned_addresses = valid_addresses[~valid_addresses['has_customer']].copy()
        valid_addresses = valid_addresses[valid_addresses['has_customer']].copy()
        
        logger.info(f"Orphaned addresses (no valid customer): {len(orphaned_addresses)}")
        
        # Enrich valid addresses
        logger.info("Enriching address records")
        enriched_addresses = self.enrich_addresses(valid_addresses, valid_customers)
        
        # Check for duplicate addresses
        duplicate_check = enriched_addresses.groupby(
            ['customer_id', 'street_address', 'city', 'postal_code']
        ).size().reset_index(name='count')
        duplicates = duplicate_check[duplicate_check['count'] > 1]
        
        logger.info(f"Duplicate address groups found: {len(duplicates)}")
        
        return {
            'valid_addresses': enriched_addresses,
            'invalid_addresses': invalid_addresses,
            'invalid_customers': invalid_customers,
            'orphaned_addresses': orphaned_addresses,
            'duplicate_summary': duplicates
        }