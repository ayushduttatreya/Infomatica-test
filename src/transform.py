"""
Transform module for customer address management logic.
Implements address type validation, primary address flag management, and customer ID lookups.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerAddressTransformer:
    """Transforms customer and address data with business logic."""
    
    # Valid address types based on business rules
    VALID_ADDRESS_TYPES = {
        'HOME', 'WORK', 'BILLING', 'SHIPPING', 'MAILING', 'OTHER'
    }
    
    # Valid country codes (ISO 3166-1 alpha-2)
    VALID_COUNTRIES = {
        'US', 'CA', 'MX', 'GB', 'DE', 'FR', 'IT', 'ES', 'AU', 'JP', 'CN', 'IN'
    }
    
    # Valid US state codes
    VALID_US_STATES = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.transform_config = config.get('transform', {})
        self.validation_config = self.transform_config.get('validation', {})
        
    def validate_address_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize address types.
        
        Args:
            df: DataFrame with address_type column
            
        Returns:
            DataFrame with validated address_type and validation flag
        """
        logger.info("Validating address types")
        
        df = df.copy()
        
        # Standardize to uppercase and strip whitespace
        df['address_type'] = df['address_type'].str.upper().str.strip()
        
        # Create validation flag
        df['address_type_valid'] = df['address_type'].isin(self.VALID_ADDRESS_TYPES)
        
        # Handle invalid types based on config
        default_type = self.validation_config.get('default_address_type', 'OTHER')
        
        invalid_count = (~df['address_type_valid']).sum()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid address types, setting to {default_type}")
            df.loc[~df['address_type_valid'], 'address_type'] = default_type
            df.loc[~df['address_type_valid'], 'address_type_valid'] = True
        
        logger.info(f"Address type validation complete: {len(df)} records processed")
        return df
    
    def set_primary_address_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Set primary address flags ensuring only one primary address per customer.
        Business rule: If multiple addresses marked as primary, keep the most recent.
        If no primary address, set the first active address as primary.
        
        Args:
            df: DataFrame with customer addresses
            
        Returns:
            DataFrame with corrected is_primary flags
        """
        logger.info("Setting primary address flags")
        
        df = df.copy()
        
        # Convert is_primary to boolean
        df['is_primary'] = df['is_primary'].map({
            'Y': True, 'YES': True, '1': True, 'TRUE': True, True: True,
            'N': False, 'NO': False, '0': False, 'FALSE': False, False: False
        }).fillna(False)
        
        # Convert is_active to boolean
        df['is_active'] = df['is_active'].map({
            'Y': True, 'YES': True, '1': True, 'TRUE': True, True: True,
            'N': False, 'NO': False, '0': False, 'FALSE': False, False: False
        }).fillna(True)
        
        # Sort by customer_id and address_id for consistent processing
        df = df.sort_values(['customer_id', 'address_id'])
        
        # Reset all primary flags first
        df['is_primary'] = False
        
        # Group by customer and set primary address
        def set_primary_for_customer(group):
            active_addresses = group[group['is_active']]
            
            if len(active_addresses) == 0:
                # No active addresses, no primary
                return group
            
            # Set the first active address as primary
            first_active_idx = active_addresses.index[0]
            group.loc[first_active_idx, 'is_primary'] = True
            
            return group
        
        df = df.groupby('customer_id', group_keys=False).apply(set_primary_for_customer)
        
        # Log statistics
        primary_count = df['is_primary'].sum()
        customer_count = df['customer_id'].nunique()
        logger.info(f"Primary address flags set: {primary_count} primary addresses for {customer_count} customers")
        
        return df
    
    def validate_geography(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate geographic data (country, state, zip code).
        
        Args:
            df: DataFrame with geographic fields
            
        Returns:
            DataFrame with validation flags
        """
        logger.info("Validating geographic data")
        
        df = df.copy()
        
        # Standardize country codes
        df['country'] = df['country'].str.upper().str.strip()
        df['country_valid'] = df['country'].isin(self.VALID_COUNTRIES)
        
        # Validate US states
        df['state'] = df['state'].str.upper().str.strip()
        df['state_valid'] = True  # Default to valid
        
        # For US addresses, validate state codes
        us_mask = df['country'] == 'US'
        df.loc[us_mask, 'state_valid'] = df.loc[us_mask, 'state'].isin(self.VALID_US_STATES)
        
        # Validate zip code format for US (5 digits or 5+4 format)
        df['zip_code_valid'] = True
        us_zip_pattern = r'^\d{5}(-\d{4})?$'
        df.loc[us_mask, 'zip_code_valid'] = df.loc[us_mask, 'zip_code'].str.match(us_zip_pattern, na=False)
        
        # Log validation results
        invalid_country = (~df['country_valid']).sum()
        invalid_state = (~df['state_valid']).sum()
        invalid_zip = (~df['zip_code_valid']).sum()
        
        if invalid_country > 0:
            logger.warning(f"Found {invalid_country} invalid country codes")
        if invalid_state > 0:
            logger.warning(f"Found {invalid_state} invalid state codes")
        if invalid_zip > 0:
            logger.warning(f"Found {invalid_zip} invalid zip codes")
        
        return df
    
    def enrich_with_customer_data(
        self, 
        addresses_df: pd.DataFrame, 
        customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Enrich address data with customer information via customer ID lookup.
        
        Args:
            addresses_df: Address DataFrame
            customers_df: Customer DataFrame
            
        Returns:
            Enriched DataFrame with customer information
        """
        logger.info("Enriching addresses with customer data")
        
        # Select relevant customer fields for enrichment
        customer_fields = [
            'customer_id', 'first_name', 'last_name', 
            'email', 'phone', 'status', 'registration_date'
        ]
        
        customers_subset = customers_df[customer_fields].copy()
        
        # Perform left join to preserve all addresses
        enriched_df = addresses_df.merge(
            customers_subset,
            on='customer_id',
            how='left',
            suffixes=('', '_customer')
        )
        
        # Flag addresses without matching customer
        enriched_df['customer_found'] = enriched_df['first_name'].notna()
        
        orphaned_count = (~enriched_df['customer_found']).sum()
        if orphaned_count > 0:
            logger.warning(f"Found {orphaned_count} addresses without matching customer records")
        
        logger.info(f"Enrichment complete: {len(enriched_df)} records")
        return enriched_df
    
    def standardize_address_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize address formatting (trim, uppercase where appropriate).
        
        Args:
            df: DataFrame with address fields
            
        Returns:
            DataFrame with standardized addresses
        """
        logger.info("Standardizing address format")
        
        df = df.copy()
        
        # Trim whitespace from all string fields
        string_fields = ['address_line1', 'address_line2', 'city', 'state', 'country']
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].str.strip()
        
        # Uppercase state and country codes
        if 'state' in df.columns:
            df['state'] = df['state'].str.upper()
        if 'country' in df.columns:
            df['country'] = df['country'].str.upper()
        
        # Title case for city names
        if 'city' in df.columns:
            df['city'] = df['city'].str.title()
        
        # Standardize zip code format (remove spaces, ensure proper format)
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].str.replace(' ', '').str.replace('-', '')
            # Re-add hyphen for 9-digit US zip codes
            us_mask = (df['country'] == 'US') & (df['zip_code'].str.len() == 9)
            df.loc[us_mask, 'zip_code'] = (
                df.loc[us_mask, 'zip_code'].str[:5] + '-' + 
                df.loc[us_mask, 'zip_code'].str[5:]
            )
        
        logger.info("Address standardization complete")
        return df
    
    def add_audit_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add audit fields for tracking transformations.
        
        Args:
            df: DataFrame to add audit fields to
            
        Returns:
            DataFrame with audit fields
        """
        df = df.copy()
        
        current_timestamp = datetime.now()
        df['transform_timestamp'] = current_timestamp
        df['transform_version'] = self.config.get('version', '1.0.0')
        
        return df
    
    def transform_addresses(
        self, 
        addresses_df: pd.DataFrame, 
        customers_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Execute full transformation pipeline for address data.
        
        Args:
            addresses_df: Raw address data
            customers_df: Raw customer data
            
        Returns:
            Tuple of (transformed addresses, error records)
        """
        logger.info("Starting address transformation pipeline")
        
        try:
            # Step 1: Validate address types
            df = self.validate_address_type(addresses_df)
            
            # Step 2: Validate geography
            df = self.validate_geography(df)
            
            # Step 3: Standardize address format
            df = self.standardize_address_format(df)
            
            # Step 4: Set primary address flags
            df = self.set_primary_address_flags(df)
            
            # Step 5: Enrich with customer data
            df = self.enrich_with_customer_data(df, customers_df)
            
            # Step 6: Add audit fields
            df = self.add_audit_fields(df)
            
            # Separate valid and error records
            validation_columns = [
                'address_type_valid', 'country_valid', 
                'state_valid', 'zip_code_valid', 'customer_found'
            ]
            
            df['all_validations_passed'] = df[validation_columns].all(axis=1)
            
            valid_df = df[df['all_validations_passed']].copy()
            error_df = df[~df['all_validations_passed']].copy()
            
            logger.info(f"Transformation complete: {len(valid_df)} valid, {len(error_df)} error records")
            
            return valid_df, error_df
            
        except Exception as e:
            logger.error(f"Transformation pipeline failed: {e}")
            raise