"""
Customer Master Data Transformation Module
Applies business rules and data quality transformations
"""
import logging
import re
from typing import Dict, List, Optional
from datetime import datetime
from email_validator import validate_email, EmailNotValidError

logger = logging.getLogger(__name__)


class CustomerDataTransformer:
    """Handles transformation of customer master data"""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer with configuration
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transformation', {})
        self.validation_rules = self.transform_config.get('validation_rules', {})
        self.default_values = self.transform_config.get('default_values', {})
        
    def transform_customers(self, customers: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Transform customer records applying business rules
        
        Args:
            customers: List of raw customer dictionaries
            
        Returns:
            Dictionary with 'valid' and 'invalid' customer lists
        """
        logger.info(f"Transforming {len(customers)} customer records")
        
        valid_customers = []
        invalid_customers = []
        
        for customer in customers:
            try:
                transformed = self._transform_customer(customer)
                
                # Validate transformed record
                validation_result = self._validate_customer(transformed)
                
                if validation_result['is_valid']:
                    valid_customers.append(transformed)
                else:
                    transformed['validation_errors'] = validation_result['errors']
                    invalid_customers.append(transformed)
                    logger.warning(
                        f"Customer {customer.get('customer_id')} failed validation: "
                        f"{validation_result['errors']}"
                    )
                    
            except Exception as e:
                logger.error(f"Error transforming customer {customer.get('customer_id')}: {e}")
                customer['transformation_error'] = str(e)
                invalid_customers.append(customer)
        
        logger.info(
            f"Transformation complete: {len(valid_customers)} valid, "
            f"{len(invalid_customers)} invalid"
        )
        
        return {
            'valid': valid_customers,
            'invalid': invalid_customers
        }
    
    def transform_addresses(self, addresses: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Transform address records
        
        Args:
            addresses: List of raw address dictionaries
            
        Returns:
            Dictionary with 'valid' and 'invalid' address lists
        """
        logger.info(f"Transforming {len(addresses)} address records")
        
        valid_addresses = []
        invalid_addresses = []
        
        for address in addresses:
            try:
                transformed = self._transform_address(address)
                
                validation_result = self._validate_address(transformed)
                
                if validation_result['is_valid']:
                    valid_addresses.append(transformed)
                else:
                    transformed['validation_errors'] = validation_result['errors']
                    invalid_addresses.append(transformed)
                    
            except Exception as e:
                logger.error(f"Error transforming address {address.get('address_id')}: {e}")
                address['transformation_error'] = str(e)
                invalid_addresses.append(address)
        
        logger.info(
            f"Address transformation complete: {len(valid_addresses)} valid, "
            f"{len(invalid_addresses)} invalid"
        )
        
        return {
            'valid': valid_addresses,
            'invalid': invalid_addresses
        }
    
    def _transform_customer(self, customer: Dict) -> Dict:
        """Apply transformations to a single customer record"""
        transformed = customer.copy()
        
        # Standardize customer_id
        transformed['customer_id'] = self._standardize_id(
            transformed.get('customer_id', '')
        )
        
        # Standardize names
        transformed['first_name'] = self._standardize_name(
            transformed.get('first_name', '')
        )
        transformed['last_name'] = self._standardize_name(
            transformed.get('last_name', '')
        )
        
        # Create full name
        transformed['full_name'] = f"{transformed['first_name']} {transformed['last_name']}".strip()
        
        # Standardize email
        transformed['email'] = self._standardize_email(
            transformed.get('email', '')
        )
        
        # Standardize phone
        transformed['phone'] = self._standardize_phone(
            transformed.get('phone', '')
        )
        
        # Standardize address fields
        transformed['address_line1'] = self._standardize_address(
            transformed.get('address_line1', '')
        )
        transformed['address_line2'] = self._standardize_address(
            transformed.get('address_line2', '')
        )
        transformed['city'] = self._standardize_name(
            transformed.get('city', '')
        )
        
        # Standardize state (uppercase)
        transformed['state'] = transformed.get('state', '').strip().upper()
        
        # Standardize zip code
        transformed['zip_code'] = self._standardize_zip(
            transformed.get('zip_code', '')
        )
        
        # Standardize country (uppercase)
        transformed['country'] = transformed.get('country', '').strip().upper()
        if not transformed['country']:
            transformed['country'] = self.default_values.get('country', 'US')
        
        # Parse and standardize registration date
        transformed['registration_date'] = self._parse_date(
            transformed.get('registration_date', '')
        )
        
        # Standardize status
        transformed['status'] = self._standardize_status(
            transformed.get('status', '')
        )
        
        # Add transformation metadata
        transformed['transform_timestamp'] = datetime.now().isoformat()
        
        return transformed
    
    def _transform_address(self, address: Dict) -> Dict:
        """Apply transformations to a single address record"""
        transformed = address.copy()
        
        # Standardize IDs
        transformed['address_id'] = self._standardize_id(
            transformed.get('address_id', '')
        )
        transformed['customer_id'] = self._standardize_id(
            transformed.get('customer_id', '')
        )
        
        # Standardize address type
        transformed['address_type'] = self._standardize_address_type(
            transformed.get('address_type', '')
        )
        
        # Add transformation metadata
        transformed['transform_timestamp'] = datetime.now().isoformat()
        
        return transformed
    
    def _validate_customer(self, customer: Dict) -> Dict:
        """Validate transformed customer record"""
        errors = []
        
        # Validate customer_id
        if not customer.get('customer_id'):
            errors.append("customer_id is required")
        elif len(customer['customer_id']) > 10:
            errors.append("customer_id exceeds maximum length of 10")
        
        # Validate names
        if not customer.get('first_name'):
            errors.append("first_name is required")
        if not customer.get('last_name'):
            errors.append("last_name is required")
        
        # Validate email
        if customer.get('email'):
            try:
                validate_email(customer['email'])
            except EmailNotValidError:
                errors.append("email format is invalid")
        elif self.validation_rules.get('email_required', False):
            errors.append("email is required")
        
        # Validate phone
        if customer.get('phone'):
            if not re.match(r'^\d{10}$', customer['phone']):
                errors.append("phone must be 10 digits")
        
        # Validate state
        if customer.get('state'):
            valid_states = self.validation_rules.get('valid_states', [])
            if valid_states and customer['state'] not in valid_states:
                errors.append(f"state '{customer['state']}' is not valid")
        
        # Validate zip code
        if customer.get('zip_code'):
            if not re.match(r'^\d{5}(-\d{4})?$', customer['zip_code']):
                errors.append("zip_code format is invalid")
        
        # Validate country
        if customer.get('country'):
            valid_countries = self.validation_rules.get('valid_countries', [])
            if valid_countries and customer['country'] not in valid_countries:
                errors.append(f"country '{customer['country']}' is not valid")
        
        # Validate status
        valid_statuses = self.validation_rules.get('valid_statuses', ['ACTIVE', 'INACTIVE', 'SUSPENDED'])
        if customer.get('status') not in valid_statuses:
            errors.append(f"status must be one of {valid_statuses}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def _validate_address(self, address: Dict) -> Dict:
        """Validate transformed address record"""
        errors = []
        
        if not address.get('address_id'):
            errors.append("address_id is required")
        
        if not address.get('customer_id'):
            errors.append("customer_id is required")
        
        valid_types = self.validation_rules.get('valid_address_types', ['BILLING', 'SHIPPING', 'MAILING'])
        if address.get('address_type') not in valid_types:
            errors.append(f"address_type must be one of {valid_types}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def _standardize_id(self, value: str) -> str:
        """Standardize ID field"""
        return value.strip().upper()
    
    def _standardize_name(self, value: str) -> str:
        """Standardize name field (title case)"""
        return value.strip().title()
    
    def _standardize_email(self, value: str) -> str:
        """Standardize email (lowercase)"""
        return value.strip().lower()
    
    def _standardize_phone(self, value: str) -> str:
        """Standardize phone number (digits only)"""
        return re.sub(r'\D', '', value)
    
    def _standardize_address(self, value: str) -> str:
        """Standardize address field"""
        return value.strip().title()
    
    def _standardize_zip(self, value: str) -> str:
        """Standardize zip code"""
        # Remove spaces and hyphens, then reformat
        digits = re.sub(r'[^\d]', '', value)
        if len(digits) == 9:
            return f"{digits[:5]}-{digits[5:]}"
        return digits[:5]
    
    def _standardize_status(self, value: str) -> str:
        """Standardize status field"""
        status = value.strip().upper()
        status_mapping = self.transform_config.get('status_mapping', {})
        return status_mapping.get(status, status)
    
    def _standardize_address_type(self, value: str) -> str:
        """Standardize address type"""
        return value.strip().upper()
    
    def _parse_date(self, value: str) -> Optional[str]:
        """Parse and standardize date"""
        if not value:
            return None
        
        # Try multiple date formats
        date_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S'
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(value.strip(), fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {value}")
        return None


def transform_data(extracted_data: Dict, config: Dict) -> Dict:
    """
    Main transformation function
    
    Args:
        extracted_data: Dictionary containing extracted customers and addresses
        config: Configuration dictionary
        
    Returns:
        Dictionary containing transformed valid and invalid records
    """
    transformer = CustomerDataTransformer(config)
    
    customer_results = transformer.transform_customers(
        extracted_data.get('customers', [])
    )
    
    address_results = transformer.transform_addresses(
        extracted_data.get('addresses', [])
    )
    
    return {
        'customers': customer_results,
        'addresses': address_results
    }