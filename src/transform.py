"""
Transform customer master data with type conversions, null handling,
and address standardization.
"""

import re
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTypeConverter:
    """Handles data type conversions for customer data."""
    
    @staticmethod
    def to_string(value: Any, max_length: Optional[int] = None) -> Optional[str]:
        """Convert value to string with optional length constraint."""
        if value is None or value == '':
            return None
        
        str_value = str(value).strip()
        
        if max_length and len(str_value) > max_length:
            logger.warning(f"String truncated from {len(str_value)} to {max_length} chars")
            return str_value[:max_length]
        
        return str_value if str_value else None
    
    @staticmethod
    def to_date(value: Any, format_str: str = '%Y-%m-%d %H:%M:%S') -> Optional[str]:
        """Convert value to ISO date string."""
        if value is None or value == '':
            return None
        
        try:
            if isinstance(value, datetime):
                return value.isoformat()
            
            if isinstance(value, str):
                dt = datetime.strptime(value.strip(), format_str)
                return dt.isoformat()
            
            return None
        except ValueError as e:
            logger.error(f"Date conversion error for value '{value}': {e}")
            return None
    
    @staticmethod
    def to_integer(value: Any) -> Optional[int]:
        """Convert value to integer."""
        if value is None or value == '':
            return None
        
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError) as e:
            logger.error(f"Integer conversion error for value '{value}': {e}")
            return None
    
    @staticmethod
    def to_decimal(value: Any, precision: int = 10, scale: int = 2) -> Optional[float]:
        """Convert value to decimal with specified precision and scale."""
        if value is None or value == '':
            return None
        
        try:
            decimal_value = float(str(value).strip())
            return round(decimal_value, scale)
        except (ValueError, TypeError) as e:
            logger.error(f"Decimal conversion error for value '{value}': {e}")
            return None


class NullHandler:
    """Handles null values and default replacements."""
    
    @staticmethod
    def handle_null(value: Any, default: Any = None, null_values: List[str] = None) -> Any:
        """Replace null or empty values with default."""
        if null_values is None:
            null_values = ['', 'NULL', 'null', 'None', 'N/A', 'NA']
        
        if value is None:
            return default
        
        if isinstance(value, str) and value.strip() in null_values:
            return default
        
        return value
    
    @staticmethod
    def coalesce(*values: Any) -> Any:
        """Return first non-null value."""
        for value in values:
            if value is not None and value != '':
                return value
        return None


class AddressStandardizer:
    """Standardizes address data."""
    
    # US state abbreviations mapping
    STATE_ABBREV = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY'
    }
    
    # Street type abbreviations
    STREET_TYPES = {
        'STREET': 'ST', 'AVENUE': 'AVE', 'BOULEVARD': 'BLVD', 'DRIVE': 'DR',
        'ROAD': 'RD', 'LANE': 'LN', 'COURT': 'CT', 'CIRCLE': 'CIR',
        'PLACE': 'PL', 'PARKWAY': 'PKWY', 'HIGHWAY': 'HWY', 'TRAIL': 'TRL'
    }
    
    @classmethod
    def standardize_state(cls, state: Optional[str]) -> Optional[str]:
        """Standardize state to 2-letter abbreviation."""
        if not state:
            return None
        
        state_upper = state.strip().upper()
        
        # Already abbreviated
        if len(state_upper) == 2 and state_upper.isalpha():
            return state_upper
        
        # Convert full name to abbreviation
        return cls.STATE_ABBREV.get(state_upper, state_upper)
    
    @classmethod
    def standardize_zip(cls, zip_code: Optional[str]) -> Optional[str]:
        """Standardize ZIP code format."""
        if not zip_code:
            return None
        
        # Remove all non-alphanumeric characters
        clean_zip = re.sub(r'[^0-9]', '', str(zip_code).strip())
        
        if len(clean_zip) == 5:
            return clean_zip
        elif len(clean_zip) == 9:
            return f"{clean_zip[:5]}-{clean_zip[5:]}"
        elif len(clean_zip) > 5:
            return clean_zip[:5]
        
        return clean_zip if clean_zip else None
    
    @classmethod
    def standardize_address_line(cls, address: Optional[str]) -> Optional[str]:
        """Standardize address line with common abbreviations."""
        if not address:
            return None
        
        address_upper = address.strip().upper()
        
        # Replace street types with abbreviations
        for full_type, abbrev in cls.STREET_TYPES.items():
            pattern = r'\b' + full_type + r'\b'
            address_upper = re.sub(pattern, abbrev, address_upper)
        
        # Remove extra whitespace
        address_upper = re.sub(r'\s+', ' ', address_upper)
        
        return address_upper.strip()
    
    @classmethod
    def standardize_phone(cls, phone: Optional[str]) -> Optional[str]:
        """Standardize phone number format."""
        if not phone:
            return None
        
        # Extract digits only
        digits = re.sub(r'[^0-9]', '', str(phone).strip())
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return digits if digits else None
    
    @classmethod
    def standardize_country(cls, country: Optional[str]) -> Optional[str]:
        """Standardize country code."""
        if not country:
            return None
        
        country_upper = country.strip().upper()
        
        # Map common country names to ISO codes
        country_map = {
            'UNITED STATES': 'US',
            'USA': 'US',
            'CANADA': 'CA',
            'MEXICO': 'MX',
            'UNITED KINGDOM': 'GB',
            'UK': 'GB'
        }
        
        return country_map.get(country_upper, country_upper[:2])


class CustomerTransformer:
    """Main transformer for customer master data."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.converter = DataTypeConverter()
        self.null_handler = NullHandler()
        self.address_std = AddressStandardizer()
        self.stats = {
            'processed': 0,
            'errors': 0,
            'nulls_handled': 0,
            'addresses_standardized': 0
        }
    
    def transform_customer(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single customer record."""
        try:
            transformed = {}
            
            # Customer ID - required field
            transformed['customer_id'] = self.converter.to_string(
                record.get('customer_id'),
                max_length=10
            )
            
            if not transformed['customer_id']:
                logger.error("Missing required customer_id")
                self.stats['errors'] += 1
                return None
            
            # Name fields with null handling
            transformed['first_name'] = self.null_handler.handle_null(
                self.converter.to_string(record.get('first_name'), max_length=50),
                default=self.config.get('default_first_name', 'UNKNOWN')
            )
            
            transformed['last_name'] = self.null_handler.handle_null(
                self.converter.to_string(record.get('last_name'), max_length=50),
                default=self.config.get('default_last_name', 'UNKNOWN')
            )
            
            # Full name concatenation
            transformed['full_name'] = f"{transformed['first_name']} {transformed['last_name']}"
            
            # Email with validation
            email = self.converter.to_string(record.get('email'), max_length=100)
            transformed['email'] = self._validate_email(email)
            
            # Phone standardization
            transformed['phone'] = self.address_std.standardize_phone(
                record.get('phone')
            )
            
            # Address standardization
            transformed['address_line1'] = self.address_std.standardize_address_line(
                record.get('address_line1')
            )
            
            transformed['address_line2'] = self.address_std.standardize_address_line(
                record.get('address_line2')
            )
            
            transformed['city'] = self.converter.to_string(
                record.get('city'),
                max_length=50
            )
            
            transformed['state'] = self.address_std.standardize_state(
                record.get('state')
            )
            
            transformed['zip_code'] = self.address_std.standardize_zip(
                record.get('zip_code')
            )
            
            transformed['country'] = self.address_std.standardize_country(
                record.get('country')
            )
            
            # Date conversion
            transformed['registration_date'] = self.converter.to_date(
                record.get('registration_date'),
                format_str=self.config.get('date_format', '%Y-%m-%d %H:%M:%S')
            )
            
            # Status with default
            transformed['status'] = self.null_handler.handle_null(
                self.converter.to_string(record.get('status'), max_length=10),
                default=self.config.get('default_status', 'ACTIVE')
            )
            
            # Add audit fields
            transformed['transformed_timestamp'] = datetime.utcnow().isoformat()
            transformed['data_quality_score'] = self._calculate_quality_score(transformed)
            
            self.stats['processed'] += 1
            
            if any(v is None for k, v in transformed.items() 
                   if k not in ['address_line2', 'email', 'phone']):
                self.stats['nulls_handled'] += 1
            
            if transformed.get('state') or transformed.get('zip_code'):
                self.stats['addresses_standardized'] += 1
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming customer record: {e}")
            self.stats['errors'] += 1
            return None
    
    def _validate_email(self, email: Optional[str]) -> Optional[str]:
        """Validate email format."""
        if not email:
            return None
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if re.match(email_pattern, email):
            return email.lower()
        
        logger.warning(f"Invalid email format: {email}")
        return None
    
    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate data quality score (0-100)."""
        total_fields = 0
        filled_fields = 0
        
        critical_fields = ['customer_id', 'first_name', 'last_name', 'email']
        optional_fields = ['phone', 'address_line1', 'city', 'state', 'zip_code']
        
        for field in critical_fields:
            total_fields += 2  # Critical fields weighted 2x
            if record.get(field):
                filled_fields += 2
        
        for field in optional_fields:
            total_fields += 1
            if record.get(field):
                filled_fields += 1
        
        return round((filled_fields / total_fields) * 100, 2) if total_fields > 0 else 0.0
    
    def get_statistics(self) -> Dict[str, int]:
        """Get transformation statistics."""
        return self.stats.copy()


def transform_customer_batch(records: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform a batch of customer records."""
    transformer = CustomerTransformer(config)
    transformed_records = []
    
    for record in records:
        transformed = transformer.transform_customer(record)
        if transformed:
            transformed_records.append(transformed)
    
    logger.info(f"Transformation statistics: {transformer.get_statistics()}")
    
    return transformed_records