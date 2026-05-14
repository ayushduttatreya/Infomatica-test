"""
Customer Master Data Extraction Module
Extracts customer and address data from source files
"""
import logging
from typing import Dict, List, Optional
from pathlib import Path
import csv
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerDataExtractor:
    """Handles extraction of customer master data from source files"""
    
    def __init__(self, config: Dict):
        """
        Initialize extractor with configuration
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('source', {})
        self.customers_path = self.source_config.get('customers_file')
        self.addresses_path = self.source_config.get('addresses_file')
        self.encoding = self.source_config.get('encoding', 'utf-8')
        self.delimiter = self.source_config.get('delimiter', ',')
        
    def extract_customers(self) -> List[Dict]:
        """
        Extract customer records from source file
        
        Returns:
            List of customer dictionaries
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If file format is invalid
        """
        logger.info(f"Extracting customers from {self.customers_path}")
        
        if not Path(self.customers_path).exists():
            raise FileNotFoundError(f"Customer source file not found: {self.customers_path}")
        
        customers = []
        try:
            with open(self.customers_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        customer = self._parse_customer_row(row, row_num)
                        customers.append(customer)
                    except Exception as e:
                        logger.error(f"Error parsing customer row {row_num}: {e}")
                        if self.config.get('error_handling', {}).get('fail_on_error', False):
                            raise
                        
            logger.info(f"Successfully extracted {len(customers)} customer records")
            return customers
            
        except Exception as e:
            logger.error(f"Failed to extract customers: {e}")
            raise
    
    def extract_addresses(self) -> List[Dict]:
        """
        Extract customer address records from source file
        
        Returns:
            List of address dictionaries
            
        Raises:
            FileNotFoundError: If source file doesn't exist
        """
        logger.info(f"Extracting addresses from {self.addresses_path}")
        
        if not Path(self.addresses_path).exists():
            raise FileNotFoundError(f"Address source file not found: {self.addresses_path}")
        
        addresses = []
        try:
            with open(self.addresses_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        address = self._parse_address_row(row, row_num)
                        addresses.append(address)
                    except Exception as e:
                        logger.error(f"Error parsing address row {row_num}: {e}")
                        if self.config.get('error_handling', {}).get('fail_on_error', False):
                            raise
                        
            logger.info(f"Successfully extracted {len(addresses)} address records")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to extract addresses: {e}")
            raise
    
    def _parse_customer_row(self, row: Dict, row_num: int) -> Dict:
        """Parse and validate customer row"""
        customer = {
            'customer_id': row.get('customer_id', '').strip(),
            'first_name': row.get('first_name', '').strip(),
            'last_name': row.get('last_name', '').strip(),
            'email': row.get('email', '').strip(),
            'phone': row.get('phone', '').strip(),
            'address_line1': row.get('address_line1', '').strip(),
            'address_line2': row.get('address_line2', '').strip(),
            'city': row.get('city', '').strip(),
            'state': row.get('state', '').strip(),
            'zip_code': row.get('zip_code', '').strip(),
            'country': row.get('country', '').strip(),
            'registration_date': row.get('registration_date', '').strip(),
            'status': row.get('status', '').strip(),
            'source_row': row_num,
            'extract_timestamp': datetime.now().isoformat()
        }
        
        # Validate required fields
        if not customer['customer_id']:
            raise ValueError(f"Missing required field: customer_id")
        
        return customer
    
    def _parse_address_row(self, row: Dict, row_num: int) -> Dict:
        """Parse and validate address row"""
        address = {
            'address_id': row.get('address_id', '').strip(),
            'customer_id': row.get('customer_id', '').strip(),
            'address_type': row.get('address_type', '').strip(),
            'source_row': row_num,
            'extract_timestamp': datetime.now().isoformat()
        }
        
        # Validate required fields
        if not address['address_id'] or not address['customer_id']:
            raise ValueError(f"Missing required fields: address_id or customer_id")
        
        return address


def extract_data(config: Dict) -> Dict[str, List[Dict]]:
    """
    Main extraction function
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary containing extracted customers and addresses
    """
    extractor = CustomerDataExtractor(config)
    
    return {
        'customers': extractor.extract_customers(),
        'addresses': extractor.extract_addresses()
    }