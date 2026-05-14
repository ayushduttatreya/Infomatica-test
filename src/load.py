"""
Customer Master Data Loading Module
Loads transformed data into target system
"""
import logging
import json
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)


class CustomerDataLoader:
    """Handles loading of customer master data into target system"""
    
    def __init__(self, config: Dict):
        """
        Initialize loader with configuration
        
        Args:
            config: Configuration dictionary containing target connection details
        """
        self.config = config
        self.target_config = config.get('target', {})
        self.batch_size = self.target_config.get('batch_size', 1000)
        self.connection = None
        
    def connect(self):
        """Establish connection to target database"""
        try:
            self.connection = psycopg2.connect(
                host=self.target_config.get('host'),
                port=self.target_config.get('port', 5432),
                database=self.target_config.get('database'),
                user=self.target_config.get('user'),
                password=self.target_config.get('password')
            )
            logger.info("Successfully connected to target database")
        except Exception as e:
            logger.error(f"Failed to connect to target database: {e}")
            raise
    
    def disconnect(self):
        """Close connection to target database"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from target database")
    
    def load_customers(self, customers: List[Dict]) -> Dict:
        """
        Load customer records into target system
        
        Args:
            customers: List of valid customer dictionaries
            
        Returns:
            Dictionary containing load statistics
        """
        logger.info(f"Loading {len(customers)} customer records")
        
        if not customers:
            logger.warning("No customers to load")
            return {'loaded': 0, 'failed': 0, 'errors': []}
        
        if not self.connection:
            self.connect()
        
        loaded_count = 0
        failed_count = 0
        errors = []
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare insert statement
            insert_sql = """
                INSERT INTO customers (
                    customer_id, first_name, last_name, full_name, email, phone,
                    address_line1, address_line2, city, state, zip_code, country,
                    registration_date, status, created_at, updated_at
                ) VALUES (
                    %(customer_id)s, %(first_name)s, %(last_name)s, %(full_name)s,
                    %(email)s, %(phone)s, %(address_line1)s, %(address_line2)s,
                    %(city)s, %(state)s, %(zip_code)s, %(country)s,
                    %(registration_date)s, %(status)s, NOW(), NOW()
                )
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    full_name = EXCLUDED.full_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    country = EXCLUDED.country,
                    registration_date = EXCLUDED.registration_date,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """
            
            # Process in batches
            for i in range(0, len(customers), self.batch_size):
                batch = customers[i:i + self.batch_size]
                
                try:
                    execute_batch(cursor, insert_sql, batch, page_size=self.batch_size)
                    self.connection.commit()
                    loaded_count += len(batch)
                    logger.info(f"Loaded batch of {len(batch)} customers")
                    
                except Exception as e:
                    self.connection.rollback()
                    logger.error(f"Failed to load batch: {e}")
                    
                    # Try loading records individually to identify failures
                    for customer in batch:
                        try:
                            cursor.execute(insert_sql, customer)
                            self.connection.commit()
                            loaded_count += 1
                        except Exception as individual_error:
                            failed_count += 1
                            error_msg = f"Customer {customer.get('customer_id')}: {individual_error}"
                            errors.append(error_msg)
                            logger.error(error_msg)
                            self.connection.rollback()
            
            cursor.close()
            
            logger.info(
                f"Customer load complete: {loaded_count} loaded, {failed_count} failed"
            )
            
        except Exception as e:
            logger.error(f"Error during customer load: {e}")
            raise
        
        return {
            'loaded': loaded_count,
            'failed': failed_count,
            'errors': errors
        }
    
    def load_addresses(self, addresses: List[Dict]) -> Dict:
        """
        Load address records into target system
        
        Args:
            addresses: List of valid address dictionaries
            
        Returns:
            Dictionary containing load statistics
        """
        logger.info(f"Loading {len(addresses)} address records")
        
        if not addresses:
            logger.warning("No addresses to load")
            return {'loaded': 0, 'failed': 0, 'errors': []}
        
        if not self.connection:
            self.connect()
        
        loaded_count = 0
        failed_count = 0
        errors = []
        
        try:
            cursor = self.connection.cursor()
            
            insert_sql = """
                INSERT INTO customer_addresses (
                    address_id, customer_id, address_type, created_at, updated_at
                ) VALUES (
                    %(address_id)s, %(customer_id)s, %(address_type)s, NOW(), NOW()
                )
                ON CONFLICT (address_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    address_type = EXCLUDED.address_type,
                    updated_at = NOW()
            """
            
            for i in range(0, len(addresses), self.batch_size):
                batch = addresses[i:i + self.batch_size]
                
                try:
                    execute_batch(cursor, insert_sql, batch, page_size=self.batch_size)
                    self.connection.commit()
                    loaded_count += len(batch)
                    logger.info(f"Loaded batch of {len(batch)} addresses")
                    
                except Exception as e:
                    self.connection.rollback()
                    logger.error(f"Failed to load address batch: {e}")
                    
                    for address in batch:
                        try:
                            cursor.execute(insert_sql, address)
                            self.connection.commit()
                            loaded_count += 1
                        except Exception as individual_error:
                            failed_count += 1
                            error_msg = f"Address {address.get('address_id')}: {individual_error}"
                            errors.append(error_msg)
                            logger.error(error_msg)
                            self.connection.rollback()
            
            cursor.close()
            
            logger.info(
                f"Address load complete: {loaded_count} loaded, {failed_count} failed"
            )
            
        except Exception as e:
            logger.error(f"Error during address load: {e}")
            raise
        
        return {
            'loaded': loaded_count,
            'failed': failed_count,
            'errors': errors
        }
    
    def write_error_file(self, invalid_records: List[Dict], record_type: str):
        """
        Write invalid records to error file
        
        Args:
            invalid_records: List of invalid record dictionaries
            record_type: Type of record (customers or addresses)
        """
        if not invalid_records:
            return
        
        error_path = Path(self.target_config.get('error_path', 'errors'))
        error_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = error_path / f"{record_type}_errors_{timestamp}.json"
        
        try:
            with open(error_file, 'w') as f:
                json.dump(invalid_records, f, indent=2, default=str)
            
            logger.info(f"Wrote {len(invalid_records)} invalid {record_type} to {error_file}")
            
        except Exception as e:
            logger.error(f"Failed to write error file: {e}")


def load_data(transformed_data: Dict, config: Dict) -> Dict:
    """
    Main loading function
    
    Args:
        transformed_data: Dictionary containing transformed valid and invalid records
        config: Configuration dictionary
        
    Returns:
        Dictionary containing load results and statistics
    """
    loader = CustomerDataLoader(config)
    
    try:
        # Load valid customers
        customer_results = loader.load_customers(
            transformed_data.get('customers', {}).get('valid', [])
        )
        
        # Load valid addresses
        address_results = loader.load_addresses(
            transformed_data.get('addresses', {}).get('valid', [])
        )
        
        # Write error files for invalid records
        loader.write_error_file(
            transformed_data.get('customers', {}).get('invalid', []),
            'customers'
        )
        loader.write_error_file(
            transformed_data.get('addresses', {}).get('invalid', []),
            'addresses'
        )
        
        return {
            'customers': customer_results,
            'addresses': address_results,
            'invalid_customers': len(transformed_data.get('customers', {}).get('invalid', [])),
            'invalid_addresses': len(transformed_data.get('addresses', {}).get('invalid', []))
        }
        
    finally:
        loader.disconnect()