# Sales Line Item Processing Migration

===FILE: src/extract.py===
"""
Extract module for Sales Line Item Processing.
Reads line item data from source files.
"""
import logging
from typing import Dict, Any, List
from pathlib import Path
import csv
from datetime import datetime

logger = logging.getLogger(__name__)


class LineItemExtractor:
    """Extracts sales line item data from source files."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor.
        
        Args:
            config: Configuration dictionary containing source settings
        """
        self.config = config
        self.source_path = Path(config['source']['line_items_path'])
        self.encoding = config['source'].get('encoding', 'utf-8')
        self.delimiter = config['source'].get('delimiter', ',')
        
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract line item records from source file.
        
        Returns:
            List of line item records as dictionaries
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If file format is invalid
        """
        logger.info(f"Starting extraction from {self.source_path}")
        
        if not self.source_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_path}")
        
        records = []
        
        try:
            with open(self.source_path, 'r', encoding=self.encoding) as file:
                reader = csv.DictReader(file, delimiter=self.delimiter)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        record = self._parse_record(row, row_num)
                        records.append(record)
                    except Exception as e:
                        logger.error(f"Error parsing row {row_num}: {e}")
                        if self.config['source'].get('fail_on_error', False):
                            raise
                        
            logger.info(f"Successfully extracted {len(records)} records")
            return records
            
        except Exception as e:
            logger.error(f"Error reading source file: {e}")
            raise
    
    def _parse_record(self, row: Dict[str, str], row_num: int) -> Dict[str, Any]:
        """
        Parse a single record from CSV row.
        
        Args:
            row: Dictionary containing row data
            row_num: Row number for error reporting
            
        Returns:
            Parsed record dictionary
        """
        try:
            return {
                'line_item_id': row.get('line_item_id', '').strip(),
                'order_id': row.get('order_id', '').strip(),
                'product_id': row.get('product_id', '').strip(),
                'product_name': row.get('product_name', '').strip(),
                'quantity': self._parse_decimal(row.get('quantity', '0')),
                'unit_price': self._parse_decimal(row.get('unit_price', '0.00')),
                'discount_amount': self._parse_decimal(row.get('discount_amount', '0.00')),
                'tax_amount': self._parse_decimal(row.get('tax_amount', '0.00')),
                'order_date': row.get('order_date', '').strip(),
                'customer_id': row.get('customer_id', '').strip(),
                'row_number': row_num,
                'extraction_timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            raise ValueError(f"Failed to parse record at row {row_num}: {e}")
    
    @staticmethod
    def _parse_decimal(value: str) -> float:
        """
        Parse string to decimal value.
        
        Args:
            value: String representation of number
            
        Returns:
            Float value
        """
        try:
            return float(value.strip().replace(',', ''))
        except (ValueError, AttributeError):
            return 0.0


===FILE: src/transform.py===
"""
Transform module for Sales Line Item Processing.
Performs calculations including discount percentage, line totals, and validations.
"""
import logging
from typing import Dict, Any, List, Tuple
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

logger = logging.getLogger(__name__)


class LineItemTransformer:
    """Transforms sales line item data with calculations and validations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.validation_rules = config['transformation']['validation_rules']
        self.calculation_rules = config['transformation']['calculation_rules']
        self.error_records = []
        
    def transform(self, records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Transform line item records with calculations and validations.
        
        Args:
            records: List of extracted line item records
            
        Returns:
            Tuple of (valid_records, error_records)
        """
        logger.info(f"Starting transformation of {len(records)} records")
        
        valid_records = []
        self.error_records = []
        
        for record in records:
            try:
                transformed_record = self._transform_record(record)
                
                if self._validate_record(transformed_record):
                    valid_records.append(transformed_record)
                else:
                    self.error_records.append(transformed_record)
                    
            except Exception as e:
                logger.error(f"Error transforming record {record.get('line_item_id')}: {e}")
                record['error_message'] = str(e)
                self.error_records.append(record)
        
        logger.info(f"Transformation complete: {len(valid_records)} valid, {len(self.error_records)} errors")
        return valid_records, self.error_records
    
    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single line item record.
        
        Args:
            record: Input record dictionary
            
        Returns:
            Transformed record with calculated fields
        """
        transformed = record.copy()
        
        # Convert to Decimal for precise calculations
        quantity = Decimal(str(record['quantity']))
        unit_price = Decimal(str(record['unit_price']))
        discount_amount = Decimal(str(record['discount_amount']))
        tax_amount = Decimal(str(record['tax_amount']))
        
        # Calculate gross amount (before discount)
        gross_amount = self._calculate_gross_amount(quantity, unit_price)
        transformed['gross_amount'] = float(gross_amount)
        
        # Calculate discount percentage
        discount_percentage = self._calculate_discount_percentage(
            discount_amount, 
            gross_amount
        )
        transformed['discount_percentage'] = float(discount_percentage)
        
        # Calculate net amount (after discount, before tax)
        net_amount = gross_amount - discount_amount
        transformed['net_amount'] = float(net_amount)
        
        # Calculate line total (final amount including tax)
        line_total = self._calculate_line_total(net_amount, tax_amount)
        transformed['line_total'] = float(line_total)
        
        # Calculate effective unit price (after discount)
        effective_unit_price = self._calculate_effective_unit_price(
            net_amount, 
            quantity
        )
        transformed['effective_unit_price'] = float(effective_unit_price)
        
        # Add validation flags
        transformed['unit_price_valid'] = self._validate_unit_price(unit_price)
        transformed['discount_valid'] = self._validate_discount(discount_amount, gross_amount)
        transformed['quantity_valid'] = self._validate_quantity(quantity)
        
        # Add metadata
        transformed['transformation_timestamp'] = record['extraction_timestamp']
        transformed['calculation_version'] = self.config['transformation'].get('version', '1.0')
        
        return transformed
    
    def _calculate_gross_amount(self, quantity: Decimal, unit_price: Decimal) -> Decimal:
        """
        Calculate gross amount (quantity * unit_price).
        
        Args:
            quantity: Item quantity
            unit_price: Price per unit
            
        Returns:
            Gross amount rounded to 2 decimal places
        """
        precision = self.calculation_rules.get('decimal_precision', 2)
        gross = quantity * unit_price
        return gross.quantize(Decimal(f'0.{"0" * precision}'), rounding=ROUND_HALF_UP)
    
    def _calculate_discount_percentage(self, discount_amount: Decimal, gross_amount: Decimal) -> Decimal:
        """
        Calculate discount percentage.
        
        Args:
            discount_amount: Discount amount
            gross_amount: Gross amount before discount
            
        Returns:
            Discount percentage rounded to 2 decimal places
        """
        if gross_amount == 0:
            return Decimal('0.00')
        
        percentage = (discount_amount / gross_amount) * Decimal('100')
        return percentage.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    def _calculate_line_total(self, net_amount: Decimal, tax_amount: Decimal) -> Decimal:
        """
        Calculate line total (net amount + tax).
        
        Args:
            net_amount: Amount after discount
            tax_amount: Tax amount
            
        Returns:
            Line total rounded to 2 decimal places
        """
        precision = self.calculation_rules.get('decimal_precision', 2)
        total = net_amount + tax_amount
        return total.quantize(Decimal(f'0.{"0" * precision}'), rounding=ROUND_HALF_UP)
    
    def _calculate_effective_unit_price(self, net_amount: Decimal, quantity: Decimal) -> Decimal:
        """
        Calculate effective unit price after discount.
        
        Args:
            net_amount: Amount after discount
            quantity: Item quantity
            
        Returns:
            Effective unit price rounded to 2 decimal places
        """
        if quantity == 0:
            return Decimal('0.00')
        
        precision = self.calculation_rules.get('decimal_precision', 2)
        effective_price = net_amount / quantity
        return effective_price.quantize(Decimal(f'0.{"0" * precision}'), rounding=ROUND_HALF_UP)
    
    def _validate_unit_price(self, unit_price: Decimal) -> bool:
        """
        Validate unit price is within acceptable range.
        
        Args:
            unit_price: Unit price to validate
            
        Returns:
            True if valid, False otherwise
        """
        min_price = Decimal(str(self.validation_rules.get('min_unit_price', 0)))
        max_price = Decimal(str(self.validation_rules.get('max_unit_price', 999999.99)))
        
        return min_price <= unit_price <= max_price
    
    def _validate_discount(self, discount_amount: Decimal, gross_amount: Decimal) -> bool:
        """
        Validate discount doesn't exceed gross amount.
        
        Args:
            discount_amount: Discount amount
            gross_amount: Gross amount
            
        Returns:
            True if valid, False otherwise
        """
        if discount_amount < 0:
            return False
        
        max_discount_pct = Decimal(str(self.validation_rules.get('max_discount_percentage', 100)))
        
        if gross_amount > 0:
            discount_pct = (discount_amount / gross_amount) * Decimal('100')
            return discount_pct <= max_discount_pct
        
        return discount_amount == 0
    
    def _validate_quantity(self, quantity: Decimal) -> bool:
        """
        Validate quantity is positive.
        
        Args:
            quantity: Quantity to validate
            
        Returns:
            True if valid, False otherwise
        """
        min_quantity = Decimal(str(self.validation_rules.get('min_quantity', 0)))
        max_quantity = Decimal(str(self.validation_rules.get('max_quantity', 999999)))
        
        return min_quantity < quantity <= max_quantity
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate entire record meets all business rules.
        
        Args:
            record: Transformed record
            
        Returns:
            True if all validations pass, False otherwise
        """
        validations = [
            record.get('unit_price_valid', False),
            record.get('discount_valid', False),
            record.get('quantity_valid', False),
            bool(record.get('line_item_id')),
            bool(record.get('order_id')),
            bool(record.get('product_id'))
        ]
        
        is_valid = all(validations)
        
        if not is_valid:
            record['validation_status'] = 'FAILED'
            record['error_message'] = self._build_error_message(record)
        else:
            record['validation_status'] = 'PASSED'
            
        return is_valid
    
    def _build_error_message(self, record: Dict[str, Any]) -> str:
        """
        Build detailed error message for failed validations.
        
        Args:
            record: Record with validation results
            
        Returns:
            Error message string
        """
        errors = []
        
        if not record.get('unit_price_valid'):
            errors.append("Invalid unit price")
        if not record.get('discount_valid'):
            errors.append("Invalid discount amount")
        if not record.get('quantity_valid'):
            errors.append("Invalid quantity")
        if not record.get('line_item_id'):
            errors.append("Missing line item ID")
        if not record.get('order_id'):
            errors.append("Missing order ID")
        if not record.get('product_id'):
            errors.append("Missing product ID")
            
        return "; ".join(errors)


===FILE: src/load.py===
"""
Load module for Sales Line Item Processing.
Writes transformed data to target destinations.
"""
import logging
from typing import Dict, Any, List
from pathlib import Path
import csv
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class LineItemLoader:
    """Loads transformed line item data to target destinations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the loader.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.target_path = Path(config['target']['output_path'])
        self.error_path = Path(config['target']['error_path'])
        self.encoding = config['target'].get('encoding', 'utf-8')
        self.delimiter = config['target'].get('delimiter', ',')
        
        # Ensure output directories exist
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        self.error_path.parent.mkdir(parents=True, exist_ok=True)
        
    def load(self, valid_records: List[Dict[str, Any]], 
             error_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load valid and error records to respective destinations.
        
        Args:
            valid_records: List of valid transformed records
            error_records: List of error records
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Starting load: {len(valid_records)} valid, {len(error_records)} errors")
        
        stats = {
            'valid_loaded': 0,
            'errors_logged': 0,
            'load_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Load valid records
            if valid_records:
                stats['valid_loaded'] = self._load_valid_records(valid_records)
            
            # Load error records
            if error_records:
                stats['errors_logged'] = self._load_error_records(error_records)
            
            # Write load statistics
            self._write_statistics(stats)
            
            logger.info(f"Load complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during load: {e}")
            raise
    
    def _load_valid_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Load valid records to target file.
        
        Args:
            records: List of valid records
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(records)} valid records to {self.target_path}")
        
        output_fields = self._get_output_fields()
        
        try:
            with open(self.target_path, 'w', encoding=self.encoding, newline='') as file:
                writer = csv.DictWriter(
                    file, 
                    fieldnames=output_fields,
                    delimiter=self.delimiter,
                    extrasaction='ignore'
                )
                
                writer.writeheader()
                
                for record in records:
                    formatted_record = self._format_output_record(record)
                    writer.writerow(formatted_record)
            
            logger.info(f"Successfully loaded {len(records)} records")
            return len(records)
            
        except Exception as e:
            logger.error(f"Error loading valid records: {e}")
            raise
    
    def _load_error_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Load error records to error file.
        
        Args:
            records: List of error records
            
        Returns:
            Number of error records logged
        """
        logger.info(f"Logging {len(records)} error records to {self.error_path}")
        
        error_fields = self._get_error_fields()
        
        try:
            with open(self.error_path, 'w', encoding=self.encoding, newline='') as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=error_fields,
                    delimiter=self.delimiter,
                    extrasaction='ignore'
                )
                
                writer.writeheader()
                writer.writerows(records)
            
            logger.info(f"Successfully logged {len(records)} error records")
            return len(records)
            
        except Exception as e:
            logger.error(f"Error loading error records: {e}")
            raise
    
    def _get_output_fields(self) -> List[str]:
        """
        Get list of output fields for valid records.
        
        Returns:
            List of field names
        """
        return [
            'line_item_id',
            'order_id',
            'product_id',
            'product_name',
            'customer_id',
            'order_date',
            'quantity',
            'unit_price',
            'gross_amount',
            'discount_amount',
            'discount_percentage',
            'net_amount',
            'tax_amount',
            'line_total',
            'effective_unit_price',
            'validation_status',
            'transformation_timestamp'
        ]
    
    def _get_error_fields(self) -> List[str]:
        """
        Get list of fields for error records.
        
        Returns:
            List of field names
        """
        return [
            'line_item_id',
            'order_id',
            'product_id',
            'row_number',
            'error_message',
            'validation_status',
            'unit_price_valid',
            'discount_valid',
            'quantity_valid',
            'extraction_timestamp'
        ]
    
    def _format_output_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format record for output with proper decimal formatting.
        
        Args:
            record: Record to format
            
        Returns:
            Formatted record
        """
        formatted = record.copy()
        
        # Format decimal fields
        decimal_fields = [
            'quantity', 'unit_price', 'gross_amount', 'discount_amount',
            'discount_percentage', 'net_amount', 'tax_amount', 'line_total',
            'effective_unit_price'
        ]
        
        precision = self.config['target'].get('decimal_precision', 2)
        
        for field in decimal_fields:
            if field in formatted and formatted[field] is not None:
                formatted[field] = f"{formatted[field]:.{precision}f}"
        
        return formatted
    
    def _write_statistics(self, stats: Dict[str, Any]) -> None:
        """
        Write load statistics to file.
        
        Args:
            stats: Statistics dictionary
        """
        stats_path = self.target_path.parent / 'load_statistics.json'
        
        try:
            with open(stats_path, 'w', encoding='utf-8') as file:
                json.dump(stats, file, indent=2)
            
            logger.info(f"Statistics written to {stats_path}")
            
        except Exception as e:
            logger.warning(f"Failed to write statistics: {e}")


===FILE: src/pipeline.py===
"""
Main pipeline orchestration for Sales Line Item Processing.
"""
import logging
from typing import Dict, Any
import yaml
from pathlib import Path

from src.extract import LineItemExtractor
from src.transform import LineItemTransformer
from src.load import LineItemLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LineItemPipeline:
    """Orchestrates the complete ETL pipeline for line items."""
    
    def __init__(self, config_path: str):
        """
        Initialize the pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.extractor = LineItemExtractor(self.config)
        self.transformer = LineItemTransformer(self.config)
        self.loader = LineItemLoader(self.config)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Pipeline execution statistics
        """
        logger.info("Starting Sales Line Item Processing Pipeline")
        
        try:
            # Extract
            logger.info("Phase 1: Extract")
            records = self.extractor.extract()
            
            # Transform
            logger.info("Phase 2: Transform")
            valid_records, error_records = self.transformer.transform(records)
            
            # Load
            logger.info("Phase 3: Load")
            load_stats = self.loader.load(valid_records, error_records)
            
            # Compile statistics
            stats = {
                'total_extracted': len(records),
                'valid_records': len(valid_records),
                'error_records': len(error_records),
                'success_rate': (len(valid_records) / len(records) * 100) if records else 0,
                'load_stats': load_stats
            }
            
            logger.info(f"Pipeline complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point."""
    config_path = 'config.yaml'
    
    try:
        pipeline = LineItemPipeline(config_path)
        stats = pipeline.run()
        
        print("\nPipeline Execution Summary:")
        print(f"Total Records Extracted: {stats['total_extracted']}")
        print(f"Valid Records: {stats['valid_records']}")
        print(f"Error Records: {stats['error_records']}")
        print(f"Success Rate: {stats['success_rate']:.2f}%")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == '__main__':
    main()


===FILE: config.yaml===
# Sales Line Item Processing Configuration

source:
  line_items_path: 'data/input/line_items.csv'
  encoding: 'utf-8'
  delimiter: ','
  fail_on_error: false

transformation:
  version: '1.0'
  
  calculation_rules:
    decimal_precision: 2
    rounding_mode: 'ROUND_HALF_UP'
    
  validation_rules:
    min_unit_price: 0.01
    max_unit_price: 999999.99
    min_quantity: 0
    max_quantity: 999999
    max_discount_percentage: 100
    
  business_rules:
    allow_zero_price: false
    allow_negative_discount: false
    require_product_id: true
    require_order_id: true

target:
  output_path: 'data/output/line_items_processed.csv'
  error_path: 'data/output/line_items_errors.csv'
  encoding: 'utf-8'
  delimiter: ','
  decimal_precision: 2
  
logging:
  level: 'INFO'
  format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  file: 'logs/line_item_processing.log'

nifi:
  processor_group_name: 'Sales_Line_Item_Processing'
  flow_file_concurrency: 10
  back_pressure_object_threshold: 10000
  back_pressure_data_size_threshold: '1 GB'


===FILE: src/nifi_deployment.py===
"""
NiFi deployment module for Sales Line Item Processing.
Creates and configures NiFi processor groups and processors.
"""
import logging
from typing import Dict, Any, Optional
import nipyapi
from nipyapi.nifi import ProcessorEntity, ProcessGroupEntity

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NiFiLineItemDeployment:
    """Deploys Sales Line Item Processing flow to NiFi."""
    
    def __init__(self, config: Dict[str, Any], nifi_url: str = 'http://localhost:8080/nifi-api'):
        """
        Initialize NiFi deployment.
        
        Args:
            config: Configuration dictionary
            nifi_url: NiFi API URL
        """
        self.config = config
        self.nifi_url = nifi_url
        nipyapi.config.nifi_config.host = nifi_url
        
    def deploy(self, parent_pg_id: Optional[str] = None) -> ProcessGroupEntity:
        """
        Deploy the complete flow to NiFi.
        
        Args:
            parent_pg_id: Parent process group ID (None for root)
            
        Returns:
            Created process group entity
        """
        logger.info("Starting NiFi deployment for Sales Line Item Processing")
        
        try:
            # Get root process group if parent not specified
            if parent_pg_id is None:
                root_pg = nipyapi.canvas.get_root_pg_id()
                parent_pg_id = root_pg
            
            # Create main process group
            pg_name = self.config['nifi']['processor_group_name']
            process_group = self._create_process_group(parent_pg_id, pg_name)
            
            # Create processors
            self._create_extract_processor(process_group.id)
            self._create_transform_processor(process_group.id)
            self._create_load_processor(process_group.id)
            self._create_error_handler(process_group.id)
            
            # Create connections
            self._create_connections(process_group.id)
            
            logger.info(f"Successfully deployed flow to process group: {pg_name}")
            return process_group
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise
    
    def _create_process_group(self, parent_id: str, name: str) -> ProcessGroupEntity:
        """
        Create a process group.
        
        Args:
            parent_id: Parent process group ID
            name: Process group name
            
        Returns:
            Created process group
        """
        logger.info(f"Creating process group: {name}")
        
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(parent_id, 'id'),
                new_pg_name=name,
                location=(400.0, 400.0)
            )
            return pg
        except Exception as e:
            logger.error(f"Failed to create process group: {e}")
            raise
    
    def _create_extract_processor(self, pg_id: str) -> ProcessorEntity:
        """
        Create GetFile processor for extraction.
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Created processor
        """
        logger.info("Creating extraction processor")
        
        config = {
            'Input Directory': self.config['source']['line_items_path'].rsplit('/', 1)[0],
            'File Filter': self.config['source']['line_items_path'].rsplit('/', 1)[1],
            'Keep Source File': 'false',
            'Batch Size': '100',
            'Polling Interval': '10 sec'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
            location=(200.0, 200.0),
            name='Extract_Line_Items',
            config=config
        )
        
        return processor
    
    def _create_transform_processor(self, pg_id: str) -> ProcessorEntity:
        """
        Create ExecuteScript processor for transformation.
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Created processor
        """
        logger.info("Creating transformation processor")
        
        script_body = self._generate_transform_script()
        
        config = {
            'Script Engine': 'python',
            'Script Body': script_body,
            'Module Directory': './python_modules'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.script.ExecuteScript'),
            location=(500.0, 200.0),
            name='Transform_Calculate_Line_Items',
            config=config
        )
        
        return processor
    
    def _create_load_processor(self, pg_id: str) -> ProcessorEntity:
        """
        Create PutFile processor for loading.
        
        Args:
            pg_id: Process group ID
            
        Returns:
            Created processor
        """
        logger.info("Creating load processor")
        
        config = {
            'Directory': self.config['target']['output_path'].rsplit('/', 1)[0],
            'Conflict Resolution Strategy': 'replace',
            'Create Missing Directories': 'true',
            'Maximum File Count': '-1'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent