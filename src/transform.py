"""
Forecast ID Validation Transform Module

This module implements validation logic for forecast IDs based on Informatica
mapping specifications. It validates format, uniqueness, and business rules.
"""

import re
import logging
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Container for validation results"""
    is_valid: bool
    forecast_id: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


@dataclass
class ValidationStats:
    """Statistics for validation operations"""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    format_errors: int = 0
    business_rule_errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class ForecastIDValidator:
    """
    Validates forecast IDs according to business rules and format specifications.
    
    Validation Rules:
    1. Format: Must match pattern [A-Z]{3}-[0-9]{6}-[A-Z]{2}
    2. Uniqueness: No duplicate IDs within batch
    3. Business Rules:
       - Prefix must be valid region code
       - Numeric portion must be within valid range
       - Suffix must be valid forecast type
    """
    
    # Format pattern: 3 uppercase letters, dash, 6 digits, dash, 2 uppercase letters
    FORECAST_ID_PATTERN = re.compile(r'^[A-Z]{3}-[0-9]{6}-[A-Z]{2}$')
    
    def __init__(self, config: Dict):
        """
        Initialize validator with configuration.
        
        Args:
            config: Configuration dictionary containing validation parameters
        """
        self.config = config
        self.valid_region_codes = set(config.get('valid_region_codes', []))
        self.valid_forecast_types = set(config.get('valid_forecast_types', []))
        self.min_sequence = config.get('min_sequence_number', 1)
        self.max_sequence = config.get('max_sequence_number', 999999)
        self.enable_uniqueness_check = config.get('enable_uniqueness_check', True)
        self.enable_business_rules = config.get('enable_business_rules', True)
        
        # Track seen IDs for uniqueness validation
        self.seen_ids: Set[str] = set()
        self.stats = ValidationStats(start_time=datetime.now())
        
        logger.info(f"Initialized ForecastIDValidator with config: {config}")
    
    def validate_format(self, forecast_id: str) -> Tuple[bool, List[str]]:
        """
        Validate forecast ID format.
        
        Args:
            forecast_id: The forecast ID to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        if not forecast_id:
            errors.append("Forecast ID is empty or null")
            return False, errors
        
        if not isinstance(forecast_id, str):
            errors.append(f"Forecast ID must be string, got {type(forecast_id)}")
            return False, errors
        
        # Check length first for better error messages
        if len(forecast_id) != 14:
            errors.append(
                f"Forecast ID length must be 14 characters, got {len(forecast_id)}"
            )
            return False, errors
        
        # Check pattern
        if not self.FORECAST_ID_PATTERN.match(forecast_id):
            errors.append(
                f"Forecast ID format invalid. Expected: XXX-NNNNNN-XX "
                f"(3 letters, dash, 6 digits, dash, 2 letters)"
            )
            return False, errors
        
        return True, errors
    
    def validate_business_rules(self, forecast_id: str) -> Tuple[bool, List[str], List[str]]:
        """
        Validate business rules for forecast ID.
        
        Args:
            forecast_id: The forecast ID to validate
            
        Returns:
            Tuple of (is_valid, error_messages, warning_messages)
        """
        errors = []
        warnings = []
        
        if not self.enable_business_rules:
            return True, errors, warnings
        
        # Parse components
        parts = forecast_id.split('-')
        region_code = parts[0]
        sequence_str = parts[1]
        forecast_type = parts[2]
        
        # Validate region code
        if self.valid_region_codes and region_code not in self.valid_region_codes:
            errors.append(
                f"Invalid region code '{region_code}'. "
                f"Valid codes: {sorted(self.valid_region_codes)}"
            )
        
        # Validate sequence number range
        sequence_num = int(sequence_str)
        if sequence_num < self.min_sequence or sequence_num > self.max_sequence:
            errors.append(
                f"Sequence number {sequence_num} out of valid range "
                f"[{self.min_sequence}, {self.max_sequence}]"
            )
        
        # Validate forecast type
        if self.valid_forecast_types and forecast_type not in self.valid_forecast_types:
            errors.append(
                f"Invalid forecast type '{forecast_type}'. "
                f"Valid types: {sorted(self.valid_forecast_types)}"
            )
        
        # Warning for sequence numbers near limits
        if sequence_num > self.max_sequence * 0.95:
            warnings.append(
                f"Sequence number {sequence_num} is near maximum limit"
            )
        
        is_valid = len(errors) == 0
        return is_valid, errors, warnings
    
    def check_uniqueness(self, forecast_id: str) -> Tuple[bool, List[str]]:
        """
        Check if forecast ID is unique within the batch.
        
        Args:
            forecast_id: The forecast ID to check
            
        Returns:
            Tuple of (is_unique, error_messages)
        """
        errors = []
        
        if not self.enable_uniqueness_check:
            return True, errors
        
        if forecast_id in self.seen_ids:
            errors.append(f"Duplicate forecast ID detected: {forecast_id}")
            self.stats.duplicate_records += 1
            return False, errors
        
        self.seen_ids.add(forecast_id)
        return True, errors
    
    def validate(self, record: Dict) -> ValidationResult:
        """
        Perform complete validation on a record.
        
        Args:
            record: Dictionary containing forecast data with 'forecast_id' key
            
        Returns:
            ValidationResult object with validation outcome
        """
        self.stats.total_records += 1
        
        forecast_id = record.get('forecast_id', '').strip()
        
        result = ValidationResult(
            is_valid=True,
            forecast_id=forecast_id,
            metadata={
                'record_number': self.stats.total_records,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        # Format validation
        format_valid, format_errors = self.validate_format(forecast_id)
        if not format_valid:
            result.is_valid = False
            result.errors.extend(format_errors)
            self.stats.format_errors += 1
            self.stats.invalid_records += 1
            return result
        
        # Business rules validation
        rules_valid, rules_errors, rules_warnings = self.validate_business_rules(forecast_id)
        if not rules_valid:
            result.is_valid = False
            result.errors.extend(rules_errors)
            self.stats.business_rule_errors += 1
        
        result.warnings.extend(rules_warnings)
        
        # Uniqueness check
        unique_valid, unique_errors = self.check_uniqueness(forecast_id)
        if not unique_valid:
            result.is_valid = False
            result.errors.extend(unique_errors)
        
        # Update statistics
        if result.is_valid:
            self.stats.valid_records += 1
        else:
            self.stats.invalid_records += 1
        
        return result
    
    def validate_batch(self, records: List[Dict]) -> List[ValidationResult]:
        """
        Validate a batch of records.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            List of ValidationResult objects
        """
        logger.info(f"Starting batch validation of {len(records)} records")
        results = []
        
        for record in records:
            result = self.validate(record)
            results.append(result)
            
            if not result.is_valid:
                logger.debug(
                    f"Validation failed for forecast_id={result.forecast_id}: "
                    f"{', '.join(result.errors)}"
                )
        
        self.stats.end_time = datetime.now()
        logger.info(f"Batch validation complete: {self.get_stats_summary()}")
        
        return results
    
    def get_stats_summary(self) -> str:
        """Get formatted statistics summary"""
        duration = None
        if self.stats.start_time and self.stats.end_time:
            duration = (self.stats.end_time - self.stats.start_time).total_seconds()
        
        return (
            f"Total: {self.stats.total_records}, "
            f"Valid: {self.stats.valid_records}, "
            f"Invalid: {self.stats.invalid_records}, "
            f"Duplicates: {self.stats.duplicate_records}, "
            f"Format Errors: {self.stats.format_errors}, "
            f"Business Rule Errors: {self.stats.business_rule_errors}"
            f"{f', Duration: {duration:.2f}s' if duration else ''}"
        )
    
    def reset_stats(self):
        """Reset validation statistics and seen IDs"""
        self.seen_ids.clear()
        self.stats = ValidationStats(start_time=datetime.now())
        logger.info("Validation statistics reset")


def create_validator_from_config(config_path: str) -> ForecastIDValidator:
    """
    Factory function to create validator from configuration file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Configured ForecastIDValidator instance
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    validation_config = config.get('validation', {})
    return ForecastIDValidator(validation_config)


def split_valid_invalid_records(
    records: List[Dict],
    results: List[ValidationResult]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Split records into valid and invalid based on validation results.
    
    Args:
        records: Original records
        results: Validation results
        
    Returns:
        Tuple of (valid_records, invalid_records)
    """
    valid_records = []
    invalid_records = []
    
    for record, result in zip(records, results):
        enriched_record = record.copy()
        enriched_record['validation_result'] = {
            'is_valid': result.is_valid,
            'errors': result.errors,
            'warnings': result.warnings,
            'metadata': result.metadata
        }
        
        if result.is_valid:
            valid_records.append(enriched_record)
        else:
            invalid_records.append(enriched_record)
    
    return valid_records, invalid_records