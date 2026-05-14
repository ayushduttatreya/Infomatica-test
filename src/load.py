"""
Load module for customer address pipeline.
Handles loading validated data to target systems and generating reports.
"""
import logging
from typing import Dict, Any
import pandas as pd
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerAddressLoader:
    """Loads validated customer address data to target systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize loader with configuration.
        
        Args:
            config: Configuration dictionary containing target paths and settings
        """
        self.config = config
        self.target_config = config.get('target', {})
        self.reporting_config = config.get('reporting', {})
        
    def load_addresses(self, df: pd.DataFrame) -> int:
        """
        Load valid addresses to target system.
        
        Args:
            df: DataFrame containing valid addresses
            
        Returns:
            Number of records loaded
        """
        target_path = Path(self.target_config.get('addresses_path'))
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Loading {len(df)} addresses to {target_path}")
        
        # Select and order columns for output
        output_columns = [
            'address_id',
            'customer_id',
            'address_type',
            'street_address',
            'street_address2',
            'city',
            'state_province',
            'postal_code',
            'country_code',
            'is_primary',
            'is_active',
            'first_name',
            'last_name',
            'email',
            'processed_date'
        ]
        
        output_df = df[output_columns].copy()
        
        # Write to target
        output_df.to_csv(target_path, index=False)
        
        logger.info(f"Successfully loaded {len(output_df)} addresses")
        return len(output_df)
        
    def load_rejected_records(self, invalid_addresses: pd.DataFrame, 
                             invalid_customers: pd.DataFrame,
                             orphaned_addresses: pd.DataFrame) -> int:
        """
        Load rejected records to error files.
        
        Args:
            invalid_addresses: DataFrame containing invalid addresses
            invalid_customers: DataFrame containing invalid customers
            orphaned_addresses: DataFrame containing orphaned addresses
            
        Returns:
            Total number of rejected records
        """
        reject_path = Path(self.target_config.get('reject_path'))
        reject_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save invalid addresses
        if not invalid_addresses.empty:
            invalid_addr_path = reject_path / f'invalid_addresses_{timestamp}.csv'
            invalid_addresses.to_csv(invalid_addr_path, index=False)
            logger.info(f"Saved {len(invalid_addresses)} invalid addresses to {invalid_addr_path}")
            
        # Save invalid customers
        if not invalid_customers.empty:
            invalid_cust_path = reject_path / f'invalid_customers_{timestamp}.csv'
            invalid_customers.to_csv(invalid_cust_path, index=False)
            logger.info(f"Saved {len(invalid_customers)} invalid customers to {invalid_cust_path}")
            
        # Save orphaned addresses
        if not orphaned_addresses.empty:
            orphaned_path = reject_path / f'orphaned_addresses_{timestamp}.csv'
            orphaned_addresses.to_csv(orphaned_path, index=False)
            logger.info(f"Saved {len(orphaned_addresses)} orphaned addresses to {orphaned_path}")
            
        total_rejected = len(invalid_addresses) + len(invalid_customers) + len(orphaned_addresses)
        return total_rejected
        
    def generate_reconciliation_report(self, results: Dict[str, Any]) -> pd.DataFrame:
        """
        Generate reconciliation report with processing statistics.
        
        Args:
            results: Dictionary containing processing results and metrics
            
        Returns:
            DataFrame containing reconciliation report
        """
        report_data = {
            'metric': [],
            'count': [],
            'percentage': []
        }
        
        total_addresses = results.get('total_addresses', 0)
        total_customers = results.get('total_customers', 0)
        
        # Address metrics
        report_data['metric'].append('Total Addresses Processed')
        report_data['count'].append(total_addresses)
        report_data['percentage'].append(100.0)
        
        valid_addresses = results.get('valid_addresses_count', 0)
        report_data['metric'].append('Valid Addresses Loaded')
        report_data['count'].append(valid_addresses)
        report_data['percentage'].append(
            round(valid_addresses / total_addresses * 100, 2) if total_addresses > 0 else 0
        )
        
        invalid_addresses = results.get('invalid_addresses_count', 0)
        report_data['metric'].append('Invalid Addresses Rejected')
        report_data['count'].append(invalid_addresses)
        report_data['percentage'].append(
            round(invalid_addresses / total_addresses * 100, 2) if total_addresses > 0 else 0
        )
        
        orphaned_addresses = results.get('orphaned_addresses_count', 0)
        report_data['metric'].append('Orphaned Addresses')
        report_data['count'].append(orphaned_addresses)
        report_data['percentage'].append(
            round(orphaned_addresses / total_addresses * 100, 2) if total_addresses > 0 else 0
        )
        
        # Customer metrics
        report_data['metric'].append('Total Customers Processed')
        report_data['count'].append(total_customers)
        report_data['percentage'].append(100.0)
        
        valid_customers = results.get('valid_customers_count', 0)
        report_data['metric'].append('Valid Customers')
        report_data['count'].append(valid_customers)
        report_data['percentage'].append(
            round(valid_customers / total_customers * 100, 2) if total_customers > 0 else 0
        )
        
        invalid_customers = results.get('invalid_customers_count', 0)
        report_data['metric'].append('Invalid Customers')
        report_data['count'].append(invalid_customers)
        report_data['percentage'].append(
            round(invalid_customers / total_customers * 100, 2) if total_customers > 0 else 0
        )
        
        # Duplicate metrics
        duplicate_groups = results.get('duplicate_groups_count', 0)
        report_data['metric'].append('Duplicate Address Groups')
        report_data['count'].append(duplicate_groups)
        report_data['percentage'].append(
            round(duplicate_groups / total_addresses * 100, 2) if total_addresses > 0 else 0
        )
        
        report_df = pd.DataFrame(report_data)
        return report_df
        
    def generate_validation_summary(self, invalid_addresses: pd.DataFrame,
                                   invalid_customers: pd.DataFrame) -> pd.DataFrame:
        """
        Generate summary of validation errors.
        
        Args:
            invalid_addresses: DataFrame containing invalid addresses
            invalid_customers: DataFrame containing invalid customers
            
        Returns:
            DataFrame containing validation error summary
        """
        error_summary = []
        
        # Address validation errors
        if not invalid_addresses.empty:
            addr_errors = invalid_addresses['validation_errors'].value_counts()
            for error, count in addr_errors.items():
                error_summary.append({
                    'record_type': 'Address',
                    'error_message': error,
                    'count': count
                })
                
        # Customer validation errors
        if not invalid_customers.empty:
            cust_errors = invalid_customers['validation_errors'].value_counts()
            for error, count in cust_errors.items():
                error_summary.append({
                    'record_type': 'Customer',
                    'error_message': error,
                    'count': count
                })
                
        summary_df = pd.DataFrame(error_summary)
        return summary_df
        
    def save_reports(self, reconciliation_report: pd.DataFrame,
                    validation_summary: pd.DataFrame,
                    duplicate_summary: pd.DataFrame) -> None:
        """
        Save all reports to configured location.
        
        Args:
            reconciliation_report: Reconciliation report DataFrame
            validation_summary: Validation summary DataFrame
            duplicate_summary: Duplicate summary DataFrame
        """
        report_path = Path(self.reporting_config.get('report_path'))
        report_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save reconciliation report
        recon_path = report_path / f'reconciliation_report_{timestamp}.csv'
        reconciliation_report.to_csv(recon_path, index=False)
        logger.info(f"Saved reconciliation report to {recon_path}")
        
        # Save validation summary
        if not validation_summary.empty:
            validation_path = report_path / f'validation_summary_{timestamp}.csv'
            validation_summary.to_csv(validation_path, index=False)
            logger.info(f"Saved validation summary to {validation_path}")
            
        # Save duplicate summary
        if not duplicate_summary.empty:
            duplicate_path = report_path / f'duplicate_summary_{timestamp}.csv'
            duplicate_summary.to_csv(duplicate_path, index=False)
            logger.info(f"Saved duplicate summary to {duplicate_path}")
            
    def load(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Execute full load pipeline.
        
        Args:
            transformed_data: Dictionary containing transformed data
            
        Returns:
            Dictionary containing load results and metrics
        """
        logger.info("Starting load pipeline")
        
        valid_addresses = transformed_data['valid_addresses']
        invalid_addresses = transformed_data['invalid_addresses']
        invalid_customers = transformed_data['invalid_customers']
        orphaned_addresses = transformed_data['orphaned_addresses']
        duplicate_summary = transformed_data['duplicate_summary']
        
        # Load valid addresses
        loaded_count = self.load_addresses(valid_addresses)
        
        # Load rejected records
        rejected_count = self.load_rejected_records(
            invalid_addresses,
            invalid_customers,
            orphaned_addresses
        )
        
        # Prepare results for reporting
        results = {
            'total_addresses': len(valid_addresses) + len(invalid_addresses) + len(orphaned_addresses),
            'total_customers': len(invalid_customers) + len(valid_addresses['customer_id'].unique()),
            'valid_addresses_count': len(valid_addresses),
            'invalid_addresses_count': len(invalid_addresses),
            'orphaned_addresses_count': len(orphaned_addresses),
            'valid_customers_count': len(valid_addresses['customer_id'].unique()),
            'invalid_customers_count': len(invalid_customers),
            'duplicate_groups_count': len(duplicate_summary),
            'loaded_count': loaded_count,
            'rejected_count': rejected_count
        }
        
        # Generate reports
        logger.info("Generating reconciliation reports")
        reconciliation_report = self.generate_reconciliation_report(results)
        validation_summary = self.generate_validation_summary(invalid_addresses, invalid_customers)
        
        # Save reports
        self.save_reports(reconciliation_report, validation_summary, duplicate_summary)
        
        logger.info(f"Load pipeline completed. Loaded: {loaded_count}, Rejected: {rejected_count}")
        
        return results