"""
Load module for customer transaction loading pipeline.
Handles data loading to target systems with batch processing and error handling.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data to target systems with performance optimization."""
    
    def __init__(self, config: Dict):
        """
        Initialize the data loader.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.target_config = config.get('target', {})
        self.batch_size = config.get('processing', {}).get('batch_size', 10000)
        self.performance_config = config.get('performance', {})
        
    def load_customers(self, df: pd.DataFrame, target_path: str) -> Dict:
        """
        Load customer data to target with batch processing.
        
        Args:
            df: Transformed customer DataFrame
            target_path: Path to target location
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            logger.info(f"Loading {len(df)} customer records to {target_path}")
            
            start_time = datetime.now()
            
            # Create target directory if it doesn't exist
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Split into batches for performance
            batches = self._split_into_batches(df, self.batch_size)
            
            load_stats = {
                'total_records': len(df),
                'batches_processed': 0,
                'records_loaded': 0,
                'records_failed': 0,
                'start_time': start_time.isoformat(),
                'errors': []
            }
            
            # Process each batch
            for batch_num, batch_df in enumerate(batches, 1):
                try:
                    batch_file = f"{target_path}_batch_{batch_num}.parquet"
                    
                    # Write batch to parquet for performance
                    batch_df.to_parquet(
                        batch_file,
                        engine='pyarrow',
                        compression='snappy',
                        index=False
                    )
                    
                    load_stats['batches_processed'] += 1
                    load_stats['records_loaded'] += len(batch_df)
                    
                    logger.info(f"Loaded batch {batch_num} with {len(batch_df)} records")
                    
                except Exception as e:
                    error_msg = f"Error loading batch {batch_num}: {str(e)}"
                    logger.error(error_msg)
                    load_stats['errors'].append(error_msg)
                    load_stats['records_failed'] += len(batch_df)
            
            # Write consolidated file
            df.to_parquet(
                f"{target_path}_consolidated.parquet",
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Write CSV for compatibility
            df.to_csv(
                f"{target_path}_consolidated.csv",
                index=False,
                encoding='utf-8'
            )
            
            end_time = datetime.now()
            load_stats['end_time'] = end_time.isoformat()
            load_stats['duration_seconds'] = (end_time - start_time).total_seconds()
            load_stats['records_per_second'] = (
                load_stats['records_loaded'] / load_stats['duration_seconds']
                if load_stats['duration_seconds'] > 0 else 0
            )
            
            # Write load statistics
            self._write_load_stats(load_stats, f"{target_path}_load_stats.json")
            
            logger.info(f"Customer load completed: {load_stats}")
            return load_stats
            
        except Exception as e:
            logger.error(f"Error loading customer data: {str(e)}")
            raise
    
    def load_transactions(self, df: pd.DataFrame, target_path: str) -> Dict:
        """
        Load transaction data to target with optimized batch processing.
        
        Args:
            df: Transformed transaction DataFrame
            target_path: Path to target location
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            logger.info(f"Loading {len(df)} transaction records to {target_path}")
            
            start_time = datetime.now()
            
            # Create target directory
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Partition by date for performance
            if 'transaction_date' in df.columns:
                df['partition_year'] = df['transaction_date'].dt.year
                df['partition_month'] = df['transaction_date'].dt.month
            
            load_stats = {
                'total_records': len(df),
                'partitions_processed': 0,
                'records_loaded': 0,
                'records_failed': 0,
                'start_time': start_time.isoformat(),
                'errors': []
            }
            
            # Write partitioned data
            if 'partition_year' in df.columns:
                for (year, month), group in df.groupby(['partition_year', 'partition_month']):
                    try:
                        partition_path = f"{target_path}/year={year}/month={month:02d}"
                        Path(partition_path).mkdir(parents=True, exist_ok=True)
                        
                        # Write partition
                        group.to_parquet(
                            f"{partition_path}/data.parquet",
                            engine='pyarrow',
                            compression='snappy',
                            index=False
                        )
                        
                        load_stats['partitions_processed'] += 1
                        load_stats['records_loaded'] += len(group)
                        
                        logger.info(f"Loaded partition {year}-{month:02d} with {len(group)} records")
                        
                    except Exception as e:
                        error_msg = f"Error loading partition {year}-{month}: {str(e)}"
                        logger.error(error_msg)
                        load_stats['errors'].append(error_msg)
                        load_stats['records_failed'] += len(group)
            else:
                # Fall back to batch processing
                batches = self._split_into_batches(df, self.batch_size)
                
                for batch_num, batch_df in enumerate(batches, 1):
                    try:
                        batch_file = f"{target_path}_batch_{batch_num}.parquet"
                        
                        batch_df.to_parquet(
                            batch_file,
                            engine='pyarrow',
                            compression='snappy',
                            index=False
                        )
                        
                        load_stats['records_loaded'] += len(batch_df)
                        
                    except Exception as e:
                        error_msg = f"Error loading batch {batch_num}: {str(e)}"
                        logger.error(error_msg)
                        load_stats['errors'].append(error_msg)
                        load_stats['records_failed'] += len(batch_df)
            
            # Write consolidated file
            df.to_parquet(
                f"{target_path}_consolidated.parquet",
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            end_time = datetime.now()
            load_stats['end_time'] = end_time.isoformat()
            load_stats['duration_seconds'] = (end_time - start_time).total_seconds()
            load_stats['records_per_second'] = (
                load_stats['records_loaded'] / load_stats['duration_seconds']
                if load_stats['duration_seconds'] > 0 else 0
            )
            
            # Write load statistics
            self._write_load_stats(load_stats, f"{target_path}_load_stats.json")
            
            logger.info(f"Transaction load completed: {load_stats}")
            return load_stats
            
        except Exception as e:
            logger.error(f"Error loading transaction data: {str(e)}")
            raise
    
    def load_aggregated_metrics(self, df: pd.DataFrame, target_path: str) -> Dict:
        """
        Load aggregated customer metrics to target.
        
        Args:
            df: Aggregated metrics DataFrame
            target_path: Path to target location
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            logger.info(f"Loading {len(df)} aggregated metric records to {target_path}")
            
            start_time = datetime.now()
            
            # Create target directory
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write metrics
            df.to_parquet(
                f"{target_path}.parquet",
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            df.to_csv(
                f"{target_path}.csv",
                index=False,
                encoding='utf-8'
            )
            
            end_time = datetime.now()
            
            load_stats = {
                'total_records': len(df),
                'records_loaded': len(df),
                'records_failed': 0,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': (end_time - start_time).total_seconds(),
                'errors': []
            }
            
            self._write_load_stats(load_stats, f"{target_path}_load_stats.json")
            
            logger.info(f"Metrics load completed: {load_stats}")
            return load_stats
            
        except Exception as e:
            logger.error(f"Error loading aggregated metrics: {str(e)}")
            raise
    
    def validate_load(self, source_df: pd.DataFrame, target_path: str) -> Dict:
        """
        Validate loaded data against source.
        
        Args:
            source_df: Source DataFrame
            target_path: Path to loaded data
            
        Returns:
            Dictionary containing validation results
        """
        try:
            logger.info(f"Validating loaded data at {target_path}")
            
            # Read loaded data
            loaded_df = pd.read_parquet(f"{target_path}_consolidated.parquet")
            
            validation_results = {
                'source_record_count': len(source_df),
                'target_record_count': len(loaded_df),
                'record_count_match': len(source_df) == len(loaded_df),
                'validation_timestamp': datetime.now().isoformat()
            }
            
            # Check for data integrity
            if validation_results['record_count_match']:
                # Sample validation - check key fields
                key_field = source_df.columns[0]
                source_keys = set(source_df[key_field].unique())
                target_keys = set(loaded_df[key_field].unique())
                
                validation_results['key_match'] = source_keys == target_keys
                validation_results['missing_keys'] = list(source_keys - target_keys)
                validation_results['extra_keys'] = list(target_keys - source_keys)
            
            logger.info(f"Validation results: {validation_results}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating loaded data: {str(e)}")
            raise
    
    def _split_into_batches(self, df: pd.DataFrame, batch_size: int) -> List[pd.DataFrame]:
        """Split DataFrame into batches."""
        num_batches = (len(df) + batch_size - 1) // batch_size
        return [df.iloc[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]
    
    def _write_load_stats(self, stats: Dict, file_path: str):
        """Write load statistics to file."""
        try:
            with open(file_path, 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            logger.info(f"Load statistics written to {file_path}")
        except Exception as e:
            logger.warning(f"Could not write load statistics: {str(e)}")