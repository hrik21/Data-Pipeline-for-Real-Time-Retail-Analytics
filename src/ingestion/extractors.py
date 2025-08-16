"""
Data extraction components for the real-time data pipeline.
"""
import time
import json
import pickle
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
import numpy as np
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.interfaces.base import (
    DataExtractor, DataSource, ChangeEvent, ValidationResult, 
    TaskResult, TaskStatus, PipelineComponent
)


class ExtractionMode(Enum):
    """Data extraction modes."""
    FULL = "full"
    INCREMENTAL = "incremental"
    DELTA = "delta"


class SerializationFormat(Enum):
    """Data serialization formats."""
    JSON = "json"
    PICKLE = "pickle"
    PARQUET = "parquet"
    CSV = "csv"


@dataclass
class ExtractionConfig:
    """Configuration for data extraction."""
    mode: ExtractionMode = ExtractionMode.INCREMENTAL
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: int = 300
    parallel_extractions: int = 1
    serialization_format: SerializationFormat = SerializationFormat.JSON
    compression: Optional[str] = None
    validate_on_extract: bool = True


@dataclass
class ExtractionResult:
    """Result of data extraction operation."""
    source_id: str
    table_name: str
    rows_extracted: int
    extraction_time: float
    data_size_bytes: int
    validation_result: Optional[ValidationResult] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class RetryableError(Exception):
    """Exception that indicates operation should be retried."""
    pass


class NonRetryableError(Exception):
    """Exception that indicates operation should not be retried."""
    pass


class DataExtractorImpl(DataExtractor, PipelineComponent):
    """Concrete implementation of DataExtractor for pulling data from various sources."""
    
    def __init__(self, extractor_id: str, config: Dict[str, Any]):
        PipelineComponent.__init__(self, extractor_id, config)
        extraction_config_dict = config.get('extraction', {})
        
        # Convert string values to enums
        if 'mode' in extraction_config_dict and isinstance(extraction_config_dict['mode'], str):
            extraction_config_dict['mode'] = ExtractionMode(extraction_config_dict['mode'])
        if 'serialization_format' in extraction_config_dict and isinstance(extraction_config_dict['serialization_format'], str):
            extraction_config_dict['serialization_format'] = SerializationFormat(extraction_config_dict['serialization_format'])
        
        self.extraction_config = ExtractionConfig(**extraction_config_dict)
        self.serializer = DataSerializer(self.extraction_config.serialization_format)
        self._extraction_stats = {}
        self._lock = threading.Lock()
    
    def extract(self, source: DataSource, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract data from source with error handling and retry logic."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting extraction from {source.source_id} for table {change_event.table_name}")
            
            # Determine extraction strategy based on change event
            if self.extraction_config.mode == ExtractionMode.FULL:
                data = self._extract_full(source, change_event)
            elif self.extraction_config.mode == ExtractionMode.INCREMENTAL:
                data = self._extract_incremental(source, change_event)
            else:  # DELTA
                data = self._extract_delta(source, change_event)
            
            extraction_time = time.time() - start_time
            
            # Validate extracted data if configured
            validation_result = None
            if self.extraction_config.validate_on_extract:
                validation_result = self.validate_extraction(data)
                if not validation_result.is_valid:
                    self.logger.error(f"Extraction validation failed: {validation_result.errors}")
                    raise NonRetryableError(f"Data validation failed: {validation_result.errors}")
            
            # Record extraction statistics
            self._record_extraction_stats(
                source.source_id,
                change_event.table_name,
                len(data),
                extraction_time,
                validation_result
            )
            
            self.logger.info(f"Successfully extracted {len(data)} rows in {extraction_time:.2f}s")
            return data
            
        except (RetryableError, Exception) as e:
            self.logger.error(f"Extraction failed: {e}")
            # For now, return empty DataFrame on failure
            # In production, you might want to raise the exception
            return pd.DataFrame()
    
    def _extract_full(self, source: DataSource, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract all data from source."""
        return self._execute_with_retry(
            lambda: source.extract_data(change_event),
            f"full extraction from {change_event.table_name}"
        )
    
    def _extract_incremental(self, source: DataSource, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract incremental data based on change event."""
        return self._execute_with_retry(
            lambda: source.extract_data(change_event),
            f"incremental extraction from {change_event.table_name}"
        )
    
    def _extract_delta(self, source: DataSource, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract only changed records."""
        # This would implement delta extraction logic
        # For now, delegate to source's extract_data method
        return self._execute_with_retry(
            lambda: source.extract_data(change_event),
            f"delta extraction from {change_event.table_name}"
        )
    
    def _execute_with_retry(self, operation: Callable, operation_name: str) -> pd.DataFrame:
        """Execute operation with retry logic."""
        last_exception = None
        
        for attempt in range(self.extraction_config.max_retries + 1):
            try:
                if attempt > 0:
                    delay = self.extraction_config.retry_delay * (2 ** (attempt - 1))
                    self.logger.info(f"Retrying {operation_name} (attempt {attempt + 1}) after {delay}s delay")
                    time.sleep(delay)
                
                result = operation()
                
                if attempt > 0:
                    self.logger.info(f"Retry successful for {operation_name}")
                
                return result
                
            except NonRetryableError:
                # Don't retry non-retryable errors
                raise
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Attempt {attempt + 1} failed for {operation_name}: {e}")
                
                # Check if this is a retryable error
                if self._is_retryable_error(e) and attempt < self.extraction_config.max_retries:
                    continue
                else:
                    break
        
        # All retries exhausted
        self.logger.error(f"All retry attempts exhausted for {operation_name}")
        raise RetryableError(f"Operation failed after {self.extraction_config.max_retries + 1} attempts: {last_exception}")
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        retryable_error_types = [
            ConnectionError,
            TimeoutError,
            # Add database-specific transient errors
        ]
        
        retryable_messages = [
            "connection timeout",
            "temporary failure",
            "service unavailable",
            "rate limit",
            "too many connections"
        ]
        
        # Check error type
        if any(isinstance(error, error_type) for error_type in retryable_error_types):
            return True
        
        # Check error message
        error_message = str(error).lower()
        return any(msg in error_message for msg in retryable_messages)
    
    def validate_extraction(self, data: pd.DataFrame) -> ValidationResult:
        """Validate extracted data quality and consistency."""
        errors = []
        warnings = []
        failed_rows = 0
        
        # Basic data validation
        if data.empty:
            warnings.append("Extracted dataset is empty")
            return ValidationResult(
                is_valid=True,  # Empty data is valid but should be noted
                errors=errors,
                warnings=warnings,
                row_count=0,
                failed_rows=0
            )
        
        # Check for completely null rows
        null_rows = data.isnull().all(axis=1).sum()
        if null_rows > 0:
            warnings.append(f"Found {null_rows} completely null rows")
            failed_rows += null_rows
        
        # Check for duplicate rows
        duplicate_rows = data.duplicated().sum()
        if duplicate_rows > 0:
            warnings.append(f"Found {duplicate_rows} duplicate rows")
        
        # Check data types consistency
        for column in data.columns:
            # Check for mixed types in object columns
            if data[column].dtype == 'object':
                non_null_data = data[column].dropna()
                if len(non_null_data) > 0:
                    unique_types = set(type(x).__name__ for x in non_null_data.head(100))
                    if len(unique_types) > 1:
                        warnings.append(f"Column '{column}' has mixed data types: {unique_types}")
            
            # Check for extreme values in numeric columns
            if pd.api.types.is_numeric_dtype(data[column]):
                if data[column].isnull().all():
                    warnings.append(f"Numeric column '{column}' is entirely null")
                else:
                    # Check for infinite values
                    inf_count = np.isinf(data[column]).sum()
                    if inf_count > 0:
                        errors.append(f"Column '{column}' contains {inf_count} infinite values")
                        failed_rows += inf_count
        
        # Check for reasonable data size
        data_size_mb = data.memory_usage(deep=True).sum() / (1024 * 1024)
        if data_size_mb > 1000:  # 1GB threshold
            warnings.append(f"Large dataset extracted: {data_size_mb:.2f} MB")
        
        # Validate critical columns are not entirely null
        critical_null_threshold = 0.95  # 95% null values is concerning
        for column in data.columns:
            null_percentage = data[column].isnull().sum() / len(data)
            if null_percentage > critical_null_threshold:
                warnings.append(f"Column '{column}' is {null_percentage:.1%} null")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            row_count=len(data),
            failed_rows=failed_rows
        )
    
    def extract_parallel(self, sources_and_events: List[tuple]) -> Dict[str, pd.DataFrame]:
        """Extract data from multiple sources in parallel."""
        results = {}
        
        if self.extraction_config.parallel_extractions <= 1:
            # Sequential extraction
            for source, change_event in sources_and_events:
                key = f"{source.source_id}_{change_event.table_name}"
                results[key] = self.extract(source, change_event)
        else:
            # Parallel extraction
            with ThreadPoolExecutor(max_workers=self.extraction_config.parallel_extractions) as executor:
                future_to_key = {}
                
                for source, change_event in sources_and_events:
                    key = f"{source.source_id}_{change_event.table_name}"
                    future = executor.submit(self.extract, source, change_event)
                    future_to_key[future] = key
                
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        results[key] = future.result()
                    except Exception as e:
                        self.logger.error(f"Parallel extraction failed for {key}: {e}")
                        results[key] = pd.DataFrame()
        
        return results
    
    def _record_extraction_stats(self, source_id: str, table_name: str, 
                                row_count: int, extraction_time: float,
                                validation_result: Optional[ValidationResult]):
        """Record extraction statistics for monitoring."""
        with self._lock:
            key = f"{source_id}_{table_name}"
            self._extraction_stats[key] = {
                'last_extraction': datetime.now(),
                'row_count': row_count,
                'extraction_time': extraction_time,
                'validation_passed': validation_result.is_valid if validation_result else None,
                'error_count': len(validation_result.errors) if validation_result else 0,
                'warning_count': len(validation_result.warnings) if validation_result else 0
            }
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get extraction statistics."""
        with self._lock:
            return self._extraction_stats.copy()
    
    def initialize(self) -> bool:
        """Initialize the data extractor."""
        self.logger.info(f"Initializing DataExtractor {self.component_id}")
        return True
    
    def cleanup(self) -> None:
        """Cleanup extractor resources."""
        self._extraction_stats.clear()
        self.logger.info(f"DataExtractor {self.component_id} cleaned up")
    
    def health_check(self) -> bool:
        """Check extractor health."""
        return True


class DataSerializer:
    """Utility class for data serialization and deserialization."""
    
    def __init__(self, format_type: SerializationFormat):
        self.format_type = format_type
    
    def serialize(self, data: pd.DataFrame, compression: Optional[str] = None) -> bytes:
        """Serialize DataFrame to bytes."""
        if self.format_type == SerializationFormat.JSON:
            json_str = data.to_json(orient='records', date_format='iso')
            return json_str.encode('utf-8')
        
        elif self.format_type == SerializationFormat.PICKLE:
            return pickle.dumps(data)
        
        elif self.format_type == SerializationFormat.PARQUET:
            # For parquet, we'd typically write to a buffer
            import io
            buffer = io.BytesIO()
            data.to_parquet(buffer, compression=compression)
            return buffer.getvalue()
        
        elif self.format_type == SerializationFormat.CSV:
            csv_str = data.to_csv(index=False)
            return csv_str.encode('utf-8')
        
        else:
            raise ValueError(f"Unsupported serialization format: {self.format_type}")
    
    def deserialize(self, data_bytes: bytes) -> pd.DataFrame:
        """Deserialize bytes to DataFrame."""
        if self.format_type == SerializationFormat.JSON:
            json_str = data_bytes.decode('utf-8')
            data_list = json.loads(json_str)
            return pd.DataFrame(data_list)
        
        elif self.format_type == SerializationFormat.PICKLE:
            return pickle.loads(data_bytes)
        
        elif self.format_type == SerializationFormat.PARQUET:
            import io
            buffer = io.BytesIO(data_bytes)
            return pd.read_parquet(buffer)
        
        elif self.format_type == SerializationFormat.CSV:
            import io
            csv_str = data_bytes.decode('utf-8')
            return pd.read_csv(io.StringIO(csv_str))
        
        else:
            raise ValueError(f"Unsupported deserialization format: {self.format_type}")
    
    def get_file_extension(self) -> str:
        """Get appropriate file extension for the format."""
        extensions = {
            SerializationFormat.JSON: '.json',
            SerializationFormat.PICKLE: '.pkl',
            SerializationFormat.PARQUET: '.parquet',
            SerializationFormat.CSV: '.csv'
        }
        return extensions.get(self.format_type, '.dat')