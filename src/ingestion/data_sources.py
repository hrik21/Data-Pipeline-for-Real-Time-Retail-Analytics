"""
Concrete implementations of data sources with connection management.
"""
import time
import json
import requests
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

from src.interfaces.base import DataSource, ChangeEvent, ChangeType, ValidationResult, PipelineComponent


@dataclass
class ConnectionConfig:
    """Configuration for database connections."""
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = "postgresql"
    connection_timeout: int = 30
    pool_size: int = 5
    max_overflow: int = 10


@dataclass
class APIConfig:
    """Configuration for API endpoints."""
    base_url: str
    auth_token: Optional[str] = None
    auth_header: str = "Authorization"
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 1


class DatabaseDataSource(DataSource, PipelineComponent):
    """Database data source implementation."""
    
    def __init__(self, source_id: str, config: Dict[str, Any]):
        DataSource.__init__(self, source_id, config)
        PipelineComponent.__init__(self, source_id, config)
        self.connection_config = ConnectionConfig(**config.get('connection', {}))
        self.tables = config.get('tables', [])
        self.change_detection_method = config.get('change_detection_method', 'timestamp')
        self.timestamp_column = config.get('timestamp_column', 'updated_at')
        self.engine = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection."""
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_size=self.connection_config.pool_size,
                max_overflow=self.connection_config.max_overflow,
                pool_timeout=self.connection_config.connection_timeout,
                pool_pre_ping=True
            )
            self.logger.info(f"Database connection initialized for source {self.source_id}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def _build_connection_string(self) -> str:
        """Build database connection string."""
        config = self.connection_config
        return (f"{config.driver}://{config.username}:{config.password}@"
                f"{config.host}:{config.port}/{config.database}")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context management."""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(f"Connection test successful for source {self.source_id}")
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed for source {self.source_id}: {e}")
            return False
    
    def detect_changes(self) -> List[ChangeEvent]:
        """Detect changes in database tables."""
        changes = []
        
        for table_name in self.tables:
            try:
                if self.change_detection_method == 'timestamp':
                    table_changes = self._detect_timestamp_changes(table_name)
                elif self.change_detection_method == 'log_based':
                    table_changes = self._detect_log_based_changes(table_name)
                else:
                    self.logger.warning(f"Unknown change detection method: {self.change_detection_method}")
                    continue
                
                changes.extend(table_changes)
                
            except Exception as e:
                self.logger.error(f"Error detecting changes for table {table_name}: {e}")
                continue
        
        return changes
    
    def _detect_timestamp_changes(self, table_name: str) -> List[ChangeEvent]:
        """Detect changes using timestamp-based approach."""
        changes = []
        
        try:
            with self.get_connection() as conn:
                # Get last processed timestamp from metadata store
                last_timestamp = self._get_last_processed_timestamp(table_name)
                
                # Query for records modified since last timestamp
                query = text(f"""
                    SELECT COUNT(*) as row_count, MAX({self.timestamp_column}) as max_timestamp
                    FROM {table_name}
                    WHERE {self.timestamp_column} > :last_timestamp
                """)
                
                result = conn.execute(query, {"last_timestamp": last_timestamp}).fetchone()
                
                if result and getattr(result, 'row_count', 0) > 0:
                    change_event = ChangeEvent(
                        source_id=self.source_id,
                        table_name=table_name,
                        change_type=ChangeType.UPDATE,  # Timestamp-based assumes updates
                        timestamp=getattr(result, 'max_timestamp', datetime.now()),
                        affected_rows=getattr(result, 'row_count', 0),
                        metadata={
                            "detection_method": "timestamp",
                            "timestamp_column": self.timestamp_column,
                            "last_processed": last_timestamp.isoformat() if last_timestamp else None
                        }
                    )
                    changes.append(change_event)
                    
        except Exception as e:
            self.logger.error(f"Error in timestamp-based change detection for {table_name}: {e}")
            
        return changes
    
    def _detect_log_based_changes(self, table_name: str) -> List[ChangeEvent]:
        """Detect changes using log-based CDC (simplified implementation)."""
        # This is a simplified implementation - in production, you'd integrate with
        # actual CDC tools like Debezium, AWS DMS, or database-specific CDC features
        changes = []
        
        try:
            with self.get_connection() as conn:
                # Simulate log-based CDC by checking a change log table
                log_table = f"{table_name}_changelog"
                
                query = text(f"""
                    SELECT operation, COUNT(*) as row_count, MAX(change_timestamp) as max_timestamp
                    FROM {log_table}
                    WHERE processed = false
                    GROUP BY operation
                """)
                
                results = conn.execute(query).fetchall()
                
                for result in results:
                    change_type_map = {
                        'I': ChangeType.INSERT,
                        'U': ChangeType.UPDATE,
                        'D': ChangeType.DELETE
                    }
                    
                    change_event = ChangeEvent(
                        source_id=self.source_id,
                        table_name=table_name,
                        change_type=change_type_map.get(getattr(result, 'operation', 'U'), ChangeType.UPDATE),
                        timestamp=getattr(result, 'max_timestamp', datetime.now()),
                        affected_rows=getattr(result, 'row_count', 0),
                        metadata={
                            "detection_method": "log_based",
                            "log_table": log_table
                        }
                    )
                    changes.append(change_event)
                    
        except Exception as e:
            self.logger.error(f"Error in log-based change detection for {table_name}: {e}")
            
        return changes
    
    def _get_last_processed_timestamp(self, table_name: str) -> Optional[datetime]:
        """Get last processed timestamp for a table."""
        # This would typically query a metadata store
        # For now, return a default timestamp (24 hours ago)
        return datetime.now() - timedelta(hours=24)
    
    def extract_data(self, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract data based on change event."""
        try:
            with self.get_connection() as conn:
                if change_event.metadata and change_event.metadata.get("detection_method") == "timestamp":
                    # Extract data modified since last timestamp
                    last_timestamp = change_event.metadata.get("last_processed")
                    query = f"""
                        SELECT * FROM {change_event.table_name}
                        WHERE {self.timestamp_column} > '{last_timestamp}'
                        ORDER BY {self.timestamp_column}
                    """
                else:
                    # Extract all data for the table
                    query = f"SELECT * FROM {change_event.table_name}"
                
                df = pd.read_sql(query, conn)
                self.logger.info(f"Extracted {len(df)} rows from {change_event.table_name}")
                return df
                
        except Exception as e:
            self.logger.error(f"Error extracting data from {change_event.table_name}: {e}")
            return pd.DataFrame()
    
    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data schema and quality."""
        errors = []
        warnings = []
        
        # Basic validation checks
        if data.empty:
            warnings.append("Dataset is empty")
        
        # Check for null values in critical columns
        null_counts = data.isnull().sum()
        for column, null_count in null_counts.items():
            if null_count > 0:
                warnings.append(f"Column '{column}' has {null_count} null values")
        
        # Check data types
        for column in data.columns:
            if data[column].dtype == 'object':
                # Check for mixed types in object columns
                unique_types = set(type(x).__name__ for x in data[column].dropna())
                if len(unique_types) > 1:
                    warnings.append(f"Column '{column}' has mixed data types: {unique_types}")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            row_count=len(data),
            failed_rows=0
        )
    
    def initialize(self) -> bool:
        """Initialize the database source."""
        return self.test_connection()
    
    def cleanup(self) -> None:
        """Cleanup database resources."""
        if self.engine:
            self.engine.dispose()
    
    def health_check(self) -> bool:
        """Check database source health."""
        return self.test_connection()


class APIDataSource(DataSource, PipelineComponent):
    """API endpoint data source implementation."""
    
    def __init__(self, source_id: str, config: Dict[str, Any]):
        DataSource.__init__(self, source_id, config)
        PipelineComponent.__init__(self, source_id, config)
        self.api_config = APIConfig(**config.get('api', {}))
        self.endpoints = config.get('endpoints', [])
        self.polling_interval = config.get('polling_interval', 300)  # 5 minutes default
        self.last_poll_times = {}
    
    def test_connection(self) -> bool:
        """Test API connection."""
        try:
            headers = self._get_headers()
            response = requests.get(
                f"{self.api_config.base_url}/health",
                headers=headers,
                timeout=self.api_config.timeout
            )
            
            if response.status_code == 200:
                self.logger.info(f"API connection test successful for source {self.source_id}")
                return True
            else:
                self.logger.warning(f"API health check returned status {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"API connection test failed for source {self.source_id}: {e}")
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        headers = {"Content-Type": "application/json"}
        
        if self.api_config.auth_token:
            headers[self.api_config.auth_header] = f"Bearer {self.api_config.auth_token}"
        
        return headers
    
    def detect_changes(self) -> List[ChangeEvent]:
        """Detect changes in API endpoints using polling."""
        changes = []
        current_time = datetime.now()
        
        for endpoint_config in self.endpoints:
            endpoint_name = endpoint_config.get('name')
            endpoint_path = endpoint_config.get('path')
            
            try:
                # Check if it's time to poll this endpoint
                last_poll = self.last_poll_times.get(endpoint_name)
                if last_poll and (current_time - last_poll).seconds < self.polling_interval:
                    continue
                
                # Poll the endpoint
                url = f"{self.api_config.base_url}{endpoint_path}"
                headers = self._get_headers()
                
                # Add timestamp parameter for incremental loading
                params = {}
                if last_poll:
                    params['since'] = last_poll.isoformat()
                
                response = self._make_request_with_retry(url, headers, params)
                
                if response and response.status_code == 200:
                    data = response.json()
                    
                    # Check if there's new data
                    if isinstance(data, list) and len(data) > 0:
                        change_event = ChangeEvent(
                            source_id=self.source_id,
                            table_name=endpoint_name,
                            change_type=ChangeType.UPDATE,
                            timestamp=current_time,
                            affected_rows=len(data),
                            metadata={
                                "detection_method": "polling",
                                "endpoint": endpoint_path,
                                "last_poll": last_poll.isoformat() if last_poll else None
                            }
                        )
                        changes.append(change_event)
                    
                    # Update last poll time
                    self.last_poll_times[endpoint_name] = current_time
                
            except Exception as e:
                self.logger.error(f"Error polling endpoint {endpoint_name}: {e}")
                continue
        
        return changes
    
    def _make_request_with_retry(self, url: str, headers: Dict[str, str], 
                                params: Dict[str, Any]) -> Optional[requests.Response]:
        """Make HTTP request with retry logic."""
        for attempt in range(self.api_config.retry_attempts):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.api_config.timeout
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = self.api_config.retry_delay * (2 ** attempt)
                    self.logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                else:
                    self.logger.warning(f"Request failed with status {response.status_code}")
                    
            except requests.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.api_config.retry_attempts - 1:
                    time.sleep(self.api_config.retry_delay)
        
        return None
    
    def extract_data(self, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract data from API endpoint."""
        try:
            endpoint_path = change_event.metadata.get('endpoint')
            url = f"{self.api_config.base_url}{endpoint_path}"
            headers = self._get_headers()
            
            # Add parameters for incremental extraction
            params = {}
            if change_event.metadata.get('last_poll'):
                params['since'] = change_event.metadata['last_poll']
            
            response = self._make_request_with_retry(url, headers, params)
            
            if response and response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict) and 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
                
                self.logger.info(f"Extracted {len(df)} rows from API endpoint {change_event.table_name}")
                return df
            else:
                self.logger.error(f"Failed to extract data from API endpoint {change_event.table_name}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error extracting data from API endpoint {change_event.table_name}: {e}")
            return pd.DataFrame()
    
    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """Validate API data schema and quality."""
        errors = []
        warnings = []
        
        if data.empty:
            warnings.append("API response is empty")
        
        # Check for required fields based on endpoint configuration
        # This would be customized based on your API schema requirements
        
        # Basic data quality checks
        for column in data.columns:
            if data[column].dtype == 'object':
                # Check for JSON strings that might need parsing
                sample_values = data[column].dropna().head(5)
                for value in sample_values:
                    if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
                        warnings.append(f"Column '{column}' may contain JSON strings that need parsing")
                        break
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            row_count=len(data),
            failed_rows=0
        )
    
    def initialize(self) -> bool:
        """Initialize the API source."""
        return self.test_connection()
    
    def cleanup(self) -> None:
        """Cleanup API resources."""
        self.last_poll_times.clear()
    
    def health_check(self) -> bool:
        """Check API source health."""
        return self.test_connection()


class DataSourceFactory:
    """Factory class for creating data source instances."""
    
    _source_types = {
        'database': DatabaseDataSource,
        'api': APIDataSource,
    }
    
    @classmethod
    def create_source(cls, source_type: str, source_id: str, config: Dict[str, Any]) -> DataSource:
        """Create a data source instance based on type."""
        if source_type not in cls._source_types:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        source_class = cls._source_types[source_type]
        return source_class(source_id, config)
    
    @classmethod
    def register_source_type(cls, source_type: str, source_class: type) -> None:
        """Register a new data source type."""
        if not issubclass(source_class, DataSource):
            raise ValueError("Source class must inherit from DataSource")
        
        cls._source_types[source_type] = source_class
    
    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported source types."""
        return list(cls._source_types.keys())