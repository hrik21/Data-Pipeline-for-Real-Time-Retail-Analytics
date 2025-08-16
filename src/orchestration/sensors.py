"""
Airflow sensors for change-based triggering of data pipelines.
"""
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
import time

try:
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.decorators import apply_defaults
    from airflow.exceptions import AirflowException
    try:
        from airflow.utils.context import Context
    except ImportError:
        # For older Airflow versions
        Context = dict
except ImportError:
    # Mock Airflow components for testing without Airflow installed
    class BaseSensorOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id')
            self.log = MockLogger()
            self.poke_interval = kwargs.get('poke_interval', 60)
            self.timeout = kwargs.get('timeout', 3600)
    
    def apply_defaults(func):
        return func
    
    class AirflowException(Exception):
        pass
    
    Context = dict
    
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")

from src.interfaces.base import ChangeEvent, DataSource
from src.ingestion.data_sources import DataSourceFactory
from src.ingestion.change_detection import ChangeDetector


class DataSourceChangeSensor(BaseSensorOperator):
    """Sensor that detects changes in data sources."""
    
    template_fields = ['source_config']
    
    @apply_defaults
    def __init__(
        self,
        source_config: Dict[str, Any],
        change_detection_config: Optional[Dict[str, Any]] = None,
        min_changes: int = 1,
        max_changes: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_config = source_config
        self.change_detection_config = change_detection_config or {}
        self.min_changes = min_changes
        self.max_changes = max_changes
        self._change_detector = None
        self._data_source = None
    
    def poke(self, context: Context) -> bool:
        """Check if changes are detected in the data source."""
        try:
            self.log.info(f"Checking for changes in source: {self.source_config['source_id']}")
            
            # Initialize components if not already done
            if not self._data_source:
                self._initialize_components()
            
            # Detect changes
            changes = self._change_detector.detect_changes(self._data_source)
            
            self.log.info(f"Detected {len(changes)} changes")
            
            # Check if minimum changes threshold is met
            if len(changes) < self.min_changes:
                self.log.info(f"Not enough changes detected. Required: {self.min_changes}, Found: {len(changes)}")
                return False
            
            # Check if maximum changes threshold is exceeded
            if self.max_changes and len(changes) > self.max_changes:
                self.log.warning(f"Too many changes detected. Maximum: {self.max_changes}, Found: {len(changes)}")
                # Optionally, you might want to fail here or process in batches
                if self.change_detection_config.get('fail_on_max_exceeded', False):
                    raise AirflowException(f"Too many changes detected: {len(changes)} > {self.max_changes}")
            
            # Store detected changes in XCom for downstream tasks
            if changes:
                context['task_instance'].xcom_push(
                    key='detected_changes',
                    value=[{
                        'source_id': change.source_id,
                        'table_name': change.table_name,
                        'change_type': change.change_type.value,
                        'timestamp': change.timestamp.isoformat(),
                        'affected_rows': change.affected_rows,
                        'metadata': change.metadata
                    } for change in changes]
                )
                
                self.log.info(f"Changes detected and stored in XCom: {len(changes)} changes")
                return True
            
            return False
            
        except Exception as e:
            self.log.error(f"Error detecting changes: {str(e)}")
            raise AirflowException(f"Change detection failed: {str(e)}")
    
    def _initialize_components(self) -> None:
        """Initialize data source and change detector."""
        # Create data source
        self._data_source = DataSourceFactory.create_source(
            self.source_config['source_type'],
            self.source_config['source_id'],
            self.source_config.get('connection_params', {})
        )
        
        # Create change detector
        self._change_detector = ChangeDetector(
            detector_id=f"sensor_{self.source_config['source_id']}",
            config=self.change_detection_config
        )
        
        # Initialize components
        if not self._change_detector.initialize():
            raise AirflowException("Failed to initialize change detector")


class TableChangeSensor(BaseSensorOperator):
    """Sensor that monitors specific tables for changes."""
    
    template_fields = ['table_names', 'connection_config']
    
    @apply_defaults
    def __init__(
        self,
        table_names: List[str],
        connection_config: Dict[str, Any],
        change_detection_method: str = 'timestamp',
        timestamp_column: str = 'updated_at',
        check_interval_seconds: int = 60,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_names = table_names
        self.connection_config = connection_config
        self.change_detection_method = change_detection_method
        self.timestamp_column = timestamp_column
        self.check_interval_seconds = check_interval_seconds
        self._last_check_timestamps = {}
    
    def poke(self, context: Context) -> bool:
        """Check if any of the monitored tables have changes."""
        try:
            self.log.info(f"Checking for changes in tables: {', '.join(self.table_names)}")
            
            changes_detected = False
            table_changes = {}
            
            for table_name in self.table_names:
                has_changes = self._check_table_changes(table_name)
                table_changes[table_name] = has_changes
                
                if has_changes:
                    changes_detected = True
                    self.log.info(f"Changes detected in table: {table_name}")
            
            # Store results in XCom
            context['task_instance'].xcom_push(
                key='table_changes',
                value=table_changes
            )
            
            if changes_detected:
                self.log.info("Changes detected in one or more tables")
                return True
            
            self.log.info("No changes detected in any monitored tables")
            return False
            
        except Exception as e:
            self.log.error(f"Error checking table changes: {str(e)}")
            raise AirflowException(f"Table change detection failed: {str(e)}")
    
    def _check_table_changes(self, table_name: str) -> bool:
        """Check if a specific table has changes."""
        if self.change_detection_method == 'timestamp':
            return self._check_timestamp_changes(table_name)
        elif self.change_detection_method == 'row_count':
            return self._check_row_count_changes(table_name)
        else:
            raise AirflowException(f"Unsupported change detection method: {self.change_detection_method}")
    
    def _check_timestamp_changes(self, table_name: str) -> bool:
        """Check for changes using timestamp column."""
        # This is a placeholder implementation
        # In practice, you'd query the database to check the latest timestamp
        current_time = datetime.now()
        last_check = self._last_check_timestamps.get(table_name, current_time - timedelta(hours=1))
        
        # Simulate checking for changes
        # In real implementation, you'd execute SQL like:
        # SELECT MAX(updated_at) FROM table_name WHERE updated_at > last_check
        
        self._last_check_timestamps[table_name] = current_time
        
        # Placeholder: assume changes exist 50% of the time
        import random
        return random.random() > 0.5
    
    def _check_row_count_changes(self, table_name: str) -> bool:
        """Check for changes using row count."""
        # This is a placeholder implementation
        # In practice, you'd query the database to check row count changes
        return True  # Placeholder


class FileSystemChangeSensor(BaseSensorOperator):
    """Sensor that monitors file system changes."""
    
    template_fields = ['file_paths', 'file_patterns']
    
    @apply_defaults
    def __init__(
        self,
        file_paths: Optional[List[str]] = None,
        file_patterns: Optional[List[str]] = None,
        check_modification_time: bool = True,
        check_file_size: bool = False,
        min_file_age_seconds: int = 0,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_paths = file_paths or []
        self.file_patterns = file_patterns or []
        self.check_modification_time = check_modification_time
        self.check_file_size = check_file_size
        self.min_file_age_seconds = min_file_age_seconds
        self._last_check_info = {}
    
    def poke(self, context: Context) -> bool:
        """Check if monitored files have changes."""
        try:
            self.log.info("Checking for file system changes")
            
            changes_detected = False
            file_changes = {}
            
            # Check specific file paths
            for file_path in self.file_paths:
                has_changes = self._check_file_changes(file_path)
                file_changes[file_path] = has_changes
                
                if has_changes:
                    changes_detected = True
                    self.log.info(f"Changes detected in file: {file_path}")
            
            # Check file patterns
            for pattern in self.file_patterns:
                matching_files = self._find_matching_files(pattern)
                for file_path in matching_files:
                    has_changes = self._check_file_changes(file_path)
                    file_changes[file_path] = has_changes
                    
                    if has_changes:
                        changes_detected = True
                        self.log.info(f"Changes detected in file: {file_path}")
            
            # Store results in XCom
            context['task_instance'].xcom_push(
                key='file_changes',
                value=file_changes
            )
            
            if changes_detected:
                self.log.info("File system changes detected")
                return True
            
            self.log.info("No file system changes detected")
            return False
            
        except Exception as e:
            self.log.error(f"Error checking file system changes: {str(e)}")
            raise AirflowException(f"File system change detection failed: {str(e)}")
    
    def _check_file_changes(self, file_path: str) -> bool:
        """Check if a specific file has changes."""
        import os
        
        try:
            if not os.path.exists(file_path):
                self.log.warning(f"File does not exist: {file_path}")
                return False
            
            stat = os.stat(file_path)
            current_mtime = stat.st_mtime
            current_size = stat.st_size
            
            # Check if file is old enough (to avoid processing files that are still being written)
            if self.min_file_age_seconds > 0:
                age_seconds = time.time() - current_mtime
                if age_seconds < self.min_file_age_seconds:
                    self.log.info(f"File too new, skipping: {file_path} (age: {age_seconds}s)")
                    return False
            
            # Get last check info
            last_info = self._last_check_info.get(file_path, {})
            last_mtime = last_info.get('mtime', 0)
            last_size = last_info.get('size', 0)
            
            # Check for changes
            has_changes = False
            
            if self.check_modification_time and current_mtime > last_mtime:
                has_changes = True
                self.log.info(f"Modification time changed for {file_path}")
            
            if self.check_file_size and current_size != last_size:
                has_changes = True
                self.log.info(f"File size changed for {file_path}")
            
            # Update last check info
            self._last_check_info[file_path] = {
                'mtime': current_mtime,
                'size': current_size
            }
            
            return has_changes
            
        except Exception as e:
            self.log.error(f"Error checking file {file_path}: {str(e)}")
            return False
    
    def _find_matching_files(self, pattern: str) -> List[str]:
        """Find files matching the given pattern."""
        import glob
        
        try:
            matching_files = glob.glob(pattern)
            self.log.info(f"Found {len(matching_files)} files matching pattern: {pattern}")
            return matching_files
        except Exception as e:
            self.log.error(f"Error finding files with pattern {pattern}: {str(e)}")
            return []


class APIChangeSensor(BaseSensorOperator):
    """Sensor that monitors API endpoints for changes."""
    
    template_fields = ['api_config']
    
    @apply_defaults
    def __init__(
        self,
        api_config: Dict[str, Any],
        change_detection_field: str = 'last_modified',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_config = api_config
        self.change_detection_field = change_detection_field
        self._last_check_value = None
    
    def poke(self, context: Context) -> bool:
        """Check if API endpoint has changes."""
        try:
            self.log.info(f"Checking API endpoint: {self.api_config.get('url')}")
            
            # Make API request
            response_data = self._make_api_request()
            
            # Extract change detection value
            current_value = self._extract_change_value(response_data)
            
            # Check for changes
            if self._last_check_value is None:
                self._last_check_value = current_value
                self.log.info("First check, storing current value")
                return False
            
            has_changes = current_value != self._last_check_value
            
            if has_changes:
                self.log.info(f"API changes detected. Previous: {self._last_check_value}, Current: {current_value}")
                
                # Store API response in XCom
                context['task_instance'].xcom_push(
                    key='api_response',
                    value=response_data
                )
                
                self._last_check_value = current_value
                return True
            
            self.log.info("No API changes detected")
            return False
            
        except Exception as e:
            self.log.error(f"Error checking API changes: {str(e)}")
            raise AirflowException(f"API change detection failed: {str(e)}")
    
    def _make_api_request(self) -> Dict[str, Any]:
        """Make API request and return response data."""
        import requests
        
        url = self.api_config['url']
        method = self.api_config.get('method', 'GET')
        headers = self.api_config.get('headers', {})
        params = self.api_config.get('params', {})
        timeout = self.api_config.get('timeout', 30)
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            timeout=timeout
        )
        
        response.raise_for_status()
        return response.json()
    
    def _extract_change_value(self, response_data: Dict[str, Any]) -> Any:
        """Extract the value used for change detection from API response."""
        # Support nested field access using dot notation
        field_parts = self.change_detection_field.split('.')
        value = response_data
        
        for part in field_parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                raise AirflowException(f"Change detection field not found: {self.change_detection_field}")
        
        return value


class CustomChangeSensor(BaseSensorOperator):
    """Generic sensor that uses a custom function to detect changes."""
    
    @apply_defaults
    def __init__(
        self,
        change_detection_callable: Callable[[], bool],
        callable_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.change_detection_callable = change_detection_callable
        self.callable_kwargs = callable_kwargs or {}
    
    def poke(self, context: Context) -> bool:
        """Check for changes using custom callable."""
        try:
            self.log.info("Executing custom change detection function")
            
            # Call the custom function
            has_changes = self.change_detection_callable(**self.callable_kwargs)
            
            if has_changes:
                self.log.info("Custom change detection function detected changes")
                return True
            
            self.log.info("Custom change detection function detected no changes")
            return False
            
        except Exception as e:
            self.log.error(f"Error in custom change detection: {str(e)}")
            raise AirflowException(f"Custom change detection failed: {str(e)}")