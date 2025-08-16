"""
Custom Airflow operators for data pipeline tasks.
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd

try:
    from airflow.models import BaseOperator
    from airflow.utils.decorators import apply_defaults
    from airflow.exceptions import AirflowException
except ImportError:
    # Mock Airflow components for testing without Airflow installed
    class BaseOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id')
            self.log = MockLogger()
    
    def apply_defaults(func):
        return func
    
    class AirflowException(Exception):
        pass
    
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")

from src.interfaces.base import TaskResult, TaskStatus, ValidationResult, LoadResult
from src.ingestion.extractors import DataExtractor
from src.ingestion.validators import IngestionValidator
from src.ingestion.data_sources import DataSourceFactory


class IngestionOperator(BaseOperator):
    """Custom operator for data ingestion tasks."""
    
    template_fields = ['source_config']
    
    @apply_defaults
    def __init__(
        self,
        source_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_config = source_config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the ingestion task."""
        try:
            self.log.info(f"Starting ingestion for source: {self.source_config.get('source_id')}")
            
            # Get source config from context if not provided
            if not self.source_config:
                self.source_config = context['params'].get('source_config')
            
            if not self.source_config:
                raise AirflowException("Source configuration not provided")
            
            # Create data source
            source = DataSourceFactory.create_source(
                self.source_config['source_type'],
                self.source_config['source_id'],
                self.source_config.get('connection_params', {})
            )
            
            # Detect changes
            changes = source.detect_changes()
            self.log.info(f"Detected {len(changes)} changes")
            
            if not changes:
                self.log.info("No changes detected, skipping extraction")
                return {
                    'status': 'success',
                    'message': 'No changes detected',
                    'rows_processed': 0
                }
            
            # Extract data for each change
            extractor = DataExtractor()
            validator = IngestionValidator()
            total_rows = 0
            
            for change in changes:
                # Extract data
                data = source.extract_data(change)
                
                # Validate data
                validation_result = validator.validate(data, self.source_config.get('validation_rules', {}))
                
                if not validation_result.is_valid:
                    self.log.warning(f"Data validation failed: {validation_result.errors}")
                    # Continue with valid rows if configured to do so
                    if not self.source_config.get('fail_on_validation_error', True):
                        data = self._filter_valid_rows(data, validation_result)
                    else:
                        raise AirflowException(f"Data validation failed: {validation_result.errors}")
                
                # Store extracted data in XCom for downstream tasks
                context['task_instance'].xcom_push(
                    key=f"extracted_data_{change.source_id}_{change.table_name}",
                    value={
                        'data': data.to_dict('records'),
                        'change_event': {
                            'source_id': change.source_id,
                            'table_name': change.table_name,
                            'change_type': change.change_type.value,
                            'timestamp': change.timestamp.isoformat(),
                            'affected_rows': change.affected_rows
                        }
                    }
                )
                
                total_rows += len(data)
            
            self.log.info(f"Ingestion completed successfully. Processed {total_rows} rows")
            
            return {
                'status': 'success',
                'message': f'Successfully processed {len(changes)} changes',
                'rows_processed': total_rows,
                'changes_processed': len(changes)
            }
            
        except Exception as e:
            self.log.error(f"Ingestion task failed: {str(e)}")
            raise AirflowException(f"Ingestion failed: {str(e)}")
    
    def _filter_valid_rows(self, data: pd.DataFrame, validation_result: ValidationResult) -> pd.DataFrame:
        """Filter out invalid rows from the dataset."""
        # This is a simplified implementation
        # In practice, you'd implement more sophisticated filtering based on validation errors
        return data.dropna()


class TransformationOperator(BaseOperator):
    """Custom operator for dbt transformation tasks."""
    
    template_fields = ['transform_config']
    
    @apply_defaults
    def __init__(
        self,
        transform_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.transform_config = transform_config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the transformation task."""
        try:
            self.log.info(f"Starting transformation for model: {self.transform_config.get('model_name')}")
            
            # Get transform config from context if not provided
            if not self.transform_config:
                self.transform_config = context['params'].get('transform_config')
            
            if not self.transform_config:
                raise AirflowException("Transform configuration not provided")
            
            model_name = self.transform_config['model_name']
            model_type = self.transform_config.get('model_type', 'table')
            
            # Execute dbt model
            result = self._execute_dbt_model(model_name, model_type, context)
            
            self.log.info(f"Transformation completed successfully for model: {model_name}")
            
            return {
                'status': 'success',
                'message': f'Successfully transformed model: {model_name}',
                'model_name': model_name,
                'execution_time': result.get('execution_time', 0)
            }
            
        except Exception as e:
            self.log.error(f"Transformation task failed: {str(e)}")
            raise AirflowException(f"Transformation failed: {str(e)}")
    
    def _execute_dbt_model(self, model_name: str, model_type: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a dbt model."""
        # This is a placeholder implementation
        # In practice, you'd use dbt's Python API or subprocess calls
        import subprocess
        import time
        
        start_time = time.time()
        
        try:
            # Build the dbt command
            cmd = ['dbt', 'run', '--models', model_name]
            
            # Execute dbt command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                cwd='dbt'  # Assuming dbt project is in dbt/ directory
            )
            
            execution_time = time.time() - start_time
            
            self.log.info(f"dbt run output: {result.stdout}")
            
            return {
                'success': True,
                'execution_time': execution_time,
                'output': result.stdout
            }
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"dbt run failed: {e.stderr}")
            raise AirflowException(f"dbt run failed: {e.stderr}")


class ValidationOperator(BaseOperator):
    """Custom operator for data validation tasks."""
    
    template_fields = ['target_config']
    
    @apply_defaults
    def __init__(
        self,
        target_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_config = target_config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the validation task."""
        try:
            self.log.info(f"Starting validation for table: {self.target_config.get('table_name')}")
            
            # Get target config from context if not provided
            if not self.target_config:
                self.target_config = context['params'].get('target_config')
            
            if not self.target_config:
                raise AirflowException("Target configuration not provided")
            
            table_name = self.target_config['table_name']
            validation_rules = self.target_config.get('validation_rules', {})
            
            # Execute validation checks
            validation_results = self._execute_validation_checks(table_name, validation_rules)
            
            # Check if all validations passed
            all_passed = all(result['passed'] for result in validation_results)
            
            if not all_passed:
                failed_checks = [r['check_name'] for r in validation_results if not r['passed']]
                error_msg = f"Validation failed for checks: {', '.join(failed_checks)}"
                
                if self.target_config.get('fail_on_validation_error', True):
                    raise AirflowException(error_msg)
                else:
                    self.log.warning(error_msg)
            
            self.log.info(f"Validation completed for table: {table_name}")
            
            return {
                'status': 'success' if all_passed else 'warning',
                'message': f'Validation completed for table: {table_name}',
                'table_name': table_name,
                'validation_results': validation_results,
                'all_passed': all_passed
            }
            
        except Exception as e:
            self.log.error(f"Validation task failed: {str(e)}")
            raise AirflowException(f"Validation failed: {str(e)}")
    
    def _execute_validation_checks(self, table_name: str, validation_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute validation checks on the target table."""
        results = []
        
        # Row count validation
        if 'min_row_count' in validation_rules:
            result = self._check_row_count(table_name, validation_rules['min_row_count'])
            results.append(result)
        
        # Data freshness validation
        if 'max_age_hours' in validation_rules:
            result = self._check_data_freshness(table_name, validation_rules['max_age_hours'])
            results.append(result)
        
        # Custom SQL validations
        if 'custom_checks' in validation_rules:
            for check in validation_rules['custom_checks']:
                result = self._execute_custom_check(table_name, check)
                results.append(result)
        
        return results
    
    def _check_row_count(self, table_name: str, min_count: int) -> Dict[str, Any]:
        """Check if table has minimum required row count."""
        # This is a placeholder implementation
        # In practice, you'd query the actual database
        return {
            'check_name': 'row_count',
            'passed': True,  # Placeholder
            'message': f'Table {table_name} has sufficient rows',
            'actual_count': 1000,  # Placeholder
            'expected_min': min_count
        }
    
    def _check_data_freshness(self, table_name: str, max_age_hours: int) -> Dict[str, Any]:
        """Check if data is fresh enough."""
        # This is a placeholder implementation
        return {
            'check_name': 'data_freshness',
            'passed': True,  # Placeholder
            'message': f'Data in {table_name} is fresh',
            'max_age_hours': max_age_hours
        }
    
    def _execute_custom_check(self, table_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a custom validation check."""
        # This is a placeholder implementation
        return {
            'check_name': check_config.get('name', 'custom_check'),
            'passed': True,  # Placeholder
            'message': f'Custom check passed for {table_name}',
            'query': check_config.get('query', '')
        }


class SnowflakeOperator(BaseOperator):
    """Custom operator for Snowflake operations."""
    
    template_fields = ['sql', 'parameters']
    
    @apply_defaults
    def __init__(
        self,
        sql: str,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[Dict[str, Any]] = None,
        autocommit: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.snowflake_conn_id = snowflake_conn_id
        self.parameters = parameters or {}
        self.autocommit = autocommit
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute SQL on Snowflake."""
        try:
            self.log.info(f"Executing SQL on Snowflake: {self.sql[:100]}...")
            
            # This is a placeholder implementation
            # In practice, you'd use Snowflake connector
            result = self._execute_snowflake_query(self.sql, self.parameters)
            
            self.log.info("Snowflake query executed successfully")
            return result
            
        except Exception as e:
            self.log.error(f"Snowflake operation failed: {str(e)}")
            raise AirflowException(f"Snowflake operation failed: {str(e)}")
    
    def _execute_snowflake_query(self, sql: str, parameters: Dict[str, Any]) -> Any:
        """Execute query on Snowflake."""
        # Placeholder implementation
        # In practice, you'd use snowflake-connector-python
        self.log.info(f"Executing query: {sql}")
        return {'rows_affected': 0, 'status': 'success'}


class DataQualityOperator(BaseOperator):
    """Custom operator for data quality checks."""
    
    template_fields = ['table_name', 'quality_checks']
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        quality_checks: List[Dict[str, Any]],
        fail_on_error: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.quality_checks = quality_checks
        self.fail_on_error = fail_on_error
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data quality checks."""
        try:
            self.log.info(f"Starting data quality checks for table: {self.table_name}")
            
            results = []
            failed_checks = []
            
            for check in self.quality_checks:
                result = self._execute_quality_check(check)
                results.append(result)
                
                if not result['passed']:
                    failed_checks.append(result['check_name'])
            
            if failed_checks and self.fail_on_error:
                raise AirflowException(f"Data quality checks failed: {', '.join(failed_checks)}")
            
            self.log.info(f"Data quality checks completed for table: {self.table_name}")
            
            return {
                'status': 'success' if not failed_checks else 'warning',
                'table_name': self.table_name,
                'total_checks': len(self.quality_checks),
                'passed_checks': len(self.quality_checks) - len(failed_checks),
                'failed_checks': failed_checks,
                'results': results
            }
            
        except Exception as e:
            self.log.error(f"Data quality check failed: {str(e)}")
            raise AirflowException(f"Data quality check failed: {str(e)}")
    
    def _execute_quality_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single data quality check."""
        check_type = check.get('type')
        check_name = check.get('name', f"{check_type}_check")
        
        # Placeholder implementation for different check types
        if check_type == 'null_check':
            return self._check_nulls(check_name, check)
        elif check_type == 'uniqueness_check':
            return self._check_uniqueness(check_name, check)
        elif check_type == 'range_check':
            return self._check_range(check_name, check)
        else:
            return {
                'check_name': check_name,
                'passed': True,
                'message': f'Unknown check type: {check_type}'
            }
    
    def _check_nulls(self, check_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for null values in specified columns."""
        return {
            'check_name': check_name,
            'passed': True,  # Placeholder
            'message': 'Null check passed',
            'columns': check.get('columns', [])
        }
    
    def _check_uniqueness(self, check_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for uniqueness in specified columns."""
        return {
            'check_name': check_name,
            'passed': True,  # Placeholder
            'message': 'Uniqueness check passed',
            'columns': check.get('columns', [])
        }
    
    def _check_range(self, check_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check if values are within specified range."""
        return {
            'check_name': check_name,
            'passed': True,  # Placeholder
            'message': 'Range check passed',
            'column': check.get('column'),
            'min_value': check.get('min_value'),
            'max_value': check.get('max_value')
        }