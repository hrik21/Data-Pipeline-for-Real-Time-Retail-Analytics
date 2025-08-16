"""
Data validation components for ingestion pipeline.
"""
import re
import json
from typing import Any, Dict, List, Optional, Union, Callable, Set
from datetime import datetime, date
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from collections import defaultdict

from src.interfaces.base import ValidationResult, PipelineComponent


class ValidationSeverity(Enum):
    """Validation rule severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DataType(Enum):
    """Supported data types for validation."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    JSON = "json"
    EMAIL = "email"
    URL = "url"


@dataclass
class ValidationRule:
    """Represents a single validation rule."""
    name: str
    column: str
    rule_type: str
    severity: ValidationSeverity
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    enabled: bool = True


@dataclass
class SchemaDefinition:
    """Schema definition for data validation."""
    columns: Dict[str, DataType]
    required_columns: Set[str] = field(default_factory=set)
    nullable_columns: Set[str] = field(default_factory=set)
    unique_columns: Set[str] = field(default_factory=set)
    primary_key: Optional[List[str]] = None


@dataclass
class ValidationReport:
    """Detailed validation report."""
    table_name: str
    total_rows: int
    validation_time: datetime
    rules_executed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    info: List[Dict[str, Any]] = field(default_factory=list)
    column_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    data_quality_score: float = 0.0


class IngestionValidator(PipelineComponent):
    """Comprehensive data validation component for ingestion pipeline."""
    
    def __init__(self, validator_id: str, config: Dict[str, Any]):
        super().__init__(validator_id, config)
        self.validation_rules = self._load_validation_rules(config.get('rules', []))
        self.schema_definitions = self._load_schema_definitions(config.get('schemas', {}))
        self.quality_thresholds = config.get('quality_thresholds', {
            'min_data_quality_score': 0.8,
            'max_error_percentage': 0.05,
            'max_null_percentage': 0.1
        })
        self.enable_profiling = config.get('enable_profiling', True)
    
    def _load_validation_rules(self, rules_config: List[Dict[str, Any]]) -> List[ValidationRule]:
        """Load validation rules from configuration."""
        rules = []
        for rule_config in rules_config:
            rule = ValidationRule(
                name=rule_config['name'],
                column=rule_config['column'],
                rule_type=rule_config['rule_type'],
                severity=ValidationSeverity(rule_config.get('severity', 'error')),
                parameters=rule_config.get('parameters', {}),
                description=rule_config.get('description'),
                enabled=rule_config.get('enabled', True)
            )
            rules.append(rule)
        return rules
    
    def _load_schema_definitions(self, schemas_config: Dict[str, Any]) -> Dict[str, SchemaDefinition]:
        """Load schema definitions from configuration."""
        schemas = {}
        for table_name, schema_config in schemas_config.items():
            columns = {col: DataType(dtype) for col, dtype in schema_config.get('columns', {}).items()}
            schema = SchemaDefinition(
                columns=columns,
                required_columns=set(schema_config.get('required_columns', [])),
                nullable_columns=set(schema_config.get('nullable_columns', [])),
                unique_columns=set(schema_config.get('unique_columns', [])),
                primary_key=schema_config.get('primary_key')
            )
            schemas[table_name] = schema
        return schemas
    
    def validate_data(self, data: pd.DataFrame, table_name: str) -> ValidationReport:
        """Perform comprehensive data validation."""
        start_time = datetime.now()
        
        report = ValidationReport(
            table_name=table_name,
            total_rows=len(data),
            validation_time=start_time
        )
        
        if data.empty:
            report.warnings.append({
                'rule': 'empty_dataset',
                'message': 'Dataset is empty',
                'column': None,
                'affected_rows': 0
            })
            return report
        
        # Schema validation
        if table_name in self.schema_definitions:
            self._validate_schema(data, table_name, report)
        
        # Rule-based validation
        self._execute_validation_rules(data, report)
        
        # Data profiling
        if self.enable_profiling:
            self._profile_data(data, report)
        
        # Calculate data quality score
        report.data_quality_score = self._calculate_quality_score(report)
        report.rules_executed = len([r for r in self.validation_rules if r.enabled])
        
        return report
    
    def _validate_schema(self, data: pd.DataFrame, table_name: str, report: ValidationReport):
        """Validate data against schema definition."""
        schema = self.schema_definitions[table_name]
        
        # Check required columns
        missing_columns = schema.required_columns - set(data.columns)
        if missing_columns:
            report.errors.append({
                'rule': 'missing_required_columns',
                'message': f'Missing required columns: {missing_columns}',
                'column': None,
                'affected_rows': len(data)
            })
        
        # Check column data types
        for column, expected_type in schema.columns.items():
            if column in data.columns:
                validation_result = self._validate_column_type(data[column], expected_type)
                if not validation_result['is_valid']:
                    report.errors.append({
                        'rule': 'invalid_data_type',
                        'message': f'Column {column} has invalid data type. Expected: {expected_type.value}',
                        'column': column,
                        'affected_rows': validation_result['invalid_count']
                    })
        
        # Check nullable constraints
        non_nullable_columns = schema.required_columns - schema.nullable_columns
        for column in non_nullable_columns:
            if column in data.columns:
                null_count = data[column].isnull().sum()
                if null_count > 0:
                    report.errors.append({
                        'rule': 'null_constraint_violation',
                        'message': f'Column {column} contains {null_count} null values but is not nullable',
                        'column': column,
                        'affected_rows': null_count
                    })
        
        # Check unique constraints
        for column in schema.unique_columns:
            if column in data.columns:
                duplicate_count = data[column].duplicated().sum()
                if duplicate_count > 0:
                    report.errors.append({
                        'rule': 'unique_constraint_violation',
                        'message': f'Column {column} contains {duplicate_count} duplicate values',
                        'column': column,
                        'affected_rows': duplicate_count
                    })
        
        # Check primary key constraint
        if schema.primary_key:
            pk_columns = [col for col in schema.primary_key if col in data.columns]
            if len(pk_columns) == len(schema.primary_key):
                duplicate_count = data[pk_columns].duplicated().sum()
                if duplicate_count > 0:
                    report.errors.append({
                        'rule': 'primary_key_violation',
                        'message': f'Primary key {schema.primary_key} contains {duplicate_count} duplicate values',
                        'column': None,
                        'affected_rows': duplicate_count
                    })
    
    def _validate_column_type(self, column_data: pd.Series, expected_type: DataType) -> Dict[str, Any]:
        """Validate column data type."""
        invalid_count = 0
        
        if expected_type == DataType.STRING:
            # All data can be converted to string
            pass
        elif expected_type == DataType.INTEGER:
            try:
                numeric_data = pd.to_numeric(column_data, errors='coerce')
                invalid_count = numeric_data.isnull().sum() - column_data.isnull().sum()
            except:
                invalid_count = len(column_data)
        elif expected_type == DataType.FLOAT:
            try:
                numeric_data = pd.to_numeric(column_data, errors='coerce')
                invalid_count = numeric_data.isnull().sum() - column_data.isnull().sum()
            except:
                invalid_count = len(column_data)
        elif expected_type == DataType.BOOLEAN:
            boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f', 'y', 'n'}
            invalid_count = sum(1 for val in column_data.dropna() 
                              if str(val).lower() not in boolean_values)
        elif expected_type == DataType.DATE:
            try:
                datetime_data = pd.to_datetime(column_data, errors='coerce')
                invalid_count = datetime_data.isnull().sum() - column_data.isnull().sum()
            except:
                invalid_count = len(column_data)
        elif expected_type == DataType.DATETIME:
            try:
                datetime_data = pd.to_datetime(column_data, errors='coerce')
                invalid_count = datetime_data.isnull().sum() - column_data.isnull().sum()
            except:
                invalid_count = len(column_data)
        elif expected_type == DataType.EMAIL:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_count = sum(1 for val in column_data.dropna() 
                              if not re.match(email_pattern, str(val)))
        elif expected_type == DataType.URL:
            url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            invalid_count = sum(1 for val in column_data.dropna() 
                              if not re.match(url_pattern, str(val)))
        elif expected_type == DataType.JSON:
            invalid_count = 0
            for val in column_data.dropna():
                try:
                    json.loads(str(val))
                except (json.JSONDecodeError, TypeError):
                    invalid_count += 1
        
        return {
            'is_valid': invalid_count == 0,
            'invalid_count': invalid_count
        }
    
    def _execute_validation_rules(self, data: pd.DataFrame, report: ValidationReport):
        """Execute custom validation rules."""
        for rule in self.validation_rules:
            if not rule.enabled or rule.column not in data.columns:
                continue
            
            try:
                result = self._execute_single_rule(data, rule)
                if result['violations'] > 0:
                    violation_entry = {
                        'rule': rule.name,
                        'message': result['message'],
                        'column': rule.column,
                        'affected_rows': result['violations']
                    }
                    
                    if rule.severity == ValidationSeverity.ERROR:
                        report.errors.append(violation_entry)
                    elif rule.severity == ValidationSeverity.WARNING:
                        report.warnings.append(violation_entry)
                    else:
                        report.info.append(violation_entry)
                        
            except Exception as e:
                self.logger.error(f"Error executing validation rule {rule.name}: {e}")
                report.errors.append({
                    'rule': rule.name,
                    'message': f'Rule execution failed: {e}',
                    'column': rule.column,
                    'affected_rows': 0
                })
    
    def _execute_single_rule(self, data: pd.DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Execute a single validation rule."""
        column_data = data[rule.column]
        violations = 0
        message = ""
        
        if rule.rule_type == 'not_null':
            violations = column_data.isnull().sum()
            message = f"Column {rule.column} has {violations} null values"
        
        elif rule.rule_type == 'range':
            min_val = rule.parameters.get('min')
            max_val = rule.parameters.get('max')
            if min_val is not None and max_val is not None:
                violations = ((column_data < min_val) | (column_data > max_val)).sum()
                message = f"Column {rule.column} has {violations} values outside range [{min_val}, {max_val}]"
        
        elif rule.rule_type == 'length':
            min_len = rule.parameters.get('min', 0)
            max_len = rule.parameters.get('max', float('inf'))
            string_lengths = column_data.astype(str).str.len()
            violations = ((string_lengths < min_len) | (string_lengths > max_len)).sum()
            message = f"Column {rule.column} has {violations} values with invalid length"
        
        elif rule.rule_type == 'pattern':
            pattern = rule.parameters.get('pattern')
            if pattern:
                violations = sum(1 for val in column_data.dropna() 
                               if not re.match(pattern, str(val)))
                message = f"Column {rule.column} has {violations} values not matching pattern"
        
        elif rule.rule_type == 'allowed_values':
            allowed = set(rule.parameters.get('values', []))
            violations = sum(1 for val in column_data.dropna() if val not in allowed)
            message = f"Column {rule.column} has {violations} values not in allowed set"
        
        elif rule.rule_type == 'custom':
            # Custom validation function
            func_name = rule.parameters.get('function')
            if func_name and hasattr(self, f'_validate_{func_name}'):
                validator_func = getattr(self, f'_validate_{func_name}')
                result = validator_func(column_data, rule.parameters)
                violations = result.get('violations', 0)
                message = result.get('message', f"Custom validation {func_name} failed")
        
        return {
            'violations': violations,
            'message': message
        }
    
    def _profile_data(self, data: pd.DataFrame, report: ValidationReport):
        """Generate data profiling statistics."""
        for column in data.columns:
            column_data = data[column]
            stats = {
                'data_type': str(column_data.dtype),
                'null_count': int(column_data.isnull().sum()),
                'null_percentage': float(column_data.isnull().sum() / len(data)),
                'unique_count': int(column_data.nunique()),
                'unique_percentage': float(column_data.nunique() / len(data))
            }
            
            # Numeric statistics
            if pd.api.types.is_numeric_dtype(column_data):
                stats.update({
                    'mean': float(column_data.mean()) if not column_data.isnull().all() else None,
                    'median': float(column_data.median()) if not column_data.isnull().all() else None,
                    'std': float(column_data.std()) if not column_data.isnull().all() else None,
                    'min': float(column_data.min()) if not column_data.isnull().all() else None,
                    'max': float(column_data.max()) if not column_data.isnull().all() else None,
                    'zero_count': int((column_data == 0).sum()),
                    'negative_count': int((column_data < 0).sum()) if not column_data.isnull().all() else 0
                })
            
            # String statistics
            elif column_data.dtype == 'object':
                string_lengths = column_data.astype(str).str.len()
                stats.update({
                    'avg_length': float(string_lengths.mean()),
                    'min_length': int(string_lengths.min()),
                    'max_length': int(string_lengths.max()),
                    'empty_string_count': int((column_data == '').sum())
                })
            
            report.column_stats[column] = stats
    
    def _calculate_quality_score(self, report: ValidationReport) -> float:
        """Calculate overall data quality score."""
        if report.total_rows == 0:
            return 0.0
        
        # Weight different types of issues
        error_weight = 1.0
        warning_weight = 0.5
        info_weight = 0.1
        
        total_issues = (
            sum(error['affected_rows'] for error in report.errors) * error_weight +
            sum(warning['affected_rows'] for warning in report.warnings) * warning_weight +
            sum(info['affected_rows'] for info in report.info) * info_weight
        )
        
        # Calculate score as percentage of clean rows
        max_possible_issues = report.total_rows * len(report.column_stats) if report.column_stats else report.total_rows
        quality_score = max(0.0, 1.0 - (total_issues / max_possible_issues))
        
        return round(quality_score, 3)
    
    def validate_schema_and_quality(self, data: pd.DataFrame, table_name: str) -> ValidationResult:
        """Main validation method that returns ValidationResult for compatibility."""
        report = self.validate_data(data, table_name)
        
        # Convert report to ValidationResult format
        errors = [f"{error['rule']}: {error['message']}" for error in report.errors]
        warnings = [f"{warning['rule']}: {warning['message']}" for warning in report.warnings]
        
        # Calculate failed rows (sum of all error affected rows, avoiding double counting)
        failed_rows = len(set().union(*[
            range(error['affected_rows']) for error in report.errors 
            if error['affected_rows'] > 0
        ])) if report.errors else 0
        
        is_valid = (
            len(errors) == 0 and 
            report.data_quality_score >= self.quality_thresholds['min_data_quality_score']
        )
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            row_count=report.total_rows,
            failed_rows=failed_rows
        )
    
    # Custom validation functions
    def _validate_business_hours(self, column_data: pd.Series, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that datetime values fall within business hours."""
        start_hour = parameters.get('start_hour', 9)
        end_hour = parameters.get('end_hour', 17)
        
        violations = 0
        for val in column_data.dropna():
            try:
                dt = pd.to_datetime(val)
                if not (start_hour <= dt.hour < end_hour):
                    violations += 1
            except:
                violations += 1
        
        return {
            'violations': violations,
            'message': f"Found {violations} values outside business hours ({start_hour}-{end_hour})"
        }
    
    def _validate_future_date(self, column_data: pd.Series, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that date values are not in the future."""
        violations = 0
        current_date = datetime.now().date()
        
        for val in column_data.dropna():
            try:
                dt = pd.to_datetime(val).date()
                if dt > current_date:
                    violations += 1
            except:
                violations += 1
        
        return {
            'violations': violations,
            'message': f"Found {violations} future date values"
        }
    
    def initialize(self) -> bool:
        """Initialize the validator."""
        self.logger.info(f"Initializing IngestionValidator {self.component_id}")
        return True
    
    def cleanup(self) -> None:
        """Cleanup validator resources."""
        self.logger.info(f"IngestionValidator {self.component_id} cleaned up")
    
    def health_check(self) -> bool:
        """Check validator health."""
        return True