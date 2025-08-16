"""
Unit tests for data validation components.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch

from src.ingestion.validators import (
    IngestionValidator, ValidationRule, SchemaDefinition, ValidationReport,
    ValidationSeverity, DataType
)
from src.interfaces.base import ValidationResult


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'david@test.com', 'eve@test.com'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'is_active': [True, True, False, True, False],
        'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])
    })


@pytest.fixture
def problematic_data():
    """Create data with various quality issues."""
    return pd.DataFrame({
        'id': [1, 2, 2, 4, None],  # Duplicate and null
        'name': ['Alice', None, 'Charlie', '', 'Eve'],  # Null and empty string
        'email': ['alice@test.com', 'invalid', None, 'david@test.com', 'not-an-email'],
        'age': [25, -5, 150, 40, None],  # Negative and extreme values
        'salary': [50000.0, np.inf, -1000.0, 80000.0, None],  # Infinite and negative
        'is_active': [True, 'yes', 0, 1, None],  # Mixed boolean representations
        'created_at': ['2023-01-01', '2025-12-31', 'invalid-date', '2023-01-04', None]  # Future date and invalid
    })


@pytest.fixture
def validation_config():
    """Create validation configuration."""
    return {
        'rules': [
            {
                'name': 'id_not_null',
                'column': 'id',
                'rule_type': 'not_null',
                'severity': 'error'
            },
            {
                'name': 'age_range',
                'column': 'age',
                'rule_type': 'range',
                'severity': 'warning',
                'parameters': {'min': 0, 'max': 120}
            },
            {
                'name': 'email_pattern',
                'column': 'email',
                'rule_type': 'pattern',
                'severity': 'error',
                'parameters': {'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}
            },
            {
                'name': 'name_length',
                'column': 'name',
                'rule_type': 'length',
                'severity': 'warning',
                'parameters': {'min': 1, 'max': 50}
            }
        ],
        'schemas': {
            'test_table': {
                'columns': {
                    'id': 'integer',
                    'name': 'string',
                    'email': 'email',
                    'age': 'integer',
                    'salary': 'float',
                    'is_active': 'boolean',
                    'created_at': 'datetime'
                },
                'required_columns': ['id', 'name', 'email'],
                'nullable_columns': ['salary'],
                'unique_columns': ['id', 'email'],
                'primary_key': ['id']
            }
        },
        'quality_thresholds': {
            'min_data_quality_score': 0.8,
            'max_error_percentage': 0.05,
            'max_null_percentage': 0.1
        },
        'enable_profiling': True
    }


@pytest.fixture
def validator(validation_config):
    """Create validator instance."""
    return IngestionValidator("test_validator", validation_config)


class TestIngestionValidator:
    """Test cases for IngestionValidator."""
    
    def test_initialization(self, validation_config):
        """Test validator initialization."""
        validator = IngestionValidator("test_validator", validation_config)
        
        assert validator.component_id == "test_validator"
        assert len(validator.validation_rules) == 4
        assert 'test_table' in validator.schema_definitions
        assert validator.enable_profiling is True
    
    def test_load_validation_rules(self, validation_config):
        """Test loading validation rules from configuration."""
        validator = IngestionValidator("test_validator", validation_config)
        
        rules = validator.validation_rules
        assert len(rules) == 4
        
        # Check first rule
        rule = rules[0]
        assert rule.name == 'id_not_null'
        assert rule.column == 'id'
        assert rule.rule_type == 'not_null'
        assert rule.severity == ValidationSeverity.ERROR
    
    def test_load_schema_definitions(self, validation_config):
        """Test loading schema definitions from configuration."""
        validator = IngestionValidator("test_validator", validation_config)
        
        schema = validator.schema_definitions['test_table']
        assert isinstance(schema, SchemaDefinition)
        assert schema.columns['id'] == DataType.INTEGER
        assert schema.columns['email'] == DataType.EMAIL
        assert 'id' in schema.required_columns
        assert 'salary' in schema.nullable_columns
        assert 'id' in schema.unique_columns
        assert schema.primary_key == ['id']
    
    def test_validate_clean_data(self, validator, sample_data):
        """Test validation of clean data."""
        report = validator.validate_data(sample_data, 'test_table')
        
        assert isinstance(report, ValidationReport)
        assert report.table_name == 'test_table'
        assert report.total_rows == 5
        assert len(report.errors) == 0  # Clean data should have no errors
        assert report.data_quality_score > 0.8
    
    def test_validate_problematic_data(self, validator, problematic_data):
        """Test validation of data with quality issues."""
        report = validator.validate_data(problematic_data, 'test_table')
        
        assert isinstance(report, ValidationReport)
        assert report.total_rows == 5
        assert len(report.errors) > 0  # Should detect errors
        assert len(report.warnings) > 0  # Should detect warnings
        assert report.data_quality_score < 0.8  # Quality should be low
    
    def test_validate_empty_data(self, validator):
        """Test validation of empty dataset."""
        empty_data = pd.DataFrame()
        report = validator.validate_data(empty_data, 'test_table')
        
        assert report.total_rows == 0
        assert len(report.warnings) == 1
        assert 'empty' in report.warnings[0]['message'].lower()
    
    def test_schema_validation_missing_columns(self, validator):
        """Test schema validation with missing required columns."""
        incomplete_data = pd.DataFrame({
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
            # Missing required 'id' and 'email' columns
        })
        
        report = validator.validate_data(incomplete_data, 'test_table')
        
        # Should have errors for missing required columns
        missing_column_errors = [e for e in report.errors if 'missing_required_columns' in e['rule']]
        assert len(missing_column_errors) > 0
    
    def test_schema_validation_data_types(self, validator):
        """Test schema validation for data types."""
        invalid_type_data = pd.DataFrame({
            'id': ['not_an_integer', 'also_not_integer'],
            'name': ['Alice', 'Bob'],
            'email': ['alice@test.com', 'bob@test.com'],
            'age': ['twenty-five', 'thirty'],  # Should be integers
            'salary': ['fifty_thousand', 'sixty_thousand'],  # Should be floats
            'is_active': ['maybe', 'perhaps'],  # Should be booleans
            'created_at': ['not_a_date', 'also_not_a_date']  # Should be datetime
        })
        
        report = validator.validate_data(invalid_type_data, 'test_table')
        
        # Should have errors for invalid data types
        type_errors = [e for e in report.errors if 'invalid_data_type' in e['rule']]
        assert len(type_errors) > 0
    
    def test_schema_validation_null_constraints(self, validator):
        """Test schema validation for null constraints."""
        null_data = pd.DataFrame({
            'id': [1, None, 3],  # id is required, shouldn't be null
            'name': ['Alice', None, 'Charlie'],  # name is required, shouldn't be null
            'email': ['alice@test.com', None, 'charlie@test.com'],  # email is required
            'salary': [50000.0, None, 70000.0]  # salary is nullable, this is OK
        })
        
        report = validator.validate_data(null_data, 'test_table')
        
        # Should have errors for null values in non-nullable columns
        null_errors = [e for e in report.errors if 'null_constraint_violation' in e['rule']]
        assert len(null_errors) > 0
    
    def test_schema_validation_unique_constraints(self, validator):
        """Test schema validation for unique constraints."""
        duplicate_data = pd.DataFrame({
            'id': [1, 2, 1],  # Duplicate id
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@test.com', 'bob@test.com', 'alice@test.com'],  # Duplicate email
            'age': [25, 30, 35]
        })
        
        report = validator.validate_data(duplicate_data, 'test_table')
        
        # Should have errors for duplicate values in unique columns
        unique_errors = [e for e in report.errors if 'unique_constraint_violation' in e['rule']]
        assert len(unique_errors) > 0
    
    def test_schema_validation_primary_key(self, validator):
        """Test schema validation for primary key constraints."""
        pk_duplicate_data = pd.DataFrame({
            'id': [1, 2, 1],  # Duplicate primary key
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
        })
        
        report = validator.validate_data(pk_duplicate_data, 'test_table')
        
        # Should have error for primary key violation
        pk_errors = [e for e in report.errors if 'primary_key_violation' in e['rule']]
        assert len(pk_errors) > 0
    
    def test_validate_column_type_string(self, validator):
        """Test column type validation for strings."""
        string_data = pd.Series(['text1', 'text2', 'text3'])
        result = validator._validate_column_type(string_data, DataType.STRING)
        assert result['is_valid'] is True
        assert result['invalid_count'] == 0
    
    def test_validate_column_type_integer(self, validator):
        """Test column type validation for integers."""
        # Valid integers
        valid_int_data = pd.Series([1, 2, 3, 4, 5])
        result = validator._validate_column_type(valid_int_data, DataType.INTEGER)
        assert result['is_valid'] == True
        
        # Invalid integers
        invalid_int_data = pd.Series(['not_int', 2.5, 'also_not_int'])
        result = validator._validate_column_type(invalid_int_data, DataType.INTEGER)
        assert result['is_valid'] == False
        assert result['invalid_count'] > 0
    
    def test_validate_column_type_email(self, validator):
        """Test column type validation for emails."""
        # Valid emails
        valid_emails = pd.Series(['test@example.com', 'user@domain.org', 'name@site.co.uk'])
        result = validator._validate_column_type(valid_emails, DataType.EMAIL)
        assert result['is_valid'] == True
        
        # Invalid emails
        invalid_emails = pd.Series(['not-email', 'missing@', '@missing.com', 'no-domain'])
        result = validator._validate_column_type(invalid_emails, DataType.EMAIL)
        assert result['is_valid'] == False
        assert result['invalid_count'] == 4
    
    def test_validate_column_type_boolean(self, validator):
        """Test column type validation for booleans."""
        # Valid boolean representations
        valid_booleans = pd.Series(['true', 'false', '1', '0', 'yes', 'no', 't', 'f'])
        result = validator._validate_column_type(valid_booleans, DataType.BOOLEAN)
        assert result['is_valid'] == True
        
        # Invalid boolean representations
        invalid_booleans = pd.Series(['maybe', 'perhaps', 'invalid'])
        result = validator._validate_column_type(invalid_booleans, DataType.BOOLEAN)
        assert result['is_valid'] == False
        assert result['invalid_count'] == 3
    
    def test_execute_validation_rules(self, validator, problematic_data):
        """Test execution of validation rules."""
        report = ValidationReport(
            table_name='test_table',
            total_rows=len(problematic_data),
            validation_time=datetime.now()
        )
        
        validator._execute_validation_rules(problematic_data, report)
        
        # Should have detected rule violations
        assert len(report.errors) > 0 or len(report.warnings) > 0
    
    def test_execute_single_rule_not_null(self, validator):
        """Test execution of not_null rule."""
        data = pd.DataFrame({'test_col': [1, 2, None, 4, None]})
        rule = ValidationRule(
            name='test_not_null',
            column='test_col',
            rule_type='not_null',
            severity=ValidationSeverity.ERROR
        )
        
        result = validator._execute_single_rule(data, rule)
        assert result['violations'] == 2  # Two null values
        assert 'null values' in result['message']
    
    def test_execute_single_rule_range(self, validator):
        """Test execution of range rule."""
        data = pd.DataFrame({'test_col': [1, 5, 10, 15, 20]})
        rule = ValidationRule(
            name='test_range',
            column='test_col',
            rule_type='range',
            severity=ValidationSeverity.WARNING,
            parameters={'min': 5, 'max': 15}
        )
        
        result = validator._execute_single_rule(data, rule)
        assert result['violations'] == 2  # Values 1 and 20 are outside range
        assert 'outside range' in result['message']
    
    def test_execute_single_rule_length(self, validator):
        """Test execution of length rule."""
        data = pd.DataFrame({'test_col': ['a', 'abc', 'abcdefghijk', 'xy']})
        rule = ValidationRule(
            name='test_length',
            column='test_col',
            rule_type='length',
            severity=ValidationSeverity.WARNING,
            parameters={'min': 2, 'max': 10}
        )
        
        result = validator._execute_single_rule(data, rule)
        assert result['violations'] == 2  # 'a' too short, 'abcdefghijk' too long
        assert 'invalid length' in result['message']
    
    def test_execute_single_rule_pattern(self, validator):
        """Test execution of pattern rule."""
        data = pd.DataFrame({'test_col': ['ABC123', 'DEF456', 'invalid', 'GHI789']})
        rule = ValidationRule(
            name='test_pattern',
            column='test_col',
            rule_type='pattern',
            severity=ValidationSeverity.ERROR,
            parameters={'pattern': r'^[A-Z]{3}\d{3}$'}
        )
        
        result = validator._execute_single_rule(data, rule)
        assert result['violations'] == 1  # 'invalid' doesn't match pattern
        assert 'not matching pattern' in result['message']
    
    def test_execute_single_rule_allowed_values(self, validator):
        """Test execution of allowed_values rule."""
        data = pd.DataFrame({'test_col': ['A', 'B', 'C', 'X', 'Y']})
        rule = ValidationRule(
            name='test_allowed',
            column='test_col',
            rule_type='allowed_values',
            severity=ValidationSeverity.ERROR,
            parameters={'values': ['A', 'B', 'C']}
        )
        
        result = validator._execute_single_rule(data, rule)
        assert result['violations'] == 2  # 'X' and 'Y' not in allowed values
        assert 'not in allowed set' in result['message']
    
    def test_profile_data(self, validator, sample_data):
        """Test data profiling functionality."""
        report = ValidationReport(
            table_name='test_table',
            total_rows=len(sample_data),
            validation_time=datetime.now()
        )
        
        validator._profile_data(sample_data, report)
        
        assert len(report.column_stats) == len(sample_data.columns)
        
        # Check numeric column stats
        id_stats = report.column_stats['id']
        assert 'mean' in id_stats
        assert 'median' in id_stats
        assert 'std' in id_stats
        assert 'min' in id_stats
        assert 'max' in id_stats
        
        # Check string column stats
        name_stats = report.column_stats['name']
        assert 'avg_length' in name_stats
        assert 'min_length' in name_stats
        assert 'max_length' in name_stats
    
    def test_calculate_quality_score(self, validator):
        """Test data quality score calculation."""
        # Create report with some issues
        report = ValidationReport(
            table_name='test_table',
            total_rows=100,
            validation_time=datetime.now()
        )
        
        # Add some errors and warnings
        report.errors = [
            {'affected_rows': 5},
            {'affected_rows': 3}
        ]
        report.warnings = [
            {'affected_rows': 10},
            {'affected_rows': 2}
        ]
        report.column_stats = {'col1': {}, 'col2': {}, 'col3': {}}
        
        score = validator._calculate_quality_score(report)
        
        assert 0.0 <= score <= 1.0
        assert score < 1.0  # Should be less than perfect due to issues
    
    def test_validate_schema_and_quality(self, validator, sample_data):
        """Test main validation method that returns ValidationResult."""
        result = validator.validate_schema_and_quality(sample_data, 'test_table')
        
        assert isinstance(result, ValidationResult)
        assert result.row_count == 5
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.errors, list)
        assert isinstance(result.warnings, list)
    
    def test_custom_validation_business_hours(self, validator):
        """Test custom business hours validation."""
        # Create datetime data with some values outside business hours
        data = pd.Series([
            datetime(2023, 1, 1, 9, 0),   # 9 AM - valid
            datetime(2023, 1, 1, 14, 0),  # 2 PM - valid
            datetime(2023, 1, 1, 6, 0),   # 6 AM - invalid
            datetime(2023, 1, 1, 20, 0),  # 8 PM - invalid
        ])
        
        result = validator._validate_business_hours(data, {'start_hour': 9, 'end_hour': 17})
        
        assert result['violations'] == 2  # 6 AM and 8 PM are outside business hours
        assert 'business hours' in result['message']
    
    def test_custom_validation_future_date(self, validator):
        """Test custom future date validation."""
        # Create date data with some future dates
        future_date = datetime.now() + timedelta(days=30)
        past_date = datetime.now() - timedelta(days=30)
        
        data = pd.Series([
            past_date,
            datetime.now(),
            future_date,
            future_date + timedelta(days=10)
        ])
        
        result = validator._validate_future_date(data, {})
        
        assert result['violations'] == 2  # Two future dates
        assert 'future date' in result['message']
    
    def test_component_lifecycle(self, validator):
        """Test component initialization, health check, and cleanup."""
        # Test initialization
        assert validator.initialize() is True
        
        # Test health check
        assert validator.health_check() is True
        
        # Test cleanup
        validator.cleanup()  # Should not raise any exceptions


class TestValidationRule:
    """Test cases for ValidationRule dataclass."""
    
    def test_validation_rule_creation(self):
        """Test creation of validation rule."""
        rule = ValidationRule(
            name='test_rule',
            column='test_column',
            rule_type='not_null',
            severity=ValidationSeverity.ERROR,
            parameters={'param1': 'value1'},
            description='Test rule description',
            enabled=True
        )
        
        assert rule.name == 'test_rule'
        assert rule.column == 'test_column'
        assert rule.rule_type == 'not_null'
        assert rule.severity == ValidationSeverity.ERROR
        assert rule.parameters == {'param1': 'value1'}
        assert rule.description == 'Test rule description'
        assert rule.enabled is True
    
    def test_validation_rule_defaults(self):
        """Test default values for validation rule."""
        rule = ValidationRule(
            name='test_rule',
            column='test_column',
            rule_type='not_null',
            severity=ValidationSeverity.ERROR
        )
        
        assert rule.parameters == {}
        assert rule.description is None
        assert rule.enabled is True


class TestSchemaDefinition:
    """Test cases for SchemaDefinition dataclass."""
    
    def test_schema_definition_creation(self):
        """Test creation of schema definition."""
        schema = SchemaDefinition(
            columns={'id': DataType.INTEGER, 'name': DataType.STRING},
            required_columns={'id', 'name'},
            nullable_columns={'description'},
            unique_columns={'id'},
            primary_key=['id']
        )
        
        assert schema.columns == {'id': DataType.INTEGER, 'name': DataType.STRING}
        assert schema.required_columns == {'id', 'name'}
        assert schema.nullable_columns == {'description'}
        assert schema.unique_columns == {'id'}
        assert schema.primary_key == ['id']
    
    def test_schema_definition_defaults(self):
        """Test default values for schema definition."""
        schema = SchemaDefinition(
            columns={'id': DataType.INTEGER}
        )
        
        assert schema.required_columns == set()
        assert schema.nullable_columns == set()
        assert schema.unique_columns == set()
        assert schema.primary_key is None


if __name__ == '__main__':
    pytest.main([__file__])