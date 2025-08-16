#!/usr/bin/env python3
"""
Example demonstrating the data extraction and validation components.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from src.ingestion.extractors import DataExtractorImpl, ExtractionConfig, ExtractionMode, SerializationFormat
from src.ingestion.validators import IngestionValidator, ValidationSeverity, DataType
from src.interfaces.base import DataSource, ChangeEvent, ChangeType, ValidationResult


class MockDataSource(DataSource):
    """Mock data source for demonstration."""
    
    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
            'is_active': [True, True, False, True, False],
            'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])
        })
    
    def detect_changes(self):
        return [ChangeEvent(
            source_id=self.source_id,
            table_name='users',
            change_type=ChangeType.UPDATE,
            timestamp=datetime.now(),
            affected_rows=len(self.sample_data)
        )]
    
    def extract_data(self, change_event):
        return self.sample_data.copy()
    
    def validate_schema(self, data):
        return ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            row_count=len(data)
        )
    
    def test_connection(self):
        return True


def main():
    """Demonstrate extraction and validation workflow."""
    print("=== Data Extraction and Validation Example ===\n")
    
    # 1. Create mock data source
    print("1. Creating mock data source...")
    data_source = MockDataSource("demo_source", {})
    print(f"   Source ID: {data_source.source_id}")
    print(f"   Connection test: {'✓' if data_source.test_connection() else '✗'}")
    
    # 2. Configure data extractor
    print("\n2. Configuring data extractor...")
    extractor_config = {
        'extraction': {
            'mode': 'incremental',
            'batch_size': 1000,
            'max_retries': 3,
            'retry_delay': 1.0,
            'serialization_format': 'json',
            'validate_on_extract': True
        }
    }
    
    extractor = DataExtractorImpl("demo_extractor", extractor_config)
    print(f"   Extractor ID: {extractor.component_id}")
    print(f"   Mode: {extractor.extraction_config.mode.value}")
    print(f"   Validation enabled: {extractor.extraction_config.validate_on_extract}")
    
    # 3. Configure data validator
    print("\n3. Configuring data validator...")
    validator_config = {
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
                'parameters': {'min': 18, 'max': 65}
            },
            {
                'name': 'email_pattern',
                'column': 'email',
                'rule_type': 'pattern',
                'severity': 'error',
                'parameters': {'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}
            }
        ],
        'schemas': {
            'users': {
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
                'unique_columns': ['id', 'email'],
                'primary_key': ['id']
            }
        },
        'enable_profiling': True
    }
    
    validator = IngestionValidator("demo_validator", validator_config)
    print(f"   Validator ID: {validator.component_id}")
    print(f"   Rules configured: {len(validator.validation_rules)}")
    print(f"   Schemas configured: {len(validator.schema_definitions)}")
    
    # 4. Detect changes
    print("\n4. Detecting changes...")
    changes = data_source.detect_changes()
    print(f"   Changes detected: {len(changes)}")
    
    for change in changes:
        print(f"   - Table: {change.table_name}")
        print(f"     Type: {change.change_type.value}")
        print(f"     Affected rows: {change.affected_rows}")
        print(f"     Timestamp: {change.timestamp}")
    
    # 5. Extract data
    print("\n5. Extracting data...")
    if changes:
        change_event = changes[0]
        extracted_data = extractor.extract(data_source, change_event)
        
        print(f"   Rows extracted: {len(extracted_data)}")
        print(f"   Columns: {list(extracted_data.columns)}")
        print("\n   Sample data:")
        print(extracted_data.head(3).to_string(index=False))
    
    # 6. Validate data
    print("\n6. Validating data...")
    if not extracted_data.empty:
        validation_report = validator.validate_data(extracted_data, 'users')
        
        print(f"   Total rows: {validation_report.total_rows}")
        print(f"   Rules executed: {validation_report.rules_executed}")
        print(f"   Data quality score: {validation_report.data_quality_score:.3f}")
        print(f"   Errors: {len(validation_report.errors)}")
        print(f"   Warnings: {len(validation_report.warnings)}")
        
        if validation_report.errors:
            print("\n   Errors found:")
            for error in validation_report.errors:
                print(f"   - {error['rule']}: {error['message']}")
        
        if validation_report.warnings:
            print("\n   Warnings found:")
            for warning in validation_report.warnings:
                print(f"   - {warning['rule']}: {warning['message']}")
        
        # Show column statistics
        print("\n   Column Statistics:")
        for col, stats in validation_report.column_stats.items():
            print(f"   - {col}:")
            print(f"     Type: {stats['data_type']}")
            print(f"     Null %: {stats['null_percentage']:.1%}")
            print(f"     Unique %: {stats['unique_percentage']:.1%}")
    
    # 7. Demonstrate serialization
    print("\n7. Demonstrating data serialization...")
    serializer = extractor.serializer
    
    if not extracted_data.empty:
        # Serialize data
        serialized_data = serializer.serialize(extracted_data)
        print(f"   Serialized size: {len(serialized_data)} bytes")
        print(f"   Format: {serializer.format_type.value}")
        
        # Deserialize data
        deserialized_data = serializer.deserialize(serialized_data)
        print(f"   Deserialized rows: {len(deserialized_data)}")
        print(f"   Data integrity: {'✓' if len(deserialized_data) == len(extracted_data) else '✗'}")
    
    # 8. Show extraction statistics
    print("\n8. Extraction statistics...")
    stats = extractor.get_extraction_stats()
    for key, stat in stats.items():
        print(f"   {key}:")
        print(f"     Last extraction: {stat['last_extraction']}")
        print(f"     Rows: {stat['row_count']}")
        print(f"     Time: {stat['extraction_time']:.3f}s")
        print(f"     Validation passed: {stat['validation_passed']}")
    
    print("\n=== Example completed successfully! ===")


if __name__ == "__main__":
    main()