"""
Unit tests for data extraction components.
"""
import pytest
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

from src.ingestion.extractors import (
    DataExtractorImpl, DataSerializer, ExtractionConfig, ExtractionMode,
    SerializationFormat, RetryableError, NonRetryableError
)
from src.interfaces.base import DataSource, ChangeEvent, ChangeType, ValidationResult


class MockDataSource(DataSource):
    """Mock data source for testing."""
    
    def __init__(self, source_id: str, config: dict, should_fail: bool = False):
        super().__init__(source_id, config)
        self.should_fail = should_fail
        self.extract_call_count = 0
    
    def detect_changes(self):
        return []
    
    def extract_data(self, change_event):
        self.extract_call_count += 1
        if self.should_fail:
            raise ConnectionError("Mock connection error")
        
        # Return mock data
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 30.1]
        })
    
    def validate_schema(self, data):
        return ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            row_count=len(data)
        )
    
    def test_connection(self):
        return not self.should_fail


@pytest.fixture
def mock_data_source():
    """Create a mock data source."""
    return MockDataSource("test_source", {})


@pytest.fixture
def failing_data_source():
    """Create a failing mock data source."""
    return MockDataSource("failing_source", {}, should_fail=True)


@pytest.fixture
def sample_change_event():
    """Create a sample change event."""
    return ChangeEvent(
        source_id="test_source",
        table_name="test_table",
        change_type=ChangeType.UPDATE,
        timestamp=datetime.now(),
        affected_rows=3,
        metadata={"test": "metadata"}
    )


@pytest.fixture
def extractor_config():
    """Create extractor configuration."""
    return {
        'extraction': {
            'mode': 'incremental',
            'batch_size': 1000,
            'max_retries': 2,
            'retry_delay': 0.1,
            'timeout': 30,
            'parallel_extractions': 1,
            'serialization_format': 'json',
            'validate_on_extract': True
        }
    }


@pytest.fixture
def data_extractor(extractor_config):
    """Create a data extractor instance."""
    return DataExtractorImpl("test_extractor", extractor_config)


class TestDataExtractorImpl:
    """Test cases for DataExtractorImpl."""
    
    def test_initialization(self, extractor_config):
        """Test extractor initialization."""
        extractor = DataExtractorImpl("test_extractor", extractor_config)
        
        assert extractor.component_id == "test_extractor"
        assert extractor.extraction_config.mode == ExtractionMode.INCREMENTAL
        assert extractor.extraction_config.max_retries == 2
        assert extractor.extraction_config.batch_size == 1000
    
    def test_successful_extraction(self, data_extractor, mock_data_source, sample_change_event):
        """Test successful data extraction."""
        result = data_extractor.extract(mock_data_source, sample_change_event)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name', 'value']
        assert mock_data_source.extract_call_count == 1
    
    def test_extraction_with_validation_failure(self, extractor_config, mock_data_source, sample_change_event):
        """Test extraction when validation fails."""
        # Create extractor with validation enabled
        extractor = DataExtractorImpl("test_extractor", extractor_config)
        
        # Mock validation to fail
        with patch.object(extractor, 'validate_extraction') as mock_validate:
            mock_validate.return_value = ValidationResult(
                is_valid=False,
                errors=["Test validation error"],
                warnings=[],
                row_count=3
            )
            
            result = extractor.extract(mock_data_source, sample_change_event)
            
            # Should return empty DataFrame on validation failure
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extraction_with_retries(self, data_extractor, sample_change_event):
        """Test extraction with retry logic."""
        # Create a source that fails first time, succeeds second time
        source = MockDataSource("retry_source", {})
        original_extract = source.extract_data
        
        def failing_extract(change_event):
            if source.extract_call_count == 0:
                source.extract_call_count += 1
                raise ConnectionError("Temporary failure")
            return original_extract(change_event)
        
        source.extract_data = failing_extract
        
        result = data_extractor.extract(source, sample_change_event)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert source.extract_call_count == 2  # One failure, one success
    
    def test_extraction_max_retries_exceeded(self, data_extractor, failing_data_source, sample_change_event):
        """Test extraction when max retries are exceeded."""
        result = data_extractor.extract(failing_data_source, sample_change_event)
        
        # Should return empty DataFrame when all retries fail
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert failing_data_source.extract_call_count == 3  # Initial + 2 retries
    
    def test_is_retryable_error(self, data_extractor):
        """Test retryable error detection."""
        # Retryable errors
        assert data_extractor._is_retryable_error(ConnectionError("Connection failed"))
        assert data_extractor._is_retryable_error(TimeoutError("Request timeout"))
        assert data_extractor._is_retryable_error(Exception("connection timeout"))
        assert data_extractor._is_retryable_error(Exception("rate limit exceeded"))
        
        # Non-retryable errors
        assert not data_extractor._is_retryable_error(ValueError("Invalid value"))
        assert not data_extractor._is_retryable_error(KeyError("Missing key"))
    
    def test_validate_extraction_empty_data(self, data_extractor):
        """Test validation of empty data."""
        empty_df = pd.DataFrame()
        result = data_extractor.validate_extraction(empty_df)
        
        assert result.is_valid is True  # Empty data is valid but noted
        assert result.row_count == 0
        assert len(result.warnings) == 1
        assert "empty" in result.warnings[0].lower()
    
    def test_validate_extraction_with_issues(self, data_extractor):
        """Test validation with data quality issues."""
        # Create data with various issues
        problematic_data = pd.DataFrame({
            'id': [1, 2, 2, 4],  # Duplicate
            'name': ['Alice', None, 'Charlie', 'David'],  # Null value
            'value': [10.5, np.inf, 30.1, -5.0],  # Infinite value
            'mixed': [1, 'text', 3.14, None]  # Mixed types
        })
        
        result = data_extractor.validate_extraction(problematic_data)
        
        assert result.is_valid is False  # Should be invalid due to infinite values
        assert result.row_count == 4
        assert len(result.errors) > 0
        assert len(result.warnings) > 0
    
    def test_validate_extraction_large_dataset(self, data_extractor):
        """Test validation warning for large datasets."""
        # Create a large dataset (simulate by mocking memory usage)
        large_data = pd.DataFrame({
            'col1': range(1000),
            'col2': ['text'] * 1000
        })
        
        with patch.object(large_data, 'memory_usage') as mock_memory:
            # Mock memory usage to be over 1GB
            mock_memory.return_value = pd.Series([1200 * 1024 * 1024] * 3)  # 1.2GB
            
            result = data_extractor.validate_extraction(large_data)
            
            assert any('Large dataset' in warning for warning in result.warnings)
    
    def test_parallel_extraction(self, extractor_config):
        """Test parallel extraction from multiple sources."""
        # Configure for parallel extraction
        extractor_config['extraction']['parallel_extractions'] = 2
        extractor = DataExtractorImpl("test_extractor", extractor_config)
        
        # Create multiple sources and events
        sources_and_events = [
            (MockDataSource(f"source_{i}", {}), ChangeEvent(
                source_id=f"source_{i}",
                table_name=f"table_{i}",
                change_type=ChangeType.UPDATE,
                timestamp=datetime.now(),
                affected_rows=3
            )) for i in range(3)
        ]
        
        results = extractor.extract_parallel(sources_and_events)
        
        assert len(results) == 3
        for key, df in results.items():
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
    
    def test_extraction_statistics(self, data_extractor, mock_data_source, sample_change_event):
        """Test extraction statistics recording."""
        # Perform extraction
        data_extractor.extract(mock_data_source, sample_change_event)
        
        # Check statistics
        stats = data_extractor.get_extraction_stats()
        key = f"{mock_data_source.source_id}_{sample_change_event.table_name}"
        
        assert key in stats
        assert stats[key]['row_count'] == 3
        assert 'last_extraction' in stats[key]
        assert 'extraction_time' in stats[key]
    
    def test_component_lifecycle(self, data_extractor):
        """Test component initialization, health check, and cleanup."""
        # Test initialization
        assert data_extractor.initialize() is True
        
        # Test health check
        assert data_extractor.health_check() is True
        
        # Test cleanup
        data_extractor.cleanup()
        stats = data_extractor.get_extraction_stats()
        assert len(stats) == 0


class TestDataSerializer:
    """Test cases for DataSerializer."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 30.1],
            'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
    
    def test_json_serialization(self, sample_dataframe):
        """Test JSON serialization and deserialization."""
        serializer = DataSerializer(SerializationFormat.JSON)
        
        # Serialize
        serialized = serializer.serialize(sample_dataframe)
        assert isinstance(serialized, bytes)
        
        # Deserialize
        deserialized = serializer.deserialize(serialized)
        assert isinstance(deserialized, pd.DataFrame)
        assert len(deserialized) == 3
        assert list(deserialized.columns) == ['id', 'name', 'value', 'date']
    
    def test_pickle_serialization(self, sample_dataframe):
        """Test Pickle serialization and deserialization."""
        serializer = DataSerializer(SerializationFormat.PICKLE)
        
        # Serialize
        serialized = serializer.serialize(sample_dataframe)
        assert isinstance(serialized, bytes)
        
        # Deserialize
        deserialized = serializer.deserialize(serialized)
        assert isinstance(deserialized, pd.DataFrame)
        pd.testing.assert_frame_equal(deserialized, sample_dataframe)
    
    def test_csv_serialization(self, sample_dataframe):
        """Test CSV serialization and deserialization."""
        serializer = DataSerializer(SerializationFormat.CSV)
        
        # Serialize
        serialized = serializer.serialize(sample_dataframe)
        assert isinstance(serialized, bytes)
        
        # Deserialize
        deserialized = serializer.deserialize(serialized)
        assert isinstance(deserialized, pd.DataFrame)
        assert len(deserialized) == 3
        assert list(deserialized.columns) == ['id', 'name', 'value', 'date']
    
    @pytest.mark.skipif(True, reason="Parquet requires pyarrow which may not be available")
    def test_parquet_serialization(self, sample_dataframe):
        """Test Parquet serialization and deserialization."""
        try:
            serializer = DataSerializer(SerializationFormat.PARQUET)
            
            # Serialize
            serialized = serializer.serialize(sample_dataframe)
            assert isinstance(serialized, bytes)
            
            # Deserialize
            deserialized = serializer.deserialize(serialized)
            assert isinstance(deserialized, pd.DataFrame)
            assert len(deserialized) == 3
        except ImportError:
            pytest.skip("Parquet serialization requires pyarrow")
    
    def test_file_extensions(self):
        """Test file extension mapping."""
        assert DataSerializer(SerializationFormat.JSON).get_file_extension() == '.json'
        assert DataSerializer(SerializationFormat.PICKLE).get_file_extension() == '.pkl'
        assert DataSerializer(SerializationFormat.CSV).get_file_extension() == '.csv'
        assert DataSerializer(SerializationFormat.PARQUET).get_file_extension() == '.parquet'
    
    def test_unsupported_format(self):
        """Test handling of unsupported serialization format."""
        # This would require modifying the enum, so we'll test error handling
        serializer = DataSerializer(SerializationFormat.JSON)
        
        # Manually set an invalid format to test error handling
        serializer.format_type = "invalid_format"
        
        with pytest.raises(ValueError):
            serializer.serialize(pd.DataFrame())
        
        with pytest.raises(ValueError):
            serializer.deserialize(b"test data")


class TestExtractionConfig:
    """Test cases for ExtractionConfig."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = ExtractionConfig()
        
        assert config.mode == ExtractionMode.INCREMENTAL
        assert config.batch_size == 1000
        assert config.max_retries == 3
        assert config.retry_delay == 1.0
        assert config.timeout == 300
        assert config.parallel_extractions == 1
        assert config.serialization_format == SerializationFormat.JSON
        assert config.compression is None
        assert config.validate_on_extract is True
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = ExtractionConfig(
            mode=ExtractionMode.FULL,
            batch_size=500,
            max_retries=5,
            retry_delay=2.0,
            timeout=600,
            parallel_extractions=4,
            serialization_format=SerializationFormat.PICKLE,
            compression='gzip',
            validate_on_extract=False
        )
        
        assert config.mode == ExtractionMode.FULL
        assert config.batch_size == 500
        assert config.max_retries == 5
        assert config.retry_delay == 2.0
        assert config.timeout == 600
        assert config.parallel_extractions == 4
        assert config.serialization_format == SerializationFormat.PICKLE
        assert config.compression == 'gzip'
        assert config.validate_on_extract is False


if __name__ == '__main__':
    pytest.main([__file__])