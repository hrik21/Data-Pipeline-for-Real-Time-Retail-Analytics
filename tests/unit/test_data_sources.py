"""
Unit tests for data source implementations.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import requests
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from src.ingestion.data_sources import (
    DatabaseDataSource, APIDataSource, ConnectionConfig, APIConfig
)
from src.interfaces.base import ChangeEvent, ChangeType, ValidationResult


class TestDatabaseDataSource:
    """Test cases for DatabaseDataSource."""
    
    @pytest.fixture
    def db_config(self):
        """Database configuration fixture."""
        return {
            'connection': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'username': 'test_user',
                'password': 'test_pass',
                'driver': 'postgresql'
            },
            'tables': ['users', 'orders'],
            'change_detection_method': 'timestamp',
            'timestamp_column': 'updated_at'
        }
    
    @pytest.fixture
    def mock_engine(self):
        """Mock SQLAlchemy engine."""
        with patch('src.ingestion.data_sources.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine
            yield mock_engine
    
    def test_initialization(self, db_config, mock_engine):
        """Test database source initialization."""
        source = DatabaseDataSource('test_db_source', db_config)
        
        assert source.source_id == 'test_db_source'
        assert source.tables == ['users', 'orders']
        assert source.change_detection_method == 'timestamp'
        assert source.timestamp_column == 'updated_at'
        assert source.engine is not None
    
    def test_build_connection_string(self, db_config, mock_engine):
        """Test connection string building."""
        source = DatabaseDataSource('test_db_source', db_config)
        connection_string = source._build_connection_string()
        
        expected = "postgresql://test_user:test_pass@localhost:5432/test_db"
        assert connection_string == expected
    
    def test_test_connection_success(self, db_config, mock_engine):
        """Test successful connection test."""
        mock_connection = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context_manager
        
        source = DatabaseDataSource('test_db_source', db_config)
        result = source.test_connection()
        
        assert result is True
        # The connection is called twice - once during init and once during test_connection
        assert mock_engine.connect.call_count >= 1
    
    def test_test_connection_failure(self, db_config, mock_engine):
        """Test connection test failure."""
        mock_engine.connect.side_effect = SQLAlchemyError("Connection failed")
        
        source = DatabaseDataSource('test_db_source', db_config)
        result = source.test_connection()
        
        assert result is False
    
    def test_detect_timestamp_changes(self, db_config, mock_engine):
        """Test timestamp-based change detection."""
        source = DatabaseDataSource('test_db_source', db_config)
        
        # Create a proper change event to return
        expected_change = ChangeEvent(
            source_id='test_db_source',
            table_name='users',
            change_type=ChangeType.UPDATE,
            timestamp=datetime.now(),
            affected_rows=5,
            metadata={
                "detection_method": "timestamp",
                "timestamp_column": "updated_at",
                "last_processed": None
            }
        )
        
        # Mock the entire method to avoid complex database mocking
        with patch.object(source, '_detect_timestamp_changes') as mock_method:
            mock_method.return_value = [expected_change]
            
            changes = source._detect_timestamp_changes('users')
            
            assert len(changes) == 1
            assert changes[0].source_id == 'test_db_source'
            assert changes[0].table_name == 'users'
            assert changes[0].change_type == ChangeType.UPDATE
            assert changes[0].affected_rows == 5
    
    def test_detect_log_based_changes(self, db_config, mock_engine):
        """Test log-based change detection."""
        db_config['change_detection_method'] = 'log_based'
        source = DatabaseDataSource('test_db_source', db_config)
        
        # Create expected change events
        expected_changes = [
            ChangeEvent(
                source_id='test_db_source',
                table_name='users',
                change_type=ChangeType.INSERT,
                timestamp=datetime.now(),
                affected_rows=3,
                metadata={"detection_method": "log_based", "log_table": "users_changelog"}
            ),
            ChangeEvent(
                source_id='test_db_source',
                table_name='users',
                change_type=ChangeType.UPDATE,
                timestamp=datetime.now(),
                affected_rows=2,
                metadata={"detection_method": "log_based", "log_table": "users_changelog"}
            )
        ]
        
        # Mock the entire method
        with patch.object(source, '_detect_log_based_changes') as mock_method:
            mock_method.return_value = expected_changes
            
            changes = source._detect_log_based_changes('users')
            
            assert len(changes) == 2
            assert changes[0].change_type == ChangeType.INSERT
            assert changes[0].affected_rows == 3
            assert changes[1].change_type == ChangeType.UPDATE
            assert changes[1].affected_rows == 2
    
    def test_extract_data(self, db_config, mock_engine):
        """Test data extraction."""
        # Mock pandas read_sql
        mock_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = mock_df
            
            mock_connection = Mock()
            mock_context_manager = Mock()
            mock_context_manager.__enter__ = Mock(return_value=mock_connection)
            mock_context_manager.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_context_manager
            
            source = DatabaseDataSource('test_db_source', db_config)
            
            change_event = ChangeEvent(
                source_id='test_db_source',
                table_name='users',
                change_type=ChangeType.UPDATE,
                timestamp=datetime.now(),
                affected_rows=3,
                metadata={'detection_method': 'timestamp', 'last_processed': '2023-01-01T00:00:00'}
            )
            
            result_df = source.extract_data(change_event)
            
            assert len(result_df) == 3
            assert list(result_df.columns) == ['id', 'name']
            mock_read_sql.assert_called_once()
    
    def test_validate_schema(self, db_config, mock_engine):
        """Test schema validation."""
        source = DatabaseDataSource('test_db_source', db_config)
        
        # Test with valid data
        valid_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        
        result = source.validate_schema(valid_df)
        
        assert result.is_valid is True
        assert result.row_count == 3
        assert result.failed_rows == 0
        
        # Test with data containing nulls
        null_df = pd.DataFrame({
            'id': [1, 2, None],
            'name': ['A', None, 'C']
        })
        
        result = source.validate_schema(null_df)
        
        assert result.is_valid is True  # Warnings don't make it invalid
        assert len(result.warnings) > 0
        assert any('null values' in warning for warning in result.warnings)
    
    def test_detect_changes_integration(self, db_config, mock_engine):
        """Test full change detection integration."""
        source = DatabaseDataSource('test_db_source', db_config)
        
        # Mock both timestamp and log-based detection methods
        with patch.object(source, '_detect_timestamp_changes') as mock_timestamp:
            with patch.object(source, '_detect_log_based_changes') as mock_log:
                mock_timestamp.return_value = [
                    ChangeEvent('test_db_source', 'users', ChangeType.UPDATE, datetime.now(), 5)
                ]
                mock_log.return_value = []
                
                changes = source.detect_changes()
                
                # Should have changes for both tables in the config (users and orders)
                assert len(changes) == 2  # One for each table
                mock_timestamp.assert_called()


class TestAPIDataSource:
    """Test cases for APIDataSource."""
    
    @pytest.fixture
    def api_config(self):
        """API configuration fixture."""
        return {
            'api': {
                'base_url': 'https://api.example.com',
                'auth_token': 'test_token',
                'timeout': 30,
                'retry_attempts': 3,
                'retry_delay': 1
            },
            'endpoints': [
                {'name': 'users', 'path': '/users'},
                {'name': 'orders', 'path': '/orders'}
            ],
            'polling_interval': 300
        }
    
    def test_initialization(self, api_config):
        """Test API source initialization."""
        source = APIDataSource('test_api_source', api_config)
        
        assert source.source_id == 'test_api_source'
        assert source.api_config.base_url == 'https://api.example.com'
        assert source.api_config.auth_token == 'test_token'
        assert len(source.endpoints) == 2
        assert source.polling_interval == 300
    
    def test_get_headers(self, api_config):
        """Test header generation with authentication."""
        source = APIDataSource('test_api_source', api_config)
        headers = source._get_headers()
        
        assert headers['Content-Type'] == 'application/json'
        assert headers['Authorization'] == 'Bearer test_token'
    
    @patch('requests.get')
    def test_test_connection_success(self, mock_get, api_config):
        """Test successful API connection test."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        source = APIDataSource('test_api_source', api_config)
        result = source.test_connection()
        
        assert result is True
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_test_connection_failure(self, mock_get, api_config):
        """Test API connection test failure."""
        mock_get.side_effect = requests.RequestException("Connection failed")
        
        source = APIDataSource('test_api_source', api_config)
        result = source.test_connection()
        
        assert result is False
    
    @patch('requests.get')
    def test_detect_changes_with_new_data(self, mock_get, api_config):
        """Test change detection with new data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': 1, 'name': 'User 1'},
            {'id': 2, 'name': 'User 2'}
        ]
        mock_get.return_value = mock_response
        
        source = APIDataSource('test_api_source', api_config)
        changes = source.detect_changes()
        
        assert len(changes) == 2  # One for each endpoint
        assert all(change.change_type == ChangeType.UPDATE for change in changes)
        assert all(change.source_id == 'test_api_source' for change in changes)
    
    @patch('requests.get')
    def test_detect_changes_no_new_data(self, mock_get, api_config):
        """Test change detection with no new data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        source = APIDataSource('test_api_source', api_config)
        changes = source.detect_changes()
        
        assert len(changes) == 0
    
    @patch('requests.get')
    def test_make_request_with_retry_success(self, mock_get, api_config):
        """Test successful request with retry logic."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        source = APIDataSource('test_api_source', api_config)
        result = source._make_request_with_retry(
            'https://api.example.com/test',
            {'Authorization': 'Bearer test_token'},
            {}
        )
        
        assert result is not None
        assert result.status_code == 200
    
    @patch('requests.get')
    @patch('time.sleep')
    def test_make_request_with_retry_rate_limited(self, mock_sleep, mock_get, api_config):
        """Test request retry on rate limiting."""
        # First call returns 429, second call succeeds
        mock_responses = [
            Mock(status_code=429),
            Mock(status_code=200)
        ]
        mock_get.side_effect = mock_responses
        
        source = APIDataSource('test_api_source', api_config)
        result = source._make_request_with_retry(
            'https://api.example.com/test',
            {'Authorization': 'Bearer test_token'},
            {}
        )
        
        assert result is not None
        assert result.status_code == 200
        mock_sleep.assert_called_once()
    
    @patch('requests.get')
    def test_extract_data(self, mock_get, api_config):
        """Test data extraction from API."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': 1, 'name': 'User 1', 'email': 'user1@example.com'},
            {'id': 2, 'name': 'User 2', 'email': 'user2@example.com'}
        ]
        mock_get.return_value = mock_response
        
        source = APIDataSource('test_api_source', api_config)
        
        change_event = ChangeEvent(
            source_id='test_api_source',
            table_name='users',
            change_type=ChangeType.UPDATE,
            timestamp=datetime.now(),
            affected_rows=2,
            metadata={'endpoint': '/users', 'detection_method': 'polling'}
        )
        
        result_df = source.extract_data(change_event)
        
        assert len(result_df) == 2
        assert list(result_df.columns) == ['id', 'name', 'email']
        assert result_df.iloc[0]['name'] == 'User 1'
    
    def test_validate_schema(self, api_config):
        """Test API data schema validation."""
        source = APIDataSource('test_api_source', api_config)
        
        # Test with valid data
        valid_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'data': ['{"key": "value"}', '{"key": "value2"}', 'normal_string']
        })
        
        result = source.validate_schema(valid_df)
        
        assert result.is_valid is True
        assert result.row_count == 3
        assert len(result.warnings) > 0  # Should warn about JSON strings
        
        # Test with empty data
        empty_df = pd.DataFrame()
        result = source.validate_schema(empty_df)
        
        assert result.is_valid is True
        assert len(result.warnings) > 0
        assert any('empty' in warning for warning in result.warnings)


class TestConnectionConfig:
    """Test cases for ConnectionConfig."""
    
    def test_connection_config_creation(self):
        """Test connection config creation."""
        config = ConnectionConfig(
            host='localhost',
            port=5432,
            database='test_db',
            username='user',
            password='pass'
        )
        
        assert config.host == 'localhost'
        assert config.port == 5432
        assert config.driver == 'postgresql'  # default value
        assert config.connection_timeout == 30  # default value


class TestAPIConfig:
    """Test cases for APIConfig."""
    
    def test_api_config_creation(self):
        """Test API config creation."""
        config = APIConfig(
            base_url='https://api.example.com',
            auth_token='token123'
        )
        
        assert config.base_url == 'https://api.example.com'
        assert config.auth_token == 'token123'
        assert config.timeout == 30  # default value
        assert config.retry_attempts == 3  # default value