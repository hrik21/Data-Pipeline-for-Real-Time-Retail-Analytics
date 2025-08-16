"""
Unit tests for logging functionality.
"""
import pytest
import json
import logging
from io import StringIO
from unittest.mock import patch

from src.utils.logging import JSONFormatter, PipelineLogger, get_logger


class TestJSONFormatter:
    """Test JSON logging formatter."""
    
    def test_json_format_basic(self):
        """Test basic JSON formatting."""
        formatter = JSONFormatter()
        
        # Create a log record
        record = logging.LogRecord(
            name='test_logger',
            level=logging.INFO,
            pathname='/test/path.py',
            lineno=42,
            msg='Test message',
            args=(),
            exc_info=None
        )
        record.module = 'test_module'
        record.funcName = 'test_function'
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data['level'] == 'INFO'
        assert log_data['logger'] == 'test_logger'
        assert log_data['message'] == 'Test message'
        assert log_data['module'] == 'test_module'
        assert log_data['function'] == 'test_function'
        assert log_data['line'] == 42
        assert 'timestamp' in log_data
    
    def test_json_format_with_extra(self):
        """Test JSON formatting with extra fields."""
        formatter = JSONFormatter(include_extra=True)
        
        record = logging.LogRecord(
            name='test_logger',
            level=logging.INFO,
            pathname='/test/path.py',
            lineno=42,
            msg='Test message',
            args=(),
            exc_info=None
        )
        record.module = 'test_module'
        record.funcName = 'test_function'
        record.custom_field = 'custom_value'
        record.request_id = '12345'
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data['custom_field'] == 'custom_value'
        assert log_data['request_id'] == '12345'


class TestPipelineLogger:
    """Test pipeline logger functionality."""
    
    def test_context_management(self):
        """Test logging context management."""
        logger = PipelineLogger('test')
        
        # Set context
        logger.set_context(request_id='12345', user_id='user123')
        
        # Capture log output
        with patch.object(logger.logger, 'log') as mock_log:
            logger.info('Test message')
            
            # Verify context was included
            mock_log.assert_called_once()
            args, kwargs = mock_log.call_args
            assert kwargs['extra']['request_id'] == '12345'
            assert kwargs['extra']['user_id'] == 'user123'
    
    def test_context_override(self):
        """Test context override in individual log calls."""
        logger = PipelineLogger('test')
        logger.set_context(request_id='12345')
        
        with patch.object(logger.logger, 'log') as mock_log:
            logger.info('Test message', request_id='67890', task_id='task123')
            
            args, kwargs = mock_log.call_args
            assert kwargs['extra']['request_id'] == '67890'  # Overridden
            assert kwargs['extra']['task_id'] == 'task123'   # Added
    
    def test_clear_context(self):
        """Test clearing logging context."""
        logger = PipelineLogger('test')
        logger.set_context(request_id='12345')
        logger.clear_context()
        
        with patch.object(logger.logger, 'log') as mock_log:
            logger.info('Test message')
            
            args, kwargs = mock_log.call_args
            assert 'request_id' not in kwargs['extra']


def test_get_logger():
    """Test logger factory function."""
    logger1 = get_logger('test_logger')
    logger2 = get_logger('test_logger')
    
    # Should return the same instance
    assert logger1 is logger2
    assert isinstance(logger1, PipelineLogger)