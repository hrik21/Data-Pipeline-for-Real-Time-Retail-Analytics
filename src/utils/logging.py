"""
Structured JSON logging configuration for the data pipeline.
"""
import logging
import logging.handlers
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from src.config.settings import get_settings


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields if enabled
        if self.include_extra:
            for key, value in record.__dict__.items():
                if key not in ('name', 'msg', 'args', 'levelname', 'levelno', 
                              'pathname', 'filename', 'module', 'lineno', 
                              'funcName', 'created', 'msecs', 'relativeCreated',
                              'thread', 'threadName', 'processName', 'process',
                              'getMessage', 'exc_info', 'exc_text', 'stack_info'):
                    log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


class PipelineLogger:
    """Pipeline-specific logger with context management."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs) -> None:
        """Set logging context that will be included in all log messages."""
        self._context.update(kwargs)
    
    def clear_context(self) -> None:
        """Clear logging context."""
        self._context.clear()
    
    def _log_with_context(self, level: int, message: str, **kwargs) -> None:
        """Log message with context."""
        extra = {**self._context, **kwargs}
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log_with_context(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self._log_with_context(logging.CRITICAL, message, **kwargs)
    
    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        extra = {**self._context, **kwargs}
        self.logger.exception(message, extra=extra)


class LoggingManager:
    """Manages logging configuration for the pipeline."""
    
    def __init__(self):
        self._configured = False
        self._loggers: Dict[str, PipelineLogger] = {}
    
    def configure_logging(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Configure logging based on settings."""
        if self._configured:
            return
        
        if config is None:
            settings = get_settings()
            config = {
                'level': settings.logging.level,
                'format': settings.logging.format,
                'file_path': settings.logging.file_path,
                'max_file_size': settings.logging.max_file_size,
                'backup_count': settings.logging.backup_count,
            }
        
        # Set root logger level
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, config['level'].upper()))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Configure formatters
        if config['format'].lower() == 'json':
            formatter = JSONFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # File handler (if specified)
        if config.get('file_path'):
            file_path = Path(config['file_path'])
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Parse max file size
            max_bytes = self._parse_file_size(config.get('max_file_size', '10MB'))
            
            file_handler = logging.handlers.RotatingFileHandler(
                filename=file_path,
                maxBytes=max_bytes,
                backupCount=config.get('backup_count', 5)
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        
        # Suppress noisy third-party loggers
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('snowflake').setLevel(logging.WARNING)
        
        self._configured = True
    
    def get_logger(self, name: str) -> PipelineLogger:
        """Get or create a pipeline logger."""
        if not self._configured:
            self.configure_logging()
        
        if name not in self._loggers:
            self._loggers[name] = PipelineLogger(name)
        
        return self._loggers[name]
    
    def _parse_file_size(self, size_str: str) -> int:
        """Parse file size string to bytes."""
        size_str = size_str.upper().strip()
        
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)


# Global logging manager
_logging_manager = LoggingManager()


def configure_logging(config: Optional[Dict[str, Any]] = None) -> None:
    """Configure logging for the pipeline."""
    _logging_manager.configure_logging(config)


def get_logger(name: str) -> PipelineLogger:
    """Get a pipeline logger instance."""
    return _logging_manager.get_logger(name)