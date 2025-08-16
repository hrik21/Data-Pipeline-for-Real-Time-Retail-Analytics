"""
Base interfaces and abstract classes for data pipeline components.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import pandas as pd


class ChangeType(Enum):
    """Types of data changes."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"


@dataclass
class ChangeEvent:
    """Represents a data change event."""
    source_id: str
    table_name: str
    change_type: ChangeType
    timestamp: datetime
    affected_rows: int
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    row_count: int
    failed_rows: int = 0


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime]
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class LoadResult:
    """Result of data loading operation."""
    table_name: str
    rows_loaded: int
    rows_failed: int
    execution_time: float
    error_message: Optional[str] = None


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    def __init__(self, source_id: str, config: Dict[str, Any]):
        self.source_id = source_id
        self.config = config
    
    @abstractmethod
    def detect_changes(self) -> List[ChangeEvent]:
        """Detect changes in the data source."""
        pass
    
    @abstractmethod
    def extract_data(self, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract data based on change event."""
        pass
    
    @abstractmethod
    def validate_schema(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data schema and quality."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to the data source."""
        pass


class DataExtractor(ABC):
    """Abstract base class for data extractors."""
    
    @abstractmethod
    def extract(self, source: DataSource, change_event: ChangeEvent) -> pd.DataFrame:
        """Extract data from source."""
        pass
    
    @abstractmethod
    def validate_extraction(self, data: pd.DataFrame) -> ValidationResult:
        """Validate extracted data."""
        pass


class DataLoader(ABC):
    """Abstract base class for data loaders."""
    
    @abstractmethod
    def load(self, data: pd.DataFrame, target_table: str, mode: str = "append") -> LoadResult:
        """Load data to target destination."""
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create table with specified schema."""
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        pass


class TaskExecutor(ABC):
    """Abstract base class for task executors."""
    
    @abstractmethod
    def execute_task(self, task_config: Dict[str, Any]) -> TaskResult:
        """Execute a pipeline task."""
        pass
    
    @abstractmethod
    def handle_failure(self, task_result: TaskResult) -> bool:
        """Handle task failure and determine if retry is needed."""
        pass


class ChangeDetector(ABC):
    """Abstract base class for change detection."""
    
    @abstractmethod
    def detect_changes(self, source: DataSource) -> List[ChangeEvent]:
        """Detect changes in data source."""
        pass
    
    @abstractmethod
    def get_last_processed_timestamp(self, source_id: str) -> Optional[datetime]:
        """Get last processed timestamp for source."""
        pass
    
    @abstractmethod
    def update_processed_timestamp(self, source_id: str, timestamp: datetime) -> None:
        """Update last processed timestamp."""
        pass


class PipelineComponent(ABC):
    """Base class for all pipeline components."""
    
    def __init__(self, component_id: str, config: Dict[str, Any]):
        self.component_id = component_id
        self.config = config
        self._logger = None
    
    @property
    def logger(self):
        """Get logger instance."""
        if self._logger is None:
            from src.utils.logging import get_logger
            self._logger = get_logger(self.__class__.__name__)
        return self._logger
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the component."""
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check component health."""
        pass