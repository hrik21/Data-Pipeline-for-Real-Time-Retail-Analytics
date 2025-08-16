"""
Airflow orchestration framework for data pipelines.

This module provides components for dynamic DAG generation, custom operators,
sensors for change-based triggering, and task dependency management.
"""

from .dag_generator import DynamicDAGGenerator, PipelineConfig, TaskConfig
from .operators import (
    IngestionOperator,
    TransformationOperator,
    ValidationOperator,
    SnowflakeOperator,
    DataQualityOperator
)
from .sensors import (
    DataSourceChangeSensor,
    TableChangeSensor,
    FileSystemChangeSensor,
    APIChangeSensor,
    CustomChangeSensor
)
from .task_manager import (
    TaskManager,
    ScheduleManager,
    TaskDependency,
    DependencyType,
    ScheduleType
)

__all__ = [
    # DAG Generation
    'DynamicDAGGenerator',
    'PipelineConfig',
    'TaskConfig',
    
    # Operators
    'IngestionOperator',
    'TransformationOperator',
    'ValidationOperator',
    'SnowflakeOperator',
    'DataQualityOperator',
    
    # Sensors
    'DataSourceChangeSensor',
    'TableChangeSensor',
    'FileSystemChangeSensor',
    'APIChangeSensor',
    'CustomChangeSensor',
    
    # Task Management
    'TaskManager',
    'ScheduleManager',
    'TaskDependency',
    'DependencyType',
    'ScheduleType',
]