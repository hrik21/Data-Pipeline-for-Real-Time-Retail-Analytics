"""
Data ingestion module for the real-time data pipeline.
"""

from .extractors import DataExtractorImpl, DataSerializer, ExtractionConfig, ExtractionMode, SerializationFormat
from .validators import IngestionValidator, ValidationRule, SchemaDefinition, ValidationReport, ValidationSeverity, DataType
from .data_sources import DatabaseDataSource, APIDataSource
from .change_detection import MultiSourceChangeDetector, TimestampBasedChangeDetector, LogBasedChangeDetector, PollingChangeDetector

__all__ = [
    'DataExtractorImpl',
    'DataSerializer', 
    'ExtractionConfig',
    'ExtractionMode',
    'SerializationFormat',
    'IngestionValidator',
    'ValidationRule',
    'SchemaDefinition', 
    'ValidationReport',
    'ValidationSeverity',
    'DataType',
    'DatabaseDataSource',
    'APIDataSource',
    'MultiSourceChangeDetector',
    'TimestampBasedChangeDetector',
    'LogBasedChangeDetector',
    'PollingChangeDetector'
]