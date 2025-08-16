"""
Unit tests for change detection system.
"""
import pytest
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from src.ingestion.change_detection import (
    MetadataStore, ChangeEventDeduplicator, TimestampBasedChangeDetector,
    LogBasedChangeDetector, PollingChangeDetector, MultiSourceChangeDetector,
    ChangeDetectionConfig
)
from src.interfaces.base import ChangeEvent, ChangeType, DataSource


class TestMetadataStore:
    """Test cases for MetadataStore."""
    
    @pytest.fixture
    def temp_store_path(self):
        """Create temporary directory for metadata store."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    def test_initialization(self, temp_store_path):
        """Test metadata store initialization."""
        store = MetadataStore(temp_store_path)
        
        assert store.store_path == Path(temp_store_path)
        assert store.store_path.exists()
    
    def test_update_and_get_timestamp(self, temp_store_path):
        """Test updating and retrieving timestamps."""
        store = MetadataStore(temp_store_path)
        
        timestamp = datetime.now()
        metadata = {'test_key': 'test_value'}
        
        # Update timestamp
        store.update_processed_timestamp('source1', 'table1', timestamp, metadata)
        
        # Retrieve timestamp
        retrieved_timestamp = store.get_last_processed_timestamp('source1', 'table1')
        
        assert retrieved_timestamp is not None
        # Allow for small time differences due to serialization
        assert abs((retrieved_timestamp - timestamp).total_seconds()) < 1
    
    def test_get_nonexistent_timestamp(self, temp_store_path):
        """Test retrieving timestamp for non-existent source/table."""
        store = MetadataStore(temp_store_path)
        
        timestamp = store.get_last_processed_timestamp('nonexistent', 'table')
        
        assert timestamp is None
    
    def test_get_all_metadata(self, temp_store_path):
        """Test retrieving all metadata for a source."""
        store = MetadataStore(temp_store_path)
        
        # Add metadata for multiple tables
        timestamp1 = datetime.now()
        timestamp2 = datetime.now() + timedelta(hours=1)
        
        store.update_processed_timestamp('source1', 'table1', timestamp1)
        store.update_processed_timestamp('source1', 'table2', timestamp2)
        
        # Retrieve all metadata
        all_metadata = store.get_all_metadata('source1')
        
        assert len(all_metadata) == 2
        assert 'table1' in all_metadata
        assert 'table2' in all_metadata
        assert all_metadata['table1']['source_id'] == 'source1'
    
    def test_concurrent_access(self, temp_store_path):
        """Test thread-safe access to metadata store."""
        import threading
        import time
        
        store = MetadataStore(temp_store_path)
        results = []
        
        def update_timestamp(source_id, table_name, delay):
            time.sleep(delay)
            timestamp = datetime.now()
            store.update_processed_timestamp(source_id, table_name, timestamp)
            results.append((source_id, table_name, timestamp))
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=update_timestamp,
                args=(f'source{i}', f'table{i}', 0.1)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        assert len(results) == 5
        
        # Verify all timestamps were stored
        for source_id, table_name, _ in results:
            retrieved = store.get_last_processed_timestamp(source_id, table_name)
            assert retrieved is not None


class TestChangeEventDeduplicator:
    """Test cases for ChangeEventDeduplicator."""
    
    def test_initialization(self):
        """Test deduplicator initialization."""
        deduplicator = ChangeEventDeduplicator(window_minutes=30)
        
        assert deduplicator.window_minutes == 30
        assert len(deduplicator._processed_events) == 0
    
    def test_duplicate_detection(self):
        """Test duplicate event detection."""
        deduplicator = ChangeEventDeduplicator()
        
        timestamp = datetime.now()
        event1 = ChangeEvent('source1', 'table1', ChangeType.UPDATE, timestamp, 5)
        event2 = ChangeEvent('source1', 'table1', ChangeType.UPDATE, timestamp, 5)
        
        # First event should not be duplicate
        assert not deduplicator.is_duplicate(event1)
        
        # Second identical event should be duplicate
        assert deduplicator.is_duplicate(event2)
    
    def test_different_events_not_duplicate(self):
        """Test that different events are not considered duplicates."""
        deduplicator = ChangeEventDeduplicator()
        
        timestamp = datetime.now()
        event1 = ChangeEvent('source1', 'table1', ChangeType.UPDATE, timestamp, 5)
        event2 = ChangeEvent('source1', 'table2', ChangeType.UPDATE, timestamp, 5)  # Different table
        event3 = ChangeEvent('source1', 'table1', ChangeType.INSERT, timestamp, 5)  # Different type
        
        assert not deduplicator.is_duplicate(event1)
        assert not deduplicator.is_duplicate(event2)
        assert not deduplicator.is_duplicate(event3)
    
    def test_cleanup_old_events(self):
        """Test cleanup of old events outside the window."""
        deduplicator = ChangeEventDeduplicator(window_minutes=1)  # 1 minute window
        
        # Add an event
        old_timestamp = datetime.now() - timedelta(minutes=2)
        event = ChangeEvent('source1', 'table1', ChangeType.UPDATE, old_timestamp, 5)
        
        # Manually add to processed events with old timestamp
        event_key = deduplicator._generate_event_key(event)
        deduplicator._processed_events.add(event_key)
        deduplicator._event_timestamps[event_key] = datetime.now() - timedelta(minutes=2)
        
        # Trigger cleanup by checking a new event
        new_event = ChangeEvent('source2', 'table2', ChangeType.UPDATE, datetime.now(), 3)
        deduplicator.is_duplicate(new_event)
        
        # Old event should be cleaned up
        assert event_key not in deduplicator._processed_events
        assert event_key not in deduplicator._event_timestamps


class TestTimestampBasedChangeDetector:
    """Test cases for TimestampBasedChangeDetector."""
    
    @pytest.fixture
    def temp_store_path(self):
        """Create temporary directory for metadata store."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def detector_config(self, temp_store_path):
        """Change detection configuration."""
        return ChangeDetectionConfig(
            method='timestamp',
            check_interval=300,
            batch_size=1000,
            watermark_column='updated_at',
            metadata_store_path=temp_store_path,
            enable_deduplication=True
        )
    
    @pytest.fixture
    def mock_source(self):
        """Mock data source."""
        source = Mock(spec=DataSource)
        source.source_id = 'test_source'
        source.tables = ['users', 'orders']
        source.change_detection_method = 'timestamp'
        return source
    
    def test_initialization(self, detector_config):
        """Test detector initialization."""
        detector = TimestampBasedChangeDetector(detector_config)
        
        assert detector.config == detector_config
        assert detector.metadata_store is not None
        assert detector.deduplicator is not None
    
    def test_detect_changes_with_new_data(self, detector_config, mock_source):
        """Test change detection with new data."""
        detector = TimestampBasedChangeDetector(detector_config)
        
        # Mock source to return changes
        change_event = ChangeEvent(
            'test_source', 'users', ChangeType.UPDATE, datetime.now(), 5
        )
        mock_source.detect_changes.return_value = [change_event]
        
        changes = detector.detect_changes(mock_source)
        
        assert len(changes) == 1
        assert changes[0] == change_event
    
    def test_detect_changes_with_deduplication(self, detector_config, mock_source):
        """Test change detection with deduplication."""
        detector = TimestampBasedChangeDetector(detector_config)
        
        # Create duplicate events
        timestamp = datetime.now()
        change_event = ChangeEvent('test_source', 'users', ChangeType.UPDATE, timestamp, 5)
        mock_source.detect_changes.return_value = [change_event, change_event]
        
        changes = detector.detect_changes(mock_source)
        
        # Should only return one event due to deduplication
        assert len(changes) == 1
    
    def test_get_last_processed_timestamp(self, detector_config, temp_store_path):
        """Test retrieving last processed timestamp."""
        detector = TimestampBasedChangeDetector(detector_config)
        
        # Set a timestamp
        timestamp = datetime.now()
        detector.update_processed_timestamp('test_source', timestamp, 'users')
        
        # Retrieve timestamp
        retrieved = detector.get_last_processed_timestamp('test_source', 'users')
        
        assert retrieved is not None
        assert abs((retrieved - timestamp).total_seconds()) < 1
    
    def test_update_processed_timestamp(self, detector_config):
        """Test updating processed timestamp."""
        detector = TimestampBasedChangeDetector(detector_config)
        
        timestamp = datetime.now()
        metadata = {'test': 'value'}
        
        # Should not raise exception
        detector.update_processed_timestamp('test_source', timestamp, 'users', metadata)
        
        # Verify timestamp was stored
        retrieved = detector.get_last_processed_timestamp('test_source', 'users')
        assert retrieved is not None


class TestLogBasedChangeDetector:
    """Test cases for LogBasedChangeDetector."""
    
    @pytest.fixture
    def temp_store_path(self):
        """Create temporary directory for metadata store."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def detector_config(self, temp_store_path):
        """Change detection configuration."""
        return ChangeDetectionConfig(
            method='log_based',
            check_interval=300,
            batch_size=1000,
            metadata_store_path=temp_store_path,
            enable_deduplication=True
        )
    
    @pytest.fixture
    def mock_source(self):
        """Mock data source."""
        source = Mock(spec=DataSource)
        source.source_id = 'test_source'
        return source
    
    def test_initialization(self, detector_config):
        """Test detector initialization."""
        detector = LogBasedChangeDetector(detector_config)
        
        assert detector.config == detector_config
        assert detector.metadata_store is not None
        assert detector.deduplicator is not None
    
    def test_detect_changes_log_based(self, detector_config, mock_source):
        """Test log-based change detection."""
        detector = LogBasedChangeDetector(detector_config)
        
        # Mock source to return log-based changes
        change_event = ChangeEvent(
            'test_source', 'users', ChangeType.INSERT, datetime.now(), 3,
            metadata={'detection_method': 'log_based'}
        )
        mock_source.detect_changes.return_value = [change_event]
        
        changes = detector.detect_changes(mock_source)
        
        assert len(changes) == 1
        assert changes[0] == change_event
    
    def test_detect_changes_filters_non_log_based(self, detector_config, mock_source):
        """Test that non-log-based changes are filtered out."""
        detector = LogBasedChangeDetector(detector_config)
        
        # Mock source to return mixed changes
        log_change = ChangeEvent(
            'test_source', 'users', ChangeType.INSERT, datetime.now(), 3,
            metadata={'detection_method': 'log_based'}
        )
        timestamp_change = ChangeEvent(
            'test_source', 'orders', ChangeType.UPDATE, datetime.now(), 5,
            metadata={'detection_method': 'timestamp'}
        )
        mock_source.detect_changes.return_value = [log_change, timestamp_change]
        
        changes = detector.detect_changes(mock_source)
        
        # Should only return log-based change
        assert len(changes) == 1
        assert changes[0] == log_change


class TestPollingChangeDetector:
    """Test cases for PollingChangeDetector."""
    
    @pytest.fixture
    def temp_store_path(self):
        """Create temporary directory for metadata store."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def detector_config(self, temp_store_path):
        """Change detection configuration."""
        return ChangeDetectionConfig(
            method='polling',
            check_interval=60,  # 1 minute
            batch_size=1000,
            metadata_store_path=temp_store_path,
            enable_deduplication=True
        )
    
    @pytest.fixture
    def mock_source(self):
        """Mock data source."""
        source = Mock(spec=DataSource)
        source.source_id = 'test_source'
        return source
    
    def test_initialization(self, detector_config):
        """Test detector initialization."""
        detector = PollingChangeDetector(detector_config)
        
        assert detector.config == detector_config
        assert detector.metadata_store is not None
        assert detector.deduplicator is not None
        assert len(detector.last_poll_times) == 0
    
    def test_detect_changes_first_poll(self, detector_config, mock_source):
        """Test change detection on first poll."""
        detector = PollingChangeDetector(detector_config)
        
        # Mock source to return polling-based changes
        change_event = ChangeEvent(
            'test_source', 'users', ChangeType.UPDATE, datetime.now(), 5,
            metadata={'detection_method': 'polling'}
        )
        mock_source.detect_changes.return_value = [change_event]
        
        changes = detector.detect_changes(mock_source)
        
        assert len(changes) == 1
        assert changes[0] == change_event
        assert 'test_source' in detector.last_poll_times
    
    def test_detect_changes_skip_recent_poll(self, detector_config, mock_source):
        """Test that recent polls are skipped."""
        detector = PollingChangeDetector(detector_config)
        
        # Set recent poll time
        detector.last_poll_times['test_source'] = datetime.now() - timedelta(seconds=30)
        
        changes = detector.detect_changes(mock_source)
        
        # Should skip polling and return no changes
        assert len(changes) == 0
        mock_source.detect_changes.assert_not_called()
    
    def test_detect_changes_after_interval(self, detector_config, mock_source):
        """Test change detection after polling interval."""
        detector = PollingChangeDetector(detector_config)
        
        # Set old poll time (beyond interval)
        detector.last_poll_times['test_source'] = datetime.now() - timedelta(seconds=120)
        
        # Mock source to return changes
        change_event = ChangeEvent(
            'test_source', 'users', ChangeType.UPDATE, datetime.now(), 5,
            metadata={'detection_method': 'polling'}
        )
        mock_source.detect_changes.return_value = [change_event]
        
        changes = detector.detect_changes(mock_source)
        
        assert len(changes) == 1
        mock_source.detect_changes.assert_called_once()


class TestMultiSourceChangeDetector:
    """Test cases for MultiSourceChangeDetector."""
    
    @pytest.fixture
    def temp_store_path(self):
        """Create temporary directory for metadata store."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def detector_config(self, temp_store_path):
        """Multi-source detector configuration."""
        return {
            'change_detection': {
                'method': 'timestamp',
                'check_interval': 300,
                'batch_size': 1000,
                'metadata_store_path': temp_store_path,
                'max_concurrent_sources': 3,
                'enable_deduplication': True
            }
        }
    
    @pytest.fixture
    def mock_sources(self):
        """Mock data sources."""
        sources = []
        for i in range(3):
            source = Mock(spec=DataSource)
            source.source_id = f'source_{i}'
            source.change_detection_method = 'timestamp'
            source.test_connection.return_value = True
            sources.append(source)
        return sources
    
    def test_initialization(self, detector_config):
        """Test multi-source detector initialization."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        assert detector.component_id == 'multi_detector'
        assert len(detector.detectors) == 3  # timestamp, log_based, polling
        assert len(detector.sources) == 0
    
    def test_register_source(self, detector_config, mock_sources):
        """Test source registration."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        for source in mock_sources:
            detector.register_source(source)
        
        assert len(detector.sources) == 3
        assert all(source.source_id in detector.sources for source in mock_sources)
    
    def test_unregister_source(self, detector_config, mock_sources):
        """Test source unregistration."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        # Unregister one source
        detector.unregister_source('source_1')
        
        assert len(detector.sources) == 2
        assert 'source_1' not in detector.sources
    
    def test_initialize_success(self, detector_config, mock_sources):
        """Test successful initialization."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        result = detector.initialize()
        
        assert result is True
        for source in mock_sources:
            source.test_connection.assert_called_once()
    
    def test_initialize_failure(self, detector_config, mock_sources):
        """Test initialization failure."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Make one source fail connection test
        mock_sources[0].test_connection.return_value = False
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        result = detector.initialize()
        
        assert result is False
    
    def test_detect_all_changes(self, detector_config, mock_sources):
        """Test detecting changes from all sources."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        # Mock change detection for each source
        for i, source in enumerate(mock_sources):
            change_event = ChangeEvent(
                source.source_id, f'table_{i}', ChangeType.UPDATE, datetime.now(), i + 1
            )
            source.detect_changes.return_value = [change_event]
        
        # Mock the detector's _detect_source_changes method
        with patch.object(detector, '_detect_source_changes') as mock_detect:
            mock_detect.side_effect = lambda source: source.detect_changes()
            
            changes = detector.detect_all_changes()
        
        assert len(changes) == 3
        assert all(isinstance(change, ChangeEvent) for change in changes)
    
    def test_get_source_status(self, detector_config, mock_sources):
        """Test getting source status information."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        status = detector.get_source_status()
        
        assert len(status) == 3
        for source_id, source_status in status.items():
            assert 'healthy' in source_status
            assert 'detection_method' in source_status
            assert 'source_type' in source_status
    
    def test_health_check(self, detector_config, mock_sources):
        """Test component health check."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        result = detector.health_check()
        
        assert result is True
        
        # Test with failed connections
        for source in mock_sources:
            source.test_connection.return_value = False
        
        result = detector.health_check()
        
        assert result is False
    
    def test_cleanup(self, detector_config, mock_sources):
        """Test component cleanup."""
        detector = MultiSourceChangeDetector('multi_detector', detector_config)
        
        # Register sources
        for source in mock_sources:
            detector.register_source(source)
        
        detector.cleanup()
        
        assert len(detector.sources) == 0
        assert len(detector.detectors) == 0