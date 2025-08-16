"""
Change detection system for monitoring data source changes.
"""
import json
import pickle
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.interfaces.base import ChangeDetector, DataSource, ChangeEvent, ChangeType, PipelineComponent


@dataclass
class ChangeDetectionConfig:
    """Configuration for change detection."""
    method: str  # timestamp, log_based, polling
    check_interval: int  # seconds
    batch_size: int
    watermark_column: str = "updated_at"
    metadata_store_path: str = "data/metadata"
    max_concurrent_sources: int = 5
    enable_deduplication: bool = True


class MetadataStore:
    """Simple file-based metadata store for tracking processed timestamps."""
    
    def __init__(self, store_path: str):
        self.store_path = Path(store_path)
        self.store_path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
    
    def get_last_processed_timestamp(self, source_id: str, table_name: str) -> Optional[datetime]:
        """Get last processed timestamp for a source table."""
        metadata_file = self.store_path / f"{source_id}_{table_name}_metadata.json"
        
        try:
            with self._lock:
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                        timestamp_str = metadata.get('last_processed_timestamp')
                        if timestamp_str:
                            return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            # Log error but don't fail - return None to trigger full scan
            pass
        
        return None
    
    def update_processed_timestamp(self, source_id: str, table_name: str, 
                                 timestamp: datetime, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Update last processed timestamp for a source table."""
        metadata_file = self.store_path / f"{source_id}_{table_name}_metadata.json"
        
        data = {
            'source_id': source_id,
            'table_name': table_name,
            'last_processed_timestamp': timestamp.isoformat(),
            'updated_at': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        try:
            with self._lock:
                with open(metadata_file, 'w') as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            # Log error but don't fail the pipeline
            pass
    
    def get_all_metadata(self, source_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all metadata for a source."""
        metadata = {}
        
        try:
            with self._lock:
                for metadata_file in self.store_path.glob(f"{source_id}_*_metadata.json"):
                    with open(metadata_file, 'r') as f:
                        data = json.load(f)
                        table_name = data.get('table_name')
                        if table_name:
                            metadata[table_name] = data
        except Exception as e:
            pass
        
        return metadata


class ChangeEventDeduplicator:
    """Deduplicates change events to avoid processing the same changes multiple times."""
    
    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes
        self._processed_events: Set[str] = set()
        self._event_timestamps: Dict[str, datetime] = {}
        self._lock = threading.Lock()
    
    def _generate_event_key(self, event: ChangeEvent) -> str:
        """Generate unique key for change event."""
        return f"{event.source_id}:{event.table_name}:{event.change_type.value}:{event.timestamp.isoformat()}"
    
    def is_duplicate(self, event: ChangeEvent) -> bool:
        """Check if event is a duplicate."""
        event_key = self._generate_event_key(event)
        
        with self._lock:
            # Clean up old events outside the window
            self._cleanup_old_events()
            
            if event_key in self._processed_events:
                return True
            
            # Mark as processed
            self._processed_events.add(event_key)
            self._event_timestamps[event_key] = datetime.now()
            
            return False
    
    def _cleanup_old_events(self):
        """Remove events older than the deduplication window."""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_minutes)
        
        expired_keys = [
            key for key, timestamp in self._event_timestamps.items()
            if timestamp < cutoff_time
        ]
        
        for key in expired_keys:
            self._processed_events.discard(key)
            self._event_timestamps.pop(key, None)


class TimestampBasedChangeDetector(ChangeDetector):
    """Timestamp-based change detection implementation."""
    
    def __init__(self, config: ChangeDetectionConfig):
        self.config = config
        self.metadata_store = MetadataStore(config.metadata_store_path)
        self.deduplicator = ChangeEventDeduplicator() if config.enable_deduplication else None
    
    def detect_changes(self, source: DataSource) -> List[ChangeEvent]:
        """Detect changes using timestamp-based approach."""
        changes = []
        
        # Get tables to monitor from source configuration
        tables = getattr(source, 'tables', [])
        if not tables:
            return changes
        
        for table_name in tables:
            try:
                table_changes = self._detect_table_changes(source, table_name)
                changes.extend(table_changes)
            except Exception as e:
                # Log error but continue with other tables
                continue
        
        # Apply deduplication if enabled
        if self.deduplicator:
            changes = [event for event in changes if not self.deduplicator.is_duplicate(event)]
        
        return changes
    
    def _detect_table_changes(self, source: DataSource, table_name: str) -> List[ChangeEvent]:
        """Detect changes for a specific table."""
        changes = []
        
        # Get last processed timestamp
        last_timestamp = self.get_last_processed_timestamp(source.source_id, table_name)
        
        # If no last timestamp, use a default lookback period
        if last_timestamp is None:
            last_timestamp = datetime.now() - timedelta(hours=24)
        
        try:
            # Use source's change detection method
            source_changes = source.detect_changes()
            
            # Filter changes for this specific table and timestamp
            for change in source_changes:
                if (change.table_name == table_name and 
                    change.timestamp > last_timestamp):
                    changes.append(change)
            
            # Update processed timestamp if we found changes
            if changes:
                max_timestamp = max(change.timestamp for change in changes)
                self.update_processed_timestamp(source.source_id, table_name, max_timestamp)
        
        except Exception as e:
            # Log error but don't fail
            pass
        
        return changes
    
    def get_last_processed_timestamp(self, source_id: str, table_name: str = None) -> Optional[datetime]:
        """Get last processed timestamp for source."""
        if table_name:
            return self.metadata_store.get_last_processed_timestamp(source_id, table_name)
        else:
            # Get the minimum timestamp across all tables for this source
            all_metadata = self.metadata_store.get_all_metadata(source_id)
            if not all_metadata:
                return None
            
            timestamps = []
            for table_data in all_metadata.values():
                timestamp_str = table_data.get('last_processed_timestamp')
                if timestamp_str:
                    timestamps.append(datetime.fromisoformat(timestamp_str))
            
            return min(timestamps) if timestamps else None
    
    def update_processed_timestamp(self, source_id: str, timestamp: datetime, 
                                 table_name: str = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Update last processed timestamp."""
        if table_name:
            self.metadata_store.update_processed_timestamp(source_id, table_name, timestamp, metadata)
        else:
            # Update all tables for this source
            all_metadata = self.metadata_store.get_all_metadata(source_id)
            for table_name in all_metadata.keys():
                self.metadata_store.update_processed_timestamp(source_id, table_name, timestamp, metadata)


class LogBasedChangeDetector(ChangeDetector):
    """Log-based change detection implementation."""
    
    def __init__(self, config: ChangeDetectionConfig):
        self.config = config
        self.metadata_store = MetadataStore(config.metadata_store_path)
        self.deduplicator = ChangeEventDeduplicator() if config.enable_deduplication else None
    
    def detect_changes(self, source: DataSource) -> List[ChangeEvent]:
        """Detect changes using log-based CDC approach."""
        changes = []
        
        try:
            # Use source's built-in change detection
            source_changes = source.detect_changes()
            
            # Process and filter changes
            for change in source_changes:
                if change.metadata and change.metadata.get('detection_method') == 'log_based':
                    changes.append(change)
            
            # Apply deduplication if enabled
            if self.deduplicator:
                changes = [event for event in changes if not self.deduplicator.is_duplicate(event)]
            
            # Update metadata for processed changes
            for change in changes:
                self.update_processed_timestamp(
                    change.source_id, 
                    change.timestamp,
                    change.table_name,
                    change.metadata
                )
        
        except Exception as e:
            # Log error but don't fail
            pass
        
        return changes
    
    def get_last_processed_timestamp(self, source_id: str, table_name: str = None) -> Optional[datetime]:
        """Get last processed timestamp for source."""
        if table_name:
            return self.metadata_store.get_last_processed_timestamp(source_id, table_name)
        else:
            all_metadata = self.metadata_store.get_all_metadata(source_id)
            if not all_metadata:
                return None
            
            timestamps = []
            for table_data in all_metadata.values():
                timestamp_str = table_data.get('last_processed_timestamp')
                if timestamp_str:
                    timestamps.append(datetime.fromisoformat(timestamp_str))
            
            return min(timestamps) if timestamps else None
    
    def update_processed_timestamp(self, source_id: str, timestamp: datetime,
                                 table_name: str = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Update last processed timestamp."""
        if table_name:
            self.metadata_store.update_processed_timestamp(source_id, table_name, timestamp, metadata)


class PollingChangeDetector(ChangeDetector):
    """Polling-based change detection for APIs and other sources."""
    
    def __init__(self, config: ChangeDetectionConfig):
        self.config = config
        self.metadata_store = MetadataStore(config.metadata_store_path)
        self.deduplicator = ChangeEventDeduplicator() if config.enable_deduplication else None
        self.last_poll_times: Dict[str, datetime] = {}
    
    def detect_changes(self, source: DataSource) -> List[ChangeEvent]:
        """Detect changes using polling approach."""
        changes = []
        current_time = datetime.now()
        
        # Check if it's time to poll this source
        source_key = f"{source.source_id}"
        last_poll = self.last_poll_times.get(source_key)
        
        if last_poll and (current_time - last_poll).seconds < self.config.check_interval:
            return changes
        
        try:
            # Use source's change detection
            source_changes = source.detect_changes()
            
            # Filter for polling-based changes
            for change in source_changes:
                if (change.metadata and 
                    change.metadata.get('detection_method') == 'polling'):
                    changes.append(change)
            
            # Apply deduplication if enabled
            if self.deduplicator:
                changes = [event for event in changes if not self.deduplicator.is_duplicate(event)]
            
            # Update last poll time
            self.last_poll_times[source_key] = current_time
            
            # Update metadata for processed changes
            for change in changes:
                self.update_processed_timestamp(
                    change.source_id,
                    change.timestamp,
                    change.table_name,
                    change.metadata
                )
        
        except Exception as e:
            # Log error but don't fail
            pass
        
        return changes
    
    def get_last_processed_timestamp(self, source_id: str, table_name: str = None) -> Optional[datetime]:
        """Get last processed timestamp for source."""
        if table_name:
            return self.metadata_store.get_last_processed_timestamp(source_id, table_name)
        else:
            source_key = f"{source_id}"
            return self.last_poll_times.get(source_key)
    
    def update_processed_timestamp(self, source_id: str, timestamp: datetime,
                                 table_name: str = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Update last processed timestamp."""
        if table_name:
            self.metadata_store.update_processed_timestamp(source_id, table_name, timestamp, metadata)
        
        # Also update in-memory poll time
        source_key = f"{source_id}"
        self.last_poll_times[source_key] = timestamp


class MultiSourceChangeDetector(PipelineComponent):
    """Orchestrates change detection across multiple data sources."""
    
    def __init__(self, component_id: str, config: Dict[str, Any]):
        super().__init__(component_id, config)
        self.detection_config = ChangeDetectionConfig(**config.get('change_detection', {}))
        self.sources: Dict[str, DataSource] = {}
        self.detectors: Dict[str, ChangeDetector] = {}
        self._initialize_detectors()
    
    def _initialize_detectors(self):
        """Initialize change detectors for different methods."""
        self.detectors = {
            'timestamp': TimestampBasedChangeDetector(self.detection_config),
            'log_based': LogBasedChangeDetector(self.detection_config),
            'polling': PollingChangeDetector(self.detection_config)
        }
    
    def initialize(self) -> bool:
        """Initialize the component."""
        try:
            # Test all registered sources
            for source_id, source in self.sources.items():
                if not source.test_connection():
                    self.logger.error(f"Failed to connect to source {source_id}")
                    return False
            
            self.logger.info(f"MultiSourceChangeDetector initialized with {len(self.sources)} sources")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize MultiSourceChangeDetector: {e}")
            return False
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        self.sources.clear()
        self.detectors.clear()
    
    def health_check(self) -> bool:
        """Check component health."""
        try:
            # Test a sample of sources
            healthy_sources = 0
            for source_id, source in list(self.sources.items())[:3]:  # Test first 3 sources
                if source.test_connection():
                    healthy_sources += 1
            
            return healthy_sources > 0
        except Exception:
            return False
    
    def register_source(self, source: DataSource) -> None:
        """Register a data source for change detection."""
        self.sources[source.source_id] = source
        self.logger.info(f"Registered data source: {source.source_id}")
    
    def unregister_source(self, source_id: str) -> None:
        """Unregister a data source."""
        if source_id in self.sources:
            del self.sources[source_id]
            self.logger.info(f"Unregistered data source: {source_id}")
    
    def detect_all_changes(self) -> List[ChangeEvent]:
        """Detect changes across all registered sources."""
        all_changes = []
        
        # Use thread pool for concurrent change detection
        with ThreadPoolExecutor(max_workers=self.detection_config.max_concurrent_sources) as executor:
            # Submit change detection tasks
            future_to_source = {
                executor.submit(self._detect_source_changes, source): source_id
                for source_id, source in self.sources.items()
            }
            
            # Collect results
            for future in as_completed(future_to_source):
                source_id = future_to_source[future]
                try:
                    changes = future.result()
                    all_changes.extend(changes)
                    self.logger.info(f"Detected {len(changes)} changes from source {source_id}")
                except Exception as e:
                    self.logger.error(f"Error detecting changes from source {source_id}: {e}")
        
        return all_changes
    
    def _detect_source_changes(self, source: DataSource) -> List[ChangeEvent]:
        """Detect changes for a single source."""
        changes = []
        
        try:
            # Determine detection method from source configuration
            detection_method = getattr(source, 'change_detection_method', 'timestamp')
            
            # Get appropriate detector
            detector = self.detectors.get(detection_method)
            if not detector:
                self.logger.warning(f"Unknown detection method: {detection_method}")
                return changes
            
            # Detect changes
            changes = detector.detect_changes(source)
            
        except Exception as e:
            self.logger.error(f"Error in change detection for source {source.source_id}: {e}")
        
        return changes
    
    def get_source_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status information for all sources."""
        status = {}
        
        for source_id, source in self.sources.items():
            try:
                is_healthy = source.test_connection()
                detection_method = getattr(source, 'change_detection_method', 'timestamp')
                detector = self.detectors.get(detection_method)
                
                last_processed = None
                if detector:
                    last_processed = detector.get_last_processed_timestamp(source_id)
                
                status[source_id] = {
                    'healthy': is_healthy,
                    'detection_method': detection_method,
                    'last_processed': last_processed.isoformat() if last_processed else None,
                    'source_type': source.__class__.__name__
                }
            except Exception as e:
                status[source_id] = {
                    'healthy': False,
                    'error': str(e),
                    'source_type': source.__class__.__name__
                }
        
        return status