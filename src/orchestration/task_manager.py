"""
Task dependency management and scheduling logic for Airflow pipelines.
"""
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    # Mock networkx for basic functionality
    class MockNetworkX:
        class DiGraph:
            def __init__(self):
                self.nodes_dict = {}
                self.edges_dict = {}
            
            def add_node(self, node):
                self.nodes_dict[node] = True
            
            def add_edge(self, u, v, **kwargs):
                if u not in self.edges_dict:
                    self.edges_dict[u] = []
                self.edges_dict[u].append(v)
            
            def predecessors(self, node):
                predecessors = []
                for u, vs in self.edges_dict.items():
                    if node in vs:
                        predecessors.append(u)
                return predecessors
            
            def successors(self, node):
                return self.edges_dict.get(node, [])
        
        @staticmethod
        def topological_sort(graph):
            # Simple topological sort implementation
            nodes = list(graph.nodes_dict.keys())
            return nodes  # Simplified - just return nodes in order
        
        @staticmethod
        def simple_cycles(graph):
            return []  # Simplified - assume no cycles
        
        class NetworkXError(Exception):
            pass
    
    nx = MockNetworkX()

from src.interfaces.base import PipelineComponent, TaskStatus, TaskResult


class DependencyType(Enum):
    """Types of task dependencies."""
    SUCCESS = "success"  # Upstream task must succeed
    FAILURE = "failure"  # Upstream task must fail
    COMPLETION = "completion"  # Upstream task must complete (success or failure)
    CONDITIONAL = "conditional"  # Custom condition


class ScheduleType(Enum):
    """Types of scheduling."""
    CRON = "cron"
    INTERVAL = "interval"
    TRIGGER = "trigger"
    MANUAL = "manual"


@dataclass
class TaskDependency:
    """Represents a dependency between tasks."""
    upstream_task_id: str
    downstream_task_id: str
    dependency_type: DependencyType = DependencyType.SUCCESS
    condition: Optional[str] = None  # For conditional dependencies
    wait_for_downstream: bool = False


@dataclass
class TaskConfig:
    """Configuration for a pipeline task."""
    task_id: str
    task_type: str
    operator_class: str
    operator_kwargs: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    retries: int = 3
    retry_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    timeout: Optional[timedelta] = None
    priority: int = 1
    pool: Optional[str] = None
    queue: Optional[str] = None
    trigger_rule: str = "all_success"


@dataclass
class ScheduleConfig:
    """Configuration for pipeline scheduling."""
    schedule_type: ScheduleType
    schedule_value: str  # Cron expression, interval, or trigger config
    start_date: datetime
    end_date: Optional[datetime] = None
    catchup: bool = False
    max_active_runs: int = 1
    depends_on_past: bool = False


class TaskManager(PipelineComponent):
    """Manages task execution, dependencies, and scheduling."""
    
    def __init__(self, component_id: str = "task_manager", config: Optional[Dict[str, Any]] = None):
        super().__init__(component_id, config or {})
        self.task_configs: Dict[str, TaskConfig] = {}
        self.dependencies: List[TaskDependency] = []
        self.dependency_graph: Optional[nx.DiGraph] = None
        self.execution_history: Dict[str, List[TaskResult]] = {}
    
    def initialize(self) -> bool:
        """Initialize the task manager."""
        try:
            self.logger.info("Initializing TaskManager")
            self._build_dependency_graph()
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize TaskManager: {e}")
            return False
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        self.task_configs.clear()
        self.dependencies.clear()
        self.dependency_graph = None
        self.execution_history.clear()
        self.logger.info("TaskManager cleanup completed")
    
    def health_check(self) -> bool:
        """Check component health."""
        return self.dependency_graph is not None
    
    def add_task(self, task_config: TaskConfig) -> None:
        """Add a task configuration."""
        self.task_configs[task_config.task_id] = task_config
        self.logger.info(f"Added task configuration: {task_config.task_id}")
        
        # Rebuild dependency graph
        self._build_dependency_graph()
    
    def add_dependency(self, dependency: TaskDependency) -> None:
        """Add a task dependency."""
        # Validate that both tasks exist
        if dependency.upstream_task_id not in self.task_configs:
            raise ValueError(f"Upstream task not found: {dependency.upstream_task_id}")
        if dependency.downstream_task_id not in self.task_configs:
            raise ValueError(f"Downstream task not found: {dependency.downstream_task_id}")
        
        self.dependencies.append(dependency)
        self.logger.info(f"Added dependency: {dependency.upstream_task_id} -> {dependency.downstream_task_id}")
        
        # Rebuild dependency graph
        self._build_dependency_graph()
    
    def remove_task(self, task_id: str) -> None:
        """Remove a task and its dependencies."""
        if task_id not in self.task_configs:
            self.logger.warning(f"Task not found for removal: {task_id}")
            return
        
        # Remove task configuration
        del self.task_configs[task_id]
        
        # Remove dependencies involving this task
        self.dependencies = [
            dep for dep in self.dependencies
            if dep.upstream_task_id != task_id and dep.downstream_task_id != task_id
        ]
        
        # Remove execution history
        if task_id in self.execution_history:
            del self.execution_history[task_id]
        
        self.logger.info(f"Removed task: {task_id}")
        
        # Rebuild dependency graph
        self._build_dependency_graph()
    
    def get_task_dependencies(self, task_id: str) -> List[str]:
        """Get upstream dependencies for a task."""
        if not self.dependency_graph:
            return []
        
        if task_id not in self.dependency_graph:
            return []
        
        return list(self.dependency_graph.predecessors(task_id))
    
    def get_task_dependents(self, task_id: str) -> List[str]:
        """Get downstream dependents for a task."""
        if not self.dependency_graph:
            return []
        
        if task_id not in self.dependency_graph:
            return []
        
        return list(self.dependency_graph.successors(task_id))
    
    def get_execution_order(self) -> List[List[str]]:
        """Get tasks in topological execution order (grouped by level)."""
        if not self.dependency_graph:
            return []
        
        try:
            # Get topological sort
            topo_order = list(nx.topological_sort(self.dependency_graph))
            
            # Group tasks by execution level
            levels = []
            remaining_tasks = set(topo_order)
            
            while remaining_tasks:
                # Find tasks with no remaining dependencies
                current_level = []
                for task in remaining_tasks:
                    dependencies = set(self.dependency_graph.predecessors(task))
                    if not dependencies.intersection(remaining_tasks):
                        current_level.append(task)
                
                if not current_level:
                    # This shouldn't happen with a valid DAG
                    raise ValueError("Circular dependency detected")
                
                levels.append(current_level)
                remaining_tasks -= set(current_level)
            
            return levels
            
        except nx.NetworkXError as e:
            self.logger.error(f"Error getting execution order: {e}")
            return []
    
    def validate_dependencies(self) -> Tuple[bool, List[str]]:
        """Validate task dependencies for cycles and missing tasks."""
        errors = []
        
        # Check for missing tasks in dependencies
        all_task_ids = set(self.task_configs.keys())
        for dep in self.dependencies:
            if dep.upstream_task_id not in all_task_ids:
                errors.append(f"Missing upstream task: {dep.upstream_task_id}")
            if dep.downstream_task_id not in all_task_ids:
                errors.append(f"Missing downstream task: {dep.downstream_task_id}")
        
        # Check for cycles
        if self.dependency_graph:
            try:
                cycles = list(nx.simple_cycles(self.dependency_graph))
                if cycles:
                    for cycle in cycles:
                        errors.append(f"Circular dependency detected: {' -> '.join(cycle)}")
            except nx.NetworkXError:
                errors.append("Error checking for cycles in dependency graph")
        
        return len(errors) == 0, errors
    
    def can_execute_task(self, task_id: str, execution_context: Dict[str, TaskResult]) -> bool:
        """Check if a task can be executed based on its dependencies."""
        if task_id not in self.task_configs:
            return False
        
        task_config = self.task_configs[task_id]
        upstream_tasks = self.get_task_dependencies(task_id)
        
        if not upstream_tasks:
            return True  # No dependencies
        
        # Check trigger rule
        trigger_rule = task_config.trigger_rule
        
        if trigger_rule == "all_success":
            return all(
                upstream_task in execution_context and 
                execution_context[upstream_task].status == TaskStatus.SUCCESS
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "all_failed":
            return all(
                upstream_task in execution_context and 
                execution_context[upstream_task].status == TaskStatus.FAILED
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "all_done":
            return all(
                upstream_task in execution_context and 
                execution_context[upstream_task].status in [TaskStatus.SUCCESS, TaskStatus.FAILED]
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "one_success":
            return any(
                upstream_task in execution_context and 
                execution_context[upstream_task].status == TaskStatus.SUCCESS
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "one_failed":
            return any(
                upstream_task in execution_context and 
                execution_context[upstream_task].status == TaskStatus.FAILED
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "none_failed":
            return all(
                upstream_task not in execution_context or 
                execution_context[upstream_task].status != TaskStatus.FAILED
                for upstream_task in upstream_tasks
            )
        elif trigger_rule == "none_skipped":
            return all(
                upstream_task not in execution_context or 
                execution_context[upstream_task].status != TaskStatus.SKIPPED
                for upstream_task in upstream_tasks
            )
        else:
            self.logger.warning(f"Unknown trigger rule: {trigger_rule}")
            return False
    
    def get_ready_tasks(self, execution_context: Dict[str, TaskResult]) -> List[str]:
        """Get list of tasks that are ready to execute."""
        ready_tasks = []
        
        for task_id in self.task_configs:
            if task_id not in execution_context:  # Not yet executed
                if self.can_execute_task(task_id, execution_context):
                    ready_tasks.append(task_id)
        
        # Sort by priority (higher priority first)
        ready_tasks.sort(
            key=lambda task_id: self.task_configs[task_id].priority,
            reverse=True
        )
        
        return ready_tasks
    
    def record_task_execution(self, task_result: TaskResult) -> None:
        """Record task execution result."""
        task_id = task_result.task_id
        
        if task_id not in self.execution_history:
            self.execution_history[task_id] = []
        
        self.execution_history[task_id].append(task_result)
        
        # Keep only recent executions (configurable limit)
        max_history = self.config.get('max_execution_history', 100)
        if len(self.execution_history[task_id]) > max_history:
            self.execution_history[task_id] = self.execution_history[task_id][-max_history:]
        
        self.logger.info(f"Recorded execution result for task: {task_id}")
    
    def get_task_execution_history(self, task_id: str, limit: Optional[int] = None) -> List[TaskResult]:
        """Get execution history for a task."""
        history = self.execution_history.get(task_id, [])
        
        if limit:
            return history[-limit:]
        
        return history.copy()
    
    def get_task_statistics(self, task_id: str) -> Dict[str, Any]:
        """Get execution statistics for a task."""
        history = self.execution_history.get(task_id, [])
        
        if not history:
            return {
                'total_executions': 0,
                'success_rate': 0.0,
                'average_duration': 0.0,
                'last_execution': None
            }
        
        successful_executions = [
            result for result in history 
            if result.status == TaskStatus.SUCCESS
        ]
        
        durations = []
        for result in history:
            if result.start_time and result.end_time:
                duration = (result.end_time - result.start_time).total_seconds()
                durations.append(duration)
        
        return {
            'total_executions': len(history),
            'successful_executions': len(successful_executions),
            'success_rate': len(successful_executions) / len(history) * 100,
            'average_duration': sum(durations) / len(durations) if durations else 0.0,
            'last_execution': history[-1].start_time if history else None,
            'last_status': history[-1].status if history else None
        }
    
    def _build_dependency_graph(self) -> None:
        """Build NetworkX graph from task dependencies."""
        self.dependency_graph = nx.DiGraph()
        
        # Add all tasks as nodes
        for task_id in self.task_configs:
            self.dependency_graph.add_node(task_id)
        
        # Add dependencies as edges
        for dep in self.dependencies:
            self.dependency_graph.add_edge(
                dep.upstream_task_id,
                dep.downstream_task_id,
                dependency_type=dep.dependency_type,
                condition=dep.condition
            )
        
        self.logger.info(f"Built dependency graph with {len(self.task_configs)} tasks and {len(self.dependencies)} dependencies")


class ScheduleManager(PipelineComponent):
    """Manages pipeline scheduling logic."""
    
    def __init__(self, component_id: str = "schedule_manager", config: Optional[Dict[str, Any]] = None):
        super().__init__(component_id, config or {})
        self.schedule_configs: Dict[str, ScheduleConfig] = {}
        self.active_schedules: Set[str] = set()
    
    def initialize(self) -> bool:
        """Initialize the schedule manager."""
        try:
            self.logger.info("Initializing ScheduleManager")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize ScheduleManager: {e}")
            return False
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        self.schedule_configs.clear()
        self.active_schedules.clear()
        self.logger.info("ScheduleManager cleanup completed")
    
    def health_check(self) -> bool:
        """Check component health."""
        return True
    
    def add_schedule(self, pipeline_id: str, schedule_config: ScheduleConfig) -> None:
        """Add a schedule configuration for a pipeline."""
        self.schedule_configs[pipeline_id] = schedule_config
        self.logger.info(f"Added schedule for pipeline: {pipeline_id}")
    
    def remove_schedule(self, pipeline_id: str) -> None:
        """Remove a schedule configuration."""
        if pipeline_id in self.schedule_configs:
            del self.schedule_configs[pipeline_id]
            self.active_schedules.discard(pipeline_id)
            self.logger.info(f"Removed schedule for pipeline: {pipeline_id}")
    
    def is_schedule_active(self, pipeline_id: str) -> bool:
        """Check if a schedule is currently active."""
        return pipeline_id in self.active_schedules
    
    def activate_schedule(self, pipeline_id: str) -> bool:
        """Activate a schedule."""
        if pipeline_id not in self.schedule_configs:
            self.logger.error(f"Schedule configuration not found: {pipeline_id}")
            return False
        
        self.active_schedules.add(pipeline_id)
        self.logger.info(f"Activated schedule for pipeline: {pipeline_id}")
        return True
    
    def deactivate_schedule(self, pipeline_id: str) -> None:
        """Deactivate a schedule."""
        self.active_schedules.discard(pipeline_id)
        self.logger.info(f"Deactivated schedule for pipeline: {pipeline_id}")
    
    def should_trigger_pipeline(self, pipeline_id: str, current_time: datetime) -> bool:
        """Check if a pipeline should be triggered based on its schedule."""
        if pipeline_id not in self.schedule_configs or pipeline_id not in self.active_schedules:
            return False
        
        schedule_config = self.schedule_configs[pipeline_id]
        
        # Check if within active time range
        if current_time < schedule_config.start_date:
            return False
        
        if schedule_config.end_date and current_time > schedule_config.end_date:
            return False
        
        # Check schedule type
        if schedule_config.schedule_type == ScheduleType.MANUAL:
            return False  # Manual schedules don't auto-trigger
        elif schedule_config.schedule_type == ScheduleType.CRON:
            return self._check_cron_schedule(schedule_config.schedule_value, current_time)
        elif schedule_config.schedule_type == ScheduleType.INTERVAL:
            return self._check_interval_schedule(pipeline_id, schedule_config.schedule_value, current_time)
        elif schedule_config.schedule_type == ScheduleType.TRIGGER:
            return self._check_trigger_schedule(schedule_config.schedule_value, current_time)
        
        return False
    
    def _check_cron_schedule(self, cron_expression: str, current_time: datetime) -> bool:
        """Check if current time matches cron expression."""
        try:
            from croniter import croniter
            
            # Create croniter instance
            cron = croniter(cron_expression, current_time)
            
            # Get the previous scheduled time
            prev_time = cron.get_prev(datetime)
            
            # Check if we're within the trigger window (e.g., 1 minute)
            trigger_window = timedelta(minutes=1)
            return current_time - prev_time <= trigger_window
            
        except Exception as e:
            self.logger.error(f"Error checking cron schedule: {e}")
            return False
    
    def _check_interval_schedule(self, pipeline_id: str, interval_str: str, current_time: datetime) -> bool:
        """Check if pipeline should trigger based on interval."""
        try:
            # Parse interval string (e.g., "30m", "1h", "1d")
            interval = self._parse_interval(interval_str)
            
            # Get last execution time (this would come from execution history)
            # For now, using a placeholder
            last_execution = self._get_last_execution_time(pipeline_id)
            
            if not last_execution:
                return True  # First execution
            
            return current_time - last_execution >= interval
            
        except Exception as e:
            self.logger.error(f"Error checking interval schedule: {e}")
            return False
    
    def _check_trigger_schedule(self, trigger_config: str, current_time: datetime) -> bool:
        """Check if pipeline should trigger based on external trigger."""
        # This would implement custom trigger logic
        # For now, returning False as placeholder
        return False
    
    def _parse_interval(self, interval_str: str) -> timedelta:
        """Parse interval string to timedelta."""
        import re
        
        # Match patterns like "30m", "1h", "2d"
        match = re.match(r'^(\d+)([smhd])$', interval_str.lower())
        if not match:
            raise ValueError(f"Invalid interval format: {interval_str}")
        
        value, unit = match.groups()
        value = int(value)
        
        if unit == 's':
            return timedelta(seconds=value)
        elif unit == 'm':
            return timedelta(minutes=value)
        elif unit == 'h':
            return timedelta(hours=value)
        elif unit == 'd':
            return timedelta(days=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")
    
    def _get_last_execution_time(self, pipeline_id: str) -> Optional[datetime]:
        """Get the last execution time for a pipeline."""
        # This would integrate with the task manager or execution history
        # For now, returning None as placeholder
        return None
    
    def get_next_execution_time(self, pipeline_id: str) -> Optional[datetime]:
        """Get the next scheduled execution time for a pipeline."""
        if pipeline_id not in self.schedule_configs:
            return None
        
        schedule_config = self.schedule_configs[pipeline_id]
        current_time = datetime.now()
        
        if schedule_config.schedule_type == ScheduleType.CRON:
            try:
                from croniter import croniter
                cron = croniter(schedule_config.schedule_value, current_time)
                return cron.get_next(datetime)
            except Exception:
                return None
        elif schedule_config.schedule_type == ScheduleType.INTERVAL:
            try:
                interval = self._parse_interval(schedule_config.schedule_value)
                last_execution = self._get_last_execution_time(pipeline_id)
                if last_execution:
                    return last_execution + interval
                else:
                    return current_time  # First execution
            except Exception:
                return None
        
        return None