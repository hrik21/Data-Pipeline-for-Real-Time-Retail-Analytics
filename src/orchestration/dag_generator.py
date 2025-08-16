"""
Dynamic DAG Generator for creating Airflow DAGs based on configuration.
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import yaml
import json

try:
    from airflow import DAG as AirflowDAG
    from airflow.operators.python import PythonOperator as AirflowPythonOperator
    from airflow.sensors.base import BaseSensorOperator as AirflowBaseSensorOperator
    from airflow.models import Variable as AirflowVariable
    # Handle different Airflow versions
    try:
        from airflow.utils.dates import days_ago
    except ImportError:
        from datetime import datetime, timedelta
        def days_ago(n):
            return datetime.now() - timedelta(days=n)
    
    # Use Airflow classes
    DAG = AirflowDAG
    PythonOperator = AirflowPythonOperator
    BaseSensorOperator = AirflowBaseSensorOperator
    Variable = AirflowVariable
    
except ImportError:
    # Mock Airflow components for testing without Airflow installed
    class DAG:
        def __init__(self, dag_id=None, description=None, schedule_interval=None, 
                     start_date=None, catchup=None, max_active_runs=None, 
                     default_args=None, tags=None, *args, **kwargs):
            self.dag_id = dag_id or 'mock_dag'
            self.description = description or ''
            self.schedule_interval = schedule_interval
            self.start_date = start_date
            self.catchup = catchup if catchup is not None else False
            self.max_active_runs = max_active_runs or 1
            self.default_args = default_args or {}
            self.tags = tags or []
    
    class PythonOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id')
            self.python_callable = kwargs.get('python_callable')
            self.op_kwargs = kwargs.get('op_kwargs', {})
            self.dag = kwargs.get('dag')
            self.retries = kwargs.get('retries', 0)
            self.retry_delay = kwargs.get('retry_delay')
    
    class BaseSensorOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id')
    
    class Variable:
        @staticmethod
        def get(key, default_var=None):
            return default_var
    
    def days_ago(n):
        from datetime import datetime, timedelta
        return datetime.now() - timedelta(days=n)

from src.interfaces.base import PipelineComponent, DataSource, ChangeEvent
from src.config.settings import get_settings


@dataclass
class PipelineConfig:
    """Configuration for a data pipeline."""
    name: str
    description: str
    schedule_interval: Optional[str]
    start_date: datetime
    catchup: bool
    max_active_runs: int
    default_args: Dict[str, Any]
    sources: List[Dict[str, Any]]
    transformations: List[Dict[str, Any]]
    targets: List[Dict[str, Any]]
    dependencies: Dict[str, List[str]]


@dataclass
class TaskConfig:
    """Configuration for a pipeline task."""
    task_id: str
    task_type: str
    operator_class: str
    operator_kwargs: Dict[str, Any]
    dependencies: List[str]
    retries: int
    retry_delay: timedelta


class DynamicDAGGenerator(PipelineComponent):
    """Generates Airflow DAGs dynamically based on configuration."""
    
    def __init__(self, component_id: str = "dag_generator", config: Optional[Dict[str, Any]] = None):
        super().__init__(component_id, config or {})
        self.config_path = self.config.get('config_path', 'config/pipelines')
        self.dag_configs: Dict[str, PipelineConfig] = {}
        
    def initialize(self) -> bool:
        """Initialize the DAG generator."""
        try:
            self.logger.info("Initializing DynamicDAGGenerator")
            self._load_pipeline_configs()
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize DynamicDAGGenerator: {e}")
            return False
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        self.dag_configs.clear()
        self.logger.info("DynamicDAGGenerator cleanup completed")
    
    def health_check(self) -> bool:
        """Check component health."""
        return len(self.dag_configs) > 0
    
    def _load_pipeline_configs(self) -> None:
        """Load pipeline configurations from files."""
        import os
        import glob
        
        config_files = glob.glob(f"{self.config_path}/*.yaml") + glob.glob(f"{self.config_path}/*.yml")
        
        for config_file in config_files:
            try:
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                    pipeline_config = self._parse_pipeline_config(config_data)
                    self.dag_configs[pipeline_config.name] = pipeline_config
                    self.logger.info(f"Loaded pipeline config: {pipeline_config.name}")
            except Exception as e:
                self.logger.error(f"Failed to load config from {config_file}: {e}")
    
    def _parse_pipeline_config(self, config_data: Dict[str, Any]) -> PipelineConfig:
        """Parse pipeline configuration from dictionary."""
        return PipelineConfig(
            name=config_data['name'],
            description=config_data.get('description', ''),
            schedule_interval=config_data.get('schedule_interval'),
            start_date=datetime.fromisoformat(config_data.get('start_date', datetime.now().isoformat())),
            catchup=config_data.get('catchup', False),
            max_active_runs=config_data.get('max_active_runs', 1),
            default_args=config_data.get('default_args', {}),
            sources=config_data.get('sources', []),
            transformations=config_data.get('transformations', []),
            targets=config_data.get('targets', []),
            dependencies=config_data.get('dependencies', {})
        )
    
    def generate_dag(self, pipeline_name: str) -> Optional[DAG]:
        """Generate an Airflow DAG for the specified pipeline."""
        if pipeline_name not in self.dag_configs:
            self.logger.error(f"Pipeline config not found: {pipeline_name}")
            return None
        
        config = self.dag_configs[pipeline_name]
        
        try:
            # Create DAG with configuration
            dag = DAG(
                dag_id=f"pipeline_{config.name}",
                description=config.description,
                schedule=config.schedule_interval,  # Changed from schedule_interval to schedule for Airflow 3.0+
                start_date=config.start_date,
                catchup=config.catchup,
                max_active_runs=config.max_active_runs,
                default_args=self._get_default_args(config.default_args),
                tags=['data-pipeline', 'dynamic']
            )
            
            # Generate tasks
            tasks = {}
            
            # Create ingestion tasks
            ingestion_tasks = self.create_ingestion_tasks(dag, config.sources)
            tasks.update(ingestion_tasks)
            
            # Create transformation tasks
            transformation_tasks = self.create_transformation_tasks(dag, config.transformations)
            tasks.update(transformation_tasks)
            
            # Create validation tasks
            validation_tasks = self.create_validation_tasks(dag, config.targets)
            tasks.update(validation_tasks)
            
            # Set task dependencies
            self.set_dependencies(tasks, config.dependencies)
            
            self.logger.info(f"Generated DAG for pipeline: {pipeline_name}")
            return dag
            
        except Exception as e:
            self.logger.error(f"Failed to generate DAG for {pipeline_name}: {e}")
            return None
    
    def create_ingestion_tasks(self, dag: DAG, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create ingestion tasks for data sources."""
        tasks = {}
        
        for source_config in sources:
            source_id = source_config['source_id']
            task_id = f"ingest_{source_id}"
            
            task = PythonOperator(
                task_id=task_id,
                python_callable=self._execute_ingestion_task,
                op_kwargs={
                    'source_config': source_config,
                    'pipeline_name': dag.dag_id
                },
                dag=dag,
                retries=source_config.get('retries', 3),
                retry_delay=timedelta(minutes=source_config.get('retry_delay_minutes', 5))
            )
            
            tasks[task_id] = task
            
        return tasks
    
    def create_transformation_tasks(self, dag: DAG, transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create transformation tasks for dbt models."""
        tasks = {}
        
        for transform_config in transformations:
            model_name = transform_config['model_name']
            task_id = f"transform_{model_name}"
            
            task = PythonOperator(
                task_id=task_id,
                python_callable=self._execute_transformation_task,
                op_kwargs={
                    'transform_config': transform_config,
                    'pipeline_name': dag.dag_id
                },
                dag=dag,
                retries=transform_config.get('retries', 2),
                retry_delay=timedelta(minutes=transform_config.get('retry_delay_minutes', 10))
            )
            
            tasks[task_id] = task
            
        return tasks
    
    def create_validation_tasks(self, dag: DAG, targets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create validation tasks for target tables."""
        tasks = {}
        
        for target_config in targets:
            table_name = target_config['table_name']
            task_id = f"validate_{table_name}"
            
            task = PythonOperator(
                task_id=task_id,
                python_callable=self._execute_validation_task,
                op_kwargs={
                    'target_config': target_config,
                    'pipeline_name': dag.dag_id
                },
                dag=dag,
                retries=target_config.get('retries', 1),
                retry_delay=timedelta(minutes=target_config.get('retry_delay_minutes', 2))
            )
            
            tasks[task_id] = task
            
        return tasks
    
    def set_dependencies(self, tasks: Dict[str, Any], dependencies: Dict[str, List[str]]) -> None:
        """Set task dependencies based on configuration."""
        for task_id, upstream_tasks in dependencies.items():
            if task_id in tasks:
                downstream_task = tasks[task_id]
                for upstream_task_id in upstream_tasks:
                    if upstream_task_id in tasks:
                        upstream_task = tasks[upstream_task_id]
                        upstream_task >> downstream_task
                    else:
                        self.logger.warning(f"Upstream task not found: {upstream_task_id}")
            else:
                self.logger.warning(f"Task not found for dependency: {task_id}")
    
    def _get_default_args(self, custom_args: Dict[str, Any]) -> Dict[str, Any]:
        """Get default arguments for DAG tasks."""
        default_args = {
            'owner': 'data-engineering',
            'depends_on_past': False,
            'start_date': days_ago(1),
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        }
        
        # Override with custom arguments
        default_args.update(custom_args)
        return default_args
    
    def _execute_ingestion_task(self, source_config: Dict[str, Any], pipeline_name: str, **context) -> Dict[str, Any]:
        """Execute an ingestion task."""
        from src.orchestration.operators import IngestionOperator
        
        operator = IngestionOperator()
        result = operator.execute(source_config, context)
        
        self.logger.info(f"Ingestion task completed for {source_config['source_id']}")
        return result
    
    def _execute_transformation_task(self, transform_config: Dict[str, Any], pipeline_name: str, **context) -> Dict[str, Any]:
        """Execute a transformation task."""
        from src.orchestration.operators import TransformationOperator
        
        operator = TransformationOperator()
        result = operator.execute(transform_config, context)
        
        self.logger.info(f"Transformation task completed for {transform_config['model_name']}")
        return result
    
    def _execute_validation_task(self, target_config: Dict[str, Any], pipeline_name: str, **context) -> Dict[str, Any]:
        """Execute a validation task."""
        from src.orchestration.operators import ValidationOperator
        
        operator = ValidationOperator()
        result = operator.execute(target_config, context)
        
        self.logger.info(f"Validation task completed for {target_config['table_name']}")
        return result
    
    def get_pipeline_configs(self) -> Dict[str, PipelineConfig]:
        """Get all loaded pipeline configurations."""
        return self.dag_configs.copy()
    
    def reload_configs(self) -> bool:
        """Reload pipeline configurations."""
        try:
            self.dag_configs.clear()
            self._load_pipeline_configs()
            self.logger.info("Pipeline configurations reloaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload configurations: {e}")
            return False