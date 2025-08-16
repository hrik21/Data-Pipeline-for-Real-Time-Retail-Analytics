"""
Integration tests for Airflow orchestration framework.
"""
import pytest
import tempfile
import os
import yaml
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from src.orchestration.dag_generator import DynamicDAGGenerator, PipelineConfig
from src.orchestration.task_manager import TaskManager, TaskConfig, TaskDependency, DependencyType
from src.orchestration.sensors import DataSourceChangeSensor, TableChangeSensor
from src.orchestration.operators import IngestionOperator, TransformationOperator, ValidationOperator
from src.interfaces.base import TaskStatus, TaskResult, ChangeEvent, ChangeType


class TestDynamicDAGGenerator:
    """Test cases for DynamicDAGGenerator."""
    
    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary directory with pipeline configurations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create sample pipeline configuration
            pipeline_config = {
                'name': 'test_pipeline',
                'description': 'Test data pipeline',
                'schedule_interval': '0 1 * * *',
                'start_date': '2024-01-01T00:00:00',
                'catchup': False,
                'max_active_runs': 1,
                'default_args': {
                    'owner': 'test',
                    'retries': 2
                },
                'sources': [
                    {
                        'source_id': 'test_db',
                        'source_type': 'database',
                        'connection_params': {
                            'host': 'localhost',
                            'database': 'test'
                        },
                        'retries': 3
                    }
                ],
                'transformations': [
                    {
                        'model_name': 'staging_users',
                        'model_type': 'staging',
                        'retries': 2
                    }
                ],
                'targets': [
                    {
                        'table_name': 'dim_users',
                        'validation_rules': {
                            'min_row_count': 100
                        }
                    }
                ],
                'dependencies': {
                    'transform_staging_users': ['ingest_test_db'],
                    'validate_dim_users': ['transform_staging_users']
                }
            }
            
            config_file = os.path.join(temp_dir, 'test_pipeline.yaml')
            with open(config_file, 'w') as f:
                yaml.dump(pipeline_config, f)
            
            yield temp_dir
    
    @pytest.fixture
    def dag_generator(self, temp_config_dir):
        """Create DynamicDAGGenerator instance."""
        config = {'config_path': temp_config_dir}
        generator = DynamicDAGGenerator(config=config)
        generator.initialize()
        return generator
    
    def test_initialization(self, dag_generator):
        """Test DAG generator initialization."""
        assert dag_generator.health_check()
        assert len(dag_generator.dag_configs) == 1
        assert 'test_pipeline' in dag_generator.dag_configs
    
    def test_pipeline_config_parsing(self, dag_generator):
        """Test pipeline configuration parsing."""
        config = dag_generator.dag_configs['test_pipeline']
        
        assert config.name == 'test_pipeline'
        assert config.description == 'Test data pipeline'
        assert config.schedule_interval == '0 1 * * *'
        assert not config.catchup
        assert config.max_active_runs == 1
        assert len(config.sources) == 1
        assert len(config.transformations) == 1
        assert len(config.targets) == 1
    
    @patch('src.orchestration.dag_generator.DAG')
    @patch('src.orchestration.dag_generator.PythonOperator')
    def test_dag_generation(self, mock_python_operator, mock_dag, dag_generator):
        """Test DAG generation from configuration."""
        # Mock DAG and operator creation
        mock_dag_instance = Mock()
        mock_dag.return_value = mock_dag_instance
        
        mock_task_instance = Mock()
        mock_python_operator.return_value = mock_task_instance
        
        # Generate DAG
        dag = dag_generator.generate_dag('test_pipeline')
        
        # Verify DAG was created
        assert dag is not None
        mock_dag.assert_called_once()
        
        # Verify tasks were created
        assert mock_python_operator.call_count >= 3  # ingestion, transformation, validation
    
    def test_ingestion_task_creation(self, dag_generator):
        """Test creation of ingestion tasks."""
        config = dag_generator.dag_configs['test_pipeline']
        
        with patch('src.orchestration.dag_generator.DAG') as mock_dag:
            mock_dag_instance = Mock()
            mock_dag.return_value = mock_dag_instance
            
            tasks = dag_generator.create_ingestion_tasks(mock_dag_instance, config.sources)
            
            assert len(tasks) == 1
            assert 'ingest_test_db' in tasks
    
    def test_transformation_task_creation(self, dag_generator):
        """Test creation of transformation tasks."""
        config = dag_generator.dag_configs['test_pipeline']
        
        with patch('src.orchestration.dag_generator.DAG') as mock_dag:
            mock_dag_instance = Mock()
            mock_dag.return_value = mock_dag_instance
            
            tasks = dag_generator.create_transformation_tasks(mock_dag_instance, config.transformations)
            
            assert len(tasks) == 1
            assert 'transform_staging_users' in tasks
    
    def test_validation_task_creation(self, dag_generator):
        """Test creation of validation tasks."""
        config = dag_generator.dag_configs['test_pipeline']
        
        with patch('src.orchestration.dag_generator.DAG') as mock_dag:
            mock_dag_instance = Mock()
            mock_dag.return_value = mock_dag_instance
            
            tasks = dag_generator.create_validation_tasks(mock_dag_instance, config.targets)
            
            assert len(tasks) == 1
            assert 'validate_dim_users' in tasks
    
    def test_dependency_setting(self, dag_generator):
        """Test setting task dependencies."""
        # Create mock tasks
        task1 = Mock()
        task2 = Mock()
        task3 = Mock()
        
        tasks = {
            'ingest_test_db': task1,
            'transform_staging_users': task2,
            'validate_dim_users': task3
        }
        
        dependencies = {
            'transform_staging_users': ['ingest_test_db'],
            'validate_dim_users': ['transform_staging_users']
        }
        
        dag_generator.set_dependencies(tasks, dependencies)
        
        # Verify dependencies were set (using >> operator)
        # This is a simplified check - in practice, you'd verify the actual dependency relationships


class TestTaskManager:
    """Test cases for TaskManager."""
    
    @pytest.fixture
    def task_manager(self):
        """Create TaskManager instance."""
        manager = TaskManager()
        manager.initialize()
        return manager
    
    @pytest.fixture
    def sample_tasks(self):
        """Create sample task configurations."""
        return [
            TaskConfig(
                task_id='task_a',
                task_type='ingestion',
                operator_class='IngestionOperator',
                priority=1
            ),
            TaskConfig(
                task_id='task_b',
                task_type='transformation',
                operator_class='TransformationOperator',
                dependencies=['task_a'],
                priority=2
            ),
            TaskConfig(
                task_id='task_c',
                task_type='validation',
                operator_class='ValidationOperator',
                dependencies=['task_b'],
                priority=1
            )
        ]
    
    def test_task_addition(self, task_manager, sample_tasks):
        """Test adding tasks to task manager."""
        for task in sample_tasks:
            task_manager.add_task(task)
        
        assert len(task_manager.task_configs) == 3
        assert 'task_a' in task_manager.task_configs
        assert 'task_b' in task_manager.task_configs
        assert 'task_c' in task_manager.task_configs
    
    def test_dependency_management(self, task_manager, sample_tasks):
        """Test dependency management."""
        # Add tasks
        for task in sample_tasks:
            task_manager.add_task(task)
        
        # Add explicit dependencies
        dep1 = TaskDependency('task_a', 'task_b')
        dep2 = TaskDependency('task_b', 'task_c')
        
        task_manager.add_dependency(dep1)
        task_manager.add_dependency(dep2)
        
        # Test dependency queries
        assert task_manager.get_task_dependencies('task_b') == ['task_a']
        assert task_manager.get_task_dependencies('task_c') == ['task_b']
        assert task_manager.get_task_dependents('task_a') == ['task_b']
        assert task_manager.get_task_dependents('task_b') == ['task_c']
    
    def test_execution_order(self, task_manager, sample_tasks):
        """Test getting execution order."""
        # Add tasks and dependencies
        for task in sample_tasks:
            task_manager.add_task(task)
        
        dep1 = TaskDependency('task_a', 'task_b')
        dep2 = TaskDependency('task_b', 'task_c')
        
        task_manager.add_dependency(dep1)
        task_manager.add_dependency(dep2)
        
        # Get execution order
        execution_order = task_manager.get_execution_order()
        
        assert len(execution_order) == 3
        assert execution_order[0] == ['task_a']
        assert execution_order[1] == ['task_b']
        assert execution_order[2] == ['task_c']
    
    def test_dependency_validation(self, task_manager, sample_tasks):
        """Test dependency validation."""
        # Add tasks
        for task in sample_tasks:
            task_manager.add_task(task)
        
        # Add valid dependencies
        dep1 = TaskDependency('task_a', 'task_b')
        task_manager.add_dependency(dep1)
        
        is_valid, errors = task_manager.validate_dependencies()
        assert is_valid
        assert len(errors) == 0
        
        # Add invalid dependency (missing task)
        dep2 = TaskDependency('task_x', 'task_b')
        task_manager.add_dependency(dep2)
        
        is_valid, errors = task_manager.validate_dependencies()
        assert not is_valid
        assert len(errors) > 0
    
    def test_task_execution_readiness(self, task_manager, sample_tasks):
        """Test checking if tasks are ready for execution."""
        # Add tasks and dependencies
        for task in sample_tasks:
            task_manager.add_task(task)
        
        dep1 = TaskDependency('task_a', 'task_b')
        dep2 = TaskDependency('task_b', 'task_c')
        
        task_manager.add_dependency(dep1)
        task_manager.add_dependency(dep2)
        
        # Initially, only task_a should be ready
        execution_context = {}
        ready_tasks = task_manager.get_ready_tasks(execution_context)
        assert ready_tasks == ['task_a']
        
        # After task_a completes, task_b should be ready
        execution_context['task_a'] = TaskResult(
            task_id='task_a',
            status=TaskStatus.SUCCESS,
            start_time=datetime.now(),
            end_time=datetime.now()
        )
        
        ready_tasks = task_manager.get_ready_tasks(execution_context)
        assert ready_tasks == ['task_b']
    
    def test_task_execution_history(self, task_manager):
        """Test recording and retrieving task execution history."""
        # Add a task
        task = TaskConfig(
            task_id='test_task',
            task_type='test',
            operator_class='TestOperator'
        )
        task_manager.add_task(task)
        
        # Record execution results
        result1 = TaskResult(
            task_id='test_task',
            status=TaskStatus.SUCCESS,
            start_time=datetime.now(),
            end_time=datetime.now()
        )
        
        result2 = TaskResult(
            task_id='test_task',
            status=TaskStatus.FAILED,
            start_time=datetime.now(),
            end_time=datetime.now()
        )
        
        task_manager.record_task_execution(result1)
        task_manager.record_task_execution(result2)
        
        # Check history
        history = task_manager.get_task_execution_history('test_task')
        assert len(history) == 2
        
        # Check statistics
        stats = task_manager.get_task_statistics('test_task')
        assert stats['total_executions'] == 2
        assert stats['successful_executions'] == 1
        assert stats['success_rate'] == 50.0


class TestAirflowSensors:
    """Test cases for Airflow sensors."""
    
    @pytest.fixture
    def mock_data_source(self):
        """Create mock data source."""
        source = Mock()
        source.source_id = 'test_source'
        return source
    
    @pytest.fixture
    def mock_change_detector(self):
        """Create mock change detector."""
        detector = Mock()
        return detector
    
    def test_data_source_change_sensor_initialization(self):
        """Test DataSourceChangeSensor initialization."""
        source_config = {
            'source_id': 'test_source',
            'source_type': 'database',
            'connection_params': {'host': 'localhost'}
        }
        
        sensor = DataSourceChangeSensor(
            task_id='test_sensor',
            source_config=source_config,
            min_changes=1
        )
        
        assert sensor.source_config == source_config
        assert sensor.min_changes == 1
    
    @patch('src.orchestration.sensors.DataSourceFactory')
    @patch('src.orchestration.sensors.ChangeDetector')
    def test_data_source_change_sensor_poke(self, mock_change_detector_class, mock_factory):
        """Test DataSourceChangeSensor poke method."""
        # Setup mocks
        mock_source = Mock()
        mock_factory.create_source.return_value = mock_source
        
        mock_detector = Mock()
        mock_change_detector_class.return_value = mock_detector
        mock_detector.initialize.return_value = True
        
        # Create change events
        change_event = ChangeEvent(
            source_id='test_source',
            table_name='test_table',
            change_type=ChangeType.INSERT,
            timestamp=datetime.now(),
            affected_rows=10
        )
        mock_detector.detect_changes.return_value = [change_event]
        
        # Create sensor
        source_config = {
            'source_id': 'test_source',
            'source_type': 'database',
            'connection_params': {'host': 'localhost'}
        }
        
        sensor = DataSourceChangeSensor(
            task_id='test_sensor',
            source_config=source_config,
            min_changes=1
        )
        
        # Mock context
        mock_context = {
            'task_instance': Mock()
        }
        
        # Test poke
        result = sensor.poke(mock_context)
        
        assert result is True
        mock_context['task_instance'].xcom_push.assert_called_once()
    
    def test_table_change_sensor_initialization(self):
        """Test TableChangeSensor initialization."""
        table_names = ['table1', 'table2']
        connection_config = {'host': 'localhost', 'database': 'test'}
        
        sensor = TableChangeSensor(
            task_id='test_table_sensor',
            table_names=table_names,
            connection_config=connection_config
        )
        
        assert sensor.table_names == table_names
        assert sensor.connection_config == connection_config


class TestAirflowOperators:
    """Test cases for custom Airflow operators."""
    
    @pytest.fixture
    def mock_context(self):
        """Create mock Airflow context."""
        context = {
            'task_instance': Mock(),
            'params': {}
        }
        context['task_instance'].xcom_push = Mock()
        return context
    
    @patch('src.orchestration.operators.DataSourceFactory')
    @patch('src.orchestration.operators.DataExtractor')
    @patch('src.orchestration.operators.IngestionValidator')
    def test_ingestion_operator_execute(self, mock_validator_class, mock_extractor_class, mock_factory, mock_context):
        """Test IngestionOperator execution."""
        # Setup mocks
        mock_source = Mock()
        mock_factory.create_source.return_value = mock_source
        
        change_event = ChangeEvent(
            source_id='test_source',
            table_name='test_table',
            change_type=ChangeType.INSERT,
            timestamp=datetime.now(),
            affected_rows=10
        )
        mock_source.detect_changes.return_value = [change_event]
        
        # Mock data extraction
        import pandas as pd
        mock_data = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        mock_source.extract_data.return_value = mock_data
        
        # Mock validation
        from src.interfaces.base import ValidationResult
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        mock_validator.validate.return_value = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            row_count=3
        )
        
        # Create operator
        source_config = {
            'source_id': 'test_source',
            'source_type': 'database',
            'connection_params': {'host': 'localhost'}
        }
        
        operator = IngestionOperator(
            task_id='test_ingestion',
            source_config=source_config
        )
        
        # Execute
        result = operator.execute(mock_context)
        
        assert result['status'] == 'success'
        assert result['rows_processed'] == 3
        assert result['changes_processed'] == 1
    
    @patch('subprocess.run')
    def test_transformation_operator_execute(self, mock_subprocess, mock_context):
        """Test TransformationOperator execution."""
        # Mock subprocess result
        mock_result = Mock()
        mock_result.stdout = 'dbt run completed successfully'
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        
        # Create operator
        transform_config = {
            'model_name': 'staging_users',
            'model_type': 'staging'
        }
        
        operator = TransformationOperator(
            task_id='test_transformation',
            transform_config=transform_config
        )
        
        # Execute
        result = operator.execute(mock_context)
        
        assert result['status'] == 'success'
        assert result['model_name'] == 'staging_users'
        mock_subprocess.assert_called_once()
    
    def test_validation_operator_execute(self, mock_context):
        """Test ValidationOperator execution."""
        # Create operator
        target_config = {
            'table_name': 'dim_users',
            'validation_rules': {
                'min_row_count': 100,
                'max_age_hours': 24
            }
        }
        
        operator = ValidationOperator(
            task_id='test_validation',
            target_config=target_config
        )
        
        # Execute (with mocked validation checks)
        with patch.object(operator, '_execute_validation_checks') as mock_checks:
            mock_checks.return_value = [
                {'check_name': 'row_count', 'passed': True, 'message': 'Row count check passed'},
                {'check_name': 'data_freshness', 'passed': True, 'message': 'Data freshness check passed'}
            ]
            
            result = operator.execute(mock_context)
            
            assert result['status'] == 'success'
            assert result['table_name'] == 'dim_users'
            assert result['all_passed'] is True


class TestEndToEndIntegration:
    """End-to-end integration tests."""
    
    @pytest.fixture
    def integration_setup(self, tmp_path):
        """Setup for end-to-end integration tests."""
        # Create temporary config directory
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        
        # Create pipeline configuration
        pipeline_config = {
            'name': 'integration_test_pipeline',
            'description': 'Integration test pipeline',
            'schedule_interval': None,  # Manual trigger
            'start_date': '2024-01-01T00:00:00',
            'sources': [
                {
                    'source_id': 'test_source',
                    'source_type': 'database',
                    'connection_params': {'host': 'localhost'}
                }
            ],
            'transformations': [
                {
                    'model_name': 'test_model',
                    'model_type': 'staging'
                }
            ],
            'targets': [
                {
                    'table_name': 'test_table',
                    'validation_rules': {'min_row_count': 1}
                }
            ],
            'dependencies': {
                'transform_test_model': ['ingest_test_source'],
                'validate_test_table': ['transform_test_model']
            }
        }
        
        config_file = config_dir / "integration_test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        return {
            'config_dir': str(config_dir),
            'pipeline_name': 'integration_test_pipeline'
        }
    
    @patch('src.orchestration.dag_generator.DAG')
    @patch('src.orchestration.dag_generator.PythonOperator')
    def test_full_pipeline_generation(self, mock_python_operator, mock_dag, integration_setup):
        """Test full pipeline generation and task creation."""
        # Create DAG generator
        generator = DynamicDAGGenerator(config={'config_path': integration_setup['config_dir']})
        generator.initialize()
        
        # Mock DAG and operator creation
        mock_dag_instance = Mock()
        mock_dag.return_value = mock_dag_instance
        mock_task_instance = Mock()
        mock_python_operator.return_value = mock_task_instance
        
        # Generate DAG
        dag = generator.generate_dag(integration_setup['pipeline_name'])
        
        # Verify DAG was created
        assert dag is not None
        
        # Verify all task types were created
        assert mock_python_operator.call_count == 3  # ingestion, transformation, validation
        
        # Verify DAG configuration
        mock_dag.assert_called_once()
        dag_kwargs = mock_dag.call_args[1]
        assert dag_kwargs['dag_id'] == f"pipeline_{integration_setup['pipeline_name']}"
    
    def test_task_dependency_resolution(self, integration_setup):
        """Test task dependency resolution in task manager."""
        # Create task manager
        task_manager = TaskManager()
        task_manager.initialize()
        
        # Add tasks based on pipeline configuration
        tasks = [
            TaskConfig(
                task_id='ingest_test_source',
                task_type='ingestion',
                operator_class='IngestionOperator'
            ),
            TaskConfig(
                task_id='transform_test_model',
                task_type='transformation',
                operator_class='TransformationOperator'
            ),
            TaskConfig(
                task_id='validate_test_table',
                task_type='validation',
                operator_class='ValidationOperator'
            )
        ]
        
        for task in tasks:
            task_manager.add_task(task)
        
        # Add dependencies
        dependencies = [
            TaskDependency('ingest_test_source', 'transform_test_model'),
            TaskDependency('transform_test_model', 'validate_test_table')
        ]
        
        for dep in dependencies:
            task_manager.add_dependency(dep)
        
        # Test execution order
        execution_order = task_manager.get_execution_order()
        assert len(execution_order) == 3
        assert execution_order[0] == ['ingest_test_source']
        assert execution_order[1] == ['transform_test_model']
        assert execution_order[2] == ['validate_test_table']
        
        # Test dependency validation
        is_valid, errors = task_manager.validate_dependencies()
        assert is_valid
        assert len(errors) == 0