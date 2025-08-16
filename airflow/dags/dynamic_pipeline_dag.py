"""
Dynamic pipeline DAG that uses the orchestration framework.

This DAG demonstrates how to use the DynamicDAGGenerator to create
data pipelines from configuration files.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import our orchestration framework
from src.orchestration.dag_generator import DynamicDAGGenerator


def generate_pipeline_dag(pipeline_name: str) -> DAG:
    """Generate a DAG for the specified pipeline."""
    
    # Initialize DAG generator
    config_path = os.path.join(os.path.dirname(__file__), '../../config/pipelines')
    generator = DynamicDAGGenerator(config={'config_path': config_path})
    
    if not generator.initialize():
        raise Exception(f"Failed to initialize DAG generator")
    
    # Generate DAG
    dag = generator.generate_dag(pipeline_name)
    
    if not dag:
        raise Exception(f"Failed to generate DAG for pipeline: {pipeline_name}")
    
    return dag


# Generate DAGs for all configured pipelines
def create_dags():
    """Create DAGs for all configured pipelines."""
    config_path = os.path.join(os.path.dirname(__file__), '../../config/pipelines')
    
    if not os.path.exists(config_path):
        print(f"Pipeline config directory not found: {config_path}")
        return {}
    
    generator = DynamicDAGGenerator(config={'config_path': config_path})
    
    if not generator.initialize():
        print("Failed to initialize DAG generator")
        return {}
    
    dags = {}
    pipeline_configs = generator.get_pipeline_configs()
    
    for pipeline_name in pipeline_configs:
        try:
            dag = generator.generate_dag(pipeline_name)
            if dag:
                dags[f"pipeline_{pipeline_name}"] = dag
                print(f"Generated DAG for pipeline: {pipeline_name}")
            else:
                print(f"Failed to generate DAG for pipeline: {pipeline_name}")
        except Exception as e:
            print(f"Error generating DAG for {pipeline_name}: {e}")
    
    return dags


# Create all DAGs
generated_dags = create_dags()

# Add DAGs to globals so Airflow can discover them
for dag_id, dag in generated_dags.items():
    globals()[dag_id] = dag


# Example: Create a specific DAG manually (alternative approach)
def create_sample_pipeline_dag():
    """Create the sample pipeline DAG manually."""
    
    default_args = {
        'owner': 'data-engineering',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
    
    dag = DAG(
        'sample_data_pipeline_manual',
        default_args=default_args,
        description='Manually created sample data pipeline',
        schedule_interval=timedelta(hours=2),
        catchup=False,
        max_active_runs=1,
        tags=['data-pipeline', 'manual']
    )
    
    def execute_ingestion_task(**context):
        """Execute ingestion task."""
        from src.orchestration.operators import IngestionOperator
        
        source_config = {
            'source_id': 'customer_db',
            'source_type': 'database',
            'connection_params': {
                'host': 'localhost',
                'database': 'customers'
            }
        }
        
        operator = IngestionOperator()
        return operator.execute(source_config, context)
    
    def execute_transformation_task(**context):
        """Execute transformation task."""
        from src.orchestration.operators import TransformationOperator
        
        transform_config = {
            'model_name': 'staging_customers',
            'model_type': 'staging'
        }
        
        operator = TransformationOperator()
        return operator.execute(transform_config, context)
    
    def execute_validation_task(**context):
        """Execute validation task."""
        from src.orchestration.operators import ValidationOperator
        
        target_config = {
            'table_name': 'dim_customers',
            'validation_rules': {
                'min_row_count': 1000,
                'max_age_hours': 4
            }
        }
        
        operator = ValidationOperator()
        return operator.execute(target_config, context)
    
    # Create tasks
    ingest_customers = PythonOperator(
        task_id='ingest_customer_db',
        python_callable=execute_ingestion_task,
        dag=dag
    )
    
    transform_staging = PythonOperator(
        task_id='transform_staging_customers',
        python_callable=execute_transformation_task,
        dag=dag
    )
    
    validate_customers = PythonOperator(
        task_id='validate_dim_customers',
        python_callable=execute_validation_task,
        dag=dag
    )
    
    # Set dependencies
    ingest_customers >> transform_staging >> validate_customers
    
    return dag


# Create the manual sample DAG
sample_pipeline_manual = create_sample_pipeline_dag()


# Example: DAG with sensors for change-based triggering
def create_sensor_triggered_dag():
    """Create a DAG that uses sensors for change-based triggering."""
    
    default_args = {
        'owner': 'data-engineering',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
    
    dag = DAG(
        'sensor_triggered_pipeline',
        default_args=default_args,
        description='Pipeline triggered by data source changes',
        schedule_interval=None,  # Triggered by sensors
        catchup=False,
        max_active_runs=1,
        tags=['data-pipeline', 'sensor-triggered']
    )
    
    # Import sensors
    from src.orchestration.sensors import DataSourceChangeSensor, TableChangeSensor
    
    # Create change detection sensor
    change_sensor = DataSourceChangeSensor(
        task_id='detect_customer_changes',
        source_config={
            'source_id': 'customer_db',
            'source_type': 'database',
            'connection_params': {
                'host': 'localhost',
                'database': 'customers'
            }
        },
        min_changes=1,
        poke_interval=300,  # Check every 5 minutes
        timeout=3600,  # Timeout after 1 hour
        dag=dag
    )
    
    # Create table monitoring sensor
    table_sensor = TableChangeSensor(
        task_id='monitor_orders_table',
        table_names=['orders'],
        connection_config={
            'host': 'localhost',
            'database': 'orders'
        },
        change_detection_method='timestamp',
        timestamp_column='updated_at',
        poke_interval=600,  # Check every 10 minutes
        dag=dag
    )
    
    def process_detected_changes(**context):
        """Process changes detected by sensors."""
        # Get detected changes from XCom
        changes = context['task_instance'].xcom_pull(task_ids='detect_customer_changes', key='detected_changes')
        table_changes = context['task_instance'].xcom_pull(task_ids='monitor_orders_table', key='table_changes')
        
        print(f"Processing {len(changes) if changes else 0} customer changes")
        print(f"Table changes: {table_changes}")
        
        # Here you would trigger the actual data processing pipeline
        # For example, by triggering another DAG or executing processing tasks
        
        return {
            'customer_changes': len(changes) if changes else 0,
            'table_changes': table_changes
        }
    
    process_changes = PythonOperator(
        task_id='process_changes',
        python_callable=process_detected_changes,
        dag=dag
    )
    
    # Set dependencies - process changes when either sensor detects changes
    [change_sensor, table_sensor] >> process_changes
    
    return dag


# Create the sensor-triggered DAG
sensor_triggered_pipeline = create_sensor_triggered_dag()


# Health check DAG for the orchestration framework
def create_health_check_dag():
    """Create a DAG for health checking the orchestration framework."""
    
    default_args = {
        'owner': 'data-engineering',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
    
    dag = DAG(
        'orchestration_health_check',
        default_args=default_args,
        description='Health check for orchestration framework components',
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=['health-check', 'monitoring']
    )
    
    def check_dag_generator_health(**context):
        """Check DAG generator health."""
        from src.orchestration.dag_generator import DynamicDAGGenerator
        
        generator = DynamicDAGGenerator()
        if generator.initialize():
            health_status = generator.health_check()
            print(f"DAG Generator health: {'OK' if health_status else 'FAILED'}")
            return health_status
        else:
            print("DAG Generator initialization failed")
            return False
    
    def check_task_manager_health(**context):
        """Check task manager health."""
        from src.orchestration.task_manager import TaskManager
        
        manager = TaskManager()
        if manager.initialize():
            health_status = manager.health_check()
            print(f"Task Manager health: {'OK' if health_status else 'FAILED'}")
            return health_status
        else:
            print("Task Manager initialization failed")
            return False
    
    # Create health check tasks
    check_dag_generator = PythonOperator(
        task_id='check_dag_generator',
        python_callable=check_dag_generator_health,
        dag=dag
    )
    
    check_task_manager = PythonOperator(
        task_id='check_task_manager',
        python_callable=check_task_manager_health,
        dag=dag
    )
    
    def aggregate_health_results(**context):
        """Aggregate health check results."""
        dag_gen_health = context['task_instance'].xcom_pull(task_ids='check_dag_generator')
        task_mgr_health = context['task_instance'].xcom_pull(task_ids='check_task_manager')
        
        overall_health = dag_gen_health and task_mgr_health
        
        print(f"Overall orchestration framework health: {'OK' if overall_health else 'FAILED'}")
        
        if not overall_health:
            # Here you could send alerts or notifications
            print("ALERT: Orchestration framework health check failed!")
        
        return {
            'dag_generator': dag_gen_health,
            'task_manager': task_mgr_health,
            'overall': overall_health
        }
    
    aggregate_results = PythonOperator(
        task_id='aggregate_health_results',
        python_callable=aggregate_health_results,
        dag=dag
    )
    
    # Set dependencies
    [check_dag_generator, check_task_manager] >> aggregate_results
    
    return dag


# Create the health check DAG
orchestration_health_check = create_health_check_dag()