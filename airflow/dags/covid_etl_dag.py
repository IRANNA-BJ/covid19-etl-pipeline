"""
Apache Airflow DAG for COVID-19 ETL Pipeline.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from extract.covid_api_extractor import CovidDataExtractor
from transform.data_transformer import CovidDataTransformer
from load.cloud_storage_loader import CloudStorageLoader
from load.warehouse_loader import DataWarehouseLoader
from utils.config import config
from utils.logger import get_logger


# DAG Configuration
DAG_ID = 'covid_etl_pipeline'
SCHEDULE_INTERVAL = '@daily'
START_DATE = days_ago(1)
CATCHUP = False
MAX_ACTIVE_RUNS = 1

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Initialize logger
logger = get_logger(__name__)


def extract_covid_data(**context) -> str:
    """
    Extract COVID-19 data from disease.sh API.
    
    Returns:
        Path to extracted data files
    """
    try:
        logger.info("Starting COVID-19 data extraction")
        
        extractor = CovidDataExtractor()
        
        # Extract all data types
        extracted_data = extractor.extract_all_data(
            include_historical=True,
            historical_days=30
        )
        
        # Save raw data locally
        saved_files = extractor.save_raw_data(extracted_data, "data/raw")
        
        # Store file paths in XCom for next tasks
        context['task_instance'].xcom_push(key='raw_data_files', value=saved_files)
        context['task_instance'].xcom_push(key='extraction_summary', 
                                         value=extractor.get_extraction_summary(extracted_data))
        
        logger.info(f"Successfully extracted {len(extracted_data)} datasets")
        return "data/raw"
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise


def validate_extracted_data(**context) -> bool:
    """
    Validate extracted data quality.
    
    Returns:
        True if validation passes
    """
    try:
        logger.info("Starting data validation")
        
        # Get extraction summary from previous task
        extraction_summary = context['task_instance'].xcom_pull(
            task_ids='extract_data', key='extraction_summary'
        )
        
        # Basic validation checks
        if not extraction_summary:
            raise ValueError("No extraction summary found")
        
        total_datasets = extraction_summary.get('total_datasets', 0)
        if total_datasets == 0:
            raise ValueError("No datasets were extracted")
        
        # Check each dataset
        for dataset_name, dataset_info in extraction_summary.get('datasets', {}).items():
            if dataset_info.get('is_empty', True):
                logger.warning(f"Dataset {dataset_name} is empty")
            
            record_count = dataset_info.get('record_count', 0)
            if record_count < 10:  # Minimum threshold
                logger.warning(f"Dataset {dataset_name} has only {record_count} records")
        
        logger.info("Data validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise


def transform_covid_data(**context) -> str:
    """
    Transform and clean COVID-19 data.
    
    Returns:
        Path to transformed data files
    """
    try:
        logger.info("Starting COVID-19 data transformation")
        
        # Get raw data files from previous task
        raw_data_files = context['task_instance'].xcom_pull(
            task_ids='extract_data', key='raw_data_files'
        )
        
        if not raw_data_files:
            raise ValueError("No raw data files found")
        
        transformer = CovidDataTransformer()
        
        # Load raw data
        raw_data = {}
        for data_type, file_path in raw_data_files.items():
            if file_path.endswith('.parquet'):
                raw_data[data_type] = pd.read_parquet(file_path)
            else:
                raw_data[data_type] = pd.read_csv(file_path)
        
        # Transform data
        transformed_data = transformer.transform_all_data(raw_data)
        
        # Save transformed data
        saved_files = transformer.save_transformed_data(transformed_data, "data/processed")
        
        # Store file paths in XCom
        context['task_instance'].xcom_push(key='transformed_data_files', value=saved_files)
        
        logger.info(f"Successfully transformed {len(transformed_data)} datasets")
        return "data/processed"
        
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise


def load_to_cloud_storage(**context) -> Dict[str, str]:
    """
    Load transformed data to cloud storage.
    
    Returns:
        Dictionary of uploaded file URLs
    """
    try:
        logger.info("Starting cloud storage upload")
        
        # Get transformed data files
        transformed_data_files = context['task_instance'].xcom_pull(
            task_ids='transform_data', key='transformed_data_files'
        )
        
        if not transformed_data_files:
            raise ValueError("No transformed data files found")
        
        storage_loader = CloudStorageLoader()
        
        # Load transformed data
        transformed_data = {}
        for data_type, file_path in transformed_data_files.items():
            if file_path.endswith('.parquet'):
                transformed_data[data_type] = pd.read_parquet(file_path)
            else:
                transformed_data[data_type] = pd.read_csv(file_path)
        
        # Upload to cloud storage
        execution_date = context['execution_date'].strftime('%Y%m%d')
        prefix = f"processed/{execution_date}/"
        
        uploaded_files = storage_loader.upload_multiple_dataframes(
            transformed_data, 
            prefix=prefix,
            file_format='parquet'
        )
        
        # Store URLs in XCom
        context['task_instance'].xcom_push(key='cloud_storage_urls', value=uploaded_files)
        
        logger.info(f"Successfully uploaded {len(uploaded_files)} files to cloud storage")
        return uploaded_files
        
    except Exception as e:
        logger.error(f"Cloud storage upload failed: {str(e)}")
        raise


def load_to_warehouse(**context) -> Dict[str, bool]:
    """
    Load data to data warehouse.
    
    Returns:
        Dictionary of load results
    """
    try:
        logger.info("Starting data warehouse loading")
        
        # Get transformed data files
        transformed_data_files = context['task_instance'].xcom_pull(
            task_ids='transform_data', key='transformed_data_files'
        )
        
        if not transformed_data_files:
            raise ValueError("No transformed data files found")
        
        warehouse_loader = DataWarehouseLoader()
        
        # Load transformed data
        transformed_data = {}
        for data_type, file_path in transformed_data_files.items():
            if file_path.endswith('.parquet'):
                transformed_data[data_type] = pd.read_parquet(file_path)
            else:
                transformed_data[data_type] = pd.read_csv(file_path)
        
        # Load to warehouse with date suffix
        execution_date = context['execution_date'].strftime('%Y%m%d')
        table_prefix = f"covid_"
        
        load_results = warehouse_loader.load_multiple_dataframes(
            transformed_data,
            table_prefix=table_prefix,
            if_exists='append'
        )
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='warehouse_load_results', value=load_results)
        
        # Check if any loads failed
        failed_loads = [table for table, success in load_results.items() if not success]
        if failed_loads:
            raise ValueError(f"Failed to load tables: {failed_loads}")
        
        logger.info(f"Successfully loaded {len(load_results)} tables to warehouse")
        return load_results
        
    except Exception as e:
        logger.error(f"Data warehouse loading failed: {str(e)}")
        raise


def validate_warehouse_data(**context) -> bool:
    """
    Validate data in warehouse.
    
    Returns:
        True if validation passes
    """
    try:
        logger.info("Starting warehouse data validation")
        
        # Get load results
        load_results = context['task_instance'].xcom_pull(
            task_ids='load_to_warehouse', key='warehouse_load_results'
        )
        
        if not load_results:
            raise ValueError("No warehouse load results found")
        
        warehouse_loader = DataWarehouseLoader()
        
        # Validate each loaded table
        validation_results = {}
        for table_name, load_success in load_results.items():
            if not load_success:
                continue
            
            # Get expected count from extraction summary
            extraction_summary = context['task_instance'].xcom_pull(
                task_ids='extract_data', key='extraction_summary'
            )
            
            data_type = table_name.replace('covid_', '')
            expected_count = extraction_summary.get('datasets', {}).get(data_type, {}).get('record_count', 0)
            
            # Validate table
            validation_result = warehouse_loader.validate_data_load(table_name, expected_count)
            validation_results[table_name] = validation_result
        
        # Check if any validations failed
        failed_validations = [table for table, result in validation_results.items() 
                            if not result.get('is_valid', False)]
        
        if failed_validations:
            logger.error(f"Validation failed for tables: {failed_validations}")
            raise ValueError(f"Data validation failed for tables: {failed_validations}")
        
        logger.info("Warehouse data validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Warehouse data validation failed: {str(e)}")
        raise


def cleanup_local_files(**context) -> bool:
    """
    Clean up local temporary files.
    
    Returns:
        True if cleanup successful
    """
    try:
        logger.info("Starting cleanup of local files")
        
        import shutil
        import os
        
        # Clean up raw data directory
        raw_data_dir = "data/raw"
        if os.path.exists(raw_data_dir):
            # Keep only the latest 3 days of files
            cutoff_date = context['execution_date'] - timedelta(days=3)
            
            for filename in os.listdir(raw_data_dir):
                file_path = os.path.join(raw_data_dir, filename)
                if os.path.isfile(file_path):
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_mtime < cutoff_date:
                        os.remove(file_path)
                        logger.info(f"Removed old file: {file_path}")
        
        # Clean up processed data directory
        processed_data_dir = "data/processed"
        if os.path.exists(processed_data_dir):
            cutoff_date = context['execution_date'] - timedelta(days=3)
            
            for filename in os.listdir(processed_data_dir):
                file_path = os.path.join(processed_data_dir, filename)
                if os.path.isfile(file_path):
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_mtime < cutoff_date:
                        os.remove(file_path)
                        logger.info(f"Removed old file: {file_path}")
        
        logger.info("Local file cleanup completed")
        return True
        
    except Exception as e:
        logger.error(f"File cleanup failed: {str(e)}")
        return False


def send_success_notification(**context) -> None:
    """Send success notification."""
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        # Get pipeline statistics
        extraction_summary = context['task_instance'].xcom_pull(
            task_ids='extract_data', key='extraction_summary'
        )
        
        load_results = context['task_instance'].xcom_pull(
            task_ids='load_to_warehouse', key='warehouse_load_results'
        )
        
        total_records = sum(
            dataset_info.get('record_count', 0) 
            for dataset_info in extraction_summary.get('datasets', {}).values()
        )
        
        successful_tables = sum(1 for success in load_results.values() if success)
        
        message = f"""
        COVID-19 ETL Pipeline completed successfully for {execution_date}
        
        Summary:
        - Total records processed: {total_records:,}
        - Datasets extracted: {extraction_summary.get('total_datasets', 0)}
        - Tables loaded to warehouse: {successful_tables}
        - Execution time: {context['task_instance'].duration}
        
        Pipeline execution completed at: {datetime.now()}
        """
        
        logger.info("Pipeline completed successfully")
        logger.info(message)
        
    except Exception as e:
        logger.error(f"Failed to send success notification: {str(e)}")


# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='COVID-19 ETL Pipeline - Extract, Transform, Load COVID data',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=['covid', 'etl', 'data-engineering'],
    doc_md=__doc__
)

# Task definitions
with dag:
    
    # Data Extraction Task Group
    with TaskGroup('extraction_group', tooltip='Data extraction tasks') as extraction_group:
        
        extract_task = PythonOperator(
            task_id='extract_data',
            python_callable=extract_covid_data,
            doc_md="Extract COVID-19 data from disease.sh API"
        )
        
        validate_extraction_task = PythonOperator(
            task_id='validate_extraction',
            python_callable=validate_extracted_data,
            doc_md="Validate extracted data quality"
        )
        
        extract_task >> validate_extraction_task
    
    # Data Transformation Task
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_covid_data,
        doc_md="Transform and clean COVID-19 data"
    )
    
    # Data Loading Task Group
    with TaskGroup('loading_group', tooltip='Data loading tasks') as loading_group:
        
        load_cloud_task = PythonOperator(
            task_id='load_to_cloud_storage',
            python_callable=load_to_cloud_storage,
            doc_md="Load transformed data to cloud storage"
        )
        
        load_warehouse_task = PythonOperator(
            task_id='load_to_warehouse',
            python_callable=load_to_warehouse,
            doc_md="Load data to data warehouse"
        )
        
        # These can run in parallel
        [load_cloud_task, load_warehouse_task]
    
    # Data Validation Task
    validate_warehouse_task = PythonOperator(
        task_id='validate_warehouse_data',
        python_callable=validate_warehouse_data,
        doc_md="Validate data in warehouse"
    )
    
    # Cleanup Task
    cleanup_task = PythonOperator(
        task_id='cleanup_local_files',
        python_callable=cleanup_local_files,
        doc_md="Clean up local temporary files",
        trigger_rule='all_done'  # Run even if previous tasks fail
    )
    
    # Success Notification Task
    success_notification_task = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        doc_md="Send pipeline success notification"
    )
    
    # Failure Notification Task
    failure_notification_task = EmailOperator(
        task_id='send_failure_notification',
        to=['data-team@company.com'],
        subject='COVID-19 ETL Pipeline Failed - {{ ds }}',
        html_content="""
        <h3>COVID-19 ETL Pipeline Failure</h3>
        <p>The COVID-19 ETL pipeline failed on {{ ds }}.</p>
        <p>Failed task: {{ task_instance.task_id }}</p>
        <p>Error: {{ task_instance.log_url }}</p>
        <p>Please check the Airflow logs for more details.</p>
        """,
        trigger_rule='one_failed'
    )

# Define task dependencies
extraction_group >> transform_task >> loading_group >> validate_warehouse_task
validate_warehouse_task >> success_notification_task
validate_warehouse_task >> cleanup_task

# Failure notification should trigger on any failure
[extraction_group, transform_task, loading_group, validate_warehouse_task] >> failure_notification_task
