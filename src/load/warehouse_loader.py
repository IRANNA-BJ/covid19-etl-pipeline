"""
Data warehouse loader for COVID-19 ETL Pipeline.
Supports Amazon Redshift and Google BigQuery.
"""

import pandas as pd
import sqlalchemy
from google.cloud import bigquery
import psycopg2
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import json

from ..utils.config import config
from ..utils.logger import get_logger


class DataWarehouseLoader:
    """Loader for uploading data to data warehouses (Redshift or BigQuery)."""
    
    def __init__(self, provider: Optional[str] = None):
        """
        Initialize data warehouse loader.
        
        Args:
            provider: Warehouse provider ('redshift' or 'bigquery'). If None, uses config.
        """
        self.logger = get_logger(__name__)
        self.db_config = config.get_database_config()
        
        self.provider = provider or self.db_config.get('provider', 'redshift')
        
        # Initialize warehouse client
        if self.provider == 'redshift':
            self._init_redshift_client()
        elif self.provider == 'bigquery':
            self._init_bigquery_client()
        else:
            raise ValueError(f"Unsupported warehouse provider: {self.provider}")
        
        self.logger.info(f"Initialized {self.provider.upper()} warehouse loader")
    
    def _init_redshift_client(self) -> None:
        """Initialize Amazon Redshift client."""
        try:
            # Build connection string
            connection_params = {
                'host': self.db_config.get('host'),
                'port': self.db_config.get('port', 5439),
                'database': self.db_config.get('name'),
                'user': self.db_config.get('user'),
                'password': self.db_config.get('password')
            }
            
            # Validate required parameters
            required_params = ['host', 'database', 'user', 'password']
            missing_params = [p for p in required_params if not connection_params.get(p)]
            if missing_params:
                raise ValueError(f"Missing required Redshift parameters: {missing_params}")
            
            # Create SQLAlchemy engine
            connection_string = (
                f"postgresql://{connection_params['user']}:{connection_params['password']}"
                f"@{connection_params['host']}:{connection_params['port']}"
                f"/{connection_params['database']}"
            )
            
            self.engine = sqlalchemy.create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
            
            self.schema = self.db_config.get('schema', 'public')
            self.logger.info("Successfully connected to Amazon Redshift")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redshift client: {str(e)}")
            raise
    
    def _init_bigquery_client(self) -> None:
        """Initialize Google BigQuery client."""
        try:
            gcp_config = config.get_gcp_config()
            
            # Initialize BigQuery client
            client_kwargs = {}
            if gcp_config.get('project_id'):
                client_kwargs['project'] = gcp_config['project_id']
            
            # Set credentials if specified
            if gcp_config.get('credentials_path'):
                import os
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_config['credentials_path']
            
            self.bq_client = bigquery.Client(**client_kwargs)
            self.dataset_id = self.db_config.get('dataset_id', 'covid_data')
            
            # Ensure dataset exists
            self._ensure_bigquery_dataset()
            
            self.logger.info("Successfully connected to Google BigQuery")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise
    
    def _ensure_bigquery_dataset(self) -> None:
        """Ensure BigQuery dataset exists."""
        try:
            dataset_ref = self.bq_client.dataset(self.dataset_id)
            self.bq_client.get_dataset(dataset_ref)
            self.logger.info(f"BigQuery dataset {self.dataset_id} exists")
            
        except Exception:
            # Create dataset if it doesn't exist
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # or get from config
            
            self.bq_client.create_dataset(dataset)
            self.logger.info(f"Created BigQuery dataset {self.dataset_id}")
    
    def create_tables(self, table_schemas: Dict[str, str]) -> Dict[str, bool]:
        """
        Create tables in the data warehouse.
        
        Args:
            table_schemas: Dictionary mapping table names to CREATE TABLE SQL
            
        Returns:
            Dictionary mapping table names to creation success status
        """
        results = {}
        
        for table_name, schema_sql in table_schemas.items():
            try:
                self.logger.info(f"Creating table: {table_name}")
                
                if self.provider == 'redshift':
                    results[table_name] = self._create_redshift_table(table_name, schema_sql)
                elif self.provider == 'bigquery':
                    results[table_name] = self._create_bigquery_table(table_name, schema_sql)
                
            except Exception as e:
                self.logger.error(f"Failed to create table {table_name}: {str(e)}")
                results[table_name] = False
        
        return results
    
    def _create_redshift_table(self, table_name: str, schema_sql: str) -> bool:
        """Create table in Redshift."""
        try:
            with self.engine.connect() as conn:
                # Drop table if exists (for development)
                drop_sql = f"DROP TABLE IF EXISTS {self.schema}.{table_name}"
                conn.execute(sqlalchemy.text(drop_sql))
                
                # Create table
                conn.execute(sqlalchemy.text(schema_sql))
                conn.commit()
            
            self.logger.info(f"Successfully created Redshift table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create Redshift table {table_name}: {str(e)}")
            return False
    
    def _create_bigquery_table(self, table_name: str, schema_sql: str) -> bool:
        """Create table in BigQuery."""
        try:
            # For BigQuery, we'll use the schema definition to create the table
            # This is a simplified approach - in practice, you'd parse the SQL
            # and convert to BigQuery schema format
            
            table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)
            
            # Check if table exists
            try:
                self.bq_client.get_table(table_ref)
                self.bq_client.delete_table(table_ref)
                self.logger.info(f"Deleted existing BigQuery table: {table_name}")
            except Exception:
                pass  # Table doesn't exist
            
            # Execute CREATE TABLE SQL
            job = self.bq_client.query(schema_sql)
            job.result()  # Wait for completion
            
            self.logger.info(f"Successfully created BigQuery table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create BigQuery table {table_name}: {str(e)}")
            return False
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str,
                      if_exists: str = 'append', method: str = 'multi') -> bool:
        """
        Load DataFrame into data warehouse table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            method: Loading method
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Loading {len(df)} records to table: {table_name}")
            
            if self.provider == 'redshift':
                return self._load_to_redshift(df, table_name, if_exists, method)
            elif self.provider == 'bigquery':
                return self._load_to_bigquery(df, table_name, if_exists)
            
        except Exception as e:
            self.logger.error(f"Failed to load data to {table_name}: {str(e)}")
            return False
    
    def _load_to_redshift(self, df: pd.DataFrame, table_name: str,
                         if_exists: str, method: str) -> bool:
        """Load DataFrame to Redshift."""
        try:
            # Prepare DataFrame for loading
            df_prepared = self._prepare_dataframe_for_redshift(df)
            
            # Load using pandas to_sql
            df_prepared.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=10000  # Load in chunks for better performance
            )
            
            self.logger.info(f"Successfully loaded {len(df)} records to Redshift table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Redshift load failed for {table_name}: {str(e)}")
            return False
    
    def _load_to_bigquery(self, df: pd.DataFrame, table_name: str, if_exists: str) -> bool:
        """Load DataFrame to BigQuery."""
        try:
            # Prepare DataFrame for BigQuery
            df_prepared = self._prepare_dataframe_for_bigquery(df)
            
            table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)
            
            # Configure load job
            job_config = bigquery.LoadJobConfig()
            
            if if_exists == 'replace':
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif if_exists == 'append':
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
            
            job_config.autodetect = True
            
            # Load data
            job = self.bq_client.load_table_from_dataframe(
                df_prepared, table_ref, job_config=job_config
            )
            job.result()  # Wait for completion
            
            self.logger.info(f"Successfully loaded {len(df)} records to BigQuery table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"BigQuery load failed for {table_name}: {str(e)}")
            return False
    
    def _prepare_dataframe_for_redshift(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for Redshift loading."""
        df_prepared = df.copy()
        
        # Handle datetime columns
        datetime_cols = df_prepared.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            df_prepared[col] = df_prepared[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Handle boolean columns (Redshift doesn't have native boolean)
        bool_cols = df_prepared.select_dtypes(include=['bool']).columns
        for col in bool_cols:
            df_prepared[col] = df_prepared[col].astype(int)
        
        # Handle NaN values
        df_prepared = df_prepared.fillna('')
        
        return df_prepared
    
    def _prepare_dataframe_for_bigquery(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for BigQuery loading."""
        df_prepared = df.copy()
        
        # BigQuery handles most data types well, but we need to handle some edge cases
        
        # Convert object columns that might be mixed types
        for col in df_prepared.select_dtypes(include=['object']).columns:
            # Convert to string to avoid mixed type issues
            df_prepared[col] = df_prepared[col].astype(str)
            df_prepared[col] = df_prepared[col].replace('nan', None)
        
        return df_prepared
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Query results as DataFrame (for SELECT queries)
        """
        try:
            self.logger.info(f"Executing SQL query")
            
            if self.provider == 'redshift':
                return self._execute_redshift_sql(sql, params)
            elif self.provider == 'bigquery':
                return self._execute_bigquery_sql(sql, params)
            
        except Exception as e:
            self.logger.error(f"SQL execution failed: {str(e)}")
            raise
    
    def _execute_redshift_sql(self, sql: str, params: Optional[Dict]) -> Optional[pd.DataFrame]:
        """Execute SQL on Redshift."""
        with self.engine.connect() as conn:
            if sql.strip().upper().startswith('SELECT'):
                # Return results for SELECT queries
                return pd.read_sql(sql, conn, params=params)
            else:
                # Execute non-SELECT queries
                conn.execute(sqlalchemy.text(sql), params or {})
                conn.commit()
                return None
    
    def _execute_bigquery_sql(self, sql: str, params: Optional[Dict]) -> Optional[pd.DataFrame]:
        """Execute SQL on BigQuery."""
        job_config = bigquery.QueryJobConfig()
        
        if params:
            # Convert parameters to BigQuery format
            query_parameters = []
            for key, value in params.items():
                param = bigquery.ScalarQueryParameter(key, "STRING", str(value))
                query_parameters.append(param)
            job_config.query_parameters = query_parameters
        
        query_job = self.bq_client.query(sql, job_config=job_config)
        
        if sql.strip().upper().startswith('SELECT'):
            # Return results for SELECT queries
            return query_job.to_dataframe()
        else:
            # Wait for completion for non-SELECT queries
            query_job.result()
            return None
    
    def load_multiple_dataframes(self, data: Dict[str, pd.DataFrame],
                                table_prefix: str = '', if_exists: str = 'append') -> Dict[str, bool]:
        """
        Load multiple DataFrames to warehouse tables.
        
        Args:
            data: Dictionary of DataFrames {name: df}
            table_prefix: Prefix for table names
            if_exists: What to do if tables exist
            
        Returns:
            Dictionary mapping table names to load success status
        """
        results = {}
        
        for data_name, df in data.items():
            if df.empty:
                self.logger.warning(f"Skipping empty DataFrame: {data_name}")
                continue
            
            table_name = f"{table_prefix}{data_name}" if table_prefix else data_name
            
            try:
                success = self.load_dataframe(df, table_name, if_exists)
                results[table_name] = success
                
            except Exception as e:
                self.logger.error(f"Failed to load {data_name} to {table_name}: {str(e)}")
                results[table_name] = False
        
        successful_loads = sum(1 for success in results.values() if success)
        self.logger.info(f"Successfully loaded {successful_loads}/{len(results)} tables")
        
        return results
    
    def validate_data_load(self, table_name: str, expected_count: int) -> Dict[str, Any]:
        """
        Validate data load by checking record counts and basic statistics.
        
        Args:
            table_name: Table to validate
            expected_count: Expected number of records
            
        Returns:
            Validation results
        """
        try:
            self.logger.info(f"Validating data load for table: {table_name}")
            
            # Get table statistics
            if self.provider == 'redshift':
                count_sql = f"SELECT COUNT(*) as record_count FROM {self.schema}.{table_name}"
            else:  # BigQuery
                count_sql = f"SELECT COUNT(*) as record_count FROM `{self.dataset_id}.{table_name}`"
            
            result_df = self.execute_sql(count_sql)
            actual_count = result_df['record_count'].iloc[0] if not result_df.empty else 0
            
            validation_result = {
                'table_name': table_name,
                'expected_count': expected_count,
                'actual_count': actual_count,
                'count_match': actual_count == expected_count,
                'validation_timestamp': datetime.now().isoformat(),
                'is_valid': True,
                'errors': []
            }
            
            # Check if counts match
            if actual_count != expected_count:
                validation_result['is_valid'] = False
                validation_result['errors'].append(
                    f"Record count mismatch: expected {expected_count}, got {actual_count}"
                )
            
            # Additional validations can be added here
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Validation failed for {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'is_valid': False,
                'errors': [f"Validation error: {str(e)}"],
                'validation_timestamp': datetime.now().isoformat()
            }
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Table information
        """
        try:
            if self.provider == 'redshift':
                return self._get_redshift_table_info(table_name)
            elif self.provider == 'bigquery':
                return self._get_bigquery_table_info(table_name)
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def _get_redshift_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get Redshift table information."""
        info_sql = f"""
        SELECT 
            COUNT(*) as record_count,
            MAX(pg_total_relation_size('{self.schema}.{table_name}')) as size_bytes
        FROM {self.schema}.{table_name}
        """
        
        result_df = self.execute_sql(info_sql)
        
        return {
            'table_name': table_name,
            'provider': 'redshift',
            'record_count': result_df['record_count'].iloc[0],
            'size_bytes': result_df['size_bytes'].iloc[0],
            'schema': self.schema
        }
    
    def _get_bigquery_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get BigQuery table information."""
        table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)
        table = self.bq_client.get_table(table_ref)
        
        return {
            'table_name': table_name,
            'provider': 'bigquery',
            'record_count': table.num_rows,
            'size_bytes': table.num_bytes,
            'dataset_id': self.dataset_id,
            'created': table.created.isoformat() if table.created else None,
            'modified': table.modified.isoformat() if table.modified else None
        }
