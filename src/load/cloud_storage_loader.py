"""
Cloud storage loader for COVID-19 ETL Pipeline.
Supports AWS S3 and Google Cloud Storage.
"""

import pandas as pd
import boto3
from google.cloud import storage as gcs
import io
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import os

from ..utils.config import config
from ..utils.logger import get_logger


class CloudStorageLoader:
    """Loader for uploading data to cloud storage (AWS S3 or Google Cloud Storage)."""
    
    def __init__(self, provider: Optional[str] = None):
        """
        Initialize cloud storage loader.
        
        Args:
            provider: Cloud provider ('aws' or 'gcp'). If None, uses config.
        """
        self.logger = get_logger(__name__)
        self.storage_config = config.get_storage_config()
        
        self.provider = provider or self.storage_config.get('provider', 'aws')
        self.bucket_name = self.storage_config.get('bucket')
        
        if not self.bucket_name:
            raise ValueError("Bucket name must be specified in configuration")
        
        # Initialize cloud client
        if self.provider == 'aws':
            self._init_aws_client()
        elif self.provider == 'gcp':
            self._init_gcp_client()
        else:
            raise ValueError(f"Unsupported cloud provider: {self.provider}")
        
        self.logger.info(f"Initialized {self.provider.upper()} storage loader for bucket: {self.bucket_name}")
    
    def _init_aws_client(self) -> None:
        """Initialize AWS S3 client."""
        try:
            aws_config = config.get_aws_config()
            
            # Initialize S3 client
            session_kwargs = {}
            if aws_config.get('access_key_id') and aws_config.get('secret_access_key'):
                session_kwargs.update({
                    'aws_access_key_id': aws_config['access_key_id'],
                    'aws_secret_access_key': aws_config['secret_access_key']
                })
            
            if aws_config.get('region'):
                session_kwargs['region_name'] = aws_config['region']
            
            self.s3_client = boto3.client('s3', **session_kwargs)
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info("Successfully connected to AWS S3")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize AWS S3 client: {str(e)}")
            raise
    
    def _init_gcp_client(self) -> None:
        """Initialize Google Cloud Storage client."""
        try:
            gcp_config = config.get_gcp_config()
            
            # Initialize GCS client
            client_kwargs = {}
            if gcp_config.get('project_id'):
                client_kwargs['project'] = gcp_config['project_id']
            
            # Set credentials if specified
            if gcp_config.get('credentials_path'):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_config['credentials_path']
            
            self.gcs_client = gcs.Client(**client_kwargs)
            self.gcs_bucket = self.gcs_client.bucket(self.bucket_name)
            
            # Test connection
            self.gcs_bucket.reload()
            self.logger.info("Successfully connected to Google Cloud Storage")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP client: {str(e)}")
            raise
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, 
                        file_format: str = 'parquet',
                        metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload DataFrame to cloud storage.
        
        Args:
            df: DataFrame to upload
            key: Storage key/path
            file_format: File format ('parquet', 'csv', 'json')
            metadata: Optional metadata to attach
            
        Returns:
            Storage URL/path of uploaded file
        """
        try:
            self.logger.info(f"Uploading DataFrame to {key} in {file_format} format")
            
            # Convert DataFrame to bytes
            buffer = io.BytesIO()
            
            if file_format.lower() == 'parquet':
                df.to_parquet(buffer, index=False)
                content_type = 'application/octet-stream'
            elif file_format.lower() == 'csv':
                df.to_csv(buffer, index=False)
                content_type = 'text/csv'
            elif file_format.lower() == 'json':
                df.to_json(buffer, orient='records', date_format='iso')
                content_type = 'application/json'
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            buffer.seek(0)
            
            # Upload based on provider
            if self.provider == 'aws':
                return self._upload_to_s3(buffer, key, content_type, metadata)
            elif self.provider == 'gcp':
                return self._upload_to_gcs(buffer, key, content_type, metadata)
            
        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame to {key}: {str(e)}")
            raise
    
    def _upload_to_s3(self, buffer: io.BytesIO, key: str, 
                     content_type: str, metadata: Optional[Dict[str, str]]) -> str:
        """Upload buffer to AWS S3."""
        try:
            extra_args = {
                'ContentType': content_type,
                'Metadata': metadata or {}
            }
            
            # Add standard metadata
            extra_args['Metadata'].update({
                'uploaded_at': datetime.now().isoformat(),
                'source': 'covid-etl-pipeline'
            })
            
            self.s3_client.upload_fileobj(
                buffer, 
                self.bucket_name, 
                key,
                ExtraArgs=extra_args
            )
            
            s3_url = f"s3://{self.bucket_name}/{key}"
            self.logger.info(f"Successfully uploaded to S3: {s3_url}")
            return s3_url
            
        except Exception as e:
            self.logger.error(f"S3 upload failed: {str(e)}")
            raise
    
    def _upload_to_gcs(self, buffer: io.BytesIO, key: str,
                      content_type: str, metadata: Optional[Dict[str, str]]) -> str:
        """Upload buffer to Google Cloud Storage."""
        try:
            blob = self.gcs_bucket.blob(key)
            
            # Set metadata
            if metadata:
                blob.metadata = metadata
            
            blob.metadata = blob.metadata or {}
            blob.metadata.update({
                'uploaded_at': datetime.now().isoformat(),
                'source': 'covid-etl-pipeline'
            })
            
            blob.upload_from_file(buffer, content_type=content_type)
            
            gcs_url = f"gs://{self.bucket_name}/{key}"
            self.logger.info(f"Successfully uploaded to GCS: {gcs_url}")
            return gcs_url
            
        except Exception as e:
            self.logger.error(f"GCS upload failed: {str(e)}")
            raise
    
    def upload_file(self, file_path: str, key: Optional[str] = None,
                   metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload local file to cloud storage.
        
        Args:
            file_path: Local file path
            key: Storage key (if None, uses filename)
            metadata: Optional metadata
            
        Returns:
            Storage URL/path of uploaded file
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if key is None:
                key = file_path.name
            
            self.logger.info(f"Uploading file {file_path} to {key}")
            
            # Determine content type
            content_type = self._get_content_type(file_path.suffix)
            
            with open(file_path, 'rb') as f:
                if self.provider == 'aws':
                    return self._upload_file_to_s3(f, key, content_type, metadata)
                elif self.provider == 'gcp':
                    return self._upload_file_to_gcs(f, key, content_type, metadata)
            
        except Exception as e:
            self.logger.error(f"Failed to upload file {file_path}: {str(e)}")
            raise
    
    def _upload_file_to_s3(self, file_obj, key: str, content_type: str,
                          metadata: Optional[Dict[str, str]]) -> str:
        """Upload file object to S3."""
        extra_args = {
            'ContentType': content_type,
            'Metadata': metadata or {}
        }
        
        extra_args['Metadata'].update({
            'uploaded_at': datetime.now().isoformat(),
            'source': 'covid-etl-pipeline'
        })
        
        self.s3_client.upload_fileobj(file_obj, self.bucket_name, key, ExtraArgs=extra_args)
        return f"s3://{self.bucket_name}/{key}"
    
    def _upload_file_to_gcs(self, file_obj, key: str, content_type: str,
                           metadata: Optional[Dict[str, str]]) -> str:
        """Upload file object to GCS."""
        blob = self.gcs_bucket.blob(key)
        
        if metadata:
            blob.metadata = metadata
        
        blob.metadata = blob.metadata or {}
        blob.metadata.update({
            'uploaded_at': datetime.now().isoformat(),
            'source': 'covid-etl-pipeline'
        })
        
        blob.upload_from_file(file_obj, content_type=content_type)
        return f"gs://{self.bucket_name}/{key}"
    
    def upload_multiple_dataframes(self, data: Dict[str, pd.DataFrame],
                                  prefix: str = '', file_format: str = 'parquet') -> Dict[str, str]:
        """
        Upload multiple DataFrames to cloud storage.
        
        Args:
            data: Dictionary of DataFrames {name: df}
            prefix: Key prefix for organization
            file_format: File format for all uploads
            
        Returns:
            Dictionary mapping data name to storage URL
        """
        uploaded_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for data_name, df in data.items():
            if df.empty:
                self.logger.warning(f"Skipping empty DataFrame: {data_name}")
                continue
            
            # Generate key
            key = f"{prefix}covid_{data_name}_{timestamp}.{file_format}"
            if prefix and not prefix.endswith('/'):
                key = f"{prefix}/{key}"
            
            # Add metadata
            metadata = {
                'data_type': data_name,
                'record_count': str(len(df)),
                'column_count': str(len(df.columns)),
                'file_format': file_format
            }
            
            try:
                storage_url = self.upload_dataframe(df, key, file_format, metadata)
                uploaded_files[data_name] = storage_url
                
            except Exception as e:
                self.logger.error(f"Failed to upload {data_name}: {str(e)}")
                continue
        
        self.logger.info(f"Successfully uploaded {len(uploaded_files)} datasets")
        return uploaded_files
    
    def list_objects(self, prefix: str = '') -> List[Dict[str, Any]]:
        """
        List objects in cloud storage.
        
        Args:
            prefix: Key prefix to filter objects
            
        Returns:
            List of object information
        """
        try:
            if self.provider == 'aws':
                return self._list_s3_objects(prefix)
            elif self.provider == 'gcp':
                return self._list_gcs_objects(prefix)
            
        except Exception as e:
            self.logger.error(f"Failed to list objects: {str(e)}")
            raise
    
    def _list_s3_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """List S3 objects."""
        objects = []
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })
        
        return objects
    
    def _list_gcs_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """List GCS objects."""
        objects = []
        
        blobs = self.gcs_bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            objects.append({
                'key': blob.name,
                'size': blob.size,
                'last_modified': blob.time_created,
                'etag': blob.etag
            })
        
        return objects
    
    def delete_object(self, key: str) -> bool:
        """
        Delete object from cloud storage.
        
        Args:
            key: Object key to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.provider == 'aws':
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            elif self.provider == 'gcp':
                blob = self.gcs_bucket.blob(key)
                blob.delete()
            
            self.logger.info(f"Successfully deleted object: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete object {key}: {str(e)}")
            return False
    
    def download_dataframe(self, key: str, file_format: str = 'parquet') -> pd.DataFrame:
        """
        Download DataFrame from cloud storage.
        
        Args:
            key: Object key
            file_format: Expected file format
            
        Returns:
            Downloaded DataFrame
        """
        try:
            self.logger.info(f"Downloading DataFrame from {key}")
            
            # Download to buffer
            buffer = io.BytesIO()
            
            if self.provider == 'aws':
                self.s3_client.download_fileobj(self.bucket_name, key, buffer)
            elif self.provider == 'gcp':
                blob = self.gcs_bucket.blob(key)
                blob.download_to_file(buffer)
            
            buffer.seek(0)
            
            # Read DataFrame based on format
            if file_format.lower() == 'parquet':
                df = pd.read_parquet(buffer)
            elif file_format.lower() == 'csv':
                df = pd.read_csv(buffer)
            elif file_format.lower() == 'json':
                df = pd.read_json(buffer, orient='records')
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            self.logger.info(f"Successfully downloaded DataFrame with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to download DataFrame from {key}: {str(e)}")
            raise
    
    def _get_content_type(self, file_extension: str) -> str:
        """Get content type based on file extension."""
        content_types = {
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.parquet': 'application/octet-stream',
            '.txt': 'text/plain',
            '.log': 'text/plain'
        }
        
        return content_types.get(file_extension.lower(), 'application/octet-stream')
    
    def get_storage_info(self) -> Dict[str, Any]:
        """
        Get storage information and statistics.
        
        Returns:
            Storage information dictionary
        """
        try:
            info = {
                'provider': self.provider,
                'bucket': self.bucket_name,
                'timestamp': datetime.now().isoformat()
            }
            
            # Get bucket statistics
            objects = self.list_objects()
            
            info.update({
                'total_objects': len(objects),
                'total_size_bytes': sum(obj['size'] for obj in objects),
                'last_modified': max(obj['last_modified'] for obj in objects) if objects else None
            })
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get storage info: {str(e)}")
            return {
                'provider': self.provider,
                'bucket': self.bucket_name,
                'error': str(e)
            }
