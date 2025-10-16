# COVID-19 ETL Pipeline Troubleshooting Guide

This guide provides solutions to common issues you might encounter while setting up, configuring, or running the COVID-19 ETL Pipeline.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Configuration Problems](#configuration-problems)
3. [API Connection Issues](#api-connection-issues)
4. [Database Connection Problems](#database-connection-problems)
5. [Cloud Storage Issues](#cloud-storage-issues)
6. [Airflow Problems](#airflow-problems)
7. [Performance Issues](#performance-issues)
8. [Data Quality Issues](#data-quality-issues)
9. [Monitoring and Logging](#monitoring-and-logging)
10. [Common Error Messages](#common-error-messages)

## Installation Issues

### Python Version Compatibility

**Problem**: `ImportError` or compatibility issues with Python packages.

**Solution**:
```bash
# Check Python version
python --version

# Ensure Python 3.8+
# If using wrong version, install correct Python version
# On Ubuntu/Debian:
sudo apt update
sudo apt install python3.9 python3.9-pip python3.9-venv

# Create virtual environment with specific Python version
python3.9 -m venv venv
source venv/bin/activate
```

### Dependency Installation Failures

**Problem**: `pip install` fails with compilation errors.

**Solution**:
```bash
# Update pip and setuptools
pip install --upgrade pip setuptools wheel

# Install system dependencies (Ubuntu/Debian)
sudo apt-get install build-essential python3-dev libpq-dev

# Install system dependencies (CentOS/RHEL)
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel postgresql-devel

# For macOS
brew install postgresql

# Retry installation
pip install -r requirements.txt
```

### Permission Errors

**Problem**: Permission denied when creating directories or files.

**Solution**:
```bash
# Fix ownership
sudo chown -R $USER:$USER /path/to/covid-etl-pipeline

# Fix permissions
chmod +x scripts/*.py
chmod -R 755 data/ logs/

# Create directories with proper permissions
mkdir -p data/{raw,processed} logs config/credentials
```

## Configuration Problems

### Environment Variables Not Loading

**Problem**: Configuration values not being read from environment variables.

**Solution**:
```bash
# Check if .env file exists
ls -la .env

# Verify environment variables are set
env | grep COVID
env | grep AWS
env | grep DB_

# Load environment variables manually
export $(cat .env | xargs)

# Verify in Python
python -c "import os; print(os.getenv('AWS_ACCESS_KEY_ID'))"
```

### YAML Configuration Errors

**Problem**: `yaml.scanner.ScannerError` when loading config.yaml.

**Solution**:
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config/config.yaml'))"

# Common YAML issues:
# - Incorrect indentation (use spaces, not tabs)
# - Missing quotes around special characters
# - Unescaped colons in values

# Example fix:
# Wrong: password: my:password
# Right: password: "my:password"
```

### Missing Configuration Files

**Problem**: `FileNotFoundError` for configuration files.

**Solution**:
```bash
# Create missing configuration files
python scripts/setup_environment.py

# Or manually create from templates
cp config/config.yaml.example config/config.yaml
cp .env.example .env

# Verify file paths
ls -la config/
ls -la .env
```

## API Connection Issues

### Rate Limiting

**Problem**: `HTTP 429 Too Many Requests` from disease.sh API.

**Solution**:
```python
# Adjust rate limiting in config.yaml
api:
  rate_limit: 50  # Reduce from 100 to 50 requests per minute
  retry_delay: 2  # Increase delay between retries

# Or set environment variable
export COVID_API_RATE_LIMIT=50
```

### Network Connectivity

**Problem**: `ConnectionError` or `TimeoutError` when accessing API.

**Solution**:
```bash
# Test network connectivity
curl -I https://disease.sh/v3/covid-19/all

# Check DNS resolution
nslookup disease.sh

# Test with different timeout
curl --connect-timeout 30 https://disease.sh/v3/covid-19/all

# Check firewall/proxy settings
# If behind corporate firewall, configure proxy:
export HTTP_PROXY=http://proxy.company.com:8080
export HTTPS_PROXY=http://proxy.company.com:8080
```

### SSL Certificate Issues

**Problem**: `SSLError` when making API requests.

**Solution**:
```python
# Temporary workaround (not recommended for production)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Better solution: Update certificates
# On Ubuntu/Debian:
sudo apt-get update && sudo apt-get install ca-certificates

# On CentOS/RHEL:
sudo yum update ca-certificates

# On macOS:
brew install ca-certificates
```

## Database Connection Problems

### Connection Refused

**Problem**: `psycopg2.OperationalError: could not connect to server`.

**Solution**:
```bash
# Check if database is running
# For PostgreSQL:
sudo systemctl status postgresql
sudo systemctl start postgresql

# For Docker:
docker ps | grep postgres
docker-compose up postgres -d

# Test connection manually
psql -h localhost -U airflow_prod -d airflow_prod

# Check network connectivity
telnet your-db-host 5432
```

### Authentication Failed

**Problem**: `FATAL: password authentication failed`.

**Solution**:
```bash
# Verify credentials
echo $DB_PASSWORD

# Reset password (PostgreSQL)
sudo -u postgres psql
ALTER USER airflow_prod PASSWORD 'new_password';

# Update configuration
# In .env file:
DB_PASSWORD=new_password

# For cloud databases, check security groups/firewall rules
```

### Database Does Not Exist

**Problem**: `FATAL: database "covid_data" does not exist`.

**Solution**:
```sql
-- Connect as superuser and create database
sudo -u postgres psql
CREATE DATABASE covid_data;
CREATE USER airflow_prod WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE covid_data TO airflow_prod;

-- Or run initialization script
psql -h localhost -U postgres -f sql/create_tables/redshift_tables.sql
```

## Cloud Storage Issues

### AWS S3 Access Denied

**Problem**: `botocore.exceptions.ClientError: An error occurred (AccessDenied)`.

**Solution**:
```bash
# Check AWS credentials
aws configure list
aws sts get-caller-identity

# Verify S3 bucket exists and permissions
aws s3 ls s3://your-bucket-name
aws s3api get-bucket-policy --bucket your-bucket-name

# Test with AWS CLI
aws s3 cp test.txt s3://your-bucket-name/test.txt

# Check IAM policy
aws iam get-user-policy --user-name your-user --policy-name your-policy
```

### Google Cloud Storage Permission Denied

**Problem**: `google.api_core.exceptions.Forbidden: 403 Forbidden`.

**Solution**:
```bash
# Check authentication
gcloud auth list
gcloud auth application-default login

# Verify service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Test access
gsutil ls gs://your-bucket-name

# Check IAM permissions
gcloud projects get-iam-policy your-project-id
```

### Bucket Not Found

**Problem**: `NoSuchBucket` or `404 Not Found` errors.

**Solution**:
```bash
# Create bucket (AWS)
aws s3 mb s3://your-bucket-name --region us-east-1

# Create bucket (GCP)
gsutil mb gs://your-bucket-name

# Verify bucket name in configuration
grep -r "bucket" config/config.yaml
```

## Airflow Problems

### Airflow Won't Start

**Problem**: Airflow webserver or scheduler fails to start.

**Solution**:
```bash
# Check Airflow configuration
airflow config list

# Initialize database
airflow db init

# Check for port conflicts
netstat -tulpn | grep :8080
lsof -i :8080

# Start with debug logging
airflow webserver --debug

# Check logs
tail -f logs/airflow-webserver.log
```

### DAG Import Errors

**Problem**: DAGs not appearing in Airflow UI or import errors.

**Solution**:
```bash
# Check DAG syntax
python airflow/dags/covid_etl_dag.py

# Verify Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Check DAG folder permissions
ls -la airflow/dags/

# Test DAG parsing
airflow dags list
airflow dags show covid_etl_pipeline
```

### Task Failures

**Problem**: Airflow tasks failing with various errors.

**Solution**:
```bash
# Check task logs
airflow tasks log covid_etl_pipeline extract_data 2024-01-01

# Test task manually
airflow tasks test covid_etl_pipeline extract_data 2024-01-01

# Check worker logs
docker-compose logs airflow-worker

# Increase task timeout
# In DAG configuration:
default_args = {
    'execution_timeout': timedelta(hours=2)
}
```

### Celery Worker Issues

**Problem**: Celery workers not processing tasks.

**Solution**:
```bash
# Check worker status
celery -A airflow.executors.celery_executor inspect active

# Restart workers
docker-compose restart airflow-worker

# Check Redis connection
redis-cli ping

# Monitor worker logs
docker-compose logs -f airflow-worker
```

## Performance Issues

### Slow Data Processing

**Problem**: ETL pipeline takes too long to complete.

**Solution**:
```python
# Optimize pandas operations
# Use chunking for large datasets
chunk_size = 10000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    process_chunk(chunk)

# Use parallel processing
from multiprocessing import Pool
with Pool(processes=4) as pool:
    results = pool.map(process_function, data_chunks)

# Optimize database operations
# Use bulk inserts instead of row-by-row
df.to_sql('table_name', engine, if_exists='append', method='multi', chunksize=1000)
```

### Memory Issues

**Problem**: `MemoryError` or out-of-memory errors.

**Solution**:
```python
# Monitor memory usage
import psutil
print(f"Memory usage: {psutil.virtual_memory().percent}%")

# Use memory-efficient data types
df = df.astype({
    'country': 'category',
    'cases': 'int32',  # instead of int64
    'deaths': 'int32'
})

# Process data in chunks
def process_in_chunks(df, chunk_size=10000):
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        yield process_chunk(chunk)

# Increase Docker memory limits
# In docker-compose.yml:
services:
  airflow-worker:
    deploy:
      resources:
        limits:
          memory: 4G
```

### Database Performance

**Problem**: Slow database queries or connection timeouts.

**Solution**:
```sql
-- Add indexes for better query performance
CREATE INDEX idx_countries_country_date ON covid_countries (country, extraction_date);
CREATE INDEX idx_historical_country_metric_date ON covid_historical (country, metric, date);

-- Analyze query performance
EXPLAIN ANALYZE SELECT * FROM covid_countries WHERE country = 'USA';

-- Optimize PostgreSQL settings
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
SELECT pg_reload_conf();
```

## Data Quality Issues

### Missing Data

**Problem**: Expected data not appearing in results.

**Solution**:
```python
# Add data validation checks
def validate_data(df):
    assert not df.empty, "DataFrame is empty"
    assert 'country' in df.columns, "Missing country column"
    assert df['country'].notna().all(), "Null values in country column"
    
    # Check for expected countries
    expected_countries = ['USA', 'China', 'India', 'Brazil']
    missing_countries = set(expected_countries) - set(df['country'].unique())
    if missing_countries:
        print(f"Warning: Missing countries: {missing_countries}")

# Add logging for debugging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.info(f"Extracted {len(df)} records")
logger.debug(f"Countries: {df['country'].unique()}")
```

### Data Type Issues

**Problem**: Incorrect data types causing processing errors.

**Solution**:
```python
# Explicit data type conversion
df['cases'] = pd.to_numeric(df['cases'], errors='coerce')
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Handle missing values before type conversion
df['cases'] = df['cases'].fillna(0).astype(int)

# Validate data types
def validate_types(df):
    type_checks = {
        'cases': 'int64',
        'deaths': 'int64',
        'date': 'datetime64[ns]'
    }
    
    for col, expected_type in type_checks.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            assert actual_type == expected_type, f"{col}: expected {expected_type}, got {actual_type}"
```

## Monitoring and Logging

### Missing Logs

**Problem**: Log files not being created or updated.

**Solution**:
```bash
# Check log directory permissions
ls -la logs/
chmod 755 logs/

# Verify logging configuration
python -c "
from src.utils.logger import get_logger
logger = get_logger('test')
logger.info('Test message')
"

# Check Airflow logging configuration
grep -r "log" airflow/airflow.cfg
```

### Log Rotation Issues

**Problem**: Log files growing too large.

**Solution**:
```python
# Configure log rotation in Python
import logging.handlers

handler = logging.handlers.RotatingFileHandler(
    'logs/covid_etl.log',
    maxBytes=100*1024*1024,  # 100MB
    backupCount=5
)

# Or use system logrotate
# Create /etc/logrotate.d/covid-etl:
/path/to/covid-etl-pipeline/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
}
```

## Common Error Messages

### `ModuleNotFoundError: No module named 'src'`

**Solution**:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
# Or add to .bashrc/.zshrc for persistence
echo 'export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"' >> ~/.bashrc
```

### `FileNotFoundError: [Errno 2] No such file or directory: 'config/config.yaml'`

**Solution**:
```bash
# Ensure you're in the project root directory
cd /path/to/covid-etl-pipeline

# Create missing config file
python scripts/setup_environment.py
```

### `requests.exceptions.ConnectionError: HTTPSConnectionPool`

**Solution**:
```bash
# Check internet connectivity
ping google.com

# Test API endpoint directly
curl https://disease.sh/v3/covid-19/all

# Check proxy settings if behind corporate firewall
export HTTP_PROXY=http://proxy:8080
export HTTPS_PROXY=http://proxy:8080
```

### `sqlalchemy.exc.OperationalError: (psycopg2.OperationalError)`

**Solution**:
```bash
# Check database connection
pg_isready -h localhost -p 5432

# Verify credentials
psql -h localhost -U airflow_prod -d airflow_prod

# Check if database service is running
docker-compose ps postgres
```

### `botocore.exceptions.NoCredentialsError: Unable to locate credentials`

**Solution**:
```bash
# Set AWS credentials
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Or use IAM roles (recommended for EC2/ECS)
```

## Getting Help

### Enable Debug Logging

```python
# In your Python scripts
import logging
logging.basicConfig(level=logging.DEBUG)

# In configuration
logging:
  level: "DEBUG"
```

### Collect System Information

```bash
# System info script
cat > debug_info.sh << 'EOF'
#!/bin/bash
echo "=== System Information ==="
uname -a
python --version
pip --version

echo "=== Environment Variables ==="
env | grep -E "(COVID|AWS|DB_|AIRFLOW)" | sort

echo "=== Disk Space ==="
df -h

echo "=== Memory Usage ==="
free -h

echo "=== Network Connectivity ==="
curl -I https://disease.sh/v3/covid-19/all

echo "=== Docker Status ==="
docker-compose ps

echo "=== Log Files ==="
ls -la logs/
EOF

chmod +x debug_info.sh
./debug_info.sh > debug_report.txt
```

### Check Service Status

```bash
# Check all services
docker-compose ps

# Check specific service logs
docker-compose logs airflow-webserver
docker-compose logs postgres
docker-compose logs redis

# Check system resources
htop
iotop
```

### Contact Support

When reporting issues, please include:

1. **Error message**: Full error traceback
2. **Environment**: OS, Python version, Docker version
3. **Configuration**: Relevant config files (remove sensitive data)
4. **Logs**: Recent log entries
5. **Steps to reproduce**: What you were doing when the error occurred
6. **System info**: Output from debug_info.sh script

### Useful Commands for Debugging

```bash
# Check Python imports
python -c "import sys; print('\n'.join(sys.path))"

# Validate configuration
python -c "from src.utils.config import config; print(config.to_dict())"

# Test database connection
python -c "from src.load.warehouse_loader import DataWarehouseLoader; loader = DataWarehouseLoader(); print('DB connection OK')"

# Test cloud storage
python -c "from src.load.cloud_storage_loader import CloudStorageLoader; loader = CloudStorageLoader(); print('Storage connection OK')"

# Check Airflow DAG
airflow dags list | grep covid
airflow dags show covid_etl_pipeline
```
