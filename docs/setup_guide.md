# COVID-19 ETL Pipeline Setup Guide

This guide provides detailed instructions for setting up and configuring the COVID-19 ETL Pipeline in different environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Configuration](#configuration)
4. [Cloud Setup](#cloud-setup)
5. [Database Setup](#database-setup)
6. [Airflow Setup](#airflow-setup)
7. [Testing](#testing)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

- **Operating System**: Linux, macOS, or Windows 10/11
- **Python**: 3.8 or higher
- **Memory**: Minimum 8GB RAM (16GB recommended)
- **Storage**: At least 10GB free space
- **Network**: Internet connection for API access

### Required Accounts

- **Cloud Storage**: AWS S3 or Google Cloud Storage account
- **Data Warehouse**: Amazon Redshift or Google BigQuery access
- **Email**: SMTP server for notifications (optional)

## Environment Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd covid-etl-pipeline
```

### 2. Run Automated Setup

```bash
python scripts/setup_environment.py
```

This script will:
- Check Python version compatibility
- Create necessary directories
- Install Python dependencies
- Generate configuration templates
- Set up Docker files

### 3. Manual Setup (Alternative)

If the automated setup fails, follow these manual steps:

#### Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

#### Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### Create Directories

```bash
mkdir -p data/raw data/processed logs config/credentials airflow/plugins tests docs
```

## Configuration

### 1. Environment Variables

Copy the `.env.example` file to `.env` and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:

```env
# API Configuration
COVID_API_BASE_URL=https://disease.sh/v3/covid-19
COVID_API_RATE_LIMIT=100

# Storage Configuration
STORAGE_PROVIDER=aws  # or 'gcp'
STORAGE_BUCKET=your-covid-data-bucket

# AWS Configuration (if using AWS)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1

# GCP Configuration (if using GCP)
GCP_PROJECT_ID=your_gcp_project_id
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json

# Database Configuration
DB_HOST=your_database_host
DB_PORT=5439
DB_NAME=covid_data
DB_USER=your_database_user
DB_PASSWORD=your_database_password

# Notification Configuration
SMTP_USERNAME=your_smtp_username
SMTP_PASSWORD=your_smtp_password
SLACK_WEBHOOK_URL=your_slack_webhook_url
```

### 2. Configuration File

Edit `config/config.yaml` to customize pipeline behavior:

```yaml
# Example configuration
api:
  rate_limit: 100
  timeout: 30

storage:
  provider: "aws"
  bucket: "your-bucket-name"
  file_format: "parquet"

airflow:
  schedule_interval: "@daily"
  retries: 2
```

## Cloud Setup

### AWS Setup

#### 1. Create S3 Bucket

```bash
aws s3 mb s3://your-covid-data-bucket
```

#### 2. Set Bucket Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::YOUR_ACCOUNT_ID:user/YOUR_USER"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::your-covid-data-bucket/*"
        }
    ]
}
```

#### 3. Create IAM User

Create an IAM user with the following permissions:
- `AmazonS3FullAccess` (or custom policy above)
- `AmazonRedshiftFullAccess` (if using Redshift)

### Google Cloud Setup

#### 1. Create Storage Bucket

```bash
gsutil mb gs://your-covid-data-bucket
```

#### 2. Create Service Account

```bash
gcloud iam service-accounts create covid-etl-service \
    --display-name="COVID ETL Service Account"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:covid-etl-service@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

gcloud iam service-accounts keys create credentials.json \
    --iam-account=covid-etl-service@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

## Database Setup

### Amazon Redshift

#### 1. Create Cluster

```bash
aws redshift create-cluster \
    --cluster-identifier covid-data-cluster \
    --node-type dc2.large \
    --master-username admin \
    --master-user-password YourPassword123 \
    --db-name covid_data
```

#### 2. Create Tables

```bash
psql -h your-redshift-endpoint -U admin -d covid_data -f sql/create_tables/redshift_tables.sql
```

### Google BigQuery

#### 1. Create Dataset

```bash
bq mk --dataset YOUR_PROJECT_ID:covid_data
```

#### 2. Create Tables

```bash
bq query --use_legacy_sql=false < sql/create_tables/bigquery_tables.sql
```

## Airflow Setup

### Option 1: Docker Compose (Recommended)

```bash
# Start services
docker-compose up -d

# Check status
docker-compose ps

# Access Airflow UI
# Open http://localhost:8080 in your browser
# Username: admin, Password: admin
```

### Option 2: Local Installation

```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start webserver
airflow webserver --port 8080 &

# Start scheduler
airflow scheduler &
```

## Testing

### 1. Test API Connection

```python
python -c "
from src.extract.covid_api_extractor import CovidDataExtractor
extractor = CovidDataExtractor()
data = extractor.extract_global_data()
print(f'Successfully extracted {len(data)} records')
"
```

### 2. Test Cloud Storage

```python
python -c "
from src.load.cloud_storage_loader import CloudStorageLoader
import pandas as pd
loader = CloudStorageLoader()
df = pd.DataFrame({'test': [1, 2, 3]})
url = loader.upload_dataframe(df, 'test/test.parquet')
print(f'Successfully uploaded to: {url}')
"
```

### 3. Test Database Connection

```python
python -c "
from src.load.warehouse_loader import DataWarehouseLoader
loader = DataWarehouseLoader()
result = loader.execute_sql('SELECT 1 as test')
print(f'Database connection successful: {result}')
"
```

### 4. Run Manual Pipeline

```bash
python scripts/run_pipeline.py --skip-cloud --skip-warehouse
```

### 5. Run Unit Tests

```bash
pytest tests/ -v
```

## Verification Checklist

- [ ] Python 3.8+ installed
- [ ] All dependencies installed successfully
- [ ] Environment variables configured
- [ ] Cloud storage accessible
- [ ] Database connection working
- [ ] Airflow UI accessible
- [ ] API connection successful
- [ ] Manual pipeline runs successfully

## Next Steps

1. **Configure Monitoring**: Set up alerts and monitoring dashboards
2. **Schedule Pipeline**: Enable the Airflow DAG for automated runs
3. **Data Quality**: Review and customize data quality checks
4. **Security**: Implement proper security measures for production
5. **Scaling**: Configure auto-scaling for high-volume processing

## Troubleshooting

### Common Issues

#### Import Errors
```bash
# Fix Python path issues
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

#### Permission Errors
```bash
# Fix file permissions
chmod +x scripts/*.py
```

#### Docker Issues
```bash
# Reset Docker environment
docker-compose down -v
docker-compose up -d
```

#### Database Connection Issues
- Verify network connectivity
- Check firewall settings
- Validate credentials
- Ensure database is running

For more detailed troubleshooting, see [troubleshooting.md](troubleshooting.md).

## Support

- **Documentation**: Check the `docs/` directory
- **Issues**: Create an issue in the repository
- **Logs**: Check `logs/covid_etl.log` for detailed error messages
