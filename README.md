# COVID-19 Automated ETL Pipeline

A complete, production-ready ETL pipeline that extracts COVID-19 data from the disease.sh API, transforms it, and loads it into cloud storage and data warehouses using Apache Airflow orchestration.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   disease.sh    │───▶│  Data Extract   │───▶│  Data Transform │
│      API        │    │   (Python)      │    │   (Pandas)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Data Warehouse  │◀───│  Cloud Storage  │◀───│   Data Load     │
│(Redshift/BigQ.) │    │   (S3/GCS)      │    │   (Python)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │              ┌─────────────────┐              │
         └──────────────│ Apache Airflow  │──────────────┘
                        │  Orchestration  │
                        └─────────────────┘
```

## Features

- **Data Extraction**: Automated COVID-19 data retrieval from disease.sh API
- **Data Transformation**: Comprehensive data cleaning, validation, and enrichment
- **Cloud Storage**: Support for AWS S3 and Google Cloud Storage
- **Data Warehouse**: Integration with Amazon Redshift and Google BigQuery
- **Orchestration**: Apache Airflow DAGs with scheduling and error handling
- **Monitoring**: Data quality validation and pipeline monitoring
- **Scalability**: Modular design for easy extension and maintenance

## Project Structure

```
covid-etl-pipeline/
├── src/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── covid_api_extractor.py
│   │   └── api_client.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── data_transformer.py
│   │   └── data_validator.py
│   ├── load/
│   │   ├── __init__.py
│   │   ├── cloud_storage_loader.py
│   │   └── warehouse_loader.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       ├── logger.py
│       └── helpers.py
├── airflow/
│   ├── dags/
│   │   └── covid_etl_dag.py
│   └── plugins/
├── sql/
│   ├── create_tables/
│   │   ├── redshift_tables.sql
│   │   └── bigquery_tables.sql
│   └── validation/
│       ├── data_quality_checks.sql
│       └── completeness_checks.sql
├── config/
│   ├── config.yaml
│   ├── airflow.cfg
│   └── credentials/
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── docs/
│   ├── setup_guide.md
│   ├── deployment_guide.md
│   └── troubleshooting.md
├── scripts/
│   ├── setup_environment.sh
│   ├── deploy_airflow.sh
│   └── run_pipeline.py
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Quick Start

1. **Clone and Setup**
   ```bash
   cd covid
   pip install -r requirements.txt
   python scripts/setup_environment.py
   ```

2. **Configure Credentials**
   ```bash
   cp config/config.yaml.example config/config.yaml
   # Edit config/config.yaml with your cloud credentials
   ```

3. **Start Airflow**
   ```bash
   docker-compose up -d
   # Access Airflow UI at http://localhost:8080
   ```

4. **Run Pipeline**
   ```bash
   python scripts/run_pipeline.py
   ```

## Data Sources

- **Primary**: [disease.sh COVID-19 API](https://disease.sh/docs/)
- **Endpoints**:
  - Global statistics: `/v3/covid-19/all`
  - Country data: `/v3/covid-19/countries`
  - Historical data: `/v3/covid-19/historical`
  - Vaccine data: `/v3/covid-19/vaccine`

## Data Pipeline Stages

### 1. Extract
- Fetch data from disease.sh API endpoints
- Handle pagination and rate limiting
- Implement retry logic and error handling
- Store raw data with timestamps

### 2. Transform
- Clean and validate data quality
- Handle missing values and outliers
- Standardize date formats and country codes
- Calculate derived metrics (growth rates, etc.)
- Apply business rules and filters

### 3. Load
- Upload processed data to cloud storage (S3/GCS)
- Load data into data warehouse (Redshift/BigQuery)
- Implement upsert logic for incremental updates
- Maintain data lineage and audit trails

### 4. Validate
- Run data quality checks
- Verify completeness and accuracy
- Generate data profiling reports
- Alert on anomalies or failures

## Configuration

The pipeline uses a centralized configuration system:

```yaml
# config/config.yaml
api:
  base_url: "https://disease.sh/v3/covid-19"
  rate_limit: 100
  timeout: 30

storage:
  provider: "aws"  # or "gcp"
  bucket: "covid-data-bucket"
  
warehouse:
  provider: "redshift"  # or "bigquery"
  connection_string: "..."
```

## Monitoring and Alerting

- **Airflow UI**: Pipeline status and logs
- **Data Quality**: Automated validation checks
- **Alerts**: Email/Slack notifications on failures
- **Metrics**: Pipeline performance and data freshness

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Apache Airflow 2.5+
- Cloud account (AWS or GCP)
- Database access (Redshift or BigQuery)

## Documentation

- [Setup Guide](docs/setup_guide.md) - Detailed installation instructions
- [Deployment Guide](docs/deployment_guide.md) - Production deployment
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For questions or issues, please create an issue in the repository or contact the development team.
