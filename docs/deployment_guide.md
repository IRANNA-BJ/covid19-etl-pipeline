# COVID-19 ETL Pipeline Deployment Guide

This guide covers deploying the COVID-19 ETL Pipeline to production environments including cloud platforms, containerization, and monitoring setup.

## Table of Contents

1. [Deployment Architecture](#deployment-architecture)
2. [Production Environment Setup](#production-environment-setup)
3. [Container Deployment](#container-deployment)
4. [Cloud Platform Deployment](#cloud-platform-deployment)
5. [Monitoring and Alerting](#monitoring-and-alerting)
6. [Security Configuration](#security-configuration)
7. [Performance Optimization](#performance-optimization)
8. [Backup and Recovery](#backup-and-recovery)

## Deployment Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   Airflow Web   │    │   Airflow       │
│                 │───▶│   Server        │    │   Scheduler     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Monitoring    │    │   PostgreSQL    │    │   Redis         │
│   (Prometheus)  │    │   (Metadata)    │    │   (Message      │
│                 │    │                 │    │    Broker)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cloud Storage │    │   Data          │    │   Airflow       │
│   (S3/GCS)      │    │   Warehouse     │    │   Workers       │
│                 │    │   (Redshift/BQ) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Component Overview

- **Airflow Webserver**: Web UI and API
- **Airflow Scheduler**: Task scheduling and orchestration
- **Airflow Workers**: Task execution (can be scaled horizontally)
- **PostgreSQL**: Airflow metadata database
- **Redis**: Message broker for Celery executor
- **Cloud Storage**: Raw and processed data storage
- **Data Warehouse**: Final data destination
- **Monitoring**: System and pipeline monitoring

## Production Environment Setup

### 1. Infrastructure Requirements

#### Minimum Production Setup
- **CPU**: 4 cores per component
- **Memory**: 8GB RAM per component
- **Storage**: 100GB SSD
- **Network**: 1Gbps bandwidth

#### Recommended Production Setup
- **CPU**: 8 cores per component
- **Memory**: 16GB RAM per component
- **Storage**: 500GB SSD with backup
- **Network**: 10Gbps bandwidth

### 2. Environment Configuration

#### Production Environment Variables

```bash
# Environment
ENVIRONMENT=production
DEBUG=false

# Database
DB_HOST=prod-postgres.internal
DB_PORT=5432
DB_NAME=airflow_prod
DB_USER=airflow_prod
DB_PASSWORD=secure_password_here

# Redis
REDIS_HOST=prod-redis.internal
REDIS_PORT=6379
REDIS_PASSWORD=redis_password_here

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_prod:secure_password@prod-postgres.internal:5432/airflow_prod
AIRFLOW__CELERY__BROKER_URL=redis://:redis_password@prod-redis.internal:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow_prod:secure_password@prod-postgres.internal:5432/airflow_prod

# Security
AIRFLOW__CORE__FERNET_KEY=your_32_character_fernet_key_here
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key_here

# Logging
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://your-logs-bucket/airflow-logs
```

## Container Deployment

### 1. Docker Production Setup

#### Production Dockerfile

```dockerfile
FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install monitoring tools
RUN curl -L https://github.com/prometheus/node_exporter/releases/download/v1.6.1/node_exporter-1.6.1.linux-amd64.tar.gz \
    | tar xz -C /tmp && \
    mv /tmp/node_exporter-*/node_exporter /usr/local/bin/

USER airflow

# Copy requirements and install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy application code
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
```

#### Production Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow_prod
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: airflow_prod
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/init:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    restart: always
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow_prod"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    command: redis-server --requirepass ${REDIS_PASSWORD}
    ports:
      - "6379:6379"
    restart: always
    healthcheck:
      test: ["CMD", "redis-cli", "--raw", "incr", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-webserver:
    build: .
    command: webserver
    ports:
      - "8080:8080"
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__BROKER_URL=redis://:${REDIS_PASSWORD}@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
      - AIRFLOW__WEBSERVER__SECRET_KEY=${SECRET_KEY}
    volumes:
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: always
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  airflow-scheduler:
    build: .
    command: scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__BROKER_URL=redis://:${REDIS_PASSWORD}@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
    volumes:
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: always

  airflow-worker:
    build: .
    command: celery worker
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow_prod:${DB_PASSWORD}@postgres/airflow_prod
      - AIRFLOW__CELERY__BROKER_URL=redis://:${REDIS_PASSWORD}@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
    volumes:
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: always
    deploy:
      replicas: 3

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
    restart: always

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana:/etc/grafana/provisioning
    restart: always

volumes:
  postgres_data:
  prometheus_data:
  grafana_data:
```

### 2. Kubernetes Deployment

#### Kubernetes Manifests

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: covid-etl

---
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
  namespace: covid-etl
data:
  airflow.cfg: |
    [core]
    executor = CeleryExecutor
    load_examples = False
    
    [webserver]
    expose_config = True
    
    [celery]
    worker_concurrency = 4

---
# secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: airflow-secrets
  namespace: covid-etl
type: Opaque
stringData:
  postgres-password: "your_postgres_password"
  redis-password: "your_redis_password"
  fernet-key: "your_fernet_key"

---
# postgres.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: covid-etl
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:13
        env:
        - name: POSTGRES_DB
          value: airflow_prod
        - name: POSTGRES_USER
          value: airflow_prod
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: postgres-password
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
      volumes:
      - name: postgres-storage
        persistentVolumeClaim:
          claimName: postgres-pvc

---
# airflow-webserver.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: covid-etl
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow-webserver
  template:
    metadata:
      labels:
        app: airflow-webserver
    spec:
      containers:
      - name: webserver
        image: your-registry/covid-etl:latest
        command: ["airflow", "webserver"]
        ports:
        - containerPort: 8080
        env:
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          value: "postgresql+psycopg2://airflow_prod:$(POSTGRES_PASSWORD)@postgres:5432/airflow_prod"
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: postgres-password
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
```

## Cloud Platform Deployment

### 1. AWS Deployment

#### ECS Deployment

```json
{
  "family": "covid-etl-airflow",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096",
  "executionRoleArn": "arn:aws:iam::ACCOUNT:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::ACCOUNT:role/covid-etl-task-role",
  "containerDefinitions": [
    {
      "name": "airflow-webserver",
      "image": "your-account.dkr.ecr.region.amazonaws.com/covid-etl:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "AIRFLOW__CORE__EXECUTOR",
          "value": "CeleryExecutor"
        }
      ],
      "secrets": [
        {
          "name": "DB_PASSWORD",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:covid-etl/db-password"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/covid-etl",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

#### CloudFormation Template

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'COVID-19 ETL Pipeline Infrastructure'

Parameters:
  VpcId:
    Type: AWS::EC2::VPC::Id
  SubnetIds:
    Type: List<AWS::EC2::Subnet::Id>

Resources:
  # ECS Cluster
  ECSCluster:
    Type: AWS::ECS::Cluster
    Properties:
      ClusterName: covid-etl-cluster
      CapacityProviders:
        - FARGATE
        - FARGATE_SPOT

  # RDS Instance
  RDSInstance:
    Type: AWS::RDS::DBInstance
    Properties:
      DBInstanceIdentifier: covid-etl-postgres
      DBInstanceClass: db.t3.medium
      Engine: postgres
      EngineVersion: '13.7'
      MasterUsername: airflow_prod
      MasterUserPassword: !Ref DBPassword
      AllocatedStorage: 100
      VPCSecurityGroups:
        - !Ref DatabaseSecurityGroup
      DBSubnetGroupName: !Ref DBSubnetGroup

  # ElastiCache Redis
  RedisCluster:
    Type: AWS::ElastiCache::CacheCluster
    Properties:
      CacheNodeType: cache.t3.micro
      Engine: redis
      NumCacheNodes: 1
      VpcSecurityGroupIds:
        - !Ref RedisSecurityGroup
      CacheSubnetGroupName: !Ref RedisSubnetGroup

  # S3 Bucket
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub 'covid-etl-data-${AWS::AccountId}'
      VersioningConfiguration:
        Status: Enabled
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true
```

### 2. Google Cloud Deployment

#### Cloud Run Deployment

```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: covid-etl-webserver
  annotations:
    run.googleapis.com/ingress: all
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/maxScale: "10"
        run.googleapis.com/cpu-throttling: "false"
    spec:
      containerConcurrency: 80
      containers:
      - image: gcr.io/PROJECT_ID/covid-etl:latest
        ports:
        - containerPort: 8080
        env:
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: database-url
        resources:
          limits:
            cpu: "2"
            memory: "4Gi"
          requests:
            cpu: "1"
            memory: "2Gi"
```

#### Terraform Configuration

```hcl
# main.tf
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_sql_database_instance" "postgres" {
  name             = "covid-etl-postgres"
  database_version = "POSTGRES_13"
  region           = var.region

  settings {
    tier = "db-f1-micro"
    
    backup_configuration {
      enabled = true
    }
    
    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        value = "0.0.0.0/0"
      }
    }
  }
}

resource "google_storage_bucket" "data_bucket" {
  name     = "${var.project_id}-covid-etl-data"
  location = var.region
  
  versioning {
    enabled = true
  }
}

resource "google_cloud_run_service" "webserver" {
  name     = "covid-etl-webserver"
  location = var.region

  template {
    spec {
      containers {
        image = "gcr.io/${var.project_id}/covid-etl:latest"
        
        ports {
          container_port = 8080
        }
        
        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
      }
    }
  }
}
```

## Monitoring and Alerting

### 1. Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/admin/metrics'

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres:5432']

  - job_name: 'redis'
    static_configs:
      - targets: ['redis:6379']

rule_files:
  - "alert_rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### 2. Alert Rules

```yaml
# alert_rules.yml
groups:
- name: covid_etl_alerts
  rules:
  - alert: AirflowDown
    expr: up{job="airflow"} == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Airflow is down"
      description: "Airflow has been down for more than 5 minutes"

  - alert: DAGFailure
    expr: airflow_dag_run_failed_total > 0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "DAG run failed"
      description: "COVID ETL DAG run has failed"

  - alert: HighMemoryUsage
    expr: (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes > 0.9
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage"
      description: "Memory usage is above 90%"
```

### 3. Grafana Dashboards

```json
{
  "dashboard": {
    "title": "COVID ETL Pipeline",
    "panels": [
      {
        "title": "DAG Success Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(airflow_dag_run_success_total[5m])"
          }
        ]
      },
      {
        "title": "Task Duration",
        "type": "graph",
        "targets": [
          {
            "expr": "airflow_task_duration_seconds"
          }
        ]
      }
    ]
  }
}
```

## Security Configuration

### 1. Network Security

```yaml
# Security Groups (AWS)
DatabaseSecurityGroup:
  Type: AWS::EC2::SecurityGroup
  Properties:
    GroupDescription: Security group for RDS database
    VpcId: !Ref VpcId
    SecurityGroupIngress:
      - IpProtocol: tcp
        FromPort: 5432
        ToPort: 5432
        SourceSecurityGroupId: !Ref ApplicationSecurityGroup

ApplicationSecurityGroup:
  Type: AWS::EC2::SecurityGroup
  Properties:
    GroupDescription: Security group for application
    VpcId: !Ref VpcId
    SecurityGroupIngress:
      - IpProtocol: tcp
        FromPort: 8080
        ToPort: 8080
        CidrIp: 10.0.0.0/8
```

### 2. IAM Roles and Policies

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::covid-etl-data-*/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "redshift:DescribeClusters",
        "redshift:GetClusterCredentials"
      ],
      "Resource": "*"
    }
  ]
}
```

### 3. Secrets Management

```bash
# AWS Secrets Manager
aws secretsmanager create-secret \
    --name covid-etl/database \
    --description "Database credentials for COVID ETL" \
    --secret-string '{"username":"airflow_prod","password":"secure_password"}'

# Google Secret Manager
gcloud secrets create covid-etl-database \
    --data-file=database-credentials.json
```

## Performance Optimization

### 1. Resource Allocation

```yaml
# Airflow Configuration
[celery]
worker_concurrency = 4
worker_prefetch_multiplier = 1

[core]
parallelism = 32
dag_concurrency = 16
max_active_runs_per_dag = 1

[scheduler]
catchup_by_default = False
max_threads = 2
```

### 2. Database Optimization

```sql
-- PostgreSQL optimizations
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

SELECT pg_reload_conf();
```

### 3. Caching Strategy

```python
# Redis caching configuration
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'redis://redis:6379/1',
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
        }
    }
}
```

## Backup and Recovery

### 1. Database Backup

```bash
#!/bin/bash
# backup_database.sh

BACKUP_DIR="/backups/postgres"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="airflow_backup_${TIMESTAMP}.sql"

# Create backup
pg_dump -h postgres -U airflow_prod -d airflow_prod > "${BACKUP_DIR}/${BACKUP_FILE}"

# Compress backup
gzip "${BACKUP_DIR}/${BACKUP_FILE}"

# Upload to S3
aws s3 cp "${BACKUP_DIR}/${BACKUP_FILE}.gz" s3://covid-etl-backups/database/

# Clean up old backups (keep last 7 days)
find ${BACKUP_DIR} -name "*.gz" -mtime +7 -delete
```

### 2. Data Backup

```python
# backup_data.py
import boto3
from datetime import datetime, timedelta

def backup_s3_data():
    s3 = boto3.client('s3')
    
    # List objects in source bucket
    response = s3.list_objects_v2(
        Bucket='covid-etl-data',
        Prefix='processed/'
    )
    
    # Copy to backup bucket
    for obj in response.get('Contents', []):
        copy_source = {
            'Bucket': 'covid-etl-data',
            'Key': obj['Key']
        }
        
        s3.copy_object(
            CopySource=copy_source,
            Bucket='covid-etl-backups',
            Key=f"data/{obj['Key']}"
        )
```

### 3. Disaster Recovery Plan

1. **RTO (Recovery Time Objective)**: 4 hours
2. **RPO (Recovery Point Objective)**: 1 hour
3. **Recovery Steps**:
   - Restore database from latest backup
   - Restore application from container registry
   - Restore data from backup storage
   - Validate data integrity
   - Resume pipeline operations

## Deployment Checklist

- [ ] Infrastructure provisioned
- [ ] Security groups configured
- [ ] Secrets stored securely
- [ ] Database initialized
- [ ] Application deployed
- [ ] Monitoring configured
- [ ] Alerts set up
- [ ] Backup strategy implemented
- [ ] Load testing completed
- [ ] Documentation updated
- [ ] Team trained on operations

## Rollback Procedures

### 1. Application Rollback

```bash
# Docker rollback
docker service update --image your-registry/covid-etl:previous-version covid-etl-webserver

# Kubernetes rollback
kubectl rollout undo deployment/airflow-webserver -n covid-etl
```

### 2. Database Rollback

```bash
# Restore from backup
pg_restore -h postgres -U airflow_prod -d airflow_prod backup_file.sql
```

### 3. Configuration Rollback

```bash
# Git-based configuration management
git checkout previous-commit config/
kubectl apply -f config/
```
