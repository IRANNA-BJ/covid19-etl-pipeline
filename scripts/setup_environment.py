#!/usr/bin/env python3
"""
Environment setup script for COVID-19 ETL Pipeline.
This script helps set up the necessary environment, dependencies, and configuration.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
import yaml
import json
from typing import Dict, List, Any, Optional


class EnvironmentSetup:
    """Setup and configuration manager for the COVID-19 ETL Pipeline."""
    
    def __init__(self):
        """Initialize the setup manager."""
        self.project_root = Path(__file__).parent.parent
        self.config_dir = self.project_root / "config"
        self.logs_dir = self.project_root / "logs"
        self.data_dir = self.project_root / "data"
        
        print("🦠 COVID-19 ETL Pipeline Environment Setup")
        print("=" * 50)
    
    def check_python_version(self) -> bool:
        """Check if Python version is compatible."""
        print("\n📋 Checking Python version...")
        
        version = sys.version_info
        if version.major == 3 and version.minor >= 8:
            print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
            return True
        else:
            print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not compatible")
            print("   Required: Python 3.8 or higher")
            return False
    
    def create_directories(self) -> None:
        """Create necessary project directories."""
        print("\n📁 Creating project directories...")
        
        directories = [
            self.logs_dir,
            self.data_dir / "raw",
            self.data_dir / "processed",
            self.config_dir / "credentials",
            self.project_root / "airflow" / "plugins",
            self.project_root / "tests",
            self.project_root / "docs"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created: {directory}")
    
    def install_dependencies(self) -> bool:
        """Install Python dependencies."""
        print("\n📦 Installing Python dependencies...")
        
        requirements_file = self.project_root / "requirements.txt"
        
        if not requirements_file.exists():
            print("❌ requirements.txt not found")
            return False
        
        try:
            # Upgrade pip first
            subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], 
                         check=True, capture_output=True)
            
            # Install requirements
            result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(requirements_file)], 
                                  check=True, capture_output=True, text=True)
            
            print("✅ Dependencies installed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            print(f"   Error output: {e.stderr}")
            return False
    
    def setup_configuration(self) -> None:
        """Setup configuration files."""
        print("\n⚙️  Setting up configuration...")
        
        config_file = self.config_dir / "config.yaml"
        env_file = self.project_root / ".env"
        
        # Create .env file template
        env_template = """# COVID-19 ETL Pipeline Environment Variables
# Copy this file to .env and fill in your actual values

# API Configuration
COVID_API_BASE_URL=https://disease.sh/v3/covid-19
COVID_API_RATE_LIMIT=100

# Storage Configuration
STORAGE_PROVIDER=aws
STORAGE_BUCKET=covid-etl-data-bucket

# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
AWS_REGION=us-east-1

# Google Cloud Configuration
GCP_PROJECT_ID=your_gcp_project_id_here
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/gcp/credentials.json

# Database Configuration
DB_HOST=your_database_host_here
DB_PORT=5439
DB_NAME=covid_data
DB_USER=your_database_user_here
DB_PASSWORD=your_database_password_here

# Notification Configuration
SMTP_USERNAME=your_smtp_username_here
SMTP_PASSWORD=your_smtp_password_here
SLACK_WEBHOOK_URL=your_slack_webhook_url_here

# Airflow Configuration
AIRFLOW_HOME={airflow_home}
AIRFLOW__CORE__DAGS_FOLDER={dags_folder}
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
""".format(
            airflow_home=self.project_root / "airflow",
            dags_folder=self.project_root / "airflow" / "dags"
        )
        
        if not env_file.exists():
            with open(env_file, 'w') as f:
                f.write(env_template)
            print(f"✅ Created environment template: {env_file}")
            print("   Please edit .env file with your actual configuration values")
        else:
            print(f"✅ Environment file already exists: {env_file}")
    
    def setup_airflow(self) -> bool:
        """Setup Apache Airflow."""
        print("\n🌪️  Setting up Apache Airflow...")
        
        airflow_home = self.project_root / "airflow"
        os.environ['AIRFLOW_HOME'] = str(airflow_home)
        
        try:
            # Initialize Airflow database
            subprocess.run([sys.executable, "-m", "airflow", "db", "init"], 
                         check=True, capture_output=True)
            
            # Create admin user
            subprocess.run([
                sys.executable, "-m", "airflow", "users", "create",
                "--username", "admin",
                "--firstname", "Admin",
                "--lastname", "User",
                "--role", "Admin",
                "--email", "admin@example.com",
                "--password", "admin"
            ], check=True, capture_output=True)
            
            print("✅ Airflow initialized successfully")
            print("   Default admin user created (username: admin, password: admin)")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to setup Airflow: {e}")
            return False
    
    def create_docker_compose(self) -> None:
        """Create Docker Compose file for easy deployment."""
        print("\n🐳 Creating Docker Compose configuration...")
        
        docker_compose_content = """version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_db_volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    build: .
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./src:/opt/airflow/src
      - ./config:/opt/airflow/config
    restart: always

  airflow-scheduler:
    build: .
    command: scheduler
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./src:/opt/airflow/src
      - ./config:/opt/airflow/config
    restart: always

  airflow-worker:
    build: .
    command: celery worker
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./src:/opt/airflow/src
      - ./config:/opt/airflow/config
    restart: always

volumes:
  postgres_db_volume:
"""
        
        docker_compose_file = self.project_root / "docker-compose.yml"
        with open(docker_compose_file, 'w') as f:
            f.write(docker_compose_content)
        
        print(f"✅ Created Docker Compose file: {docker_compose_file}")
    
    def create_dockerfile(self) -> None:
        """Create Dockerfile for the application."""
        print("\n🐳 Creating Dockerfile...")
        
        dockerfile_content = """FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy application code
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
"""
        
        dockerfile = self.project_root / "Dockerfile"
        with open(dockerfile, 'w') as f:
            f.write(dockerfile_content)
        
        print(f"✅ Created Dockerfile: {dockerfile}")
    
    def test_installation(self) -> bool:
        """Test the installation by importing key modules."""
        print("\n🧪 Testing installation...")
        
        test_imports = [
            "pandas",
            "requests", 
            "boto3",
            "google.cloud.storage",
            "sqlalchemy",
            "airflow"
        ]
        
        failed_imports = []
        
        for module in test_imports:
            try:
                __import__(module)
                print(f"✅ {module}")
            except ImportError as e:
                print(f"❌ {module}: {e}")
                failed_imports.append(module)
        
        if failed_imports:
            print(f"\n❌ Failed to import: {', '.join(failed_imports)}")
            return False
        else:
            print("\n✅ All imports successful!")
            return True
    
    def generate_setup_report(self) -> None:
        """Generate a setup completion report."""
        print("\n📊 Setup Report")
        print("=" * 30)
        
        report = {
            "setup_completed": True,
            "project_root": str(self.project_root),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "directories_created": [
                str(self.logs_dir),
                str(self.data_dir),
                str(self.config_dir)
            ],
            "next_steps": [
                "1. Edit .env file with your configuration values",
                "2. Set up cloud credentials (AWS/GCP)",
                "3. Configure database connection",
                "4. Run 'docker-compose up' to start services",
                "5. Access Airflow UI at http://localhost:8080"
            ]
        }
        
        report_file = self.project_root / "setup_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"✅ Setup report saved to: {report_file}")
        
        print("\n🎉 Setup completed successfully!")
        print("\nNext steps:")
        for step in report["next_steps"]:
            print(f"   {step}")
    
    def run_setup(self) -> None:
        """Run the complete setup process."""
        try:
            # Check Python version
            if not self.check_python_version():
                sys.exit(1)
            
            # Create directories
            self.create_directories()
            
            # Install dependencies
            if not self.install_dependencies():
                print("⚠️  Dependency installation failed, but continuing...")
            
            # Setup configuration
            self.setup_configuration()
            
            # Create Docker files
            self.create_docker_compose()
            self.create_dockerfile()
            
            # Test installation
            if not self.test_installation():
                print("⚠️  Some imports failed, please check your installation")
            
            # Generate report
            self.generate_setup_report()
            
        except KeyboardInterrupt:
            print("\n\n❌ Setup interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Setup failed with error: {e}")
            sys.exit(1)


def main():
    """Main entry point."""
    setup = EnvironmentSetup()
    setup.run_setup()


if __name__ == "__main__":
    main()
