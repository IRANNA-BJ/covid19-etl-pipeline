#!/usr/bin/env python3
"""
Manual pipeline runner for COVID-19 ETL Pipeline.
This script allows running the ETL pipeline manually without Airflow.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import argparse
import json

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from extract.covid_api_extractor import CovidDataExtractor
from transform.data_transformer import CovidDataTransformer
from load.cloud_storage_loader import CloudStorageLoader
from load.warehouse_loader import DataWarehouseLoader
from utils.config import config
from utils.logger import get_logger


class PipelineRunner:
    """Manual pipeline runner for development and testing."""
    
    def __init__(self, skip_cloud: bool = False, skip_warehouse: bool = False):
        """
        Initialize pipeline runner.
        
        Args:
            skip_cloud: Skip cloud storage upload
            skip_warehouse: Skip warehouse loading
        """
        self.logger = get_logger(__name__)
        self.skip_cloud = skip_cloud
        self.skip_warehouse = skip_warehouse
        
        self.execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"Initializing pipeline runner with execution ID: {self.execution_id}")
    
    def run_extraction(self) -> dict:
        """Run data extraction step."""
        self.logger.info("=" * 50)
        self.logger.info("STEP 1: DATA EXTRACTION")
        self.logger.info("=" * 50)
        
        try:
            extractor = CovidDataExtractor()
            
            # Extract all data
            extracted_data = extractor.extract_all_data(
                include_historical=True,
                historical_days=30
            )
            
            # Save raw data
            saved_files = extractor.save_raw_data(extracted_data, "data/raw")
            
            # Get summary
            summary = extractor.get_extraction_summary(extracted_data)
            
            self.logger.info(f"✅ Extraction completed successfully")
            self.logger.info(f"   - Datasets extracted: {len(extracted_data)}")
            self.logger.info(f"   - Files saved: {len(saved_files)}")
            
            return {
                'status': 'success',
                'data': extracted_data,
                'files': saved_files,
                'summary': summary
            }
            
        except Exception as e:
            self.logger.error(f"❌ Extraction failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}
    
    def run_transformation(self, extracted_data: dict) -> dict:
        """Run data transformation step."""
        self.logger.info("=" * 50)
        self.logger.info("STEP 2: DATA TRANSFORMATION")
        self.logger.info("=" * 50)
        
        try:
            transformer = CovidDataTransformer()
            
            # Transform data
            transformed_data = transformer.transform_all_data(extracted_data['data'])
            
            # Save transformed data
            saved_files = transformer.save_transformed_data(transformed_data, "data/processed")
            
            self.logger.info(f"✅ Transformation completed successfully")
            self.logger.info(f"   - Datasets transformed: {len(transformed_data)}")
            self.logger.info(f"   - Files saved: {len(saved_files)}")
            
            return {
                'status': 'success',
                'data': transformed_data,
                'files': saved_files
            }
            
        except Exception as e:
            self.logger.error(f"❌ Transformation failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}
    
    def run_cloud_loading(self, transformed_data: dict) -> dict:
        """Run cloud storage loading step."""
        if self.skip_cloud:
            self.logger.info("⏭️  Skipping cloud storage loading")
            return {'status': 'skipped'}
        
        self.logger.info("=" * 50)
        self.logger.info("STEP 3: CLOUD STORAGE LOADING")
        self.logger.info("=" * 50)
        
        try:
            storage_loader = CloudStorageLoader()
            
            # Upload to cloud storage
            prefix = f"processed/{datetime.now().strftime('%Y%m%d')}/"
            uploaded_files = storage_loader.upload_multiple_dataframes(
                transformed_data['data'],
                prefix=prefix,
                file_format='parquet'
            )
            
            self.logger.info(f"✅ Cloud loading completed successfully")
            self.logger.info(f"   - Files uploaded: {len(uploaded_files)}")
            
            return {
                'status': 'success',
                'uploaded_files': uploaded_files
            }
            
        except Exception as e:
            self.logger.error(f"❌ Cloud loading failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}
    
    def run_warehouse_loading(self, transformed_data: dict) -> dict:
        """Run warehouse loading step."""
        if self.skip_warehouse:
            self.logger.info("⏭️  Skipping warehouse loading")
            return {'status': 'skipped'}
        
        self.logger.info("=" * 50)
        self.logger.info("STEP 4: WAREHOUSE LOADING")
        self.logger.info("=" * 50)
        
        try:
            warehouse_loader = DataWarehouseLoader()
            
            # Load to warehouse
            load_results = warehouse_loader.load_multiple_dataframes(
                transformed_data['data'],
                table_prefix="covid_",
                if_exists='append'
            )
            
            # Validate loads
            validation_results = {}
            for table_name, load_success in load_results.items():
                if load_success:
                    # Get expected count (simplified)
                    data_type = table_name.replace('covid_', '')
                    expected_count = len(transformed_data['data'].get(data_type, []))
                    
                    validation_result = warehouse_loader.validate_data_load(table_name, expected_count)
                    validation_results[table_name] = validation_result
            
            successful_loads = sum(1 for success in load_results.values() if success)
            
            self.logger.info(f"✅ Warehouse loading completed")
            self.logger.info(f"   - Tables loaded: {successful_loads}/{len(load_results)}")
            
            return {
                'status': 'success',
                'load_results': load_results,
                'validation_results': validation_results
            }
            
        except Exception as e:
            self.logger.error(f"❌ Warehouse loading failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}
    
    def run_full_pipeline(self) -> dict:
        """Run the complete ETL pipeline."""
        self.logger.info("🦠 Starting COVID-19 ETL Pipeline")
        self.logger.info(f"Execution ID: {self.execution_id}")
        
        pipeline_start = datetime.now()
        results = {
            'execution_id': self.execution_id,
            'start_time': pipeline_start.isoformat(),
            'steps': {}
        }
        
        try:
            # Step 1: Extract
            extraction_result = self.run_extraction()
            results['steps']['extraction'] = extraction_result
            
            if extraction_result['status'] != 'success':
                raise Exception("Extraction failed")
            
            # Step 2: Transform
            transformation_result = self.run_transformation(extraction_result)
            results['steps']['transformation'] = transformation_result
            
            if transformation_result['status'] != 'success':
                raise Exception("Transformation failed")
            
            # Step 3: Cloud Loading
            cloud_result = self.run_cloud_loading(transformation_result)
            results['steps']['cloud_loading'] = cloud_result
            
            # Step 4: Warehouse Loading
            warehouse_result = self.run_warehouse_loading(transformation_result)
            results['steps']['warehouse_loading'] = warehouse_result
            
            # Calculate duration
            pipeline_end = datetime.now()
            duration = (pipeline_end - pipeline_start).total_seconds()
            
            results.update({
                'end_time': pipeline_end.isoformat(),
                'duration_seconds': duration,
                'status': 'success'
            })
            
            self.logger.info("=" * 50)
            self.logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 50)
            self.logger.info(f"Total duration: {duration:.2f} seconds")
            
            return results
            
        except Exception as e:
            pipeline_end = datetime.now()
            duration = (pipeline_end - pipeline_start).total_seconds()
            
            results.update({
                'end_time': pipeline_end.isoformat(),
                'duration_seconds': duration,
                'status': 'failed',
                'error': str(e)
            })
            
            self.logger.error("=" * 50)
            self.logger.error("❌ PIPELINE FAILED")
            self.logger.error("=" * 50)
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Duration before failure: {duration:.2f} seconds")
            
            return results
    
    def save_results(self, results: dict) -> None:
        """Save pipeline results to file."""
        try:
            results_dir = Path("data/results")
            results_dir.mkdir(parents=True, exist_ok=True)
            
            results_file = results_dir / f"pipeline_results_{self.execution_id}.json"
            
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            self.logger.info(f"📊 Results saved to: {results_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {str(e)}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='COVID-19 ETL Pipeline Runner')
    
    parser.add_argument('--skip-cloud', action='store_true',
                       help='Skip cloud storage upload')
    parser.add_argument('--skip-warehouse', action='store_true',
                       help='Skip warehouse loading')
    parser.add_argument('--step', choices=['extract', 'transform', 'load', 'all'],
                       default='all', help='Run specific step or all steps')
    
    args = parser.parse_args()
    
    try:
        runner = PipelineRunner(
            skip_cloud=args.skip_cloud,
            skip_warehouse=args.skip_warehouse
        )
        
        if args.step == 'all':
            results = runner.run_full_pipeline()
        elif args.step == 'extract':
            results = runner.run_extraction()
        elif args.step == 'transform':
            # For transform step, we need extracted data
            print("Transform step requires extracted data. Running extraction first...")
            extraction_result = runner.run_extraction()
            if extraction_result['status'] == 'success':
                results = runner.run_transformation(extraction_result)
            else:
                results = extraction_result
        elif args.step == 'load':
            # For load step, we need transformed data
            print("Load step requires transformed data. Running extraction and transformation first...")
            extraction_result = runner.run_extraction()
            if extraction_result['status'] == 'success':
                transformation_result = runner.run_transformation(extraction_result)
                if transformation_result['status'] == 'success':
                    cloud_result = runner.run_cloud_loading(transformation_result)
                    warehouse_result = runner.run_warehouse_loading(transformation_result)
                    results = {
                        'cloud_loading': cloud_result,
                        'warehouse_loading': warehouse_result
                    }
                else:
                    results = transformation_result
            else:
                results = extraction_result
        
        # Save results
        runner.save_results(results)
        
        # Exit with appropriate code
        if results.get('status') == 'success':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n❌ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
