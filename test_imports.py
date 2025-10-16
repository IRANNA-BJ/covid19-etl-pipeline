#!/usr/bin/env python3
"""
Quick test script to verify imports and basic functionality.
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def test_imports():
    """Test if all modules can be imported."""
    print("🧪 Testing module imports...")
    
    try:
        from extract.covid_api_extractor import CovidDataExtractor
        print("✅ CovidDataExtractor imported successfully")
    except Exception as e:
        print(f"❌ Failed to import CovidDataExtractor: {e}")
        return False
    
    try:
        from transform.data_transformer import CovidDataTransformer
        print("✅ CovidDataTransformer imported successfully")
    except Exception as e:
        print(f"❌ Failed to import CovidDataTransformer: {e}")
        return False
    
    try:
        from utils.config import config
        print("✅ Config imported successfully")
    except Exception as e:
        print(f"❌ Failed to import config: {e}")
        return False
    
    return True

def test_api_connection():
    """Test API connection."""
    print("\n🌐 Testing API connection...")
    
    try:
        from extract.covid_api_extractor import CovidDataExtractor
        extractor = CovidDataExtractor()
        
        # Test API health check
        if extractor.api_client.health_check():
            print("✅ API connection successful")
            return True
        else:
            print("❌ API health check failed")
            return False
            
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

def test_basic_extraction():
    """Test basic data extraction."""
    print("\n📊 Testing basic data extraction...")
    
    try:
        from extract.covid_api_extractor import CovidDataExtractor
        extractor = CovidDataExtractor()
        
        # Extract global data
        global_data = extractor.extract_global_data()
        
        if not global_data.empty:
            print(f"✅ Successfully extracted global data: {len(global_data)} records")
            print(f"   Columns: {list(global_data.columns)[:5]}...")  # Show first 5 columns
            return True
        else:
            print("❌ No global data extracted")
            return False
            
    except Exception as e:
        print(f"❌ Data extraction failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🦠 COVID-19 ETL Pipeline - Quick Test")
    print("=" * 40)
    
    # Test 1: Imports
    if not test_imports():
        print("\n❌ Import tests failed. Please check your Python environment.")
        return
    
    # Test 2: API Connection
    if not test_api_connection():
        print("\n⚠️  API connection failed. Check your internet connection.")
        print("   You can still proceed with local testing.")
    
    # Test 3: Basic Extraction
    if test_api_connection():  # Only test extraction if API is working
        if test_basic_extraction():
            print("\n🎉 All tests passed! Your pipeline is ready to use.")
        else:
            print("\n⚠️  Basic extraction failed. Check the logs for details.")
    
    print("\n📋 Next Steps:")
    print("1. Run full pipeline: python scripts/run_pipeline.py")
    print("2. Check documentation: docs/setup_guide.md")
    print("3. Configure cloud credentials if needed")

if __name__ == "__main__":
    main()
