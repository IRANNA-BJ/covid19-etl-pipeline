#!/usr/bin/env python3
"""
Quick test to verify the COVID-19 ETL pipeline setup.
"""

import requests
import pandas as pd
from datetime import datetime

def test_api_connection():
    """Test direct API connection."""
    print("🌐 Testing COVID-19 API connection...")
    
    try:
        # Test the disease.sh API directly
        response = requests.get("https://disease.sh/v3/covid-19/all", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API connection successful!")
            print(f"   Global cases: {data.get('cases', 'N/A'):,}")
            print(f"   Global deaths: {data.get('deaths', 'N/A'):,}")
            print(f"   Last updated: {datetime.fromtimestamp(data.get('updated', 0)/1000)}")
            return True
        else:
            print(f"❌ API returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

def test_data_processing():
    """Test basic data processing."""
    print("\n📊 Testing data processing...")
    
    try:
        # Create sample data
        sample_data = {
            'country': ['USA', 'India', 'Brazil'],
            'cases': [100000000, 45000000, 35000000],
            'deaths': [1000000, 500000, 700000]
        }
        
        df = pd.DataFrame(sample_data)
        
        # Basic processing
        df['mortality_rate'] = df['deaths'] / df['cases']
        df['processed_at'] = datetime.now()
        
        print("✅ Data processing successful!")
        print(f"   Sample data shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data processing failed: {e}")
        return False

def test_file_operations():
    """Test file operations."""
    print("\n📁 Testing file operations...")
    
    try:
        # Create test directories
        import os
        os.makedirs('data/raw', exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Test file write
        test_data = pd.DataFrame({'test': [1, 2, 3]})
        test_file = 'data/raw/test_file.csv'
        test_data.to_csv(test_file, index=False)
        
        # Test file read
        read_data = pd.read_csv(test_file)
        
        if len(read_data) == 3:
            print("✅ File operations successful!")
            
            # Clean up
            os.remove(test_file)
            return True
        else:
            print("❌ File read/write mismatch")
            return False
            
    except Exception as e:
        print(f"❌ File operations failed: {e}")
        return False

def test_countries_data():
    """Test fetching countries data."""
    print("\n🌍 Testing countries data extraction...")
    
    try:
        response = requests.get("https://disease.sh/v3/covid-19/countries", timeout=15)
        
        if response.status_code == 200:
            countries_data = response.json()
            
            if len(countries_data) > 0:
                df = pd.DataFrame(countries_data)
                print(f"✅ Countries data extracted successfully!")
                print(f"   Total countries: {len(df)}")
                print(f"   Sample countries: {df['country'].head(3).tolist()}")
                
                # Save sample data
                sample_file = 'data/raw/sample_countries.csv'
                df.head(10).to_csv(sample_file, index=False)
                print(f"   Sample saved to: {sample_file}")
                
                return True
            else:
                print("❌ No countries data received")
                return False
        else:
            print(f"❌ Countries API returned status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Countries data extraction failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🦠 COVID-19 ETL Pipeline - Quick Verification")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 4
    
    # Test 1: API Connection
    if test_api_connection():
        tests_passed += 1
    
    # Test 2: Data Processing
    if test_data_processing():
        tests_passed += 1
    
    # Test 3: File Operations
    if test_file_operations():
        tests_passed += 1
    
    # Test 4: Countries Data
    if test_countries_data():
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Your environment is ready.")
        print("\n📋 Next Steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Configure cloud credentials (optional)")
        print("3. Run the full pipeline: python scripts/run_pipeline.py")
        print("4. Check the documentation in docs/ folder")
    elif tests_passed >= 2:
        print("⚠️  Most tests passed. You can proceed with basic functionality.")
        print("   Some advanced features may require additional setup.")
    else:
        print("❌ Multiple tests failed. Please check your environment.")
        print("   Refer to docs/troubleshooting.md for help.")

if __name__ == "__main__":
    main()
