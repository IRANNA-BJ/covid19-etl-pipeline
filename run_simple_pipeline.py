#!/usr/bin/env python3
"""
Simple pipeline runner for COVID-19 ETL without complex imports.
"""

import requests
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import os

def extract_covid_data():
    """Extract COVID-19 data from API."""
    print("🦠 Starting COVID-19 Data Extraction")
    print("=" * 40)
    
    # Create directories
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    extracted_data = {}
    
    try:
        # Extract global data
        print("📊 Extracting global data...")
        response = requests.get("https://disease.sh/v3/covid-19/all", timeout=30)
        if response.status_code == 200:
            global_data = response.json()
            df_global = pd.DataFrame([global_data])
            df_global['extraction_date'] = datetime.now()
            df_global['data_source'] = 'disease.sh'
            df_global['data_type'] = 'global'
            extracted_data['global'] = df_global
            print(f"✅ Global data: {len(df_global)} records")
        
        # Extract countries data
        print("🌍 Extracting countries data...")
        response = requests.get("https://disease.sh/v3/covid-19/countries", timeout=30)
        if response.status_code == 200:
            countries_data = response.json()
            df_countries = pd.DataFrame(countries_data)
            df_countries['extraction_date'] = datetime.now()
            df_countries['data_source'] = 'disease.sh'
            df_countries['data_type'] = 'countries'
            extracted_data['countries'] = df_countries
            print(f"✅ Countries data: {len(df_countries)} records")
        
        # Extract continents data
        print("🌎 Extracting continents data...")
        response = requests.get("https://disease.sh/v3/covid-19/continents", timeout=30)
        if response.status_code == 200:
            continents_data = response.json()
            df_continents = pd.DataFrame(continents_data)
            df_continents['extraction_date'] = datetime.now()
            df_continents['data_source'] = 'disease.sh'
            df_continents['data_type'] = 'continents'
            extracted_data['continents'] = df_continents
            print(f"✅ Continents data: {len(df_continents)} records")
        
        # Extract US states data
        print("🇺🇸 Extracting US states data...")
        response = requests.get("https://disease.sh/v3/covid-19/states", timeout=30)
        if response.status_code == 200:
            states_data = response.json()
            df_states = pd.DataFrame(states_data)
            df_states['extraction_date'] = datetime.now()
            df_states['data_source'] = 'disease.sh'
            df_states['data_type'] = 'states'
            extracted_data['states'] = df_states
            print(f"✅ US States data: {len(df_states)} records")
        
        return extracted_data
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return {}

def transform_data(extracted_data):
    """Transform and clean the extracted data."""
    print("\n🔄 Starting Data Transformation")
    print("=" * 40)
    
    transformed_data = {}
    
    for data_type, df in extracted_data.items():
        print(f"🔧 Transforming {data_type} data...")
        
        try:
            df_transformed = df.copy()
            
            # Basic transformations
            # Convert timestamp columns
            if 'updated' in df_transformed.columns:
                df_transformed['updated'] = pd.to_datetime(df_transformed['updated'], unit='ms', errors='coerce')
            
            # Add derived metrics for case data
            if data_type in ['global', 'countries', 'continents', 'states']:
                if 'deaths' in df_transformed.columns and 'cases' in df_transformed.columns:
                    df_transformed['mortality_rate'] = df_transformed['deaths'] / df_transformed['cases'].replace(0, 1)
                
                if 'recovered' in df_transformed.columns and 'cases' in df_transformed.columns:
                    df_transformed['recovery_rate'] = df_transformed['recovered'] / df_transformed['cases'].replace(0, 1)
            
            # Add processing timestamp
            df_transformed['processed_at'] = datetime.now()
            
            # Clean numeric columns
            numeric_cols = ['cases', 'deaths', 'recovered', 'active', 'critical']
            for col in numeric_cols:
                if col in df_transformed.columns:
                    df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0)
            
            transformed_data[data_type] = df_transformed
            print(f"✅ {data_type}: {len(df_transformed)} records transformed")
            
        except Exception as e:
            print(f"❌ Failed to transform {data_type}: {e}")
            continue
    
    return transformed_data

def save_data(data, data_stage="raw"):
    """Save data to files."""
    print(f"\n💾 Saving {data_stage} data")
    print("=" * 40)
    
    saved_files = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for data_type, df in data.items():
        try:
            filename = f"covid_{data_type}_{data_stage}_{timestamp}.csv"
            filepath = f"data/{data_stage}/{filename}"
            
            df.to_csv(filepath, index=False)
            saved_files[data_type] = filepath
            
            print(f"✅ Saved {data_type}: {filepath} ({len(df)} records)")
            
        except Exception as e:
            print(f"❌ Failed to save {data_type}: {e}")
    
    return saved_files

def generate_summary(extracted_data, transformed_data):
    """Generate pipeline summary."""
    print("\n📊 Pipeline Summary")
    print("=" * 40)
    
    total_extracted = sum(len(df) for df in extracted_data.values())
    total_transformed = sum(len(df) for df in transformed_data.values())
    
    print(f"📈 Data Extraction:")
    for data_type, df in extracted_data.items():
        print(f"   {data_type}: {len(df):,} records")
    
    print(f"\n🔄 Data Transformation:")
    for data_type, df in transformed_data.items():
        print(f"   {data_type}: {len(df):,} records")
    
    print(f"\n📋 Summary:")
    print(f"   Total extracted: {total_extracted:,} records")
    print(f"   Total transformed: {total_transformed:,} records")
    print(f"   Datasets processed: {len(transformed_data)}")
    print(f"   Execution time: {datetime.now()}")
    
    # Sample data preview
    if 'countries' in transformed_data:
        df_countries = transformed_data['countries']
        print(f"\n🌍 Sample Countries Data:")
        print(f"   Top 5 by cases: {df_countries.nlargest(5, 'cases')['country'].tolist()}")
        if 'mortality_rate' in df_countries.columns:
            avg_mortality = df_countries['mortality_rate'].mean()
            print(f"   Average mortality rate: {avg_mortality:.4f} ({avg_mortality*100:.2f}%)")

def main():
    """Run the simple ETL pipeline."""
    start_time = datetime.now()
    
    print("🦠 COVID-19 Simple ETL Pipeline")
    print("=" * 50)
    print(f"Started at: {start_time}")
    
    try:
        # Step 1: Extract
        extracted_data = extract_covid_data()
        if not extracted_data:
            print("❌ No data extracted. Exiting.")
            return
        
        # Save raw data
        raw_files = save_data(extracted_data, "raw")
        
        # Step 2: Transform
        transformed_data = transform_data(extracted_data)
        if not transformed_data:
            print("❌ No data transformed. Exiting.")
            return
        
        # Save processed data
        processed_files = save_data(transformed_data, "processed")
        
        # Step 3: Summary
        generate_summary(extracted_data, transformed_data)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n🎉 Pipeline completed successfully!")
        print(f"   Duration: {duration:.2f} seconds")
        print(f"   Raw files: {len(raw_files)}")
        print(f"   Processed files: {len(processed_files)}")
        
        print(f"\n📁 Output files:")
        print("   Raw data:")
        for data_type, filepath in raw_files.items():
            print(f"     {data_type}: {filepath}")
        print("   Processed data:")
        for data_type, filepath in processed_files.items():
            print(f"     {data_type}: {filepath}")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
