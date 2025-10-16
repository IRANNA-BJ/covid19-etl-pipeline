#!/usr/bin/env python3
"""
Analyze the output from COVID-19 ETL Pipeline.
"""

import pandas as pd
import os
from datetime import datetime

def analyze_output():
    """Analyze the generated output files."""
    print("🔍 COVID-19 ETL Pipeline Output Analysis")
    print("=" * 50)
    
    # Find the latest processed files
    processed_dir = "data/processed"
    
    if not os.path.exists(processed_dir):
        print("❌ No processed data found. Run the pipeline first.")
        return
    
    files = [f for f in os.listdir(processed_dir) if f.endswith('.csv')]
    
    if not files:
        print("❌ No CSV files found in processed directory.")
        return
    
    print(f"📁 Found {len(files)} processed files:")
    for file in files:
        print(f"   - {file}")
    
    print("\n" + "=" * 50)
    
    # Analyze each file
    for file in files:
        if 'global' in file:
            analyze_global_data(os.path.join(processed_dir, file))
        elif 'countries' in file:
            analyze_countries_data(os.path.join(processed_dir, file))
        elif 'continents' in file:
            analyze_continents_data(os.path.join(processed_dir, file))
        elif 'states' in file:
            analyze_states_data(os.path.join(processed_dir, file))

def analyze_global_data(filepath):
    """Analyze global COVID-19 data."""
    print("\n🌍 GLOBAL DATA ANALYSIS")
    print("-" * 30)
    
    try:
        df = pd.read_csv(filepath)
        
        if df.empty:
            print("❌ No global data found")
            return
        
        row = df.iloc[0]
        
        print(f"📊 Global COVID-19 Statistics:")
        print(f"   Total Cases: {row['cases']:,}")
        print(f"   Total Deaths: {row['deaths']:,}")
        print(f"   Total Recovered: {row['recovered']:,}")
        print(f"   Active Cases: {row['active']:,}")
        print(f"   Critical Cases: {row['critical']:,}")
        
        if 'mortality_rate' in row:
            print(f"   Mortality Rate: {row['mortality_rate']:.4f} ({row['mortality_rate']*100:.2f}%)")
        
        if 'recovery_rate' in row:
            print(f"   Recovery Rate: {row['recovery_rate']:.4f} ({row['recovery_rate']*100:.2f}%)")
        
        print(f"   Countries Affected: {row['affectedCountries']}")
        print(f"   Last Updated: {row['updated']}")
        
    except Exception as e:
        print(f"❌ Error analyzing global data: {e}")

def analyze_countries_data(filepath):
    """Analyze countries COVID-19 data."""
    print("\n🌍 COUNTRIES DATA ANALYSIS")
    print("-" * 30)
    
    try:
        df = pd.read_csv(filepath)
        
        if df.empty:
            print("❌ No countries data found")
            return
        
        print(f"📊 Countries Statistics:")
        print(f"   Total Countries: {len(df)}")
        
        # Top 10 countries by cases
        top_cases = df.nlargest(10, 'cases')[['country', 'cases', 'deaths', 'mortality_rate']]
        print(f"\n🔝 Top 10 Countries by Cases:")
        for idx, row in top_cases.iterrows():
            mortality_pct = row['mortality_rate'] * 100 if pd.notna(row['mortality_rate']) else 0
            print(f"   {row['country']}: {row['cases']:,} cases, {row['deaths']:,} deaths ({mortality_pct:.2f}%)")
        
        # Continent breakdown
        if 'continent' in df.columns:
            continent_stats = df.groupby('continent').agg({
                'cases': 'sum',
                'deaths': 'sum',
                'recovered': 'sum'
            }).sort_values('cases', ascending=False)
            
            print(f"\n🌎 By Continent:")
            for continent, stats in continent_stats.iterrows():
                print(f"   {continent}: {stats['cases']:,} cases, {stats['deaths']:,} deaths")
        
        # Summary statistics
        print(f"\n📈 Summary Statistics:")
        print(f"   Total Global Cases: {df['cases'].sum():,}")
        print(f"   Total Global Deaths: {df['deaths'].sum():,}")
        print(f"   Average Mortality Rate: {df['mortality_rate'].mean()*100:.2f}%")
        print(f"   Highest Mortality Rate: {df['mortality_rate'].max()*100:.2f}%")
        print(f"   Lowest Mortality Rate: {df['mortality_rate'].min()*100:.2f}%")
        
    except Exception as e:
        print(f"❌ Error analyzing countries data: {e}")

def analyze_continents_data(filepath):
    """Analyze continents COVID-19 data."""
    print("\n🌎 CONTINENTS DATA ANALYSIS")
    print("-" * 30)
    
    try:
        df = pd.read_csv(filepath)
        
        if df.empty:
            print("❌ No continents data found")
            return
        
        print(f"📊 Continents Statistics:")
        
        # Sort by cases
        df_sorted = df.sort_values('cases', ascending=False)
        
        for idx, row in df_sorted.iterrows():
            mortality_pct = row['mortality_rate'] * 100 if 'mortality_rate' in row and pd.notna(row['mortality_rate']) else 0
            print(f"   {row['continent']}: {row['cases']:,} cases, {row['deaths']:,} deaths ({mortality_pct:.2f}%)")
        
    except Exception as e:
        print(f"❌ Error analyzing continents data: {e}")

def analyze_states_data(filepath):
    """Analyze US states COVID-19 data."""
    print("\n🇺🇸 US STATES DATA ANALYSIS")
    print("-" * 30)
    
    try:
        df = pd.read_csv(filepath)
        
        if df.empty:
            print("❌ No states data found")
            return
        
        print(f"📊 US States Statistics:")
        print(f"   Total States/Territories: {len(df)}")
        
        # Top 10 states by cases
        top_states = df.nlargest(10, 'cases')[['state', 'cases', 'deaths', 'mortality_rate']]
        print(f"\n🔝 Top 10 States by Cases:")
        for idx, row in top_states.iterrows():
            mortality_pct = row['mortality_rate'] * 100 if pd.notna(row['mortality_rate']) else 0
            print(f"   {row['state']}: {row['cases']:,} cases, {row['deaths']:,} deaths ({mortality_pct:.2f}%)")
        
        print(f"\n📈 US Summary:")
        print(f"   Total US Cases: {df['cases'].sum():,}")
        print(f"   Total US Deaths: {df['deaths'].sum():,}")
        print(f"   Average State Mortality Rate: {df['mortality_rate'].mean()*100:.2f}%")
        
    except Exception as e:
        print(f"❌ Error analyzing states data: {e}")

def check_data_quality():
    """Check data quality metrics."""
    print("\n🔍 DATA QUALITY CHECK")
    print("-" * 30)
    
    processed_dir = "data/processed"
    files = [f for f in os.listdir(processed_dir) if f.endswith('.csv')]
    
    for file in files:
        filepath = os.path.join(processed_dir, file)
        try:
            df = pd.read_csv(filepath)
            
            print(f"\n📄 {file}:")
            print(f"   Records: {len(df):,}")
            print(f"   Columns: {len(df.columns)}")
            print(f"   Missing Values: {df.isnull().sum().sum()}")
            print(f"   File Size: {os.path.getsize(filepath):,} bytes")
            
            # Check for required columns
            if 'extraction_date' in df.columns:
                print(f"   ✅ Has extraction timestamp")
            if 'processed_at' in df.columns:
                print(f"   ✅ Has processing timestamp")
            if 'mortality_rate' in df.columns:
                print(f"   ✅ Has calculated mortality rate")
            
        except Exception as e:
            print(f"   ❌ Error reading {file}: {e}")

if __name__ == "__main__":
    analyze_output()
    check_data_quality()
    
    print("\n" + "=" * 50)
    print("🎉 Analysis Complete!")
    print("\n📋 To view files manually:")
    print("   1. Open File Explorer: data/processed/")
    print("   2. Open in Excel or text editor")
    print("   3. Use: notepad data\\processed\\covid_countries_processed_*.csv")
