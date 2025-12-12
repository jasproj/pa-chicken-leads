#!/usr/bin/env python3
"""
USDA NASS Quick Stats API Collector

Fetches county-level poultry statistics from the USDA National Agricultural 
Statistics Service. This gives us aggregate data like:
- Number of poultry farms per county
- Total birds by county
- Average farm size

While this doesn't give us individual farm data, it helps:
1. Identify which counties to focus on
2. Validate our permit data (should match census counts)
3. Estimate total addressable market

API Documentation: https://quickstats.nass.usda.gov/api
Free API key required - get one at the link above.
"""

import time
from typing import List, Dict, Optional
import requests

from config import NASS_API_KEY, PA_POULTRY_COUNTIES, REQUEST_DELAY_SECONDS
from db import get_db

SOURCE_NAME = 'USDA NASS Census'
BASE_URL = 'https://quickstats.nass.usda.gov/api/api_GET/'


def query_nass(params: dict) -> Optional[List[dict]]:
    """
    Query the NASS Quick Stats API.
    
    The API returns CSV by default or JSON with format=JSON.
    Limited to 50,000 records per query.
    """
    params['key'] = NASS_API_KEY
    params['format'] = 'JSON'
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=60)
        if response.status_code != 200:
            print(f"NASS API error: {response.status_code}")
            print(response.text[:500])
            return None
        
        data = response.json()
        return data.get('data', [])
    
    except Exception as e:
        print(f"NASS query failed: {e}")
        return None


def get_pa_poultry_stats() -> List[dict]:
    """
    Get Pennsylvania poultry statistics by county.
    
    Census of Agriculture provides county-level data.
    Last full census was 2022 (published 2024).
    """
    results = []
    
    # Query parameters for PA poultry operations
    params = {
        'source_desc': 'CENSUS',
        'sector_desc': 'ANIMALS & PRODUCTS',
        'group_desc': 'POULTRY',
        'state_alpha': 'PA',
        'agg_level_desc': 'COUNTY',
        'year': '2022',  # Most recent census
    }
    
    # Get inventory data (number of birds)
    inventory_params = params.copy()
    inventory_params['statisticcat_desc'] = 'INVENTORY'
    
    print("Fetching PA poultry inventory data...")
    inventory_data = query_nass(inventory_params)
    
    if inventory_data:
        print(f"  Found {len(inventory_data)} inventory records")
        
        # Process by county
        county_stats = {}
        for record in inventory_data:
            county = record.get('county_name', '').title()
            if county not in county_stats:
                county_stats[county] = {
                    'county': county,
                    'total_birds': 0,
                    'broilers': 0,
                    'layers': 0,
                    'turkeys': 0,
                    'other_poultry': 0,
                }
            
            value = record.get('Value', '').replace(',', '')
            if value and value != '(D)':  # (D) = withheld to avoid disclosing individual data
                try:
                    count = int(value)
                    commodity = record.get('commodity_desc', '').lower()
                    
                    if 'broiler' in commodity:
                        county_stats[county]['broilers'] += count
                    elif 'layer' in commodity or 'egg' in commodity:
                        county_stats[county]['layers'] += count
                    elif 'turkey' in commodity:
                        county_stats[county]['turkeys'] += count
                    else:
                        county_stats[county]['other_poultry'] += count
                    
                    county_stats[county]['total_birds'] += count
                except ValueError:
                    pass
        
        results = list(county_stats.values())
    
    time.sleep(REQUEST_DELAY_SECONDS)
    
    # Get number of operations
    operations_params = params.copy()
    operations_params['statisticcat_desc'] = 'OPERATIONS'
    
    print("Fetching PA poultry operations data...")
    operations_data = query_nass(operations_params)
    
    if operations_data:
        print(f"  Found {len(operations_data)} operations records")
        
        # Add operations count to county stats
        ops_by_county = {}
        for record in operations_data:
            county = record.get('county_name', '').title()
            value = record.get('Value', '').replace(',', '')
            if value and value != '(D)':
                try:
                    ops_by_county[county] = ops_by_county.get(county, 0) + int(value)
                except ValueError:
                    pass
        
        for result in results:
            result['num_operations'] = ops_by_county.get(result['county'], 0)
    
    return results


def get_county_rankings(stats: List[dict]) -> List[dict]:
    """Rank counties by poultry production."""
    # Sort by total birds
    sorted_stats = sorted(stats, key=lambda x: x.get('total_birds', 0), reverse=True)
    
    for i, stat in enumerate(sorted_stats, 1):
        stat['rank'] = i
    
    return sorted_stats


def identify_target_counties(stats: List[dict], min_birds: int = 100000) -> List[str]:
    """Identify counties worth targeting based on poultry production."""
    targets = []
    
    for stat in stats:
        if stat.get('total_birds', 0) >= min_birds:
            targets.append(stat['county'])
    
    return targets


def collect():
    """Main collection function."""
    db = get_db()
    run_id = db.start_run(SOURCE_NAME, {'year': '2022'})
    
    error_msg = None
    
    try:
        # Check for API key
        if not NASS_API_KEY or NASS_API_KEY == 'your-nass-api-key':
            raise ValueError(
                "NASS API key not configured. "
                "Get a free key at https://quickstats.nass.usda.gov/api"
            )
        
        # Fetch PA poultry stats
        stats = get_pa_poultry_stats()
        
        if not stats:
            raise ValueError("No data returned from NASS API")
        
        # Rank counties
        ranked = get_county_rankings(stats)
        
        # Print summary
        print("\n" + "="*60)
        print("PA POULTRY COUNTY RANKINGS (2022 Census)")
        print("="*60)
        print(f"{'Rank':<6}{'County':<20}{'Operations':<12}{'Total Birds':>15}")
        print("-"*60)
        
        for stat in ranked[:20]:  # Top 20
            print(f"{stat.get('rank', '?'):<6}"
                  f"{stat.get('county', 'Unknown'):<20}"
                  f"{stat.get('num_operations', 'N/A'):<12}"
                  f"{stat.get('total_birds', 0):>15,}")
        
        # Identify target counties
        targets = identify_target_counties(ranked, min_birds=500000)
        print(f"\nHigh-value target counties (500k+ birds): {', '.join(targets)}")
        
        # Store results as notes for reference
        # In a production system, you might store this in a separate table
        total_ops = sum(s.get('num_operations', 0) for s in stats)
        total_birds = sum(s.get('total_birds', 0) for s in stats)
        
        print(f"\nPA Totals:")
        print(f"  Operations: {total_ops:,}")
        print(f"  Total birds: {total_birds:,}")
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error: {e}")
    
    db.complete_run(run_id, len(stats) if 'stats' in dir() else 0, 0, 0, error_msg)


if __name__ == '__main__':
    collect()
