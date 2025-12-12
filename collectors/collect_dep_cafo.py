#!/usr/bin/env python3
"""
PA DEP CAFO Permit Collector

Scrapes the PA DEP CAFO database which contains all permitted concentrated 
animal feeding operations in Pennsylvania. This is the gold mine for finding
chicken farms - any operation with 300+ AEUs needs a permit.

Data includes:
- Facility name
- Permit number  
- Client/operator name
- Municipality
- Animal type (we filter for poultry)
- AEU count

The data is in an SSRS report which is tricky to scrape, so we use
multiple approaches.
"""

import re
import time
import json
from datetime import datetime
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup

from config import REQUEST_DELAY_SECONDS, USER_AGENT, PA_POULTRY_COUNTIES
from db import get_db

SOURCE_NAME = 'PA DEP CAFO Permits'

# PA DEP CAFO report URL
CAFO_REPORT_URL = 'http://cedatareporting.pa.gov/ReportServer/Pages/ReportViewer.aspx?/Public/DEP/CW/SSRS/WMS_CAFO_Details_Ext'

# Alternative: Use the PA Bulletin which publishes permit applications/renewals
PA_BULLETIN_SEARCH = 'https://www.pacodeandbulletin.gov/Search'


def parse_aeu(aeu_str: str) -> Optional[float]:
    """Parse AEU string like '1,166.25' to float."""
    if not aeu_str:
        return None
    try:
        return float(aeu_str.replace(',', ''))
    except ValueError:
        return None


def detect_operation_type(text: str) -> str:
    """Detect type of poultry operation from description text."""
    text = text.lower()
    if 'layer' in text or 'egg' in text:
        return 'layer'
    elif 'broiler' in text:
        return 'broiler'
    elif 'turkey' in text:
        return 'turkey'
    elif 'pullet' in text:
        return 'pullet'
    elif 'poultry' in text or 'chicken' in text:
        return 'poultry'
    return 'unknown'


def detect_integrator(text: str) -> Optional[str]:
    """Try to detect the integrator from text."""
    text = text.lower()
    integrators = {
        'bell & evans': 'Bell & Evans',
        'bell and evans': 'Bell & Evans',
        'perdue': 'Perdue',
        'tyson': 'Tyson',
        'pilgrim': "Pilgrim's Pride",
        'koch': 'Koch Foods',
        'wenger': 'Wenger Feeds',
    }
    for key, value in integrators.items():
        if key in text:
            return value
    return None


def scrape_pa_bulletin_cafo_notices() -> List[Dict]:
    """
    Scrape PA Bulletin for CAFO permit applications.
    The PA Bulletin publishes all permit applications and renewals.
    
    This is actually more reliable than the SSRS report.
    """
    farms = []
    
    # Search PA Bulletin for CAFO poultry permits
    # We need to search for recent publications
    headers = {'User-Agent': USER_AGENT}
    
    # PA Bulletin CAFO notices are in the "Actions" section
    # Example URL patterns we're looking for
    search_terms = [
        'CAFO poultry',
        'concentrated animal feeding operation poultry',
        'PAG-12 poultry',
    ]
    
    print("Note: PA Bulletin requires manual searching or more complex scraping.")
    print("The SSRS report is the better automated source.")
    
    return farms


def fetch_cafo_report_csv() -> Optional[str]:
    """
    Try to fetch the CAFO report in CSV format.
    The SSRS report can export to CSV if we construct the right URL.
    """
    # SSRS reports can be exported by adding format parameter
    # This URL structure may need adjustment
    csv_url = (
        'http://cedatareporting.pa.gov/ReportServer?'
        '/Public/DEP/CW/SSRS/WMS_CAFO_Details_Ext'
        '&rs:Command=Render'
        '&rs:Format=CSV'
    )
    
    headers = {'User-Agent': USER_AGENT}
    
    try:
        response = requests.get(csv_url, headers=headers, timeout=60)
        if response.status_code == 200 and 'text/csv' in response.headers.get('content-type', ''):
            return response.text
    except Exception as e:
        print(f"CSV fetch failed: {e}")
    
    return None


def parse_csv_report(csv_text: str) -> List[Dict]:
    """Parse the CSV export of the CAFO report."""
    import csv
    from io import StringIO
    
    farms = []
    reader = csv.DictReader(StringIO(csv_text))
    
    for row in reader:
        # Filter for poultry operations
        animal_type = row.get('Animal Type', '').lower()
        if not any(x in animal_type for x in ['poultry', 'chicken', 'layer', 'broiler', 'turkey', 'pullet']):
            continue
        
        farm = {
            'external_id': row.get('PERMIT NO', '').strip(),
            'name': row.get('PRIMARY FACILITY NAME', '').strip(),
            'owner_name': row.get('CLIENT NAME', '').strip(),
            'county': row.get('COUNTY', '').strip(),
            'city': row.get('MUNICIPALITY', '').strip(),
            'animal_equivalent_units': parse_aeu(row.get('AEU', '')),
            'operation_type': detect_operation_type(animal_type),
        }
        
        farms.append(farm)
    
    return farms


def scrape_dep_cafo_html() -> List[Dict]:
    """
    Scrape the HTML version of the CAFO report.
    This is messier but more reliable than CSV.
    """
    farms = []
    headers = {'User-Agent': USER_AGENT}
    
    # The report page renders HTML tables
    url = (
        'http://cedatareporting.pa.gov/ReportServer?'
        '/Public/DEP/CW/SSRS/WMS_CAFO_Details_Ext'
        '&rs:Command=Render'
        '&rs:Format=HTML4.0'
    )
    
    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code != 200:
            print(f"Failed to fetch report: {response.status_code}")
            return farms
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all table rows
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 5:
                    # Parse based on column structure
                    # This will need adjustment based on actual report structure
                    pass
        
    except Exception as e:
        print(f"HTML scrape failed: {e}")
    
    return farms


def get_known_pa_poultry_permits() -> List[Dict]:
    """
    Fallback: Return known permit data from PA Bulletin archives.
    This is manually curated from published notices.
    """
    # These are real examples from PA Bulletin 49-52 (Dec 2019)
    # You would build this up over time from bulletin notices
    known_farms = [
        {
            'external_id': 'PA0260274',
            'name': 'Hillside Poultry Farm',
            'owner_name': 'G Clifford Gayman',
            'address_line1': '1849 Letterkenny Road',
            'city': 'Chambersburg',
            'county': 'Franklin',
            'state': 'PA',
            'zip': '17201',
            'animal_equivalent_units': 1166.25,
            'animal_count': 402000,
            'operation_type': 'layer',
        },
        {
            'external_id': 'PA0229091',
            'name': 'Haladay Farm',
            'owner_name': 'Greg A Haladay',
            'address_line1': '224 White Church Road',
            'city': 'Elysburg',
            'county': 'Columbia',
            'state': 'PA',
            'zip': '17824',
            'animal_equivalent_units': 627.80,
            'operation_type': 'mixed',  # Poultry layers + swine + beef
        },
    ]
    
    return known_farms


def estimate_roof_size(aeu: float, operation_type: str) -> Optional[int]:
    """
    Estimate roof square footage based on AEU and operation type.
    
    Typical chicken house dimensions:
    - Broiler house: 40' x 500' = 20,000 sqft per house
    - Layer house: 60' x 600' = 36,000 sqft per house
    - Each house holds roughly 25,000-30,000 broilers or 100,000 layers (caged)
    
    AEU calculation:
    - 1 AEU = 1,000 lbs live weight
    - Broiler at market: ~6 lbs, so 1 AEU ≈ 167 broilers
    - Layer: ~4 lbs, so 1 AEU ≈ 250 layers
    """
    if not aeu:
        return None
    
    if operation_type == 'layer':
        # Layers: roughly 250 birds per AEU, ~100,000 per house, house is ~36,000 sqft
        estimated_birds = aeu * 250
        estimated_houses = estimated_birds / 100000
        return int(estimated_houses * 36000)
    elif operation_type in ['broiler', 'poultry']:
        # Broilers: roughly 167 birds per AEU, ~25,000 per house, house is ~20,000 sqft
        estimated_birds = aeu * 167
        estimated_houses = estimated_birds / 25000
        return int(estimated_houses * 20000)
    elif operation_type == 'turkey':
        # Turkeys: larger birds, fewer per house
        estimated_houses = aeu / 200  # rough estimate
        return int(estimated_houses * 25000)
    
    # Default estimate
    return int(aeu * 50)  # rough average


def collect():
    """Main collection function."""
    db = get_db()
    run_id = db.start_run(SOURCE_NAME, {'method': 'multi-source'})
    
    farms_found = []
    records_new = 0
    records_updated = 0
    error_msg = None
    
    try:
        # Try CSV export first
        print("Attempting CSV export...")
        csv_data = fetch_cafo_report_csv()
        if csv_data:
            farms_found = parse_csv_report(csv_data)
            print(f"Found {len(farms_found)} poultry farms in CSV")
        
        # If CSV failed, try HTML
        if not farms_found:
            print("CSV failed, trying HTML scrape...")
            farms_found = scrape_dep_cafo_html()
            print(f"Found {len(farms_found)} poultry farms in HTML")
        
        # If that failed too, use known permits
        if not farms_found:
            print("Scraping failed, using known permit data...")
            farms_found = get_known_pa_poultry_permits()
            print(f"Using {len(farms_found)} known farms")
        
        # Process each farm
        for farm_data in farms_found:
            # Add estimated roof size
            if farm_data.get('animal_equivalent_units'):
                farm_data['estimated_roof_sqft'] = estimate_roof_size(
                    farm_data['animal_equivalent_units'],
                    farm_data.get('operation_type', 'poultry')
                )
            
            # Set default state
            farm_data['state'] = 'PA'
            
            # Upsert to database
            external_id = farm_data.pop('external_id', f"unknown-{farm_data.get('name', 'farm')}")
            farm_id, is_new = db.upsert_farm(
                farm_data,
                SOURCE_NAME,
                external_id,
                raw_data=farm_data.copy()
            )
            
            if is_new:
                records_new += 1
                print(f"  NEW: {farm_data.get('name')} ({farm_data.get('county')} Co.)")
            else:
                records_updated += 1
            
            time.sleep(0.1)  # Brief pause
        
        # Refresh lead scores
        print("Refreshing lead scores...")
        db.refresh_lead_scores()
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error: {e}")
    
    db.complete_run(run_id, len(farms_found), records_new, records_updated, error_msg)
    
    print(f"\nCollection complete:")
    print(f"  Found: {len(farms_found)}")
    print(f"  New: {records_new}")
    print(f"  Updated: {records_updated}")
    if error_msg:
        print(f"  Error: {error_msg}")


if __name__ == '__main__':
    collect()
