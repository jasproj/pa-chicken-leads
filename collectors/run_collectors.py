#!/usr/bin/env python3
"""
PA Chicken Farm Lead Collector - Main Runner

Orchestrates data collection from all sources.

Usage:
    python run_collectors.py all          # Run all collectors
    python run_collectors.py dep          # Just PA DEP CAFO
    python run_collectors.py nass         # Just USDA NASS stats
    python run_collectors.py stats        # Show database statistics
    python run_collectors.py export       # Export leads to CSV
"""

import sys
from datetime import datetime

from db import get_db


def run_all():
    """Run all collectors."""
    print("="*60)
    print("PA CHICKEN FARM LEAD COLLECTOR")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Run DEP CAFO collector
    print("\n[1/2] Running PA DEP CAFO collector...")
    try:
        from collect_dep_cafo import collect as collect_dep
        collect_dep()
    except Exception as e:
        print(f"DEP collector error: {e}")
    
    # Run NASS collector
    print("\n[2/2] Running USDA NASS collector...")
    try:
        from collect_nass import collect as collect_nass
        collect_nass()
    except Exception as e:
        print(f"NASS collector error: {e}")
    
    # Show final stats
    print("\n" + "="*60)
    print("COLLECTION COMPLETE")
    show_stats()


def run_dep():
    """Run just the PA DEP collector."""
    from collect_dep_cafo import collect
    collect()


def run_nass():
    """Run just the USDA NASS collector."""
    from collect_nass import collect
    collect()


def show_stats():
    """Show database statistics."""
    db = get_db()
    stats = db.get_stats()
    
    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)
    print(f"Total farms: {stats['total_farms']}")
    print("\nBy lead status:")
    for status, count in sorted(stats['by_status'].items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")


def export_csv(filename: str = 'leads_export.csv'):
    """Export all leads to CSV."""
    import csv
    
    db = get_db()
    
    # Get all farms via Supabase
    result = db.client.table('farms').select('*').order('lead_score', desc=True).execute()
    farms = result.data
    
    if not farms:
        print("No farms to export")
        return
    
    # Get column names from first record
    columns = list(farms[0].keys())
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(farms)
    
    print(f"Exported {len(farms)} farms to {filename}")


def show_top_leads(n: int = 20):
    """Show top leads by score."""
    db = get_db()
    
    result = db.client.table('farms')\
        .select('id,name,county,operation_type,animal_equivalent_units,estimated_roof_sqft,owner_name,phone,lead_status,lead_score')\
        .order('lead_score', desc=True)\
        .limit(n)\
        .execute()
    
    print("\n" + "="*80)
    print(f"TOP {n} LEADS")
    print("="*80)
    print(f"{'ID':<6}{'Name':<30}{'County':<15}{'AEU':<10}{'Roof (sqft)':<15}{'Score':<8}{'Status':<15}")
    print("-"*80)
    
    for farm in result.data:
        print(f"{farm['id']:<6}"
              f"{(farm['name'] or 'Unknown')[:28]:<30}"
              f"{(farm['county'] or '?'):<15}"
              f"{farm.get('animal_equivalent_units') or 'N/A':<10}"
              f"{farm.get('estimated_roof_sqft') or 'N/A':<15}"
              f"{farm['lead_score']:<8}"
              f"{farm['lead_status']:<15}")


def main():
    if len(sys.argv) < 2:
        print("PA Chicken Farm Lead Collector")
        print("\nUsage:")
        print("  python run_collectors.py all      - Run all collectors")
        print("  python run_collectors.py dep      - Run PA DEP CAFO collector")
        print("  python run_collectors.py nass     - Run USDA NASS collector")
        print("  python run_collectors.py stats    - Show database statistics")
        print("  python run_collectors.py export   - Export leads to CSV")
        print("  python run_collectors.py top      - Show top 20 leads")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'all':
        run_all()
    elif command == 'dep':
        run_dep()
    elif command == 'nass':
        run_nass()
    elif command == 'stats':
        show_stats()
    elif command == 'export':
        filename = sys.argv[2] if len(sys.argv) > 2 else 'leads_export.csv'
        export_csv(filename)
    elif command == 'top':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        show_top_leads(n)
    else:
        print(f"Unknown command: {command}")


if __name__ == '__main__':
    main()
