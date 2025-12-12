#!/usr/bin/env python3
"""
Manual Lead Entry Helper

A simple CLI for manually adding farm leads discovered through research:
- Google searches
- LinkedIn
- Driving by farms
- Referrals
- Industry contacts

Usage:
    python manual_entry.py add
    python manual_entry.py import farms.csv
"""

import sys
import csv
from datetime import datetime

from db import get_db

SOURCE_NAME = 'Manual Research'


def add_farm_interactive():
    """Interactive prompts to add a farm."""
    print("\n" + "="*50)
    print("ADD NEW FARM LEAD")
    print("="*50)
    print("(Press Enter to skip optional fields)\n")
    
    # Required
    name = input("Farm/Business Name*: ").strip()
    if not name:
        print("Name is required.")
        return
    
    county = input("County*: ").strip()
    if not county:
        print("County is required.")
        return
    
    # Optional but important
    print("\n-- Contact Info --")
    owner = input("Owner/Operator Name: ").strip() or None
    phone = input("Phone: ").strip() or None
    email = input("Email: ").strip() or None
    
    print("\n-- Location --")
    address = input("Street Address: ").strip() or None
    city = input("City: ").strip() or None
    zip_code = input("ZIP: ").strip() or None
    
    print("\n-- Farm Details --")
    operation_type = input("Type (broiler/layer/turkey/mixed): ").strip() or 'poultry'
    integrator = input("Integrator (Bell & Evans/Perdue/etc): ").strip() or None
    
    aeu_str = input("Estimated AEU (if known): ").strip()
    aeu = float(aeu_str) if aeu_str else None
    
    houses_str = input("Number of chicken houses: ").strip()
    houses = int(houses_str) if houses_str else None
    
    print("\n-- Notes --")
    notes = input("Notes (how you found them, etc): ").strip() or None
    
    # Build farm data
    farm_data = {
        'name': name,
        'county': county,
        'state': 'PA',
        'owner_name': owner,
        'phone': phone,
        'email': email,
        'address_line1': address,
        'city': city,
        'zip': zip_code,
        'operation_type': operation_type,
        'integrator': integrator,
        'animal_equivalent_units': aeu,
        'estimated_houses': houses,
        'data_confidence': 0.8,  # Manual research is usually pretty good
        'last_verified': datetime.utcnow().isoformat(),
    }
    
    # Estimate roof if we have houses
    if houses:
        # Typical house is 20,000 - 40,000 sqft
        farm_data['estimated_roof_sqft'] = houses * 25000
    
    # Save to database
    db = get_db()
    external_id = f"manual-{name.lower().replace(' ', '-')}-{county.lower()}"
    
    farm_id, is_new = db.upsert_farm(
        farm_data,
        SOURCE_NAME,
        external_id,
        raw_data={'source': 'manual_entry', 'notes': notes}
    )
    
    # Add notes if provided
    if notes:
        db.add_note(farm_id, notes, note_type='research')
    
    status = "Created new" if is_new else "Updated existing"
    print(f"\n✓ {status} farm record (ID: {farm_id})")
    
    # Offer to add more
    another = input("\nAdd another? (y/n): ").strip().lower()
    if another == 'y':
        add_farm_interactive()


def import_csv(filepath: str):
    """
    Import farms from a CSV file.
    
    Expected columns (flexible - will use what's available):
    - name (required)
    - county (required)
    - owner_name
    - phone
    - email
    - address
    - city
    - zip
    - operation_type
    - integrator
    - aeu
    - houses
    - notes
    """
    db = get_db()
    
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        
        count_new = 0
        count_updated = 0
        
        for row in reader:
            # Map CSV columns to database fields
            farm_data = {
                'name': row.get('name', '').strip(),
                'county': row.get('county', '').strip(),
                'state': 'PA',
                'owner_name': row.get('owner_name') or row.get('owner') or None,
                'phone': row.get('phone') or None,
                'email': row.get('email') or None,
                'address_line1': row.get('address') or row.get('address_line1') or None,
                'city': row.get('city') or None,
                'zip': row.get('zip') or row.get('zip_code') or None,
                'operation_type': row.get('operation_type') or row.get('type') or 'poultry',
                'integrator': row.get('integrator') or None,
                'data_confidence': 0.7,
            }
            
            # Skip if missing required fields
            if not farm_data['name'] or not farm_data['county']:
                print(f"Skipping row - missing name or county: {row}")
                continue
            
            # Parse numeric fields
            aeu = row.get('aeu') or row.get('animal_equivalent_units')
            if aeu:
                try:
                    farm_data['animal_equivalent_units'] = float(aeu)
                except ValueError:
                    pass
            
            houses = row.get('houses') or row.get('estimated_houses')
            if houses:
                try:
                    farm_data['estimated_houses'] = int(houses)
                    farm_data['estimated_roof_sqft'] = int(houses) * 25000
                except ValueError:
                    pass
            
            # Save
            external_id = f"csv-{farm_data['name'].lower().replace(' ', '-')}"
            farm_id, is_new = db.upsert_farm(
                farm_data,
                SOURCE_NAME,
                external_id,
                raw_data=dict(row)
            )
            
            # Add notes if present
            notes = row.get('notes')
            if notes:
                db.add_note(farm_id, notes, note_type='research')
            
            if is_new:
                count_new += 1
                print(f"  NEW: {farm_data['name']}")
            else:
                count_updated += 1
        
        print(f"\nImport complete: {count_new} new, {count_updated} updated")


def log_activity_interactive():
    """Log an outreach activity for a farm."""
    db = get_db()
    
    print("\n" + "="*50)
    print("LOG ACTIVITY")
    print("="*50)
    
    farm_id = input("Farm ID: ").strip()
    if not farm_id:
        return
    
    farm = db.get_farm(int(farm_id))
    if not farm:
        print("Farm not found")
        return
    
    print(f"\nFarm: {farm['name']} ({farm['county']} Co.)")
    
    activity_type = input("Type (call/email/meeting/site_visit/mail/note): ").strip()
    description = input("Description: ").strip()
    outcome = input("Outcome (interested/not_interested/callback/no_answer): ").strip() or None
    next_action = input("Next action: ").strip() or None
    next_date = input("Next action date (YYYY-MM-DD): ").strip() or None
    
    db.add_activity(
        farm_id=int(farm_id),
        activity_type=activity_type,
        description=description,
        outcome=outcome,
        next_action=next_action,
        next_action_date=next_date,
        performed_by='manual'
    )
    
    # Update lead status if appropriate
    if outcome == 'interested':
        db.update_farm(int(farm_id), {'lead_status': 'qualified'})
        print("Updated status to 'qualified'")
    elif outcome == 'not_interested':
        db.update_farm(int(farm_id), {'lead_status': 'not_interested'})
        print("Updated status to 'not_interested'")
    
    print("✓ Activity logged")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python manual_entry.py add        - Add farm interactively")
        print("  python manual_entry.py import FILE.csv  - Import from CSV")
        print("  python manual_entry.py log        - Log an activity")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'add':
        add_farm_interactive()
    elif command == 'import' and len(sys.argv) > 2:
        import_csv(sys.argv[2])
    elif command == 'log':
        log_activity_interactive()
    else:
        print(f"Unknown command: {command}")


if __name__ == '__main__':
    main()
