# db.py - Supabase database helper
"""
Simple Supabase client wrapper for the lead database.
Uses the supabase-py library.

Install: pip install supabase
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
import json

try:
    from supabase import create_client, Client
except ImportError:
    print("Install supabase: pip install supabase")
    raise

from config import SUPABASE_URL, SUPABASE_KEY


class LeadDB:
    """Database helper for chicken farm leads."""
    
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # ============================================
    # DATA SOURCE MANAGEMENT
    # ============================================
    
    def get_source_id(self, source_name: str) -> int:
        """Get the ID of a data source by name."""
        result = self.client.table('data_sources').select('id').eq('name', source_name).single().execute()
        return result.data['id']
    
    def start_run(self, source_name: str, metadata: dict = None) -> int:
        """Start a data collection run. Returns run_id."""
        source_id = self.get_source_id(source_name)
        result = self.client.table('data_runs').insert({
            'source_id': source_id,
            'status': 'running',
            'run_metadata': metadata
        }).execute()
        return result.data[0]['id']
    
    def complete_run(self, run_id: int, records_found: int, records_new: int, 
                     records_updated: int, error: str = None):
        """Mark a run as complete."""
        status = 'failed' if error else 'success'
        self.client.table('data_runs').update({
            'completed_at': datetime.utcnow().isoformat(),
            'status': status,
            'records_found': records_found,
            'records_new': records_new,
            'records_updated': records_updated,
            'error_message': error
        }).eq('id', run_id).execute()
        
        # Update last successful run on source
        if not error:
            source_id = self.client.table('data_runs').select('source_id').eq('id', run_id).single().execute().data['source_id']
            self.client.table('data_sources').update({
                'last_successful_run': datetime.utcnow().isoformat()
            }).eq('id', source_id).execute()
    
    # ============================================
    # FARM OPERATIONS
    # ============================================
    
    def upsert_farm(self, farm_data: dict, source_name: str, external_id: str, 
                    raw_data: dict = None) -> tuple[int, bool]:
        """
        Insert or update a farm record.
        Returns (farm_id, is_new).
        
        Deduplication strategy:
        1. Try to match by external_id + source
        2. Try to match by name + county (fuzzy)
        3. If no match, insert new
        """
        source_id = self.get_source_id(source_name)
        
        # Check if we already have this farm from this source
        existing = self.client.table('farm_sources')\
            .select('farm_id')\
            .eq('source_id', source_id)\
            .eq('external_id', external_id)\
            .execute()
        
        if existing.data:
            # Update existing farm
            farm_id = existing.data[0]['farm_id']
            self.client.table('farms').update(farm_data).eq('id', farm_id).execute()
            
            # Update last_seen in farm_sources
            self.client.table('farm_sources').update({
                'last_seen': datetime.utcnow().isoformat(),
                'raw_data': raw_data
            }).eq('farm_id', farm_id).eq('source_id', source_id).execute()
            
            return farm_id, False
        
        # Try to find by name + county
        if farm_data.get('name') and farm_data.get('county'):
            existing = self.client.table('farms')\
                .select('id')\
                .eq('name', farm_data['name'])\
                .eq('county', farm_data['county'])\
                .execute()
            
            if existing.data:
                farm_id = existing.data[0]['id']
                # Merge data (don't overwrite existing good data with nulls)
                update_data = {k: v for k, v in farm_data.items() if v is not None}
                self.client.table('farms').update(update_data).eq('id', farm_id).execute()
                
                # Link this source to the farm
                self.client.table('farm_sources').upsert({
                    'farm_id': farm_id,
                    'source_id': source_id,
                    'external_id': external_id,
                    'raw_data': raw_data,
                    'last_seen': datetime.utcnow().isoformat()
                }).execute()
                
                return farm_id, False
        
        # Insert new farm
        farm_data['external_id'] = external_id
        result = self.client.table('farms').insert(farm_data).execute()
        farm_id = result.data[0]['id']
        
        # Link source
        self.client.table('farm_sources').insert({
            'farm_id': farm_id,
            'source_id': source_id,
            'external_id': external_id,
            'raw_data': raw_data
        }).execute()
        
        return farm_id, True
    
    def get_farm(self, farm_id: int) -> Optional[dict]:
        """Get a single farm by ID."""
        result = self.client.table('farms').select('*').eq('id', farm_id).single().execute()
        return result.data
    
    def get_farms_by_status(self, status: str) -> List[dict]:
        """Get all farms with a given lead status."""
        result = self.client.table('farms').select('*').eq('lead_status', status).execute()
        return result.data
    
    def get_farms_needing_enrichment(self, limit: int = 100) -> List[dict]:
        """Get farms that need contact/property enrichment."""
        result = self.client.table('farms')\
            .select('*')\
            .is_('phone', 'null')\
            .is_('email', 'null')\
            .eq('is_active', True)\
            .order('lead_score', desc=True)\
            .limit(limit)\
            .execute()
        return result.data
    
    def update_farm(self, farm_id: int, data: dict):
        """Update a farm record."""
        self.client.table('farms').update(data).eq('id', farm_id).execute()
    
    # ============================================
    # CRM OPERATIONS
    # ============================================
    
    def add_activity(self, farm_id: int, activity_type: str, description: str,
                     contact_id: int = None, direction: str = 'outbound',
                     outcome: str = None, next_action: str = None,
                     next_action_date: str = None, performed_by: str = None):
        """Log an activity (call, email, meeting, etc.)."""
        self.client.table('activities').insert({
            'farm_id': farm_id,
            'contact_id': contact_id,
            'activity_type': activity_type,
            'direction': direction,
            'description': description,
            'outcome': outcome,
            'next_action': next_action,
            'next_action_date': next_action_date,
            'performed_by': performed_by
        }).execute()
    
    def add_contact(self, farm_id: int, name: str, phone: str = None,
                    email: str = None, title: str = None, 
                    is_primary: bool = False) -> int:
        """Add a contact for a farm."""
        result = self.client.table('contacts').insert({
            'farm_id': farm_id,
            'name': name,
            'phone': phone,
            'email': email,
            'title': title,
            'is_primary': is_primary
        }).execute()
        return result.data[0]['id']
    
    def add_note(self, farm_id: int, note: str, note_type: str = 'general',
                 created_by: str = None):
        """Add a note to a farm."""
        self.client.table('farm_notes').insert({
            'farm_id': farm_id,
            'note': note,
            'note_type': note_type,
            'created_by': created_by
        }).execute()
    
    def get_today_followups(self) -> List[dict]:
        """Get farms that need follow-up today."""
        result = self.client.rpc('v_today_followups').execute()
        return result.data
    
    # ============================================
    # PROPERTY DATA
    # ============================================
    
    def add_property_data(self, farm_id: int, property_data: dict):
        """Add property/assessor data for a farm."""
        property_data['farm_id'] = farm_id
        self.client.table('property_data').upsert(property_data).execute()
    
    # ============================================
    # UTILITY
    # ============================================
    
    def refresh_lead_scores(self):
        """Recalculate lead scores for all farms."""
        self.client.rpc('refresh_lead_scores').execute()
    
    def get_stats(self) -> dict:
        """Get summary statistics."""
        total = self.client.table('farms').select('id', count='exact').execute()
        by_status = self.client.table('farms').select('lead_status').execute()
        
        status_counts = {}
        for row in by_status.data:
            status = row['lead_status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'total_farms': total.count,
            'by_status': status_counts
        }


# Singleton instance
_db = None

def get_db() -> LeadDB:
    """Get database instance."""
    global _db
    if _db is None:
        _db = LeadDB()
    return _db
