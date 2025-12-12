#!/usr/bin/env python3
"""
Contact Enrichment Crawler
Searches for phone numbers and emails for farms in the database.
"""

import re
import time
import random
import argparse
import os
from urllib.parse import quote_plus
from typing import Optional, Tuple, List
import requests
from bs4 import BeautifulSoup

from supabase import create_client

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 5

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
]

PHONE_PATTERNS = [
    r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
    r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',
]

EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'


def random_delay():
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }


def clean_phone(phone: str) -> str:
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def extract_phones(text: str) -> List[str]:
    phones = []
    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, text)
        for match in matches:
            cleaned = clean_phone(match)
            if cleaned not in phones:
                phones.append(cleaned)
    return phones


def extract_emails(text: str) -> List[str]:
    emails = re.findall(EMAIL_PATTERN, text.lower())
    filtered = []
    for email in emails:
        if not any(x in email for x in ['example.com', 'domain.com', '.png', '.jpg']):
            if email not in filtered:
                filtered.append(email)
    return filtered


def search_duckduckgo(query: str) -> List[str]:
    urls = []
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        response = requests.get(search_url, headers=get_headers(), timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for link in soup.select('.result__a'):
                href = link.get('href', '')
                if href and href.startswith('http'):
                    urls.append(href)
    except Exception as e:
        print(f"    Search error: {e}")
    return urls[:5]


def fetch_page(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            return response.text
    except:
        pass
    return None


def find_contact_info(farm_name: str, owner_name: str, city: str, county: str) -> Tuple[Optional[str], Optional[str]]:
    phone = None
    email = None
    
    queries = [
        f'"{farm_name}" {city} PA phone contact',
        f'{farm_name} {county} county Pennsylvania',
    ]
    
    print(f"    Searching for: {farm_name}")
    
    for query in queries:
        urls = search_duckduckgo(query)
        if urls:
            break
        random_delay()
    
    for url in urls[:3]:
        content = fetch_page(url)
        if not content:
            continue
        
        phones = extract_phones(content)
        emails = extract_emails(content)
        
        if phones and not phone:
            phone = phones[0]
            print(f"      Found phone: {phone}")
        
        if emails and not email:
            for e in emails:
                if any(x in e for x in ['farm', 'poultry', 'egg', 'info', 'contact']):
                    email = e
                    break
            if not email and emails:
                email = emails[0]
            if email:
                print(f"      Found email: {email}")
        
        if phone and email:
            break
        
        random_delay()
    
    return phone, email


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10)
    args = parser.parse_args()
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY required")
        return
    
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Get farms missing contact info, prioritized by lead_score
    result = client.table('farms')\
        .select('*')\
        .is_('phone', 'null')\
        .eq('is_active', True)\
        .order('lead_score', desc=True)\
        .limit(args.limit)\
        .execute()
    
    farms = result.data
    print(f"Found {len(farms)} farms to enrich\n")
    
    enriched = 0
    for i, farm in enumerate(farms, 1):
        print(f"[{i}/{len(farms)}] {farm['name']} ({farm.get('city', '?')}, {farm.get('county', '?')} Co.)")
        
        phone, email = find_contact_info(
            farm.get('name', ''),
            farm.get('owner_name', ''),
            farm.get('city', ''),
            farm.get('county', '')
        )
        
        updates = {}
        if phone:
            updates['phone'] = phone
        if email:
            updates['email'] = email
        
        if updates:
            client.table('farms').update(updates).eq('id', farm['id']).execute()
            enriched += 1
            print(f"    ✓ Updated")
        else:
            print(f"    ✗ No contact info found")
        
        print()
        random_delay()
    
    print(f"\nDone: {enriched}/{len(farms)} farms enriched")


if __name__ == '__main__':
    main()
