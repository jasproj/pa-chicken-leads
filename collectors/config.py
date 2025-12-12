# config.py - Configuration for PA Chicken Farm Lead Collector
# Copy this to config_local.py and fill in your actual values

import os

# Supabase connection
# Get these from: Supabase Dashboard > Settings > API
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://YOUR_PROJECT.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'your-anon-key-here')

# USDA NASS API Key (free)
# Get one at: https://quickstats.nass.usda.gov/api
NASS_API_KEY = os.getenv('NASS_API_KEY', 'your-nass-api-key')

# Optional: Apollo.io for contact enrichment (has free tier)
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY', '')

# Scraping settings
REQUEST_DELAY_SECONDS = 2  # Be nice to servers
USER_AGENT = 'PAChickenFarmResearch/1.0 (educational solar research)'

# Target counties in PA with significant poultry
# Lancaster County alone has ~600 poultry operations
PA_POULTRY_COUNTIES = [
    'Lancaster',
    'Lebanon',
    'Berks',
    'Chester',
    'York',
    'Adams',
    'Franklin',
    'Cumberland',
    'Perry',
    'Dauphin',
    'Northumberland',
    'Snyder',
    'Union',
    'Juniata',
    'Mifflin',
    'Centre',
    'Clinton',
    'Lycoming',
    'Bradford',
    'Wayne',
]

# Major integrators to track
INTEGRATORS = [
    'Bell & Evans',
    'Perdue',
    'Tyson',
    'Pilgrim\'s Pride',
    'Koch Foods',
    'Wenger Feeds',
]
