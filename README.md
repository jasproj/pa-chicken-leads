# PA Chicken Farm Lead Collector

A lead generation system for finding chicken farms in Pennsylvania suitable for solar roof lease contracts.

## Business Model

1. **Find chicken farms** with large roof footprints (commercial operations have 20,000-40,000 sqft per house)
2. **Lease their roof space** - pay farmers ~$100k upfront
3. **Install solar panels** on the leased roofs
4. **Sell power back to grid** and collect state credits

## Data Sources

| Source | Type | What We Get | Update Frequency |
|--------|------|-------------|------------------|
| PA DEP CAFO Permits | Scrape | All permitted poultry ops (300+ AEU), facility name, operator, location | Weekly |
| USDA NASS Census | API | County-level statistics, total birds, # operations | Census years |
| Manual Research | CLI | Direct research, Google, LinkedIn, referrals | Ongoing |
| County Property Records | Enrichment | Parcel data, roof size, owner info | As needed |

## Quick Start

### 1. Set Up Supabase

1. Create a free account at [supabase.com](https://supabase.com)
2. Create a new project
3. Go to **SQL Editor** and run the schema:
   ```sql
   -- Paste contents of sql/001_schema.sql
   ```
4. Get your API credentials from **Settings > API**:
   - Project URL (looks like `https://xxxx.supabase.co`)
   - `anon` public key

### 2. Configure Environment

```bash
# Copy config template
cp collectors/config.py collectors/config_local.py

# Edit with your credentials
# Or set environment variables:
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-anon-key"
export NASS_API_KEY="your-nass-key"  # Get free at https://quickstats.nass.usda.gov/api
```

### 3. Install Dependencies

```bash
pip install requests beautifulsoup4 supabase
```

### 4. Run Collectors

```bash
cd collectors

# Run all collectors
python run_collectors.py all

# Or run individually
python run_collectors.py dep    # PA DEP permits
python run_collectors.py nass   # USDA statistics

# View results
python run_collectors.py stats
python run_collectors.py top 20
```

### 5. Manual Lead Entry

```bash
# Add a farm interactively
python manual_entry.py add

# Import from CSV
python manual_entry.py import my_leads.csv

# Log an activity
python manual_entry.py log
```

## Database Schema

### Core Tables

- **farms** - Master lead table with all farm data
- **contacts** - People at each farm
- **activities** - Call/email/meeting log (CRM)
- **farm_notes** - Research notes
- **tags** - For segmentation

### Key Views

- **v_lead_pipeline** - Main CRM working list
- **v_today_followups** - Leads needing action today
- **v_best_prospects** - High-value targets (large AEU)

### Lead Scoring

Farms are automatically scored 0-100 based on:
- AEU size (bigger = more roof)
- Contact info availability
- Data freshness
- Verification status

## Using Supabase as a CRM

Supabase's **Table Editor** works great as a basic CRM:

1. Go to your project's **Table Editor**
2. Click on the `farms` table
3. Use filters to segment:
   - `lead_status = 'new'` for fresh leads
   - `county = 'Lancaster'` for geographic focus
   - `animal_equivalent_units > 500` for large operations
4. Click any row to edit status, add notes
5. Use the `activities` table to log calls/emails

### CRM Workflow

```
new → researching → contacted → qualified → proposal → won/lost
```

## Automated Collection (GitHub Actions)

1. Push this repo to GitHub
2. Go to **Settings > Secrets and variables > Actions**
3. Add secrets:
   - `SUPABASE_URL`
   - `SUPABASE_KEY`
   - `NASS_API_KEY`
4. The workflow runs weekly (Sunday 6 AM UTC)
5. Or trigger manually: **Actions > Collect PA Chicken Farm Leads > Run workflow**

## PA Poultry Industry Notes

### Top Counties by Production
1. Lancaster County - ~600 poultry operations, 40M+ birds
2. Lebanon County
3. Berks County
4. Chester County
5. York County

### Major Integrators
Most PA chicken farms are **contract growers** for:
- **Bell & Evans** - Premium/organic, Central PA (Fredericksburg)
- **Perdue** - Lancaster, York counties

### Typical Farm Specs
- **Broiler house**: 40' × 500' = 20,000 sqft roof
- **Layer house**: 60' × 600' = 36,000 sqft roof
- Most farms have 2-6 houses
- **Solar potential**: 100-200 kW per house

## File Structure

```
pa-chicken-leads/
├── collectors/
│   ├── config.py           # Configuration
│   ├── db.py               # Supabase database helper
│   ├── collect_dep_cafo.py # PA DEP CAFO permit scraper
│   ├── collect_nass.py     # USDA NASS API collector
│   ├── manual_entry.py     # Manual lead entry CLI
│   └── run_collectors.py   # Main orchestration script
├── sql/
│   └── 001_schema.sql      # Database schema
├── .github/
│   └── workflows/
│       └── collect.yml     # GitHub Actions workflow
└── README.md
```

## Extending the System

### Adding a New Data Source

1. Create `collectors/collect_newsource.py`
2. Implement a `collect()` function
3. Use `db.upsert_farm()` to store data
4. Add to `run_collectors.py`

### Contact Enrichment

For phone/email enrichment, you could add:
- **Apollo.io** - Has a free tier, good for B2B
- **Hunter.io** - Email finder
- **LinkedIn Sales Navigator** - Manual but effective

### Property Data

For roof sizing, integrate:
- County assessor websites (varies by county)
- Google Maps API for aerial measurements
- Regrid.com API for parcel data

## Cost

| Component | Cost |
|-----------|------|
| Supabase | Free tier (500MB, 50K requests/mo) |
| USDA NASS API | Free |
| PA DEP Data | Free (public records) |
| GitHub Actions | Free (2,000 min/mo) |
| **Total** | **$0/month** |

Paid enrichment (optional):
- Apollo.io: $49/mo for 1,200 credits
- Hunter.io: $49/mo for 1,000 requests

## License

MIT - Use freely for your solar prospecting!

## Support

Built for Jason's PA solar chicken farm project. Questions? Open an issue.
