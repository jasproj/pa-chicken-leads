-- PA Chicken Farm Lead Database Schema
-- Run this in Supabase SQL Editor

-- ============================================
-- CORE TABLES
-- ============================================

-- Data sources we pull from
CREATE TABLE data_sources (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL, -- 'api', 'scrape', 'manual', 'enrichment'
    base_url TEXT,
    last_successful_run TIMESTAMPTZ,
    run_frequency_hours INTEGER DEFAULT 168, -- weekly default
    is_active BOOLEAN DEFAULT true,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Raw data ingestion log (for debugging/audit)
CREATE TABLE data_runs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES data_sources(id),
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running', -- 'running', 'success', 'failed'
    records_found INTEGER DEFAULT 0,
    records_new INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT,
    run_metadata JSONB
);

-- ============================================
-- FARM/LEAD DATA
-- ============================================

-- Master farm table - the core of your CRM
CREATE TABLE farms (
    id SERIAL PRIMARY KEY,
    
    -- Identifiers
    external_id TEXT, -- permit number, USDA id, etc.
    name TEXT NOT NULL,
    dba_name TEXT, -- "doing business as"
    
    -- Location
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    county TEXT,
    state TEXT DEFAULT 'PA',
    zip TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Farm characteristics (for solar sizing)
    operation_type TEXT, -- 'broiler', 'layer', 'turkey', 'mixed'
    animal_count INTEGER,
    animal_equivalent_units DECIMAL(10,2), -- AEUs from CAFO permit
    estimated_houses INTEGER,
    estimated_roof_sqft INTEGER,
    
    -- Ownership/contact
    owner_name TEXT,
    operator_name TEXT,
    phone TEXT,
    email TEXT,
    
    -- Business relationships
    integrator TEXT, -- 'Bell & Evans', 'Perdue', etc.
    
    -- Data quality
    data_confidence DECIMAL(3,2) DEFAULT 0.5, -- 0-1 score
    last_verified TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT true,
    
    -- CRM status
    lead_status TEXT DEFAULT 'new', -- 'new', 'researching', 'contacted', 'qualified', 'proposal', 'won', 'lost', 'not_interested'
    lead_score INTEGER DEFAULT 0, -- 0-100, higher = better prospect
    assigned_to TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Prevent duplicates
    UNIQUE(external_id, name, county)
);

-- Track which sources contributed to each farm record
CREATE TABLE farm_sources (
    id SERIAL PRIMARY KEY,
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    source_id INTEGER REFERENCES data_sources(id),
    external_id TEXT, -- ID in the source system
    raw_data JSONB, -- store original record for debugging
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(farm_id, source_id)
);

-- ============================================
-- CRM / OUTREACH TRACKING
-- ============================================

-- Contact people at farms
CREATE TABLE contacts (
    id SERIAL PRIMARY KEY,
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    title TEXT,
    phone TEXT,
    email TEXT,
    linkedin_url TEXT,
    is_primary BOOLEAN DEFAULT false,
    is_decision_maker BOOLEAN DEFAULT false,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Activity log (calls, emails, meetings)
CREATE TABLE activities (
    id SERIAL PRIMARY KEY,
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    contact_id INTEGER REFERENCES contacts(id),
    activity_type TEXT NOT NULL, -- 'call', 'email', 'meeting', 'site_visit', 'mail', 'note'
    direction TEXT, -- 'outbound', 'inbound'
    subject TEXT,
    description TEXT,
    outcome TEXT, -- 'interested', 'not_interested', 'callback', 'no_answer', 'wrong_number'
    next_action TEXT,
    next_action_date DATE,
    performed_by TEXT,
    performed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Notes/comments on farms
CREATE TABLE farm_notes (
    id SERIAL PRIMARY KEY,
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    note TEXT NOT NULL,
    note_type TEXT DEFAULT 'general', -- 'general', 'research', 'objection', 'opportunity'
    created_by TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tags for filtering/segmenting
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    color TEXT DEFAULT '#3B82F6' -- hex color for UI
);

CREATE TABLE farm_tags (
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (farm_id, tag_id)
);

-- ============================================
-- PROPERTY DATA (for roof sizing)
-- ============================================

CREATE TABLE property_data (
    id SERIAL PRIMARY KEY,
    farm_id INTEGER REFERENCES farms(id) ON DELETE CASCADE,
    parcel_id TEXT,
    parcel_address TEXT,
    owner_name TEXT,
    land_acres DECIMAL(10,2),
    building_sqft INTEGER,
    roof_sqft INTEGER, -- estimated or from aerial
    year_built INTEGER,
    assessed_value DECIMAL(12,2),
    last_sale_date DATE,
    last_sale_price DECIMAL(12,2),
    zoning TEXT,
    source TEXT, -- which county assessor
    raw_data JSONB,
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX idx_farms_county ON farms(county);
CREATE INDEX idx_farms_lead_status ON farms(lead_status);
CREATE INDEX idx_farms_operation_type ON farms(operation_type);
CREATE INDEX idx_farms_integrator ON farms(integrator);
CREATE INDEX idx_farms_lead_score ON farms(lead_score DESC);
CREATE INDEX idx_activities_farm_id ON activities(farm_id);
CREATE INDEX idx_activities_next_action_date ON activities(next_action_date);

-- ============================================
-- VIEWS FOR EASY QUERYING
-- ============================================

-- Main CRM view - your daily working list
CREATE VIEW v_lead_pipeline AS
SELECT 
    f.id,
    f.name,
    f.county,
    f.city,
    f.operation_type,
    f.animal_equivalent_units as aeu,
    f.estimated_roof_sqft,
    f.integrator,
    f.lead_status,
    f.lead_score,
    f.owner_name,
    f.phone,
    f.email,
    f.assigned_to,
    f.updated_at,
    (SELECT COUNT(*) FROM activities a WHERE a.farm_id = f.id) as activity_count,
    (SELECT MAX(performed_at) FROM activities a WHERE a.farm_id = f.id) as last_activity,
    (SELECT MIN(next_action_date) FROM activities a WHERE a.farm_id = f.id AND a.next_action_date >= CURRENT_DATE) as next_action_due
FROM farms f
WHERE f.is_active = true
ORDER BY f.lead_score DESC, f.updated_at DESC;

-- Hot leads needing follow-up today
CREATE VIEW v_today_followups AS
SELECT * FROM v_lead_pipeline
WHERE next_action_due <= CURRENT_DATE
ORDER BY next_action_due, lead_score DESC;

-- Best prospects (high AEU = big roofs)
CREATE VIEW v_best_prospects AS
SELECT * FROM v_lead_pipeline
WHERE lead_status IN ('new', 'researching', 'qualified')
AND animal_equivalent_units >= 300
ORDER BY animal_equivalent_units DESC;

-- ============================================
-- FUNCTIONS
-- ============================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER farms_updated_at
    BEFORE UPDATE ON farms
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- Calculate lead score based on farm characteristics
CREATE OR REPLACE FUNCTION calculate_lead_score(farm_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    score INTEGER := 0;
    farm_record RECORD;
BEGIN
    SELECT * INTO farm_record FROM farms WHERE id = farm_id;
    
    -- AEU scoring (bigger = better roof)
    IF farm_record.animal_equivalent_units >= 1000 THEN score := score + 30;
    ELSIF farm_record.animal_equivalent_units >= 500 THEN score := score + 20;
    ELSIF farm_record.animal_equivalent_units >= 300 THEN score := score + 10;
    END IF;
    
    -- Has contact info
    IF farm_record.phone IS NOT NULL THEN score := score + 15; END IF;
    IF farm_record.email IS NOT NULL THEN score := score + 15; END IF;
    
    -- Has owner name
    IF farm_record.owner_name IS NOT NULL THEN score := score + 10; END IF;
    
    -- Has estimated roof
    IF farm_record.estimated_roof_sqft IS NOT NULL THEN score := score + 10; END IF;
    
    -- Recent verification
    IF farm_record.last_verified > NOW() - INTERVAL '90 days' THEN score := score + 10; END IF;
    
    -- Data confidence
    score := score + ROUND(farm_record.data_confidence * 10);
    
    RETURN LEAST(score, 100); -- cap at 100
END;
$$ LANGUAGE plpgsql;

-- Update all lead scores (run periodically)
CREATE OR REPLACE FUNCTION refresh_lead_scores()
RETURNS void AS $$
BEGIN
    UPDATE farms SET lead_score = calculate_lead_score(id);
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- SEED DATA
-- ============================================

-- Insert data sources
INSERT INTO data_sources (name, source_type, base_url, notes) VALUES
('PA DEP CAFO Permits', 'scrape', 'http://cedatareporting.pa.gov', 'Primary source - all permitted poultry operations 300+ AEU'),
('USDA NASS Census', 'api', 'https://quickstats.nass.usda.gov/api', 'County-level poultry statistics'),
('PA Dept of Agriculture', 'scrape', 'https://www.agriculture.pa.gov', 'Farm directory listings'),
('Manual Research', 'manual', NULL, 'Direct outreach, LinkedIn, Google searches'),
('County Property Records', 'enrichment', NULL, 'Property assessor data for roof sizing');

-- Insert common tags
INSERT INTO tags (name, color) VALUES
('high-priority', '#EF4444'),
('large-operation', '#F59E0B'),
('has-contact', '#10B981'),
('needs-research', '#6366F1'),
('bell-evans', '#8B5CF6'),
('perdue', '#EC4899'),
('organic', '#22C55E'),
('contacted', '#3B82F6');
