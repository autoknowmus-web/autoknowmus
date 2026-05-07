-- ============================================================================
-- migration_9_listing_calibration_pipeline.sql
-- ----------------------------------------------------------------------------
-- AutoKnowMus  ·  v3.5.1-r7  ·  Listing Calibration Pipeline (Phase 1)
--
-- Adds schema support for the Listing Calibration Pipeline — an internal
-- admin tool to calibrate the AutoKnowMus pricing engine using marketplace
-- listings (CarWale Bangalore in v1) until verified deal volume reaches
-- sustained scale.
--
-- Companion design doc: listing_calibration_pipeline_spec.md
-- Companion policy doc: listing_data_internal_use_policy.md
--
-- WHAT THIS MIGRATION DOES
--   1. Creates listing_uploads — provenance for each CSV upload
--   2. Creates buffer_config — brand×segment buffer matrix (~50 cells)
--   3. Creates buffer_config_audit — audit log for buffer matrix changes
--   4. Adds research_log.listing_upload_id (nullable FK) — links listing
--      entries back to their source CSV upload
--   5. Adds 4 indexes to support the queries the admin UI will run
--   6. Seeds buffer_config with the default brand×segment buffer table
--      from spec section 3
--
-- DESIGN DECISIONS (locked, mirrors Migration 8 patterns)
--   - data_source remains plain TEXT (no CHECK constraint added).
--     The new value 'Listing Aggregator' is enforced in Python via the
--     RESEARCH_SOURCES list in app.py, NOT at the DB level. Same reasoning
--     as Migration 8: keeps schema flexible for future sources.
--   - buffer_config has 1 cell per (brand_group, segment) combination.
--     UNIQUE(brand_group, segment) enforces this at DB level.
--   - buffer_config.is_default is TRUE for seeded defaults; flips to FALSE
--     the first time an admin overrides that cell. Lets us distinguish
--     "untouched seed" from "deliberately tuned" for the buffer matrix UI.
--   - buffer_config_audit captures BOTH old + new buffer_pct so the UI
--     can render diffs without joining back to the live config.
--   - listing_upload_id is NULLABLE — existing research_log rows are
--     Mystery Shopping / Friends & Family / Forum Post and have no
--     upload to link to. Only Listing Aggregator entries populate it.
--   - state_code in listing_uploads is intentionally NOT FK'd to
--     state_multipliers. Reason: state_multipliers is admin-managed and
--     we don't want a missing state row blocking a CSV upload. Validation
--     happens in Python.
--   - listing entries land with buyer_type=NULL (listings aren't deals,
--     they're asks). No change needed to existing buyer_type CHECK
--     constraint from Migration 8.
--
-- IDEMPOTENT
--   Every CREATE TABLE, ADD COLUMN, ADD CONSTRAINT, and CREATE INDEX uses
--   IF NOT EXISTS (or DO block guard). Safe to run multiple times.
--   Buffer matrix seed uses ON CONFLICT DO NOTHING — re-running won't
--   overwrite admin-tuned cells.
--
-- ROLLBACK
--   See "ROLLBACK SCRIPT" at the very bottom of this file (commented out).
--   Drops the new tables / columns / indexes if you need to undo this.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. CREATE TABLE listing_uploads
-- ----------------------------------------------------------------------------
-- Tracks provenance of each CSV upload. One row per /admin/listing-calibration
-- upload event. Drives the "recent uploads" panel and the per-upload drill-down.

CREATE TABLE IF NOT EXISTS listing_uploads (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  uploaded_by     TEXT NOT NULL,
  uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  marketplace     TEXT NOT NULL,
  city            TEXT NOT NULL,
  state_code      TEXT NOT NULL,
  total_rows      INTEGER NOT NULL DEFAULT 0,
  parsed_rows     INTEGER NOT NULL DEFAULT 0,
  skipped_rows    INTEGER NOT NULL DEFAULT 0,
  filename        TEXT,
  notes           TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ----------------------------------------------------------------------------
-- 2. CREATE TABLE buffer_config
-- ----------------------------------------------------------------------------
-- The brand×segment buffer matrix. Each cell stores the asking-to-sale gap
-- assumed for that combination. Used by the calibration math:
--   estimated_sale_price = listing_asking_price × (1 - buffer_pct/100)
--
-- Cells flagged is_default=TRUE were seeded from the spec; FALSE means an
-- admin has overridden them. Re-tuning happens after Bangalore mystery shop
-- data lands (validation strategy in spec section 7).

CREATE TABLE IF NOT EXISTS buffer_config (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  brand_group      TEXT NOT NULL,
  segment          TEXT NOT NULL,
  buffer_pct       NUMERIC(4,2) NOT NULL,
  is_default       BOOLEAN NOT NULL DEFAULT TRUE,
  last_changed_by  TEXT,
  last_changed_at  TIMESTAMPTZ,
  notes            TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Enforce one cell per (brand_group, segment). Required for ON CONFLICT seed.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'buffer_config_brand_segment_unique'
      AND conrelid = 'buffer_config'::regclass
  ) THEN
    ALTER TABLE buffer_config
      ADD CONSTRAINT buffer_config_brand_segment_unique
      UNIQUE (brand_group, segment);
  END IF;
END $$;

-- segment must be one of the 3 known tiers
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'buffer_config_segment_check'
      AND conrelid = 'buffer_config'::regclass
  ) THEN
    ALTER TABLE buffer_config
      ADD CONSTRAINT buffer_config_segment_check
      CHECK (segment IN ('mass_market', 'premium', 'luxury'));
  END IF;
END $$;

-- buffer_pct sanity bounds: 0% to 30%. Anything outside = data entry error.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'buffer_config_buffer_pct_check'
      AND conrelid = 'buffer_config'::regclass
  ) THEN
    ALTER TABLE buffer_config
      ADD CONSTRAINT buffer_config_buffer_pct_check
      CHECK (buffer_pct >= 0 AND buffer_pct <= 30);
  END IF;
END $$;


-- ----------------------------------------------------------------------------
-- 3. CREATE TABLE buffer_config_audit
-- ----------------------------------------------------------------------------
-- Audit log for buffer matrix changes. Captures BOTH old and new buffer_pct
-- so the UI can render before/after diffs without joining buffer_config.
--
-- old_buffer_pct is NULLABLE because the very first edit of a freshly seeded
-- cell has no "old value" in the audit sense (the seed itself isn't audited —
-- only deltas from seeds are). On the very first override of a cell, we
-- record old_buffer_pct = the seeded default.

CREATE TABLE IF NOT EXISTS buffer_config_audit (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  brand_group      TEXT NOT NULL,
  segment          TEXT NOT NULL,
  old_buffer_pct   NUMERIC(4,2),
  new_buffer_pct   NUMERIC(4,2) NOT NULL,
  changed_by       TEXT NOT NULL,
  changed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reason           TEXT
);


-- ----------------------------------------------------------------------------
-- 4. EXTEND research_log WITH listing_upload_id
-- ----------------------------------------------------------------------------
-- Nullable FK back to listing_uploads.id. Only Listing Aggregator entries
-- populate this column; existing rows (Mystery Shopping, Friends & Family,
-- Forum Post) leave it NULL.
--
-- ON DELETE SET NULL: if an upload is purged, we keep the research_log
-- rows but lose the link. Calibration math doesn't depend on this FK
-- being intact — it's purely for audit ("show me the listings that drove
-- this calibration suggestion").

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS listing_upload_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_listing_upload_id_fkey'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_listing_upload_id_fkey
      FOREIGN KEY (listing_upload_id)
      REFERENCES listing_uploads(id)
      ON DELETE SET NULL;
  END IF;
END $$;


-- ----------------------------------------------------------------------------
-- 5. ADD INDEXES
-- ----------------------------------------------------------------------------
-- Four new indexes to support the queries the admin UI will run:
--
--   idx_listing_uploads_uploaded_at  — recent uploads dashboard, newest first
--   idx_listing_uploads_state_city   — filter uploads by state/city for
--                                      per-state calibration views
--   idx_buffer_config_brand_group    — quickly fetch all segments for a brand
--                                      when rendering the buffer matrix UI
--   idx_research_log_listing_upload_id
--                                    — partial index, only listing entries.
--                                      Drives the "show me all rows from this
--                                      upload" drill-down + calibration math
--                                      that filters research_log by upload.

CREATE INDEX IF NOT EXISTS idx_listing_uploads_uploaded_at
  ON listing_uploads (uploaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_listing_uploads_state_city
  ON listing_uploads (state_code, city);

CREATE INDEX IF NOT EXISTS idx_buffer_config_brand_group
  ON buffer_config (brand_group);

CREATE INDEX IF NOT EXISTS idx_research_log_listing_upload_id
  ON research_log (listing_upload_id)
  WHERE listing_upload_id IS NOT NULL;


-- ----------------------------------------------------------------------------
-- 6. SEED buffer_config WITH DEFAULT MATRIX
-- ----------------------------------------------------------------------------
-- Default buffer matrix from spec section 3. ON CONFLICT DO NOTHING ensures
-- re-running this migration does NOT overwrite any cells that an admin has
-- already tuned (which would have flipped is_default to FALSE).
--
-- Reasoning behind the values:
--   - Brand demand drives gap (Maruti = 4% reflects no-negotiation reality,
--     premium European = 12% reflects luxury negotiation theater)
--   - Discontinued/declining brands have larger gaps (Ford, VW, Skoda
--     inventory sits longer)
--   - Segment within brand reflects ticket-size negotiation behavior
--
-- Cells marked '—' in the spec table are intentionally NOT seeded. If a
-- listing's brand+segment combination has no seed (e.g. a Maruti listing
-- priced > ₹30L would land in 'luxury' but Maruti has no luxury cell),
-- the calibration math falls back to a default buffer (handled in Python).

INSERT INTO buffer_config (brand_group, segment, buffer_pct, is_default, notes) VALUES
  ('Maruti Suzuki',           'mass_market',  4.0, TRUE, 'Seed: Maruti retains value, low negotiation room'),
  ('Maruti Suzuki',           'premium',      5.0, TRUE, 'Seed'),

  ('Hyundai',                 'mass_market',  5.0, TRUE, 'Seed'),
  ('Hyundai',                 'premium',      6.0, TRUE, 'Seed'),

  ('Toyota',                  'mass_market',  4.0, TRUE, 'Seed: strong resale, low gap'),
  ('Toyota',                  'premium',      6.0, TRUE, 'Seed'),
  ('Toyota',                  'luxury',       8.0, TRUE, 'Seed: Lexus/Vellfire territory'),

  ('Honda',                   'mass_market',  5.0, TRUE, 'Seed'),
  ('Honda',                   'premium',      7.0, TRUE, 'Seed'),

  ('Tata',                    'mass_market',  6.0, TRUE, 'Seed'),
  ('Tata',                    'premium',      7.0, TRUE, 'Seed: Harrier/Safari range'),

  ('Mahindra',                'mass_market',  6.0, TRUE, 'Seed'),
  ('Mahindra',                'premium',      8.0, TRUE, 'Seed: XUV700/Scorpio-N range'),

  ('Kia',                     'mass_market',  5.0, TRUE, 'Seed'),
  ('Kia',                     'premium',      7.0, TRUE, 'Seed: Carnival/EV6 range'),

  ('Renault/Nissan',          'mass_market',  8.0, TRUE, 'Seed: weaker resale, longer market time'),

  ('Ford',                    'mass_market',  9.0, TRUE, 'Seed: discontinued brand penalty'),
  ('Ford',                    'premium',     10.0, TRUE, 'Seed: discontinued brand penalty'),

  ('Volkswagen/Skoda',        'mass_market', 10.0, TRUE, 'Seed: declining demand, long market time'),
  ('Volkswagen/Skoda',        'premium',     12.0, TRUE, 'Seed'),

  ('MG/Jeep',                 'mass_market',  8.0, TRUE, 'Seed'),
  ('MG/Jeep',                 'premium',     10.0, TRUE, 'Seed'),

  ('Mercedes-Benz/BMW/Audi',  'luxury',      12.0, TRUE, 'Seed: luxury negotiation theater'),

  ('Jaguar/Land Rover/Volvo', 'luxury',      14.0, TRUE, 'Seed: niche luxury, longer sale cycle')
ON CONFLICT (brand_group, segment) DO NOTHING;


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these AFTER the migration to confirm everything landed correctly.
-- Each query returns row counts that should match the expected values.
-- ============================================================================

-- 1. Confirm 3 new tables exist
SELECT
  'New tables' AS check_name,
  COUNT(*) AS found,
  3 AS expected
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('listing_uploads', 'buffer_config', 'buffer_config_audit');

-- 2. Confirm research_log got listing_upload_id column
SELECT
  'research_log.listing_upload_id' AS check_name,
  COUNT(*) AS found,
  1 AS expected
FROM information_schema.columns
WHERE table_name = 'research_log'
  AND column_name = 'listing_upload_id';

-- 3. Confirm buffer_config constraints exist
SELECT
  'buffer_config constraints' AS check_name,
  COUNT(*) AS found,
  3 AS expected
FROM pg_constraint
WHERE conrelid = 'buffer_config'::regclass
  AND conname IN (
    'buffer_config_brand_segment_unique',
    'buffer_config_segment_check',
    'buffer_config_buffer_pct_check'
  );

-- 4. Confirm research_log FK exists
SELECT
  'research_log FK to listing_uploads' AS check_name,
  COUNT(*) AS found,
  1 AS expected
FROM pg_constraint
WHERE conrelid = 'research_log'::regclass
  AND conname = 'research_log_listing_upload_id_fkey';

-- 5. Confirm 4 new indexes exist
SELECT
  'New indexes' AS check_name,
  COUNT(*) AS found,
  4 AS expected
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname IN (
    'idx_listing_uploads_uploaded_at',
    'idx_listing_uploads_state_city',
    'idx_buffer_config_brand_group',
    'idx_research_log_listing_upload_id'
  );

-- 6. Confirm buffer_config seed loaded (24 cells per spec section 3 matrix)
SELECT
  'Buffer matrix seed' AS check_name,
  COUNT(*) AS found,
  24 AS expected
FROM buffer_config
WHERE is_default = TRUE;

-- 7. Spot-check the seeded buffer matrix
SELECT
  brand_group,
  segment,
  buffer_pct,
  is_default,
  notes
FROM buffer_config
ORDER BY brand_group, segment;


-- ============================================================================
-- ROLLBACK SCRIPT  (commented out — uncomment + run to undo this migration)
-- ============================================================================
-- DO NOT run unless you genuinely need to revert. This DROPS the 3 new
-- tables (any uploaded listings + buffer matrix overrides will be lost) and
-- the listing_upload_id column on research_log. There is no recovery path
-- other than a Supabase backup.
-- ============================================================================
--
-- -- Drop indexes first
-- DROP INDEX IF EXISTS idx_listing_uploads_uploaded_at;
-- DROP INDEX IF EXISTS idx_listing_uploads_state_city;
-- DROP INDEX IF EXISTS idx_buffer_config_brand_group;
-- DROP INDEX IF EXISTS idx_research_log_listing_upload_id;
--
-- -- Drop FK + column on research_log
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_listing_upload_id_fkey;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS listing_upload_id;
--
-- -- Drop CHECK + UNIQUE constraints on buffer_config
-- ALTER TABLE buffer_config DROP CONSTRAINT IF EXISTS buffer_config_brand_segment_unique;
-- ALTER TABLE buffer_config DROP CONSTRAINT IF EXISTS buffer_config_segment_check;
-- ALTER TABLE buffer_config DROP CONSTRAINT IF EXISTS buffer_config_buffer_pct_check;
--
-- -- Drop tables (CASCADE not used — fail loudly if anything references them)
-- DROP TABLE IF EXISTS buffer_config_audit;
-- DROP TABLE IF EXISTS buffer_config;
-- DROP TABLE IF EXISTS listing_uploads;
--
-- ============================================================================
-- END migration_9_listing_calibration_pipeline.sql
-- ============================================================================
