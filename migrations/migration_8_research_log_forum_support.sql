-- ============================================================================
-- migration_8_research_log_forum_support.sql
-- ----------------------------------------------------------------------------
-- AutoKnowMus  ·  v3.5.1-r6  ·  Phase A: forum data harvesting
--
-- Extends the research_log table to support a third data source: forum posts
-- (Team-BHP, Reddit, Facebook Groups, Quora). This unlocks evening harvesting
-- of historical transaction discussions to feed the calibration engine BEFORE
-- the May 23 Bangalore mystery-shopping trip.
--
-- WHAT THIS MIGRATION DOES
--   1. Adds 7 columns to research_log (forum_name, forum_url, transaction_date,
--      transaction_completed, buyer_type, data_quality, harvest_notes)
--   2. Adds CHECK constraints to keep enum-like fields tidy
--   3. Adds 3 indexes to speed up filtering by forum / quality / txn date
--   4. Backfills sensible defaults for existing rows
--
-- DESIGN DECISIONS (locked)
--   - data_source remains plain TEXT (no CHECK constraint added).
--     Validation is enforced in Python (RESEARCH_SOURCES list in app.py).
--     Reason: keeps schema flexible if we add a 4th source later (e.g.
--     "Inspection Provider" partnerships from May 23 trip).
--   - forum_name + forum_url are NULLABLE at the column level. The
--     "if data_source='Forum Post' then both required" rule is enforced
--     in Python form validation (in admin_research.html POST handler),
--     not via a DB-level CHECK. Reason: easier to evolve, lets us still
--     manually correct old rows via SQL if needed.
--   - data_quality defaults to 'high' for new rows. Existing
--     Mystery Shopping / Friends & Family rows get backfilled to 'high'
--     since you observed those personally — they are the gold standard.
--   - transaction_completed is nullable. We can't always tell from a
--     forum post whether the deal actually closed. Calibration math
--     should treat NULL as "treat as completed" for backwards compat
--     with existing rows.
--
-- IDEMPOTENT
--   Every ADD COLUMN, ADD CONSTRAINT, and CREATE INDEX uses IF NOT EXISTS.
--   Safe to run multiple times.
--
-- ROLLBACK
--   See "ROLLBACK SCRIPT" at the very bottom of this file (commented out).
--   Drops the new columns / constraints / indexes if you need to undo this.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. ADD COLUMNS
-- ----------------------------------------------------------------------------

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS forum_name TEXT;

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS forum_url TEXT;

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS transaction_date DATE;

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS transaction_completed BOOLEAN;

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS buyer_type TEXT;

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS data_quality TEXT DEFAULT 'high';

ALTER TABLE research_log
  ADD COLUMN IF NOT EXISTS harvest_notes TEXT;


-- ----------------------------------------------------------------------------
-- 2. BACKFILL EXISTING ROWS
-- ----------------------------------------------------------------------------
-- All existing rows are Mystery Shopping or Friends & Family — both directly
-- observed by the admin (Rajeev). Mark them as 'high' quality so they get
-- full weight in the calibration engine.
--
-- transaction_completed defaults to TRUE for these rows because:
--   - Mystery Shopping observations are real listings (still active sales)
--   - Friends & Family entries are completed deals you have direct knowledge of
--
-- We use COALESCE-style updates so this is safe to re-run: only fills NULLs.

UPDATE research_log
SET data_quality = 'high'
WHERE data_quality IS NULL;

UPDATE research_log
SET transaction_completed = TRUE
WHERE transaction_completed IS NULL
  AND data_source IN ('Mystery Shopping', 'Friends & Family');


-- ----------------------------------------------------------------------------
-- 3. ADD CHECK CONSTRAINTS
-- ----------------------------------------------------------------------------
-- We use DO blocks instead of plain ADD CONSTRAINT IF NOT EXISTS because
-- Postgres doesn't support IF NOT EXISTS on ADD CONSTRAINT. Each block
-- checks pg_constraint first and skips if the constraint already exists.

-- forum_name: must be one of the 5 supported forums OR NULL
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_forum_name_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_forum_name_check
      CHECK (forum_name IS NULL OR forum_name IN
        ('Team-BHP', 'Reddit', 'Facebook', 'Quora', 'Other'));
  END IF;
END $$;

-- forum_url: max 1000 chars (forum URLs can be long with query params)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_forum_url_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_forum_url_check
      CHECK (forum_url IS NULL OR length(forum_url) <= 1000);
  END IF;
END $$;

-- buyer_type: must be one of the 7 known channels OR NULL
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_buyer_type_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_buyer_type_check
      CHECK (buyer_type IS NULL OR buyer_type IN
        ('Private', 'Dealer', 'Cars24', 'Spinny', 'CarTrade', 'OLX', 'Other'));
  END IF;
END $$;

-- data_quality: gold/silver/bronze tiering for calibration weighting
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_data_quality_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_data_quality_check
      CHECK (data_quality IS NULL OR data_quality IN
        ('high', 'medium', 'low'));
  END IF;
END $$;

-- harvest_notes: cap at 1000 chars (general notes column already has 2000 cap)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_harvest_notes_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_harvest_notes_check
      CHECK (harvest_notes IS NULL OR length(harvest_notes) <= 1000);
  END IF;
END $$;

-- transaction_date: must be on or after 2010-01-01 and not in the future.
-- Allows backdating forum posts that reference older deals (e.g. a 2018
-- Team-BHP post about a 2016 sale — txn_date = 2016-XX-XX).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'research_log_transaction_date_check'
      AND conrelid = 'research_log'::regclass
  ) THEN
    ALTER TABLE research_log
      ADD CONSTRAINT research_log_transaction_date_check
      CHECK (transaction_date IS NULL OR
        (transaction_date >= '2010-01-01' AND transaction_date <= CURRENT_DATE));
  END IF;
END $$;


-- ----------------------------------------------------------------------------
-- 4. ADD INDEXES
-- ----------------------------------------------------------------------------
-- Three new indexes to support the queries the admin UI will run:
--
--   idx_research_log_forum         — filter by Team-BHP / Reddit / etc.
--   idx_research_log_txn_date      — sort by actual transaction date
--                                    (separate from entry_date, which is
--                                    when YOU logged it). Important for
--                                    time-decay weighting in calibration.
--   idx_research_log_quality       — for the calibration engine to filter
--                                    or weight by data_quality
--
-- All three use partial indexes WHERE the column IS NOT NULL to keep the
-- index small (existing Mystery Shopping rows have NULL forum_name etc.,
-- so they shouldn't pollute these indexes).

CREATE INDEX IF NOT EXISTS idx_research_log_forum
  ON research_log (forum_name)
  WHERE forum_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_log_txn_date
  ON research_log (transaction_date DESC)
  WHERE transaction_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_log_quality
  ON research_log (data_quality, include_in_calibration)
  WHERE include_in_calibration = TRUE;


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these AFTER the migration to confirm everything landed correctly.
-- Each query returns row counts that should match the expected values.
-- ============================================================================

-- 1. Confirm all 7 new columns exist
SELECT
  'New columns' AS check_name,
  COUNT(*) AS found,
  7 AS expected
FROM information_schema.columns
WHERE table_name = 'research_log'
  AND column_name IN (
    'forum_name', 'forum_url', 'transaction_date',
    'transaction_completed', 'buyer_type', 'data_quality', 'harvest_notes'
  );

-- 2. Confirm all 6 new constraints exist
SELECT
  'New CHECK constraints' AS check_name,
  COUNT(*) AS found,
  6 AS expected
FROM pg_constraint
WHERE conrelid = 'research_log'::regclass
  AND conname IN (
    'research_log_forum_name_check',
    'research_log_forum_url_check',
    'research_log_buyer_type_check',
    'research_log_data_quality_check',
    'research_log_harvest_notes_check',
    'research_log_transaction_date_check'
  );

-- 3. Confirm all 3 new indexes exist
SELECT
  'New indexes' AS check_name,
  COUNT(*) AS found,
  3 AS expected
FROM pg_indexes
WHERE tablename = 'research_log'
  AND indexname IN (
    'idx_research_log_forum',
    'idx_research_log_txn_date',
    'idx_research_log_quality'
  );

-- 4. Confirm existing rows got backfilled to 'high' data_quality
SELECT
  'Backfilled data_quality' AS check_name,
  COUNT(*) FILTER (WHERE data_quality = 'high') AS high_count,
  COUNT(*) FILTER (WHERE data_quality IS NULL) AS null_count,
  COUNT(*) AS total_rows
FROM research_log;

-- 5. Spot-check the new columns on the most recent 5 rows
SELECT
  id, data_source, entry_date, data_quality,
  forum_name, transaction_date, buyer_type
FROM research_log
ORDER BY created_at DESC
LIMIT 5;


-- ============================================================================
-- ROLLBACK SCRIPT  (commented out — uncomment + run to undo this migration)
-- ============================================================================
-- DO NOT run unless you genuinely need to revert. This DROPS data in the
-- 7 new columns. There is no recovery path other than a Supabase backup.
-- ============================================================================
--
-- -- Drop indexes first (safe to drop in any order)
-- DROP INDEX IF EXISTS idx_research_log_forum;
-- DROP INDEX IF EXISTS idx_research_log_txn_date;
-- DROP INDEX IF EXISTS idx_research_log_quality;
--
-- -- Drop CHECK constraints
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_forum_name_check;
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_forum_url_check;
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_buyer_type_check;
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_data_quality_check;
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_harvest_notes_check;
-- ALTER TABLE research_log DROP CONSTRAINT IF EXISTS research_log_transaction_date_check;
--
-- -- Drop columns (CASCADE not used — fail loudly if anything references them)
-- ALTER TABLE research_log DROP COLUMN IF EXISTS forum_name;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS forum_url;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS transaction_date;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS transaction_completed;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS buyer_type;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS data_quality;
-- ALTER TABLE research_log DROP COLUMN IF EXISTS harvest_notes;
--
-- ============================================================================
-- END migration_8_research_log_forum_support.sql
-- ============================================================================
