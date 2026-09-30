DROP INDEX IF EXISTS jobs_pending_not_before_idx;
ALTER TABLE jobs DROP COLUMN IF EXISTS not_before;
