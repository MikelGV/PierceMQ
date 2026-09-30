ALTER TABLE jobs ADD COLUMN not_before TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS jobs_pending_not_before_idx ON jobs (status, not_before) WHERE status IN ('pending', 'queued');
