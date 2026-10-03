ALTER TABLE jobs ADD COLUMN owner_user_id UUID;
CREATE INDEX IF NOT EXISTS jobs_owner_idx ON jobs (owner_user_id, created_at);
