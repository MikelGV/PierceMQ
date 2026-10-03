DROP INDEX IF EXISTS jobs_owner_idx;
ALTER TABLE jobs DROP COLUMN IF EXISTS owner_user_id;
