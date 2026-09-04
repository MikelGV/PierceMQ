-- 000006_job_indexes rollback: dropping a parent index automatically
-- drops all of its per-partition children.
DROP INDEX IF EXISTS job_events_job_id_idx;
DROP INDEX IF EXISTS jobs_idempotency_idx;
DROP INDEX IF EXISTS jobs_job_id_idx;
DROP INDEX IF EXISTS jobs_running_idx;
DROP INDEX IF EXISTS jobs_pending_idx;
DROP INDEX IF EXISTS jobs_scheduled_idx;
