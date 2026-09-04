-- 000006_job_indexes: query-path indexes for jobs + job_events.
-- Defined once on the parents: Postgres materializes each as physically
-- local indexes on every current partition and auto-creates them on every
-- future partition. No global indexes involved.
-- Partial indexes target the hot statuses workers actually scan, keeping
-- them small instead of covering terminal completed/failed rows.
-- NOTE: the partial UNIQUE on idempotency_key is enforced per partition
-- only (Postgres limitation without the partition key); a duplicate key
-- in two different months will not conflict.
-- Multi-statement file: requires x-multi-statement=true (see migrate.go).
-- Plain (non-CONCURRENTLY) creation briefly locks writes; safe on fresh
-- tables, never use CONCURRENTLY here (PgBouncer transaction mode).
CREATE INDEX jobs_scheduled_idx ON jobs (scheduled_at, status) WHERE status = 'scheduled';
CREATE INDEX jobs_pending_idx ON jobs (queue_name, status, created_at) WHERE status = 'pending';
CREATE INDEX jobs_running_idx ON jobs (worker_id, heartbeat_at) WHERE status = 'running';
CREATE INDEX jobs_job_id_idx ON jobs (job_id);
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX job_events_job_id_idx ON job_events (job_id);
