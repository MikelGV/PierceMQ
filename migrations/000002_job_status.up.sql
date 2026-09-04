CREATE TYPE job_status AS ENUM (
    'pending',
    'queued',
    'scheduled',
    'running',
    'completed',
    'failed',
    'cancelled'
);
