-- 000007_job_events_partitions rollback: detach partitions by dropping
-- them. Only use on an empty or disposable database: dropping partitions
-- deletes their rows.
DROP TABLE IF EXISTS job_events_p2026_11;
DROP TABLE IF EXISTS job_events_p2026_10;
DROP TABLE IF EXISTS job_events_p2026_09;
DROP TABLE IF EXISTS job_events_default;
