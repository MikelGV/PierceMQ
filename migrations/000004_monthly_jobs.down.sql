-- 000004_monthly_jobs rollback: detach partitions by dropping them.
-- Reverse order (monthlies first, DEFAULT last). Only use on an empty
-- or disposable database: dropping partitions deletes their rows.
DROP TABLE IF EXISTS jobs_p2026_11;
DROP TABLE IF EXISTS jobs_p2026_10;
DROP TABLE IF EXISTS jobs_p2026_09;
DROP TABLE IF EXISTS jobs_default;
