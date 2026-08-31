UPDATE export_jobs SET status = 'succeeded' WHERE status = 'expired';
ALTER TABLE export_jobs DROP CONSTRAINT export_jobs_status_check;
ALTER TABLE export_jobs ADD CONSTRAINT export_jobs_status_check CHECK (status IN ('queued','running','succeeded','failed','canceled'));
