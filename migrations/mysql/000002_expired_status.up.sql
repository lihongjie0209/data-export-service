ALTER TABLE export_jobs DROP CHECK export_jobs_status_check;
ALTER TABLE export_jobs ADD CONSTRAINT export_jobs_status_check CHECK (status IN ('queued','running','succeeded','failed','canceled','expired'));
