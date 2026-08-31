CREATE INDEX export_jobs_metadata_retention_idx ON export_jobs (status(32), updated_at, id(64));
