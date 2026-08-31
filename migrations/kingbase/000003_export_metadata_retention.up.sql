CREATE INDEX export_jobs_metadata_retention_idx ON export_jobs (status, updated_at, id);
