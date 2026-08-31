CREATE INDEX export_jobs_metadata_retention_idx ON export_jobs (updated_at, id) WHERE status = 'expired';
