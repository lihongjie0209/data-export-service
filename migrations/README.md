# Migrations

Database-specific migrations live under `mysql`, `postgres`, and `kingbase`. Set `migration.path` to the matching directory, for example `migrations/postgres`. Review indexes, collation and online-DDL impact against production data before deployment.

Migration `000005_application_scope` adds application ownership to export jobs. Existing jobs have no authoritative application mapping: queued/running jobs are canceled, and legacy terminal rows remain with an empty application ID until an operator performs an authoritative backfill. New API requests require a non-empty tenant/application pair.
