# Migrations

`init/01_init.sql` runs exactly once, when Postgres initialises an empty data volume. The
production database was initialised long ago, so **editing `01_init.sql` does nothing to
production**. Anything added after the first deploy belongs here.

## How they run

The files are embedded in the drain binary (`//go:embed migrations/*.sql`) and applied by:

```sh
drain -migrate
```

which is what the one-shot `migrate` service in the stack runs on every deploy. Each file
is recorded in `schema_migrations` by SHA-256, so an unchanged file is skipped and an
edited one is re-applied.

## Rules for a new migration

- **Number it** — files are applied in filename order (`003_…`, `004_…`).
- **Make it idempotent** — `IF NOT EXISTS`, `CREATE OR REPLACE`, `if_not_exists => TRUE`,
  or a guarding `DO $$ ... $$` block. Re-applying it must be a no-op.
- **Do not wrap it in a transaction** — statements are applied one at a time because
  TimescaleDB refuses to create a continuous aggregate inside a transaction block. That
  also means a half-finished run must be safe to resume, which is what idempotency buys.

## What is here

| File | |
| --- | --- |
| `001_analytics.sql` | the derived tables the dashboard reads, plus the hourly continuous aggregate |
| `002_refresh.sql` | `drain_refresh_analytics()`, which rebuilds them |

## Refreshing

The `view-refresh` service runs this hourly through ofelia:

```sql
CALL drain_refresh_analytics();       -- incremental: reprocesses a trailing window
CALL drain_refresh_analytics(true);   -- full rebuild from the continuous aggregates
```

A full rebuild is only needed after a backfill, a change to how a snapshot column is
derived, or the first time these migrations land on an existing database. Run it once by
hand after deploying them, or the dashboard will only have the trailing window:

```sh
docker --context beacon exec -it $(docker --context beacon ps -q -f name=data_view-refresh) \
  psql "$DATABASE_URL" -c 'CALL drain_refresh_analytics(true)'
```

Phase timings land in `analytics_meta`, which `/api/meta` reads back.
