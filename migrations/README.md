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
- **Do not add a column for something already in the metadata JSONB.** Add a `drain_*`
  helper in `001` and read it from the API instead.
- **Adding a column to a table that already exists needs its own `ALTER TABLE ... ADD
  COLUMN IF NOT EXISTS`.** `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table,
  so editing the `CREATE` alone leaves deployed databases without the column.
- **Do not wrap it in a transaction** — statements are applied one at a time because
  TimescaleDB refuses to create a continuous aggregate inside a transaction block. That
  also means a half-finished run must be safe to resume, which is what idempotency buys.

## What is here

| File | |
| --- | --- |
| `001_analytics.sql` | the SQL helpers that read the beacon payload, the views and derived tables the dashboard reads, and the hourly continuous aggregate |
| `002_refresh.sql` | `drain_refresh_analytics()`, which maintains them |
| `003_schedule.sql` | registers that refresh on TimescaleDB's job scheduler, hourly |

## The one design rule

**No derived table mirrors the beacon payload.** The metadata JSONB is copied through
verbatim and parsed at query time by the `drain_*` helpers in `001`. Adding a field to
the beacon therefore needs nothing here at all — no column, no migration, no rebuild.

The cost is parsing JSONB on read rather than once on write: the Features and
Environment pages take 1.5-8s over a year of data where a typed schema served them in
under 500ms. Everything else is unaffected, because it reads fixed-shape counts.

Take that trade again when it comes up. These are internal pages loaded a few times a
day, and the alternative is a schema change threaded through several tables plus a full
rebuild every time the beacon gains a field. If one page becomes genuinely too slow,
materialise that single column rather than reinstating a mirror of the payload.

## Refreshing

```sql
CALL drain_refresh_analytics();       -- incremental: reprocesses a trailing window
CALL drain_refresh_analytics(true);   -- full rebuild from the continuous aggregates
```

`003_schedule.sql` runs the incremental form hourly on TimescaleDB's job scheduler, the
same one that runs the compression and retention policies. There is no scheduler
container: the job lives in the database, so it survives a restart and needs neither the
Docker socket nor a copy of the password in a service label.

Its runs are reported alongside every other policy:

```sql
SELECT * FROM timescaledb_information.job_stats;   -- successes, failures, last duration
SELECT * FROM timescaledb_information.jobs;        -- schedule and config
```

A full rebuild is only needed after a backfill, a change to how a derived table is built,
or the first time these migrations land on an existing database. Run it once by hand after
deploying them, or the dashboard will only have the trailing window:

```sh
docker --context beacon exec -it $(docker --context beacon ps -q -f name=data_timescaledb) \
  sh -c 'psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-drain}" -c "CALL drain_refresh_analytics(true)"'
```

Phase timings land in `analytics_meta`, which `/api/meta` reads back.
