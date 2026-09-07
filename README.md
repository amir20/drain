# drain

The beacon endpoint and analytics stack behind [Dozzle](https://github.com/amir20/dozzle).

Dozzle installs POST a small JSON payload to `b.dozzle.dev/event`; `drain` receives it,
writes it to TimescaleDB and to Parquet, and a Nuxt dashboard at `dashboard.dozzle.dev`
reads the aggregates back out.

```
Dozzle install ──POST /event──▶ drain (Go) ──▶ TimescaleDB ──▶ dashboard (Nuxt + ECharts)
                                     └────────▶ Parquet files on disk
```

## Layout

| Path | |
| --- | --- |
| `main.go`, `internal/` | the Go beacon receiver, Postgres/Parquet writers, and the migration runner |
| `init/01_init.sql` | first-boot schema. Only runs on an empty Postgres volume |
| `migrations/` | everything added after the first deploy, applied by `drain -migrate` |
| `dashboard/` | the Nuxt analytics dashboard |
| `notebooks/` | ad-hoc Polars analysis over the Parquet files |
| `grafana/` | the superseded Grafana dashboards, kept as a reference for the SQL |

## Running locally

```sh
docker compose up -d timescaledb        # TimescaleDB on :5432
docker compose up beacon                # the Go receiver on :4000
docker compose up migrate               # apply migrations
docker compose up dashboard             # the dashboard on :3000
```

The dashboard is a Nuxt app on Bun; `cd dashboard && bun install && bun run dev` runs it
directly.

The dashboard needs a GitHub OAuth app and a session password — see
[`dashboard/README.md`](dashboard/README.md).

## Schema changes

`init/01_init.sql` only runs when Postgres initialises an empty volume, so it is *not*
how production gets a schema change. Add a numbered file to `migrations/` instead; it is
embedded in the drain binary and applied by the one-shot `migrate` service on every
deploy. See [`migrations/README.md`](migrations/README.md).

## Deploying

Tagging `v*` builds `amir20/drain` and `amir20/drain-dashboard` and runs
`docker stack deploy` against the beacon host. By hand:

```sh
# Credentials reach the stack as Docker secrets, read from ./secrets (gitignored).
mkdir -p secrets
printf '%s' "$POSTGRES_PASSWORD"          > secrets/postgres_password
printf '%s' "$OAUTH_GITHUB_CLIENT_SECRET" > secrets/oauth_github_client_secret
printf '%s' "$SESSION_PASSWORD"           > secrets/session_password
CONFIG_VERSION=$(git rev-parse --short=12 HEAD) OAUTH_GITHUB_CLIENT_ID=... \
  docker --context beacon stack deploy -c docker-compose.yml -c docker-compose.prod.yml data
```

The deploy needs these set in the repository settings:

| | |
| --- | --- |
| `vars.DASHBOARD_ALLOWED_USERS` | comma-separated GitHub logins allowed into the dashboard (Actions reserves the `GITHUB_` prefix, so it cannot be named after the env var it feeds) |
| `secrets.OAUTH_GITHUB_CLIENT_ID` / `secrets.OAUTH_GITHUB_CLIENT_SECRET` | the dashboard's GitHub OAuth app |
| `secrets.SESSION_PASSWORD` | 32+ random chars for the dashboard's session cookie |
| `secrets.POSTGRES_PASSWORD` | the database password. It is delivered as a Docker secret, but Postgres only reads it when initialising an empty volume — on the existing production volume the value **must match the role's current password** until `ALTER ROLE postgres PASSWORD '…'` is run against the live database. A mismatch fails the migration and takes beacon ingest down |
| `secrets.DOCKER_USERNAME` / `secrets.DOCKER_PASSWORD` | Docker Hub |
| `secrets.SSH_CERT` / `secrets.SSH_KEY` | access to the beacon host |
