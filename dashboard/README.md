# Dozzle analytics dashboard

A Nuxt app over the beacon database, replacing the provisioned Grafana dashboards.

```sh
cp .env.example .env   # fill in the GitHub OAuth app + a session password
pnpm install
pnpm dev               # http://localhost:3000
```

## Why it is fast

The dashboard never reads `beacon` and never parses JSONB at request time. Every panel
reads a derived table maintained by `drain_refresh_analytics()` (see `../migrations/`):

| Table | Shape | What it serves |
| --- | --- | --- |
| `active_counts_daily` | one row per day | DAU / WAU / MAU, new, churned, resurrected |
| `active_counts_hourly` | one row per hour | the 24-hour view |
| `client_snapshot_daily` | one row per install per day, typed columns | features, versions, environment, up to 90 days |
| `client_snapshot_weekly` | the same rolled up to ISO weeks | the same panels over 90 days to a year |
| `client_lifecycle` | one row per install ever seen | activation, tenure, the launch-only funnel |
| `cohort_retention_weekly` | cohort week x week index | the whole retention page |
| `weekly_lifecycle` | one row per week | new / retained / resurrected / churned |

The trailing-window distinct counts behind DAU/WAU/MAU are the expensive part, and they
are computed once per refresh rather than per page load.

## Time ranges

`?range=1d|7d|30d|90d|1y`, or `?from=YYYY-MM-DD&to=YYYY-MM-DD` for a custom window. The
range lives in the URL, so any view is linkable.

Granularity is derived from the window rather than chosen: up to 2 days is an hour axis,
up to 92 days a day axis, and anything longer a week axis reading the weekly snapshot.
That is what keeps a one-year range in the same latency class as a 30-day one.

## Access

GitHub OAuth with an explicit allowlist of logins (`NUXT_GITHUB_ALLOWED_USERS`). An empty
allowlist means nobody, not everybody. Everything under `/api` requires a session.

Create the OAuth app under GitHub → Settings → Developer settings → OAuth Apps with the
callback URL `https://<host>/auth/github`.

| Variable | |
| --- | --- |
| `NUXT_DATABASE_URL` | Postgres connection string |
| `NUXT_OAUTH_GITHUB_CLIENT_ID` | OAuth app client id |
| `NUXT_OAUTH_GITHUB_CLIENT_SECRET` | OAuth app client secret |
| `NUXT_GITHUB_ALLOWED_USERS` | comma-separated GitHub logins |
| `NUXT_SESSION_PASSWORD` | 32+ random chars, `openssl rand -base64 32` |
