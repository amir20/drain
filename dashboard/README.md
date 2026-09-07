# Dozzle analytics dashboard

A Nuxt app over the beacon database, replacing the provisioned Grafana dashboards.

```sh
cp .env.example .env   # fill in the GitHub OAuth app + a session password
bun install
bun run dev            # http://localhost:3000
```

## Adding a field to the dashboard

**Nothing here mirrors the beacon payload.** The metadata JSONB is carried through the
derived tables verbatim and parsed on read by the SQL helpers in
`../migrations/001_analytics.sql`. So a new beacon field needs no column, no migration
and no rebuild — it is already in the JSONB.

A new boolean feature is one entry in `FEATURES` in `server/api/features.get.ts`:

```ts
{ key: 'hasNewThing', label: 'The new thing', short: 'New thing' },
```

The SQL passes that list to Postgres as an array and unnests it, so the queries never
change. Only a field that is not a plain boolean (like `auth`, which is "a provider other
than none") needs a case in `drain_feature()`.

The tenure and engagement-depth band boundaries live in the API for the same reason —
changing them is an edit, not a migration.

## Why it is still fast

The dashboard never reads `beacon`. Every panel reads a view or a table maintained by
`drain_refresh_analytics()` (see `../migrations/`):

| Source | Shape | What it serves |
| --- | --- | --- |
| `client_daily` (view) | the daily aggregate, empty client ids excluded | dimension panels up to a month |
| `client_weekly` | the same rolled up to ISO weeks, ~7x smaller | dimension panels from a month to a year |
| `client_lifecycle` | one row per install ever seen, with its latest metadata | activation, tenure, the funnel, "right now" panels |
| `active_counts_daily` | one row per day | DAU / WAU / MAU, new, churned, resurrected |
| `active_counts_hourly` | one row per hour | the 24-hour view |
| `cohort_retention_weekly` | cohort week x week index | the whole retention page |
| `weekly_lifecycle` | one row per week | new / retained / resurrected / churned |

Only two things are materialised that could in principle be computed on demand: the
weekly rollup, because the alternative is a `DISTINCT ON` over ~10M rows per panel, and
the trailing-window distinct counts behind DAU/WAU/MAU, which are the single most
expensive thing on the old Grafana dashboards. Everything else is a fixed-shape count
that never gains a column.

Parsing JSONB on read is not free. Measured against ~9.6M event beacons (227k active
installs, 400 days):

| Page | 24h | 7d | 30d | 90d | 1y |
| --- | --- | --- | --- | --- | --- |
| Overview | 179ms | 183ms | 154ms | 284ms | 381ms |
| Retention | 76ms | 72ms | 98ms | 103ms | 377ms |
| Engagement | 133ms | 115ms | 74ms | 178ms | 713ms |
| Features | 3.0s | 2.4s | 6.3s | 4.6s | 8.1s |
| Environment | 3.4s | 2.9s | 4.7s | 1.5s | 3.3s |

The first three read only fixed-shape counts and never touch JSONB. Features and
Environment parse it for every install in the range, and that is where the seconds go —
an earlier version of this dashboard kept those columns typed and served the same pages
in 30-500ms.

That is the deliberate trade, and it is the right way round: these are internal pages
loaded a few times a day, and the alternative cost a schema change threaded through three
tables plus a full rebuild every time the beacon gained a field. If a page ever becomes
too slow to use, the fix is to materialise *that one column* — not to go back to
mirroring the payload.

## Time ranges

`?range=1d|7d|30d|90d|1y`, or `?from=YYYY-MM-DD&to=YYYY-MM-DD` for a custom window. The
range lives in the URL, so any view is linkable.

Granularity is derived from the window rather than chosen: up to 2 days is an hour axis,
up to 31 days a day axis, and anything longer a week axis reading the weekly snapshot. A
`bucket` query parameter can coarsen that but never refine it. Weekly buckets past a month
are both the better read (13 points for a quarter, not 90) and ~7x less data, which is
what keeps a one-year range in the same latency class as a 30-day one.

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
| `NUXT_GITHUB_ALLOWED_USERS` | comma-separated GitHub logins, re-checked on every request |
| `NUXT_SESSION_PASSWORD` | 32+ random chars, `openssl rand -base64 32` |
