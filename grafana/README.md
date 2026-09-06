# Grafana dashboards

Three provisioned dashboards over the beacon database, in the **Dozzle** folder:

| Dashboard | UID | Answers |
| --- | --- | --- |
| Dozzle · Retention | `dozzle-retention` | Do installs stick around? |
| Dozzle · Usage over time | `dozzle-usage` | How many installs are active, how hard are they used? |
| Dozzle · Feature penetration | `dozzle-features` | Which features do people actually turn on? |

## What counts as an install

An install is one Dozzle `ServerID` (`beacon.client_id`). Beacons that report no
`ServerID` — about 6% of events — all land on the empty key, so the daily aggregates
roll them into a single phantom install with a beacon every minute. Every query here
excludes `client_id = ''`.

All panels read the `daily_client_events` / `daily_client_starts` continuous aggregates
rather than the raw `beacon` table, so they keep working past the one-year retention
policy on raw beacons.

## Activation, and why `start` beacons are not cohorts

Activation is an install's **first `events` beacon**, never its first `start`.

`start` beacons come from 1.65M distinct installs; `events` beacons from only 379k. 1.27M
installs send a start and are never heard from again — they launch and die before the
periodic events beacon fires (short-lived containers, CI runs, quick trials). Seeding
cohorts from `start` while measuring activity from `events` inflates every denominator
roughly 4x and makes retention look like a cliff. Cohort membership and cohort activity
have to come from the same beacon.

That population is not thrown away — *First launches: did they stick?* on the usage
dashboard charts it directly, and it is the single biggest number on these dashboards:
roughly three in four first launches never become an active install.

`mv_client_activations` is deliberately unused: it is a plain materialized view built
from raw `beacon`, so once the one-year retention policy starts dropping chunks, each
hourly `REFRESH` walks the activation dates forward and older cohorts disappear.

## Telemetry that never reports

`hasShell`, `remoteAgents`, `remoteClients` and `filterLength` are in the `Event` struct
and always come back zero/false; `mode` and `subCommand` are effectively empty too. They
are left off the feature charts rather than drawn as flat zero lines. If those features
shipped, the beacon is not reporting them — worth checking on the Dozzle side.

## Editing

Dashboards are generated as JSON and mounted read-only. `allowUiUpdates` is on, so you
can tweak a panel in the UI, export the JSON (Dashboard settings → JSON Model) and paste
it back into `grafana/dashboards/*.json`.

## Deploying

The provisioning files and dashboards ship as Docker Swarm configs. Swarm configs are
**immutable**, so each one is named with a `CONFIG_VERSION` suffix that the deploy
workflow sets to the commit SHA. Deploying by hand needs the same:

```sh
CONFIG_VERSION=$(git rev-parse --short HEAD) \
  docker --context beacon stack deploy -c docker-compose.yml -c docker-compose.prod.yml data
```

Superseded configs are left behind on the node; `docker config ls` and `docker config rm`
clean them up.

## Removing the old dashboard

Provisioning cannot delete a dashboard that was created by hand. Once the new ones look
right, delete the old one from the UI (Dashboard settings → Delete), or via the API:

```sh
curl -s -u admin:$GRAFANA_PASSWORD https://dashboard.dozzle.dev/api/search?type=dash-db \
  | jq -r '.[] | "\(.uid)\t\(.title)"'
curl -X DELETE -u admin:$GRAFANA_PASSWORD https://dashboard.dozzle.dev/api/dashboards/uid/<uid>
```

## Local preview

```sh
docker compose up -d timescaledb grafana   # http://localhost:3000, admin / your_password
```
