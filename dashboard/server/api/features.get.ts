/**
 * Feature penetration.
 *
 * Adding a feature to this dashboard is one entry in FEATURES below. Nothing else
 * changes: no column, no migration, no rebuild. The SQL passes the list to Postgres as
 * an array and unnests it, so the queries are written once and do not know or care how
 * many features there are.
 *
 * `drain_feature(metadata, key)` (see migrations/001) resolves a key against the beacon
 * payload. A plain boolean field needs nothing but its name here; the two that are not
 * plain booleans - `auth` and `multiClient` - are named in that function.
 *
 * hasShell, remoteAgents, remoteClients and filterLength are in the payload but always
 * report zero/false, and mode/subCommand are effectively always empty. They are left off
 * rather than drawn as flat zero lines.
 */
export const FEATURES = [
  { key: 'hasActions', label: 'Container actions', short: 'Actions' },
  { key: 'auth', label: 'Authentication', short: 'Auth' },
  { key: 'hasHostname', label: 'Custom hostname', short: 'Hostname' },
  { key: 'hasCustomAddress', label: 'Custom address', short: 'Address' },
  { key: 'hasCustomBase', label: 'Custom base path', short: 'Base path' },
  { key: 'isSwarmMode', label: 'Swarm mode', short: 'Swarm' },
  { key: 'multiClient', label: 'Multiple browsers', short: 'Multi-browser' },
] as const

/**
 * The features that are actually settings, for "how many has this install turned on".
 * Multiple browsers is an observation, not something anyone configures.
 */
const CONFIGURABLE = FEATURES.filter((f) => f.key !== 'multiClient').map((f) => f.key)
const ALL = FEATURES.map((f) => f.key)

export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const latest = latestState(range)

  const [current, overTime, histogram, bySize] = await Promise.all([
    // Share of installs active in the range with each feature on, at their last beacon.
    query<{ key: string; enabled: number; installs: number }>(
      `WITH latest AS (${latest.sql})
       SELECT f.key,
              count(*) FILTER (WHERE drain_feature(latest.metadata, f.key))::int AS enabled,
              count(*)::int AS installs
         FROM latest CROSS JOIN unnest($3::text[]) AS f(key)
        GROUP BY 1`,
      [...latest.params, ALL],
    ),

    query<{ bucket: string; key: string; enabled: number; installs: number }>(
      `SELECT t.${s.timeCol} AS bucket, f.key,
              count(*) FILTER (WHERE drain_feature(t.metadata, f.key))::int AS enabled,
              count(*)::int AS installs
         FROM ${s.table} t CROSS JOIN unnest($3::text[]) AS f(key)
        WHERE t.${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to, ALL],
    ),

    // How many of the configurable features each install has on. A tall bar at 0-1 means
    // most people never move past the defaults.
    query<{ features: number; installs: number }>(
      `WITH latest AS (${latest.sql}),
       scored AS (
         SELECT (SELECT count(*) FROM unnest($3::text[]) AS f(key)
                  WHERE drain_feature(latest.metadata, f.key)) AS features
           FROM latest
       )
       SELECT features::int, count(*)::int AS installs FROM scored GROUP BY 1 ORDER BY 1`,
      [...latest.params, CONFIGURABLE],
    ),

    query<{ size_bucket: number; key: string; enabled: number; installs: number }>(
      `WITH latest AS (${latest.sql})
       SELECT drain_size_bucket(latest.metadata) AS size_bucket, f.key,
              count(*) FILTER (WHERE drain_feature(latest.metadata, f.key))::int AS enabled,
              count(*)::int AS installs
         FROM latest CROSS JOIN unnest($3::text[]) AS f(key)
        GROUP BY 1, 2 ORDER BY 1`,
      [...latest.params, ALL],
    ),
  ])

  return {
    range,
    features: FEATURES,
    maxFeatureCount: CONFIGURABLE.length,
    current,
    overTime,
    histogram,
    bySize,
  }
})
