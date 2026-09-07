/**
 * Feature penetration.
 *
 * All of this reads the pre-parsed snapshot: the booleans are real columns, so there is
 * no JSONB extraction, no CROSS JOIN LATERAL (VALUES ...) unpivot and no per-panel
 * DISTINCT ON over the raw aggregate.
 *
 * hasShell, remoteAgents, remoteClients and filterLength are in the beacon payload but
 * always report zero/false, and mode/subCommand are effectively always empty. They are
 * left off rather than drawn as flat zero lines.
 */
// `short` is for the by-deployment-size table, where nine full-width headers overflow the
// card and clip the last column.
export const FEATURES = [
  { key: 'has_actions', label: 'Container actions', short: 'Actions' },
  { key: 'has_auth', label: 'Authentication', short: 'Auth' },
  { key: 'has_hostname', label: 'Custom hostname', short: 'Hostname' },
  { key: 'has_custom_address', label: 'Custom address', short: 'Address' },
  { key: 'has_custom_base', label: 'Custom base path', short: 'Base path' },
  { key: 'is_swarm', label: 'Swarm mode', short: 'Swarm' },
  { key: 'multi_client', label: 'Multiple browsers', short: 'Multi-browser' },
] as const

/** The six configurable features an install can score on (multi-browser is not a setting). */
export const MAX_FEATURE_COUNT = 6

export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const latest = latestState(range)

  const filters = FEATURES.map(
    (f) => `count(*) FILTER (WHERE ${f.key})::int AS ${f.key}`,
  ).join(',\n           ')

  const [current, overTime, histogram, bySize] = await Promise.all([
    // State of every install at its last activity inside the range.
    query<Record<string, number>>(
      `WITH latest AS (${latest.sql})
       SELECT count(*)::int AS installs, ${filters} FROM latest`,
      latest.params,
    ),

    query<Record<string, number | string>>(
      `SELECT ${s.timeCol} AS bucket, count(*)::int AS installs, ${filters}
         FROM ${s.table}
        WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1 ORDER BY 1`,
      [s.from, range.to],
    ),

    // How many of the six configurable features each install has on. A tall bar at 0-1
    // means most people never move past the defaults.
    query<{ features: number; installs: number }>(
      `WITH latest AS (${latest.sql})
       SELECT feature_count AS features, count(*)::int AS installs
         FROM latest GROUP BY 1 ORDER BY 1`,
      latest.params,
    ),

    query<Record<string, number>>(
      `WITH latest AS (${latest.sql})
       SELECT size_bucket, count(*)::int AS installs, ${filters}
         FROM latest GROUP BY 1 ORDER BY 1`,
      latest.params,
    ),
  ])

  return {
    range,
    features: FEATURES,
    maxFeatureCount: MAX_FEATURE_COUNT,
    current: current[0] ?? { installs: 0 },
    overTime,
    histogram,
    bySize,
  }
})
