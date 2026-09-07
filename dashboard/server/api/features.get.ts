/**
 * Feature penetration.
 *
 * Adding a feature to this dashboard is one entry in FEATURES below. Nothing else
 * changes: no column, no migration, no rebuild. The per-feature SQL columns are
 * generated from that list, so the queries still do not know how many features there
 * are - they are just built rather than written out.
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
const CONFIGURABLE = FEATURES.filter((f) => f.key !== 'multiClient')
const ALL = FEATURES.map((f) => f.key)

// The keys are compile-time constants from the list above and are pasted into SQL, so
// hold them to an identifier shape rather than trusting that by eye.
for (const key of ALL) {
  if (!/^[A-Za-z][A-Za-z0-9]*$/.test(key)) throw new Error(`unusable feature key: ${key}`)
}

/** `count(*) FILTER (WHERE <cond>)::int AS e0, …` - one counted column per feature. */
const enabledColumns = (cond: (index: number, key: string) => string) =>
  ALL.map((k, i) => `count(*) FILTER (WHERE ${cond(i, k)})::int AS e${i}`).join(',\n              ')

/** Turns the generated e0..eN columns back into one row per feature. */
const unpivot = <T extends Record<string, number>>(row: T) =>
  ALL.map((key, i) => ({ key, enabled: Number(row[`e${i}` as keyof T] ?? 0) }))

export default cachedAnalytics(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const latest = latestState(range)

  // One scan of the install set, not three.
  //
  // `current`, the histogram and the size split all read exactly the same rows and all
  // parse the same JSONB. Run as three statements they cost three scans, and the size
  // split additionally CROSS JOINed the feature list, so every install was expanded to
  // seven rows before a single field was read. On a 30-day window (95,681 installs) that
  // one query took 8.8s on its own and regularly hit the 20s statement_timeout.
  //
  // Parsing once into booleans and aggregating twice over the result brings all three to
  // ~1.4s. `current` is not queried at all: it is the size split summed up.
  const featureSql = `
    WITH latest AS MATERIALIZED (${latest.sql}),
    scored AS MATERIALIZED (
      SELECT drain_size_bucket(metadata) AS size_bucket,
             ${ALL.map((k, i) => `drain_feature(metadata, '${k}') AS f${i}`).join(',\n             ')}
        FROM latest
    )
    SELECT
      (SELECT coalesce(json_agg(a ORDER BY a.size_bucket), '[]'::json) FROM (
         SELECT size_bucket,
                count(*)::int AS installs,
                ${enabledColumns((i) => `f${i}`)}
           FROM scored GROUP BY size_bucket) a) AS by_size,
      (SELECT coalesce(json_agg(b ORDER BY b.features), '[]'::json) FROM (
         SELECT (${CONFIGURABLE.map((f) => `f${ALL.indexOf(f.key)}::int`).join(' + ')})::int AS features,
                count(*)::int AS installs
           FROM scored GROUP BY 1) b) AS histogram`

  const [aggregate, overTimeRows] = await Promise.all([
    query<{
      by_size: ({ size_bucket: number; installs: number } & Record<string, number>)[]
      histogram: { features: number; installs: number }[]
    }>(featureSql, latest.params),

    // Same idea: one column per feature rather than CROSS JOIN unnest, so each bucket's
    // rows are read once instead of once per feature.
    query<{ bucket: string; installs: number } & Record<string, number>>(
      `SELECT t.${s.timeCol} AS bucket,
              count(*)::int AS installs,
              ${enabledColumns((_, k) => `drain_feature(t.metadata, '${k}')`)}
         FROM ${s.table} t
        WHERE t.${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1 ORDER BY 1`,
      [s.from, range.to],
    ),
  ])

  const bySizeRows = aggregate[0]?.by_size ?? []
  const histogram = aggregate[0]?.histogram ?? []

  // `current` is the size split collapsed: drain_size_bucket never returns NULL (an
  // unknown count is its own bucket), so every install appears in exactly one group and
  // summing them is the same total the old standalone query produced.
  const current = ALL.map((key, i) => ({
    key,
    enabled: bySizeRows.reduce((n, r) => n + Number(r[`e${i}`] ?? 0), 0),
    installs: bySizeRows.reduce((n, r) => n + r.installs, 0),
  }))

  return {
    range,
    features: FEATURES,
    maxFeatureCount: CONFIGURABLE.length,
    current,
    overTime: overTimeRows.flatMap((r) =>
      unpivot(r).map((f) => ({ bucket: r.bucket, key: f.key, enabled: f.enabled, installs: r.installs })),
    ),
    histogram,
    bySize: bySizeRows.flatMap((r) =>
      unpivot(r).map((f) => ({
        size_bucket: r.size_bucket,
        key: f.key,
        enabled: f.enabled,
        installs: r.installs,
      })),
    ),
  }
})
