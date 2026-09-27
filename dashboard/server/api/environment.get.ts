/**
 * Where Dozzle runs and what it runs against: browser, OS, auth provider, deployment
 * size and version. Every field is read out of the beacon payload by a helper, so none
 * of this has a schema to keep in step.
 */
const TOP_VERSIONS = 5

/** hostsByType keys, in the order the chart draws them. */
export const HOST_TYPES = [
  { key: 'local', label: 'Local socket' },
  { key: 'agent', label: 'Agent' },
  { key: 'remote', label: 'Remote socket' },
  { key: 'swarm', label: 'Swarm node' },
  { key: 'k8s', label: 'Kubernetes' },
] as const

export const USER_BUCKETS = ['1', '2-5', '6-20', '21+'] as const

export default cachedAnalytics(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const state = latestState(range)

  // Five statements, run concurrently, rather than two that each do more.
  //
  // An earlier revision merged these into two - browser with OS, and auth with size and
  // version - on the reasoning that they re-read the same rows. That was the wrong
  // trade: the five already ran in parallel, so merging only serialised work and made
  // the page slower. Measured on production for a 30-day window, once the filters became
  // sargable: auth 508ms, size 1962ms, version 714ms - about 1.9s of wall clock in
  // parallel - against 2.7s for the single combined pass; browser 1651ms and OS 1281ms
  // against 2.2s merged.
  //
  // Merging is worth it where a statement does redundant work of its own, which is why
  // the Features page keeps it: there the pass was CROSS JOINing the feature list and
  // expanding every install sevenfold before reading a field.
  const [browsers, oses, auth, sizes, versionSeries, fleetRows] = await Promise.all([
    query<{ key: string; installs: number }>(
      `WITH latest AS (${state.sql})
       SELECT drain_browser(metadata) AS key, count(*)::int AS installs
         FROM latest GROUP BY 1 ORDER BY 2 DESC`,
      state.params,
    ),
    query<{ key: string; installs: number }>(
      `WITH latest AS (${state.sql})
       SELECT drain_os(metadata) AS key, count(*)::int AS installs
         FROM latest GROUP BY 1 ORDER BY 2 DESC`,
      state.params,
    ),
    query<{ bucket: string; key: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, drain_auth_provider(metadata) AS key, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.window()}
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
    query<{ bucket: string; key: number; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, drain_size_bucket(metadata) AS key, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.window()}
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
    query<{ bucket: string; version: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, drain_minor_version(metadata) AS version, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.window()}
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
    // Fleet shape and sharing, from each install's latest state. Only installs whose
    // Dozzle sends hostsByType / users are in the denominators: older releases do not
    // report them at all, and counting them as "no agents" would read the upgrade curve
    // as adoption.
    query<{
      reporting: number
      with_agents: number
      agents_down: number
      simple_reporting: number
      users: { bucket: string; installs: number }[]
      [k: string]: unknown
    }>(
      `WITH latest AS MATERIALIZED (${state.sql})
       SELECT
         count(*) FILTER (WHERE metadata ? 'hostsByType')::int AS reporting,
         ${HOST_TYPES.map(
           (h) =>
             `count(*) FILTER (WHERE drain_map_int(metadata, 'hostsByType', '${h.key}') > 0)::int AS "t_${h.key}"`,
         ).join(',\n         ')},
         count(*) FILTER (WHERE drain_map_int(metadata, 'hostsByType', 'agent') > 0)::int AS with_agents,
         count(*) FILTER (WHERE drain_map_int(metadata, 'hostsByType', 'agent') > 0
                            AND COALESCE((metadata ->> 'agentsDown')::int, 0) > 0)::int AS agents_down,
         count(*) FILTER (WHERE metadata ? 'users')::int AS simple_reporting,
         (SELECT coalesce(json_agg(u), '[]'::json) FROM (
            SELECT metadata ->> 'users' AS bucket, count(*)::int AS installs
              FROM latest WHERE metadata ? 'users' GROUP BY 1) u) AS users
       FROM latest`,
      state.params,
    ),
  ])

  const fleet = fleetRows[0]

  // Which versions get their own band.
  //
  // Ranking by current install count looks right and reads wrong over a long range: the
  // five biggest versions *today* did not exist a year ago, so the whole left of the
  // chart collapses into Other and the upgrade story disappears. Ranking by each
  // version's PEAK share keeps whichever versions actually dominated at some point, old
  // and new, which is what makes the hand-off between releases visible.
  //
  // Patch releases are already rolled up to the minor by drain_minor_version (a year
  // holds ~554 distinct patches); everything outside the top N collapses into Other.
  const totals = new Map<string, number>()
  for (const r of versionSeries) totals.set(r.bucket, (totals.get(r.bucket) ?? 0) + r.installs)

  const peak = new Map<string, number>()
  for (const r of versionSeries) {
    const total = totals.get(r.bucket) ?? 0
    if (!total) continue
    peak.set(r.version, Math.max(peak.get(r.version) ?? 0, r.installs / total))
  }
  const versionMix = [...peak.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, TOP_VERSIONS)
    .map(([version]) => ({ version }))

  const top = new Set(versionMix.map((v) => v.version))
  const rolled = new Map<string, Map<string, number>>()
  for (const r of versionSeries) {
    const key = top.has(r.version) ? r.version : 'Other'
    const row = rolled.get(r.bucket) ?? new Map<string, number>()
    row.set(key, (row.get(key) ?? 0) + r.installs)
    rolled.set(r.bucket, row)
  }

  return {
    range,
    fleet: {
      reporting: fleet?.reporting ?? 0,
      hostTypes: HOST_TYPES.map((h) => ({
        key: h.key,
        label: h.label,
        installs: Number(fleet?.[`t_${h.key}`] ?? 0),
      })),
      withAgents: fleet?.with_agents ?? 0,
      agentsDown: fleet?.agents_down ?? 0,
      simpleReporting: fleet?.simple_reporting ?? 0,
      users: USER_BUCKETS.map((bucket) => ({
        bucket,
        installs: (fleet?.users ?? []).find((u) => u.bucket === bucket)?.installs ?? 0,
      })),
    },
    browsers,
    oses,
    auth,
    sizes,
    versionMix,
    versions: [...rolled.entries()]
      .map(([bucket, row]) => ({ bucket, counts: Object.fromEntries(row) }))
      .sort((a, b) => a.bucket.localeCompare(b.bucket)),
  }
})
