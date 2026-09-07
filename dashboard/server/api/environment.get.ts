/**
 * Where Dozzle runs and what it runs against: browser, OS, auth provider, deployment
 * size and version. All single scans of the snapshot table for the range.
 */
const TOP_VERSIONS = 7

export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const state = latestState(range)
  const latest = `WITH latest AS (${state.sql})`

  const [browsers, oses, auth, sizes, versionMix, versionSeries] = await Promise.all([
    query<{ key: string; installs: number }>(
      `${latest} SELECT browser AS key, count(*)::int AS installs FROM latest GROUP BY 1 ORDER BY 2 DESC`,
      state.params,
    ),
    query<{ key: string; installs: number }>(
      `${latest} SELECT os AS key, count(*)::int AS installs FROM latest GROUP BY 1 ORDER BY 2 DESC`,
      state.params,
    ),
    query<{ bucket: string; key: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, auth_provider AS key, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
    query<{ bucket: string; key: number; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, size_bucket AS key, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
    // Rank on the tail of the range, not the whole of it, so a release that shipped last
    // week does not get swallowed by the Other bucket.
    query<{ version: string; installs: number }>(
      `${latest} SELECT version, count(*)::int AS installs FROM latest GROUP BY 1 ORDER BY 2 DESC LIMIT $3`,
      [...state.params, TOP_VERSIONS],
    ),
    query<{ bucket: string; version: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, version, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
  ])

  // Patch releases are rolled up to the minor in the snapshot already (a year holds 554
  // distinct patch versions); everything outside the top N collapses into Other here.
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
