/**
 * Where Dozzle runs and what it runs against: browser, OS, auth provider, deployment
 * size and version. All single scans of the snapshot table for the range.
 */
// Five, not seven: version adoption is an ordered scale and is drawn with the one-hue
// ordinal ramp, which only holds five distinguishable steps. Everything past the top five
// minors folds into 'Other'.
const TOP_VERSIONS = 5

export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const s = snapshot(range)
  const state = latestState(range)
  const latest = `WITH latest AS (${state.sql})`

  const [browsers, oses, auth, sizes, versionSeries] = await Promise.all([
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
    query<{ bucket: string; version: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket, version, count(*)::int AS installs
         FROM ${s.table} WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2 ORDER BY 1`,
      [s.from, range.to],
    ),
  ])

  // Which versions get their own band.
  //
  // Ranking by current install count looks right and reads wrong over a long range: the
  // five biggest versions *today* did not exist a year ago, so the whole left of the chart
  // collapses into Other and the upgrade story disappears. Ranking by each version's PEAK
  // share instead keeps whichever versions actually dominated at some point in the range,
  // old and new, which is what makes the hand-off between releases visible.
  //
  // Patch releases are already rolled up to the minor in the snapshot (a year holds ~554
  // distinct patch versions); everything outside the top N collapses into Other.
  const bucketTotals = new Map<string, number>()
  for (const r of versionSeries) {
    bucketTotals.set(r.bucket, (bucketTotals.get(r.bucket) ?? 0) + r.installs)
  }
  const peak = new Map<string, number>()
  for (const r of versionSeries) {
    const total = bucketTotals.get(r.bucket) ?? 0
    if (!total) continue
    const share = r.installs / total
    if (share > (peak.get(r.version) ?? 0)) peak.set(r.version, share)
  }
  const versionMix = [...peak.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, TOP_VERSIONS)
    .map(([version, peakShare]) => ({ version, peakShare: Number((100 * peakShare).toFixed(2)) }))

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
