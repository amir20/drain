/**
 * Where Dozzle runs and what it runs against: browser, OS, auth provider, deployment
 * size and version. Every field is read out of the beacon payload by a helper, so none
 * of this has a schema to keep in step.
 */
const TOP_VERSIONS = 5

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
  const [browsers, oses, auth, sizes, versionSeries] = await Promise.all([
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
  ])

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
