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

  // Five statements became two, because they were five scans of two row sets.
  //
  // Browser and OS read the same install set; auth, size and version read the same
  // window of the per-bucket table. Every one of them parses the same JSONB, so running
  // them separately paid for that parse three and two times over - and `drain_os` and
  // `drain_browser` were among the statements hitting the 20s statement_timeout in
  // production. Grouping once and pivoting in TypeScript costs one parse each.
  const [installState, series] = await Promise.all([
    query<{
      browsers: { key: string; installs: number }[]
      oses: { key: string; installs: number }[]
    }>(
      `WITH latest AS MATERIALIZED (${state.sql}),
       parsed AS MATERIALIZED (
         SELECT drain_browser(metadata) AS browser, drain_os(metadata) AS os FROM latest
       )
       SELECT
         (SELECT coalesce(json_agg(a ORDER BY a.installs DESC), '[]'::json) FROM (
            SELECT browser AS key, count(*)::int AS installs FROM parsed GROUP BY 1) a) AS browsers,
         (SELECT coalesce(json_agg(b ORDER BY b.installs DESC), '[]'::json) FROM (
            SELECT os AS key, count(*)::int AS installs FROM parsed GROUP BY 1) b) AS oses`,
      state.params,
    ),

    // One grouped pass over the window; the three charts are cuts of the same rows.
    query<{ bucket: string; auth: string; size: number; version: string; installs: number }>(
      `SELECT ${s.timeCol} AS bucket,
              drain_auth_provider(metadata) AS auth,
              drain_size_bucket(metadata)   AS size,
              drain_minor_version(metadata) AS version,
              count(*)::int AS installs
         FROM ${s.table}
        WHERE ${s.timeCol} BETWEEN $1 AND $2
        GROUP BY 1, 2, 3, 4 ORDER BY 1`,
      [s.from, range.to],
    ),
  ])

  const browsers = installState[0]?.browsers ?? []
  const oses = installState[0]?.oses ?? []

  /** Collapse the combined rows down to one dimension, re-adding across the others. */
  function fold<K>(pick: (r: (typeof series)[number]) => K) {
    const acc = new Map<string, { bucket: string; key: K; installs: number }>()
    for (const r of series) {
      const k = `${r.bucket}\u0000${String(pick(r))}`
      const hit = acc.get(k)
      if (hit) hit.installs += r.installs
      else acc.set(k, { bucket: r.bucket, key: pick(r), installs: r.installs })
    }
    return [...acc.values()].sort((a, b) => a.bucket.localeCompare(b.bucket))
  }

  const auth = fold((r) => r.auth)
  const sizes = fold((r) => r.size)
  const versionSeries = fold((r) => r.version).map((r) => ({
    bucket: r.bucket,
    version: r.key,
    installs: r.installs,
  }))

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
