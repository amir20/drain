/**
 * What people actually do in Dozzle, from the daily `usage` beacon.
 *
 * Each usage row carries `usage`, a map of counters since the previous row (view.container,
 * action.restart, host.add.ok ...), plus `locales` and a bucketed `activeMinutes`. None of
 * that is on the `events` rows the other pages read, so this page reads `beacon` directly:
 * name = 'usage' over the hour-resolution window, which idx_beacon_time_name_client covers
 * and the `name` compression segment prunes. At one row per install per day it is small
 * next to the daily aggregate (see migrations/007).
 *
 * Counters are summed per install before anything else, so "installs that used X" means
 * "reported X at least once in the range", and one install that restarted 400 times does
 * not read as 400 installs.
 *
 * The groups and funnels below are the only place that knows which counter means what.
 * A counter Dozzle adds later still shows up in `counters` with no change here.
 */

export const GROUPS = [
  {
    key: 'views',
    label: 'Log views',
    keys: ['view.container', 'view.merged', 'view.group', 'view.host', 'view.stack', 'view.service', 'view.namespace'],
  },
  {
    key: 'logTools',
    label: 'Log tools',
    keys: ['logs.search', 'logs.older', 'logs.download', 'logs.sql', 'palette.open', 'pinned.open'],
  },
  {
    key: 'actions',
    label: 'Container actions',
    keys: ['action.start', 'action.stop', 'action.restart', 'action.update', 'action.remove'],
  },
  { key: 'shell', label: 'Shell', keys: ['shell.exec', 'shell.attach'] },
  { key: 'images', label: 'Image updates', keys: ['image.check', 'image.update'] },
  { key: 'alerts', label: 'Alerts sent', keys: ['notify.log', 'notify.event', 'notify.metric'] },
  { key: 'rules', label: 'Alert rules edited', keys: ['rules.create', 'rules.edit'] },
] as const

export const FUNNELS = {
  addHost: [
    { key: 'host.add.ok', label: 'Added' },
    { key: 'host.add.refused', label: 'Refused' },
    { key: 'host.add.cert', label: 'Certificate mismatch' },
    { key: 'host.add.duplicate', label: 'Duplicate host' },
    { key: 'host.add.timeout', label: 'Timed out' },
    { key: 'host.add.other', label: 'Other error' },
  ],
  wizard: [
    { key: 'wizard.shown', label: 'Shown' },
    { key: 'wizard.finished', label: 'Finished' },
    { key: 'wizard.skip.login', label: 'Skipped login' },
    { key: 'wizard.skip.actions', label: 'Skipped actions' },
    { key: 'wizard.skip.hosts', label: 'Skipped hosts' },
    { key: 'wizard.skip.cloud', label: 'Skipped Cloud' },
    { key: 'wizard.skip.update', label: 'Skipped auto-update' },
  ],
  cloud: [
    { key: 'cloud.welcome', label: 'Welcome shown' },
    { key: 'cloud.connect', label: 'Connect clicked' },
  ],
  reliability: [
    { key: 'stream.reconnect', label: 'Stream reconnects' },
    { key: 'agent.disconnect', label: 'Agent disconnects' },
  ],
} as const

export const ACTIVE_MINUTES = ['0', '1-10', '11-60', '61-240', '241+'] as const

export default cachedAnalytics(async (event) => {
  const range = resolveRange(event)
  const groupParams = GROUPS.map((g) => [...g.keys])

  // One statement. Every panel reads the same usage rows, so they are materialised once
  // and the per-install counter sums once; the panels are aggregates over those.
  //
  // The group columns are generated, each binding its own key list, so the query does not
  // know how many groups there are.
  const groupColumns = GROUPS.map(
    (_, i) =>
      `count(DISTINCT client_id) FILTER (WHERE v > 0 AND key = ANY($${i + 3}::text[]))::int AS g${i}`,
  ).join(',\n               ')

  const [row] = await query<{
    installs: number
    usage_days: number
    counters: { key: string; installs: number; total: number }[]
    groups: Record<string, number>
    active: { bucket: string; days: number }[]
    locales: { locale: string; installs: number; sessions: number }[]
  }>(
    `WITH u AS MATERIALIZED (
       SELECT client_id, metadata
         FROM beacon
        WHERE name = 'usage' AND client_id <> ''
          AND time >= $1::timestamptz AND time < $2::timestamptz
     ),
     counters AS MATERIALIZED (
       SELECT u.client_id, e.key, sum((e.value #>> '{}')::numeric)::bigint AS v
         FROM u,
              jsonb_each(CASE WHEN jsonb_typeof(u.metadata -> 'usage') = 'object'
                              THEN u.metadata -> 'usage' ELSE '{}'::jsonb END) AS e
        WHERE jsonb_typeof(e.value) = 'number'
        GROUP BY 1, 2
     )
     SELECT
       (SELECT count(DISTINCT client_id)::int FROM u) AS installs,
       (SELECT count(*)::int FROM u) AS usage_days,
       (SELECT coalesce(json_agg(c ORDER BY c.key), '[]'::json) FROM (
          SELECT key, count(*) FILTER (WHERE v > 0)::int AS installs, sum(v)::bigint AS total
            FROM counters GROUP BY key) c) AS counters,
       (SELECT row_to_json(g) FROM (
          SELECT ${groupColumns}
            FROM counters) g) AS groups,
       (SELECT coalesce(json_agg(a), '[]'::json) FROM (
          SELECT COALESCE(NULLIF(metadata ->> 'activeMinutes', ''), 'unknown') AS bucket,
                 count(*)::int AS days
            FROM u GROUP BY 1) a) AS active,
       (SELECT coalesce(json_agg(l ORDER BY l.installs DESC), '[]'::json) FROM (
          SELECT e.key AS locale,
                 count(DISTINCT u.client_id)::int AS installs,
                 sum((e.value #>> '{}')::numeric)::bigint AS sessions
            FROM u,
                 jsonb_each(CASE WHEN jsonb_typeof(u.metadata -> 'locales') = 'object'
                                 THEN u.metadata -> 'locales' ELSE '{}'::jsonb END) AS e
           WHERE jsonb_typeof(e.value) = 'number'
           GROUP BY 1) l) AS locales`,
    [range.hourFrom, range.hourTo, ...groupParams],
  )

  const installs = row?.installs ?? 0
  const counters = row?.counters ?? []
  const byKey = new Map(counters.map((c) => [c.key, c]))
  const pick = (keys: readonly { key: string; label: string }[]) =>
    keys.map(({ key, label }) => ({
      key,
      label,
      installs: byKey.get(key)?.installs ?? 0,
      total: byKey.get(key)?.total ?? 0,
    }))

  return {
    range,
    installs,
    usageDays: row?.usage_days ?? 0,
    groups: GROUPS.map((g, i) => ({
      key: g.key,
      label: g.label,
      installs: Number(row?.groups?.[`g${i}`] ?? 0),
      total: g.keys.reduce((n, k) => n + (byKey.get(k)?.total ?? 0), 0),
      counters: g.keys.map((k) => ({
        key: k,
        installs: byKey.get(k)?.installs ?? 0,
        total: byKey.get(k)?.total ?? 0,
      })),
    })),
    addHost: pick(FUNNELS.addHost),
    wizard: pick(FUNNELS.wizard),
    cloud: pick(FUNNELS.cloud),
    reliability: pick(FUNNELS.reliability),
    active: ACTIVE_MINUTES.map((bucket) => ({
      bucket,
      days: (row?.active ?? []).find((a) => a.bucket === bucket)?.days ?? 0,
    })),
    locales: row?.locales ?? [],
    counters,
  }
})
