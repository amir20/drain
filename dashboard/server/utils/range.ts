import { createError, getQuery, type H3Event } from 'h3'

export const PRESETS = {
  '1d': 1,
  '7d': 7,
  '30d': 30,
  '90d': 90,
  '1y': 365,
} as const

export type Preset = keyof typeof PRESETS
export type Bucket = 'hour' | 'day' | 'week'

export interface Range {
  /** inclusive, YYYY-MM-DD */
  from: string
  /** inclusive, YYYY-MM-DD */
  to: string
  /** start of the week containing `from`, for the weekly tables */
  weekFrom: string
  days: number
  bucket: Bucket
  /** the equivalent window immediately before `from`, for period-over-period deltas */
  prevFrom: string
  prevTo: string
  /**
   * The hour-resolution window, as ISO instants. A range ending today ends at *now*, not
   * at midnight tomorrow, so the "24 hours" preset really is the trailing 24 hours
   * rather than however much of today has elapsed.
   */
  hourFrom: string
  hourTo: string
  /**
   * True when the range runs up to today. For these - every preset, and most custom
   * ranges - "installs active in the range" is exactly "last seen on or after `from`",
   * which `client_latest` answers with one indexed scan.
   */
  endsToday: boolean
  label: string
}

const DAY_MS = 86_400_000

function iso(d: Date): string {
  return d.toISOString().slice(0, 10)
}

function parseDate(value: string, field: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw createError({ statusCode: 400, statusMessage: `${field} must be YYYY-MM-DD` })
  }
  const d = new Date(`${value}T00:00:00Z`)
  if (Number.isNaN(d.getTime())) {
    throw createError({ statusCode: 400, statusMessage: `${field} is not a valid date` })
  }
  return d
}

/** Monday-start week, matching date_trunc('week', ...) in Postgres. */
function startOfWeek(d: Date): Date {
  const out = new Date(d)
  const dow = (out.getUTCDay() + 6) % 7
  out.setUTCDate(out.getUTCDate() - dow)
  return out
}

/**
 * Granularity is derived from the window, not chosen by the caller: an hour axis over a
 * year is 8760 points nobody can read, and a week axis over one day is a single bar.
 * `bucket` can still be forced for the odd panel that wants a coarser view.
 *
 * The day/week cut sits at a month for two reasons that agree: 90 daily points is a
 * noisier read of a quarter than 13 weekly ones, and the weekly snapshot is ~7x smaller
 * (measured: a 90-day dimension series drops from 273ms to 70ms). DAU/WAU/MAU keeps
 * daily resolution at every range - it reads active_counts_daily, not the snapshot.
 */
function bucketFor(days: number): Bucket {
  if (days <= 2) return 'hour'
  if (days <= 31) return 'day'
  return 'week'
}

export function resolveRange(event: H3Event): Range {
  const q = getQuery(event)
  const today = new Date(`${iso(new Date())}T00:00:00Z`)


  let from: Date
  let to: Date
  let label: string

  if (q.from || q.to) {
    // Custom range. Both ends required so a half-specified range can't silently mean
    // "since the beginning of time".
    if (!q.from || !q.to) {
      throw createError({ statusCode: 400, statusMessage: 'custom ranges need both from and to' })
    }
    from = parseDate(String(q.from), 'from')
    to = parseDate(String(q.to), 'to')
    if (from > to) throw createError({ statusCode: 400, statusMessage: 'from is after to' })
    if ((to.getTime() - from.getTime()) / DAY_MS > 366 * 5) {
      throw createError({ statusCode: 400, statusMessage: 'range is longer than five years' })
    }
    label = `${iso(from)} → ${iso(to)}`
  } else {
    const preset = String(q.range ?? '30d') as Preset
    if (!(preset in PRESETS)) {
      throw createError({
        statusCode: 400,
        statusMessage: `range must be one of ${Object.keys(PRESETS).join(', ')} or a from/to pair`,
      })
    }
    const days = PRESETS[preset]
    to = today
    from = new Date(today.getTime() - (days - 1) * DAY_MS)
    label = preset
  }

  const days = Math.round((to.getTime() - from.getTime()) / DAY_MS) + 1
  const forced = q.bucket ? String(q.bucket) : undefined
  const bucket: Bucket =
    forced === 'hour' || forced === 'day' || forced === 'week' ? forced : bucketFor(days)

  const now = new Date()
  const hourTo = to.getTime() === today.getTime() ? now : new Date(to.getTime() + DAY_MS)
  const hourFrom = new Date(hourTo.getTime() - days * DAY_MS)

  return {
    from: iso(from),
    to: iso(to),
    weekFrom: iso(startOfWeek(from)),
    days,
    bucket,
    prevFrom: iso(new Date(from.getTime() - days * DAY_MS)),
    prevTo: iso(new Date(from.getTime() - DAY_MS)),
    hourFrom: hourFrom.toISOString(),
    hourTo: hourTo.toISOString(),
    endsToday: to.getTime() >= today.getTime(),
    label,
  }
}

/**
 * Some measures only exist per week - how many days an install reported, whether it was
 * retained week over week, a cohort's age. A range shorter than a couple of weeks has no
 * complete week in it, and filtering on it strictly returns nothing at all. So the weekly
 * panels widen to a minimum number of complete weeks and say that they did, rather than
 * rendering an empty card on the 24-hour and 7-day views.
 */
export const MIN_WEEKS = 8

export function weeklyWindow(range: Range): { from: string; widened: boolean } {
  const lastComplete = startOfWeek(new Date(`${range.to}T00:00:00Z`))
  lastComplete.setUTCDate(lastComplete.getUTCDate() - 7)
  const completeWeeks =
    Math.floor(
      (lastComplete.getTime() - new Date(`${range.weekFrom}T00:00:00Z`).getTime()) / DAY_MS / 7,
    ) + 1

  if (completeWeeks >= 2) return { from: range.weekFrom, widened: false }

  const widened = new Date(lastComplete)
  widened.setUTCDate(widened.getUTCDate() - (MIN_WEEKS - 1) * 7)
  return { from: iso(widened), widened: true }
}

/**
 * Which pre-aggregated snapshot to read. The weekly table is ~7x smaller and is what
 * keeps a one-year range in the same latency class as a 30-day one.
 */
export function snapshot(range: Range): { table: string; timeCol: string; from: string } {
  return range.bucket === 'week'
    ? { table: 'client_snapshot_weekly', timeCol: 'week', from: range.weekFrom }
    : { table: 'client_snapshot_daily', timeCol: 'day', from: range.from }
}

/**
 * The source for "state of every install active in this range". Ranges ending today read
 * the precomputed per-install latest state; a range ending in the past has to resolve
 * each install's last activity *inside* that window, which only the snapshot can answer.
 *
 * Both return one row per install with the same columns, so callers write one query.
 */
export function latestState(range: Range): { sql: string; params: [string, string] } {
  if (range.endsToday) {
    return {
      sql: `SELECT * FROM client_latest
             WHERE last_event_day >= $1::date AND last_event_day <= $2::date`,
      params: [range.from, range.to],
    }
  }
  // Hash-aggregate to each install's last bucket, then join the row back. Measured at
  // roughly half the cost of DISTINCT ON, which has to sort the whole window.
  const s = snapshot(range)
  return {
    sql: `WITH last AS (
            SELECT client_id, max(${s.timeCol}) AS ${s.timeCol}
              FROM ${s.table}
             WHERE ${s.timeCol} BETWEEN $1 AND $2
             GROUP BY client_id
          )
          SELECT t.*
            FROM ${s.table} t
            JOIN last l ON l.client_id = t.client_id AND l.${s.timeCol} = t.${s.timeCol}
           WHERE t.${s.timeCol} BETWEEN $1 AND $2`,
    params: [s.from, range.to],
  }
}
