import pg from 'pg'

// Postgres returns bigint/numeric as strings to avoid precision loss. Every count and
// percentage here fits in a double comfortably, and the charts want numbers.
pg.types.setTypeParser(pg.types.builtins.INT8, (v) => Number.parseInt(v, 10))
pg.types.setTypeParser(pg.types.builtins.NUMERIC, (v) => Number.parseFloat(v))
// DATE as a plain YYYY-MM-DD string: these are calendar buckets, not instants, and
// letting the driver build a Date drags the server's timezone into every axis label.
pg.types.setTypeParser(pg.types.builtins.DATE, (v) => v)

let pool: pg.Pool | undefined

export function db(): pg.Pool {
  if (!pool) {
    const url = useRuntimeConfig().databaseUrl
    if (!url) throw new Error('DATABASE_URL is not set')
    pool = new pg.Pool({
      connectionString: url,
      max: 10,
      idleTimeoutMillis: 30_000,
      // Nothing here should take seconds. Failing loudly beats a hung dashboard.
      statement_timeout: 20_000,
    })
  }
  return pool
}

export async function query<T = Record<string, unknown>>(
  text: string,
  params: unknown[] = [],
): Promise<T[]> {
  const started = performance.now()
  const res = await db().query(text, params)
  const ms = performance.now() - started
  if (ms > 500) console.warn(`slow query ${ms.toFixed(0)}ms: ${text.slice(0, 120).replace(/\s+/g, ' ')}`)
  return res.rows as T[]
}
