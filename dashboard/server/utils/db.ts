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
      // Failing loudly still beats a hung dashboard, but 20s was below what the widest
      // ranges legitimately cost. A year of data means grouping 1.18M client_weekly rows
      // by a dimension parsed out of `metadata`, and that JSONB parse is ~13.4s of a
      // 14.8s query - the same shape over a typed column is 1.4s. Five of those run
      // concurrently per page, so the Environment and Features pages were erroring at
      // the timeout rather than being slow.
      //
      // The real fix is to materialise those dimensions so the parse happens once, on
      // write, instead of on every read; this only stops the page failing while it is
      // still parsing on read. With the response cache in front, one load per hour pays
      // the cost and the rest are served in ~1ms.
      statement_timeout: 60_000,
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
