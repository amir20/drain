export const PRESETS = [
  { key: '1d', label: '24 hours' },
  { key: '7d', label: '7 days' },
  { key: '30d', label: '30 days' },
  { key: '90d', label: '90 days' },
  { key: '1y', label: '1 year' },
] as const

export type PresetKey = (typeof PRESETS)[number]['key']

export interface RangeQuery {
  range?: string
  from?: string
  to?: string
}

/**
 * The range lives in the URL so a view is linkable and a reload keeps it. Every API call
 * on the page takes the same params, so switching range refetches everything at once.
 */
export function useRange() {
  const route = useRoute()
  const router = useRouter()

  const params = computed<RangeQuery>(() => {
    const { range, from, to } = route.query
    if (from && to) return { from: String(from), to: String(to) }
    return { range: String(range ?? '30d') }
  })

  const active = computed<PresetKey | 'custom'>(() =>
    params.value.from ? 'custom' : ((params.value.range ?? '30d') as PresetKey),
  )

  function setPreset(key: PresetKey) {
    router.replace({ query: { ...route.query, range: key, from: undefined, to: undefined } })
  }

  function setCustom(from: string, to: string) {
    router.replace({ query: { ...route.query, range: undefined, from, to } })
  }

  return { params, active, setPreset, setCustom }
}

/**
 * Every page fetches with the same key shape, so the range switch invalidates cleanly.
 *
 * The request does not block the page. These queries aggregate over the whole install
 * base and take seconds on the wider ranges; resolving them during SSR meant the browser
 * got no HTML at all until the slowest one finished, so the entire page - nav, range
 * picker, headings - waited on the charts. `server: false` renders the shell
 * immediately and fetches after hydration, and every chart already has a loading state.
 *
 * Nothing here is indexable (the whole dashboard is behind a session) and the charts are
 * client-only anyway, so there is nothing to lose by not rendering them on the server.
 */
export function useRangedFetch<T>(path: string) {
  const { params } = useRange()
  const result = useFetch<T>(path, {
    query: params,
    key: path,
    watch: [params],
    lazy: true,
    server: false,
    // The API is session-protected, and during SSR a plain $fetch would call it without
    // the incoming cookie and get a 401.
    $fetch: useRequestFetch(),
  })

  // `pending` is false while the status is still `idle`, which is what it is during SSR
  // and on the first client tick when `server: false`. Using it directly would render
  // "No data in this range." for a frame before the request even starts, so treat
  // anything that is not yet settled as loading.
  const pending = computed(() => result.status.value === 'idle' || result.status.value === 'pending')

  return { ...result, pending }
}
