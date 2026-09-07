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

/** Every page fetches with the same key shape, so the range switch invalidates cleanly. */
export function useRangedFetch<T>(path: string) {
  const { params } = useRange()
  return useFetch<T>(path, {
    query: params,
    key: path,
    watch: [params],
    // The API is session-protected, and during SSR a plain $fetch would call it without
    // the incoming cookie and get a 401.
    $fetch: useRequestFetch(),
  })
}
