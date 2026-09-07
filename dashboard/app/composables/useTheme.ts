/**
 * Chart chrome read back out of the CSS custom properties, so the palette lives in one
 * place and the ECharts options follow the light/dark switch with the rest of the page.
 */
export interface ChartTheme {
  series: string[]
  sequential: string[]
  text: string
  secondary: string
  muted: string
  grid: string
  axis: string
  surface: string
  good: string
  critical: string
  dark: boolean
}

function read(name: string, fallback: string): string {
  if (typeof window === 'undefined') return fallback
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
}

export function useChartTheme() {
  const theme = shallowRef<ChartTheme>(snapshotTheme())
  const version = ref(0)

  function refresh() {
    theme.value = snapshotTheme()
    version.value++
  }

  onMounted(() => {
    refresh()
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    mq.addEventListener('change', refresh)
    onBeforeUnmount(() => mq.removeEventListener('change', refresh))
  })

  return { theme, version }
}

function snapshotTheme(): ChartTheme {
  const dark =
    typeof window !== 'undefined' &&
    window.matchMedia?.('(prefers-color-scheme: dark)').matches &&
    document.documentElement.dataset.theme !== 'light'

  return {
    // Fixed order. A ninth series is never a generated hue - it folds into "Other".
    series: [
      read('--series-1', '#2a78d6'),
      read('--series-2', '#eb6834'),
      read('--series-3', '#1baf7a'),
      read('--series-4', '#eda100'),
      read('--series-5', '#e87ba4'),
      read('--series-6', '#008300'),
      read('--series-7', '#4a3aa7'),
      read('--series-8', '#e34948'),
    ],
    sequential: [
      read('--seq-100', '#cde2fb'),
      read('--seq-200', '#9ec5f4'),
      read('--seq-300', '#6da7ec'),
      read('--seq-400', '#3987e5'),
      read('--seq-500', '#256abf'),
      read('--seq-600', '#184f95'),
      read('--seq-700', '#0d366b'),
    ],
    text: read('--text-primary', '#0b0b0b'),
    secondary: read('--text-secondary', '#52514e'),
    muted: read('--text-muted', '#898781'),
    grid: read('--grid', '#e1e0d9'),
    axis: read('--axis', '#c3c2b7'),
    surface: read('--surface', '#fcfcfb'),
    good: read('--good', '#0ca30c'),
    critical: read('--critical', '#d03b3b'),
    dark: Boolean(dark),
  }
}
