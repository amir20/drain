import type { ChartTheme } from './useTheme'

export type Point = [string, number | null]

const NUM = new Intl.NumberFormat('en-US')

export function fmtInt(n: number | null | undefined): string {
  return n === null || n === undefined ? '—' : NUM.format(Math.round(n))
}

export function fmtPct(n: number | null | undefined, digits = 1): string {
  return n === null || n === undefined ? '—' : `${n.toFixed(digits)}%`
}

/**
 * Shared chart chrome: recessive grid and axes, a crosshair tooltip, and the axis label
 * density left to ECharts rather than forced. Nothing here sets series colours - callers
 * assign them by slot so identity never depends on how many series survive a filter.
 */
export function baseOptions(theme: ChartTheme, opts: { legend?: boolean; percent?: boolean } = {}) {
  return {
    animation: false,
    backgroundColor: 'transparent',
    textStyle: { fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif' },
    grid: {
      left: 8,
      right: 16,
      top: opts.legend === false ? 12 : 36,
      bottom: 4,
      containLabel: true,
    },
    legend: opts.legend === false
      ? undefined
      : {
          top: 0,
          left: 0,
          icon: 'roundRect',
          itemWidth: 10,
          itemHeight: 10,
          itemGap: 14,
          textStyle: { color: theme.secondary, fontSize: 12 },
        },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line', lineStyle: { color: theme.axis, width: 1 } },
      backgroundColor: theme.surface,
      borderColor: theme.grid,
      borderWidth: 1,
      padding: [8, 10],
      textStyle: { color: theme.text, fontSize: 12 },
      extraCssText: 'box-shadow: 0 4px 16px rgba(0,0,0,0.12); border-radius: 8px;',
    },
    xAxis: {
      type: 'time' as const,
      axisLine: { lineStyle: { color: theme.axis } },
      axisTick: { show: false },
      axisLabel: { color: theme.muted, fontSize: 11, hideOverlap: true },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value' as const,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: theme.muted,
        fontSize: 11,
        formatter: opts.percent
          ? (v: number) => `${v}%`
          : (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k` : String(v)),
      },
      splitLine: { lineStyle: { color: theme.grid, width: 1 } },
      max: opts.percent ? 100 : undefined,
    },
  }
}

/**
 * `n` steps sampled evenly across the ordinal ramp, so a four-band scale still spans
 * light to dark instead of crowding into one end.
 *
 * The ramp tops out at FIVE steps and callers must not ask for more: below roughly a 0.06
 * lightness gap adjacent steps stop reading as distinct, and 250 -> 650 in 100-unit
 * increments is all that fits between "barely darker than the surface" and black. Past
 * five the index clamps and steps repeat - visibly wrong rather than silently undefined,
 * but still wrong. Fold the extra bands into an "other" instead.
 */
export function ordinalRamp(theme: ChartTheme, n: number): string[] {
  const ramp = theme.ordinal
  const last = ramp.length - 1
  if (n <= 1) return [ramp[last]!]
  return Array.from({ length: n }, (_, i) =>
    ramp[Math.min(last, Math.round((i * last) / (n - 1)))]!,
  )
}

/** 2px lines, >=8px hover markers, no dot on every point. */
export function lineSeries(
  name: string,
  color: string,
  data: Point[],
  extra: Record<string, unknown> = {},
) {
  return {
    name,
    type: 'line' as const,
    data,
    showSymbol: false,
    symbolSize: 8,
    lineStyle: { width: 2, color },
    itemStyle: { color },
    emphasis: { focus: 'series' as const, scale: false },
    ...extra,
  }
}

export function barSeries(
  name: string,
  color: string,
  data: (number | null)[] | Point[],
  extra: Record<string, unknown> = {},
) {
  return {
    name,
    type: 'bar' as const,
    data,
    // 4px rounded data-ends anchored to the baseline.
    itemStyle: { color, borderRadius: [4, 4, 0, 0] },
    emphasis: { focus: 'series' as const },
    ...extra,
  }
}
