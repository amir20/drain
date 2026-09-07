<script setup lang="ts">
import { baseOptions, barSeries, fmtInt, ordinalRamp } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Environment · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/environment')
const { theme, version } = useChartTheme()

const SIZES = ['None', '1–5', '6–20', '21–200', 'Over 200', 'Unknown']

/** Numeric compare of 'v8.9' / 'v8.10' style minors; unparseable versions sort first. */
function compareVersion(a: string, b: string) {
  const parts = (v: string) => (v.match(/\d+/g) ?? []).map(Number)
  const pa = parts(a)
  const pb = parts(b)
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const d = (pa[i] ?? -1) - (pb[i] ?? -1)
    if (d !== 0) return d
  }
  return a.localeCompare(b)
}

/**
 * Distributions are horizontal bars in a single hue, not pies: the category axis carries
 * identity, so colour has no job here and seven pie wedges would need seven hues that
 * cannot all stay separable under CVD.
 */
function distributionOption(rows: { key: string; installs: number }[] | undefined) {
  void version.value
  const t = theme.value
  if (!rows?.length) return null
  const total = rows.reduce((s, r) => s + r.installs, 0)
  const sorted = [...rows].sort((a, b) => a.installs - b.installs)
  // Plotted as share, not count: a composition is read as "what fraction", and mixing a
  // count axis with percentage labels puts two units on one mark. The absolute number is
  // in the tooltip.
  const share = (n: number) => (total ? Number(((100 * n) / total).toFixed(1)) : 0)
  return {
    ...baseOptions(t, { legend: false }),
    grid: { left: 8, right: 72, top: 8, bottom: 4, containLabel: true },
    xAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 11, formatter: (v: number) => `${v}%` },
      splitLine: { lineStyle: { color: t.grid } },
    },
    yAxis: {
      type: 'category',
      data: sorted.map((r) => r.key),
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.secondary, fontSize: 12 },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) =>
        `<b>${p[0].name}</b><br>${p[0].value}% of active installs<br><span style="opacity:.7">${fmtInt(sorted[p[0].dataIndex]!.installs)} installs</span>`,
    },
    series: [
      {
        type: 'bar',
        data: sorted.map((r) => share(r.installs)),
        // Nominal categories: the axis label carries identity, so colour has no job here
        // and every bar takes the same slot-1 hue.
        itemStyle: { color: t.series[0], borderRadius: [0, 4, 4, 0] },
        label: {
          show: true,
          position: 'right',
          color: t.secondary,
          fontSize: 11.5,
          formatter: (p: any) => `${p.value}%`,
        },
      },
    ],
  }
}

const browserOption = computed(() => distributionOption(data.value?.browsers))
const osOption = computed(() => distributionOption(data.value?.oses))

/** Shares of the active base per bucket, stacked to 100%. */
function shareOption(
  rows: { bucket: string; key: string | number; installs: number }[] | undefined,
  labelOf: (k: any) => string,
  order?: string[],
  /**
   * Colours per series. Omitted means the categorical slots (identity); pass a ramp for
   * an ordered scale so the order reads out of the colour.
   */
  palette?: string[],
) {
  void version.value
  const t = theme.value
  if (!rows?.length) return null
  const buckets = [...new Set(rows.map((r) => r.bucket))].sort()
  const idx = new Map(buckets.map((b, i) => [b, i]))
  const totals = buckets.map(() => 0)
  for (const r of rows) totals[idx.get(r.bucket)!] += r.installs

  const keys = order ?? [...new Set(rows.map((r) => labelOf(r.key)))].sort()
  const colours = palette ?? t.series
  const series = keys.slice(0, colours.length).map((key, i) => {
    const values = buckets.map(() => 0)
    for (const r of rows) if (labelOf(r.key) === key) values[idx.get(r.bucket)!] += r.installs
    return barSeries(
      key,
      colours[i]!,
      values.map((v, j) => (totals[j] ? Number(((100 * v) / totals[j]!).toFixed(2)) : 0)),
      { stack: 'share', itemStyle: { color: colours[i], borderColor: t.surface, borderWidth: 1 } },
    )
  })

  return {
    ...baseOptions(t, { percent: true }),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: buckets },
    series,
  }
}

const authOption = computed(() => shareOption(data.value?.auth, (k) => String(k)))
// Deployment size is ordered, so the five real bands take the ordinal ramp and Unknown -
// which has no position on that scale - takes the reserved neutral.
const sizeOption = computed(() =>
  shareOption(data.value?.sizes, (k) => SIZES[Number(k)] ?? 'Unknown', SIZES, [
    ...ordinalRamp(theme.value, SIZES.length - 1),
    theme.value.ordinalNone,
  ]),
)

/** Version adoption. Everything outside the top minors is already folded to Other. */
const versionOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.versions ?? []
  if (!rows.length) return null
  // Releases are ordered, so oldest-to-newest gets the ordinal ramp. That ordering has to
  // be numeric: a plain string sort puts v8.10 and v8.12 before v8.9 and paints the ramp
  // backwards over exactly the hand-off the chart exists to show.
  //
  // 'unknown' and 'Other' are not points on that scale and take the reserved neutral.
  const mix = (data.value?.versionMix ?? []).map((v: any) => v.version)
  const named = mix.filter((v: string) => v !== 'unknown').sort(compareVersion)
  const rest = mix.filter((v: string) => v === 'unknown')
  const keys = [...named, ...rest, 'Other']
  // ordinalRamp(t, 0) still returns one colour, which would land on 'unknown'.
  const colours = [
    ...(named.length ? ordinalRamp(t, named.length) : []),
    ...rest.map(() => t.ordinalNone),
    t.ordinalNone,
  ]
  const buckets = rows.map((r: any) => r.bucket)
  const totals = rows.map((r: any) =>
    Object.values(r.counts as Record<string, number>).reduce((s, n) => s + n, 0),
  )
  return {
    ...baseOptions(t, { percent: true }),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: buckets },
    series: keys.map((key, i) =>
      barSeries(
        key,
        colours[i]!,
        rows.map((r: any, j: number) =>
          totals[j] ? Number(((100 * (r.counts[key] ?? 0)) / totals[j]).toFixed(2)) : 0,
        ),
        { stack: 'ver', itemStyle: { color: colours[i], borderColor: t.surface, borderWidth: 1 } },
      ),
    ),
  }
})
</script>

<template>
  <div>
    <p class="section-title">Where Dozzle runs</p>
    <div class="grid cols-2">
      <ChartCard
        title="Browser"
        hint="Browser family of the most recent beacon per install."
        :option="browserOption"
        :loading="pending"
        :height="260"
      />
      <ChartCard
        title="Operating system"
        hint="OS family parsed from the reported user agent."
        :option="osOption"
        :loading="pending"
        :height="260"
      />
    </div>

    <div class="grid cols-2" style="margin-top: 16px">
      <ChartCard
        title="Authentication provider mix"
        hint="Share of active installs by configured auth provider. 'none' is the unauthenticated default."
        :option="authOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="Deployment size mix"
        hint="Active installs bucketed by the number of running containers they report. Shows whether Dozzle is spreading into bigger deployments."
        :option="sizeOption"
        :loading="pending"
        :height="300"
      />
    </div>

    <div class="grid" style="margin-top: 16px">
      <ChartCard
        title="Version adoption"
        hint="Share of active installs on each minor version, for the five that reached the
              largest share at any point in this range — patch releases are rolled up, since a
              year holds hundreds. How fast the old bands hand over to the new ones is your
              upgrade velocity."
        :option="versionOption"
        :loading="pending"
        :height="330"
      />
    </div>
  </div>
</template>
