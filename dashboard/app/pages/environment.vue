<script setup lang="ts">
import { baseOptions, barSeries, fmtInt } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Environment · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/environment')
const { theme, version } = useChartTheme()

const SIZES = ['None', '1–5', '6–20', '21–50', '51–200', 'Over 200', 'Unknown']

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
  return {
    ...baseOptions(t, { legend: false }),
    grid: { left: 8, right: 72, top: 8, bottom: 4, containLabel: true },
    xAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: t.muted,
        fontSize: 11,
        formatter: (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v)),
      },
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
        `<b>${p[0].name}</b><br>${fmtInt(p[0].value)} installs (${((100 * p[0].value) / total).toFixed(1)}%)`,
    },
    series: [
      {
        type: 'bar',
        data: sorted.map((r) => r.installs),
        itemStyle: { color: t.series[0], borderRadius: [0, 4, 4, 0] },
        label: {
          show: true,
          position: 'right',
          color: t.secondary,
          fontSize: 11.5,
          formatter: (p: any) => `${((100 * p.value) / total).toFixed(1)}%`,
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
) {
  void version.value
  const t = theme.value
  if (!rows?.length) return null
  const buckets = [...new Set(rows.map((r) => r.bucket))].sort()
  const idx = new Map(buckets.map((b, i) => [b, i]))
  const totals = buckets.map(() => 0)
  for (const r of rows) totals[idx.get(r.bucket)!] += r.installs

  const keys = order ?? [...new Set(rows.map((r) => labelOf(r.key)))].sort()
  const series = keys.slice(0, 8).map((key, i) => {
    const values = buckets.map(() => 0)
    for (const r of rows) if (labelOf(r.key) === key) values[idx.get(r.bucket)!] += r.installs
    return barSeries(
      key,
      t.series[i]!,
      values.map((v, j) => (totals[j] ? Number(((100 * v) / totals[j]!).toFixed(2)) : 0)),
      { stack: 'share', itemStyle: { color: t.series[i], borderColor: t.surface, borderWidth: 1 } },
    )
  })

  return {
    ...baseOptions(t, { percent: true }),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: buckets },
    series,
  }
}

const authOption = computed(() => shareOption(data.value?.auth, (k) => String(k)))
const sizeOption = computed(() =>
  shareOption(data.value?.sizes, (k) => SIZES[Number(k)] ?? 'Unknown', SIZES),
)

/** Version adoption. Everything outside the top seven minors is already folded to Other. */
const versionOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.versions ?? []
  if (!rows.length) return null
  const keys = [...(data.value?.versionMix ?? []).map((v: any) => v.version), 'Other']
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
        t.series[i]!,
        rows.map((r: any, j: number) =>
          totals[j] ? Number(((100 * (r.counts[key] ?? 0)) / totals[j]).toFixed(2)) : 0,
        ),
        { stack: 'ver', itemStyle: { color: t.series[i], borderColor: t.surface, borderWidth: 1 } },
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
        hint="Share of active installs on each of the seven most common minor versions — patch releases are rolled up, since a year holds hundreds. How fast the old bands shrink is your upgrade velocity."
        :option="versionOption"
        :loading="pending"
        :height="330"
      />
    </div>
  </div>
</template>
