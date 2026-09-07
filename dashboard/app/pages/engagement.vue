<script setup lang="ts">
import { baseOptions, barSeries, fmtInt, lineSeries, ordinalRamp } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Engagement · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/engagement')
const { theme, version } = useChartTheme()

const DEPTH = ['A single day', '2 to 3 days', '4 to 6 days', 'Every day']

const intensityOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.intensity ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t, { legend: false }),
    yAxis: {
      ...baseOptions(t).yAxis,
      max: 7,
      axisLabel: { color: t.muted, fontSize: 11, formatter: (v: number) => `${v}d` },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) =>
        `<b>${p[0].axisValueLabel}</b><br>${p[0].value[1]} days per install<br><span style="opacity:.7">${fmtInt(rows[p[0].dataIndex].installs)} installs</span>`,
    },
    series: [
      lineSeries(
        'Avg active days per install',
        t.series[0]!,
        rows.map((r: any) => [r.week, r.avg_days]),
        { areaStyle: { color: t.series[0], opacity: 0.12 } },
      ),
    ],
  }
})

const depthOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.depth ?? []
  if (!rows.length) return null
  const weeks = [...new Set(rows.map((r: any) => r.week))] as string[]
  const idx = new Map(weeks.map((w, i) => [w, i]))
  return {
    ...baseOptions(t),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: weeks },
    // How many days a week an install reports is an ordered scale, not four unrelated
    // categories, so it takes the one-hue ordinal ramp.
    series: DEPTH.map((label, band) => {
      const ramp = ordinalRamp(t, DEPTH.length)
      const values = weeks.map(() => 0)
      for (const r of rows) if (r.band === band) values[idx.get(r.week)!] = r.installs
      return barSeries(label, ramp[band]!, values, {
        stack: 'depth',
        itemStyle: { color: ramp[band], borderColor: t.surface, borderWidth: 1 },
      })
    }),
  }
})

const clientsOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.clients ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t),
    series: [
      lineSeries('Average', t.series[0]!, rows.map((r: any) => [r.week, r.avg])),
      lineSeries('95th percentile', t.series[1]!, rows.map((r: any) => [r.week, r.p95])),
    ],
  }
})
</script>

<template>
  <div>
    <p class="section-title">How hard is Dozzle used?</p>
    <WidenedNote :widened="data?.weekly?.widened" />
    <div class="grid cols-2">
      <ChartCard
        title="Usage intensity"
        hint="Average number of distinct days per week an install reports in. 7 means it runs continuously; near 1 means it is started for a specific task and closed."
        :option="intensityOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="Browser clients per install"
        hint="Concurrent browser sessions reported by each install, taken as its weekly peak. The p95 line shows the multi-user installs; the average shows the typical one."
        :option="clientsOption"
        :loading="pending"
        :height="300"
      />
    </div>

    <div class="grid" style="margin-top: 16px">
      <ChartCard
        title="Engagement depth"
        hint="The active base split by how many days that week each install reported. A growing 'Every day' band means Dozzle is being left running as infrastructure."
        :option="depthOption"
        :loading="pending"
        :height="330"
      />
    </div>
  </div>
</template>
