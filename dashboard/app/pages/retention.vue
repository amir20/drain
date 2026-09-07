<script setup lang="ts">
import { baseOptions, barSeries, fmtInt, fmtPct, lineSeries, ordinalRamp } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Retention · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/retention')
const { theme, version } = useChartTheme()

const TENURE = [
  'Under a week',
  '1 to 4 weeks',
  '1 to 3 months',
  '3 to 12 months',
  'Over a year',
]

/** The cohort table collapsed into one line: the shape to move. */
const curveOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.curve ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t, { legend: false, percent: true }),
    xAxis: {
      ...baseOptions(t).xAxis,
      type: 'category',
      data: rows.map((r: any) => `W${r.week_index}`),
    },
    yAxis: { ...baseOptions(t, { percent: true }).yAxis, max: undefined },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) => {
        const r = rows[p[0].dataIndex]
        return `<b>Week ${r.week_index}</b><br>${fmtPct(r.pct)} still active<br><span style="opacity:.7">across ${r.cohorts} cohorts</span>`
      },
    },
    series: [
      barSeries('Installs still active', t.series[0]!, rows.map((r: any) => r.pct), {
        label: {
          show: true,
          position: 'top',
          color: t.secondary,
          fontSize: 11,
          formatter: (p: any) => `${p.value}%`,
        },
      }),
    ],
  }
})

/** Three columns of the cohort table over time. Diverging lines mean a release moved it. */
const trendOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.trend ?? []
  if (!rows.length) return null
  const byIndex = (i: number) =>
    rows.filter((r: any) => r.index === i).map((r: any) => [r.week, r.pct])
  return {
    ...baseOptions(t, { percent: true }),
    yAxis: { ...baseOptions(t, { percent: true }).yAxis, max: undefined },
    series: [
      lineSeries('Week 1', t.series[0]!, byIndex(1)),
      lineSeries('Week 4', t.series[1]!, byIndex(4)),
      lineSeries('Week 12', t.series[2]!, byIndex(12)),
    ],
  }
})

/** Active base by how long ago each install first reported. */
const tenureOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.tenure ?? []
  if (!rows.length) return null
  const weeks = [...new Set(rows.map((r: any) => r.week))] as string[]
  const idx = new Map(weeks.map((w, i) => [w, i]))
  // Tenure is an ordered scale, so it takes the one-hue ordinal ramp: older installs sit
  // darker, and the reader sees the order in the colour rather than having to decode a
  // legend of unrelated hues.
  const ramp = ordinalRamp(t, TENURE.length)
  const series = TENURE.map((label, band) => {
    const values: (number | null)[] = weeks.map(() => 0)
    for (const r of rows) if (r.band === band) values[idx.get(r.week)!] = r.installs
    return barSeries(label, ramp[band]!, values, {
      stack: 'tenure',
      // 2px surface gap keeps adjacent stacked segments separable.
      itemStyle: { color: ramp[band], borderColor: t.surface, borderWidth: 1 },
    })
  })
  return {
    ...baseOptions(t),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: weeks },
    series,
  }
})

const STEPS = ['--seq-100', '--seq-200', '--seq-300', '--seq-400', '--seq-500', '--seq-600', '--seq-700']

function heat(pct: number | null) {
  if (pct === null) return { background: 'transparent', color: 'var(--text-muted)' }
  // Sequential single hue, deepening with magnitude.
  const i = Math.min(STEPS.length - 1, Math.floor((pct / 100) ** 0.6 * STEPS.length))
  // The ramp's polarity flips between modes: --seq-700 is the darkest blue on the light
  // surface and the lightest on the dark one. A fixed "white above step 4" would put
  // white text on the palest cells in dark mode, so the high end takes the ink that
  // contrasts with the ramp's high end in the mode actually being rendered.
  const deep = i >= 4
  const onDeep = theme.value.dark ? 'var(--text-primary)' : '#fff'
  return {
    background: `var(${STEPS[i]})`,
    color: deep ? onDeep : 'var(--text-primary)',
  }
}
</script>

<template>
  <div>
    <p class="section-title">Do installs stick around?</p>
    <WidenedNote :widened="data?.weekly?.widened" />
    <div class="grid cols-2">
      <ChartCard
        title="Average retention curve"
        hint="The share of a cohort still active N weeks after install, averaged over every cohort in range that has actually lived that long."
        :option="curveOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="Week 1 / 4 / 12 retention by cohort"
        hint="The same numbers plotted over time. Diverging lines mean a release changed how well new installs stick."
        :option="trendOption"
        :loading="pending"
        :height="300"
      />
    </div>

    <div class="grid" style="margin-top: 16px">
      <ChartCard
        title="Active installs by tenure"
        hint="A base that is mostly under a month old is churning through users; a thickening 1-year band is real retention."
        :option="tenureOption"
        :loading="pending"
        :height="320"
      />
    </div>

    <section class="card" style="margin-top: 16px">
      <header>
        <h2>Weekly cohort retention</h2>
        <p class="hint">
          Each row is the set of installs whose first events beacon landed that week.
          W<em>N</em> is the share still reporting N weeks later. Read down a column to see
          whether newer cohorts retain better than older ones.
        </p>
      </header>
      <div class="scroll-x">
        <table class="data">
          <thead>
            <tr>
              <th>Cohort (week of)</th>
              <th>Installs</th>
              <th v-for="i in (data?.maxWeekIndex ?? 12)" :key="i">W{{ i }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in data?.cohorts ?? []" :key="row.cohort">
              <td>{{ row.cohort }}</td>
              <td>{{ fmtInt(row.size) }}</td>
              <td
                v-for="i in (data?.maxWeekIndex ?? 12)"
                :key="i"
                class="cell"
                :style="heat(row.weeks[i])"
              >
                {{ row.weeks[i] === null ? '' : fmtPct(row.weeks[i], 1) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-if="!pending && !(data?.cohorts ?? []).length" class="hint muted">
        No cohorts activated in this range.
      </p>
    </section>
  </div>
</template>

<style scoped>
.cell { font-variant-numeric: tabular-nums; border-bottom: 2px solid var(--surface); }
table.data td.cell { padding: 5px 8px; }
</style>
