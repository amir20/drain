<script setup lang="ts">
import { baseOptions, barSeries, fmtInt, fmtPct } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Features · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/features')
const { theme, version } = useChartTheme()

const SIZES = ['None', '1–5', '6–20', '21–200', 'Over 200', 'Unknown']

const pct = (n: number, total: number) => (total ? Number(((100 * n) / total).toFixed(2)) : 0)

/** Adoption today. One measure per category, so one hue and a direct label per bar. */
const adoptionOption = computed(() => {
  void version.value
  const t = theme.value
  const cur = data.value?.current
  const feats = data.value?.features ?? []
  if (!cur?.installs) return null
  const rows = feats
    .map((f: any) => ({ label: f.label, value: pct(cur[f.key] ?? 0, cur.installs) }))
    .sort((a: any, b: any) => a.value - b.value)
  return {
    ...baseOptions(t, { legend: false }),
    grid: { left: 8, right: 56, top: 8, bottom: 4, containLabel: true },
    xAxis: {
      type: 'value',
      max: 100,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 11, formatter: (v: number) => `${v}%` },
      splitLine: { lineStyle: { color: t.grid } },
    },
    yAxis: {
      type: 'category',
      data: rows.map((r: any) => r.label),
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.secondary, fontSize: 12 },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) =>
        `<b>${p[0].name}</b><br>${p[0].value}% of ${fmtInt(cur.installs)} active installs`,
    },
    series: [
      {
        type: 'bar',
        data: rows.map((r: any) => r.value),
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
})

/** Penetration over time. Seven series is the cap; identity is by fixed slot. */
const overTimeOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.overTime ?? []
  const feats = data.value?.features ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t, { percent: true }),
    yAxis: { ...baseOptions(t, { percent: true }).yAxis, max: undefined },
    series: feats.map((f: any, i: number) => ({
      name: f.label,
      type: 'line',
      data: rows.map((r: any) => [r.bucket, pct(r[f.key], r.installs)]),
      showSymbol: false,
      symbolSize: 8,
      lineStyle: { width: 2, color: t.series[i] },
      itemStyle: { color: t.series[i] },
      emphasis: { focus: 'series' },
    })),
  }
})

const histogramOption = computed(() => {
  void version.value
  const t = theme.value
  const raw = data.value?.histogram ?? []
  if (!raw.length) return null
  // The query only returns counts that occur. Fill the gaps so a missing value reads as a
  // zero-height bar rather than silently collapsing the axis (0,1,2,3,5 with no 4).
  const max = data.value?.maxFeatureCount ?? 6
  const byCount = new Map(raw.map((r: any) => [r.features, r.installs]))
  const rows = Array.from({ length: max + 1 }, (_, i) => ({
    features: i,
    installs: byCount.get(i) ?? 0,
  }))
  const total = rows.reduce((s: number, r: any) => s + r.installs, 0)
  return {
    ...baseOptions(t, { legend: false }),
    xAxis: {
      type: 'category',
      data: rows.map((r: any) => String(r.features)),
      name: 'features enabled',
      nameLocation: 'middle',
      nameGap: 28,
      nameTextStyle: { color: t.muted, fontSize: 11 },
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 11 },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) =>
        `<b>${p[0].name} enabled</b><br>${fmtInt(p[0].value)} installs (${pct(p[0].value, total)}%)`,
    },
    series: [barSeries('Installs', t.series[0]!, rows.map((r: any) => r.installs))],
  }
})

const sizeRows = computed(() => {
  const rows = data.value?.bySize ?? []
  const feats = data.value?.features ?? []
  return rows.map((r: any) => ({
    label: SIZES[r.size_bucket] ?? 'Unknown',
    installs: r.installs,
    cells: feats.map((f: any) => pct(r[f.key], r.installs)),
  }))
})
</script>

<template>
  <div>
    <p class="section-title">Which features do people actually turn on?</p>
    <div class="grid cols-2">
      <ChartCard
        title="Feature penetration today"
        hint="Share of installs active in this range whose most recent beacon has each feature on. Read it as: of everyone using Dozzle right now, how many have found this."
        :option="adoptionOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="Features per install"
        hint="How many of the six configurable features each install has on. A tall bar at 0–1 means most people never move past the defaults."
        :option="histogramOption"
        :loading="pending"
        :height="300"
      />
    </div>

    <div class="grid" style="margin-top: 16px">
      <ChartCard
        title="Feature penetration over time"
        hint="Each feature as a share of the active base. A flat line under a release means the feature is not reaching existing installs."
        :option="overTimeOption"
        :loading="pending"
        :height="330"
      />
    </div>

    <section class="card" style="margin-top: 16px">
      <header>
        <h2>Feature penetration by deployment size</h2>
        <p class="hint">
          The same features cut by how many containers the install runs. Features that only
          land in large deployments show up as a rising column.
        </p>
      </header>
      <div class="scroll-x">
        <table class="data">
          <thead>
            <tr>
              <th>Deployment size</th>
              <th>Installs</th>
              <th v-for="f in data?.features ?? []" :key="f.key">{{ f.short }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in sizeRows" :key="row.label">
              <td>{{ row.label }}</td>
              <td>{{ fmtInt(row.installs) }}</td>
              <td v-for="(c, i) in row.cells" :key="i">{{ fmtPct(c, 1) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p class="hint muted">
        hasShell, remoteAgents, remoteClients and filterLength are in the beacon payload
        but always report zero, and mode/subCommand are always empty — they are left off
        rather than drawn as flat zero lines. If those features shipped, the beacon is not
        reporting them.
      </p>
    </section>
  </div>
</template>
