<script setup lang="ts">
import { baseOptions, barSeries, fmtInt, fmtPct, lineSeries } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Overview · Dozzle analytics' })

const { data, pending } = useRangedFetch<any>('/api/overview')
const { theme, version } = useChartTheme()

const k = computed(() => data.value?.kpis)

/** DAU / WAU / MAU on one axis. Three trailing windows of the same measure. */
const activeOption = computed(() => {
  void version.value
  const t = theme.value
  const hourly = data.value?.hourly ?? []
  if (data.value?.range?.bucket === 'hour' && hourly.length) {
    return {
      ...baseOptions(t, { legend: false }),
      series: [
        lineSeries(
          'Active installs',
          t.series[0]!,
          hourly.map((r: any) => [r.hour, r.active_installs]),
          { areaStyle: { color: t.series[0], opacity: 0.12 } },
        ),
      ],
    }
  }
  const rows = data.value?.series ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t),
    series: [
      lineSeries('Daily active', t.series[0]!, rows.map((r: any) => [r.day, r.dau])),
      lineSeries('Weekly active', t.series[1]!, rows.map((r: any) => [r.day, r.wau])),
      lineSeries('Monthly active', t.series[2]!, rows.map((r: any) => [r.day, r.mau])),
    ],
  }
})

/** WAU/MAU. Its own chart rather than a second axis on the one above. */
const stickinessOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = (data.value?.series ?? []).filter((r: any) => r.mau > 0)
  if (!rows.length) return null
  return {
    ...baseOptions(t, { legend: false, percent: true }),
    // No area fill: the ratio sits around 75-80% and never approaches zero, so filling to
    // the baseline paints a block of colour that encodes nothing.
    series: [
      lineSeries(
        'WAU / MAU',
        t.series[0]!,
        rows.map((r: any) => [r.day, Number(((100 * r.wau) / r.mau).toFixed(1))]),
      ),
    ],
    yAxis: { ...baseOptions(t, { percent: true }).yAxis, max: undefined },
  }
})

/** New / retained / resurrected above the axis, churned below. Net is real growth. */
const lifecycleOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.lifecycle ?? []
  if (!rows.length) return null
  const x = rows.map((r: any) => r.week)
  const stack = { stack: 'lifecycle', barCategoryGap: '28%' }
  return {
    ...baseOptions(t),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: x },
    series: [
      barSeries('New', t.series[0]!, rows.map((r: any) => r.new_installs), {
        ...stack,
        itemStyle: { color: t.series[0], borderColor: t.surface, borderWidth: 1 },
      }),
      barSeries('Retained', t.series[2]!, rows.map((r: any) => r.retained), {
        ...stack,
        itemStyle: { color: t.series[2], borderColor: t.surface, borderWidth: 1 },
      }),
      barSeries('Resurrected', t.series[3]!, rows.map((r: any) => r.resurrected), {
        ...stack,
        itemStyle: { color: t.series[3], borderColor: t.surface, borderWidth: 1 },
      }),
      barSeries('Churned', t.series[7]!, rows.map((r: any) => -r.churned), {
        ...stack,
        itemStyle: { color: t.series[7], borderColor: t.surface, borderWidth: 1 },
      }),
    ],
  }
})

/** The launch-only funnel: three in four first launches never become an active install. */
const funnelOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.funnel ?? []
  if (!rows.length) return null
  return {
    ...baseOptions(t),
    xAxis: { ...baseOptions(t).xAxis, type: 'category', data: rows.map((r: any) => r.week) },
    series: [
      barSeries('Became active', t.series[0]!, rows.map((r: any) => r.became_active), {
        stack: 'launch',
        itemStyle: { color: t.series[0], borderColor: t.surface, borderWidth: 1 },
      }),
      barSeries('Launch only', t.series[1]!, rows.map((r: any) => r.launch_only), {
        stack: 'launch',
        itemStyle: { color: t.series[1], borderColor: t.surface, borderWidth: 1 },
      }),
    ],
  }
})
</script>

<template>
  <div>
    <p class="section-title">The active base</p>
    <div class="grid cols-4">
      <StatTile
        label="Daily active"
        :value="fmtInt(k?.dau.value)"
        :current="k?.dau.value"
        :previous="k?.dau.previous"
        :hint="`vs ${fmtInt(k?.dau.previous)} prior period`"
      />
      <StatTile
        label="Weekly active"
        :value="fmtInt(k?.wau.value)"
        :current="k?.wau.value"
        :previous="k?.wau.previous"
        :hint="`vs ${fmtInt(k?.wau.previous)} prior period`"
      />
      <StatTile
        label="Monthly active"
        :value="fmtInt(k?.mau.value)"
        :current="k?.mau.value"
        :previous="k?.mau.previous"
        :hint="`vs ${fmtInt(k?.mau.previous)} prior period`"
      />
      <StatTile
        label="Stickiness"
        :value="fmtPct(k?.stickiness.value)"
        :current="k?.stickiness.value"
        :previous="k?.stickiness.previous"
        hint="WAU / MAU"
      />
    </div>

    <p class="section-title">Movement in this range</p>
    <div class="grid cols-4">
      <StatTile label="New installs" :value="fmtInt(k?.newInstalls)" hint="first events beacon" />
      <StatTile label="Resurrected" :value="fmtInt(k?.resurrected)" hint="back after 28+ days silent" />
      <StatTile label="Churned" :value="fmtInt(k?.churned)" hint="28 days silent" />
      <StatTile
        label="Net growth"
        :value="fmtInt(k?.netGrowth)"
        hint="new + resurrected − churned"
      />
    </div>

    <div class="grid cols-2" style="margin-top: 16px">
      <ChartCard
        title="Active installs"
        :hint="
          data?.range?.bucket === 'hour'
            ? 'Installs sending an events beacon each hour.'
            : 'Distinct installs seen in a trailing 1-, 7- and 28-day window, evaluated for every day. The gap between the lines is how episodic usage is.'
        "
        :option="activeOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="Stickiness (WAU / MAU)"
        hint="Share of the monthly base that shows up in a given week. Flat is healthy; a slide means the base is drifting into occasional use."
        :option="stickinessOption"
        :loading="pending"
        :height="300"
      />
    </div>

    <WidenedNote :widened="data?.weekly?.widened" />

    <div class="grid cols-2" style="margin-top: 16px">
      <ChartCard
        title="Install lifecycle"
        hint="New = first ever seen. Retained = also active last week. Resurrected = active before, but not last week. Churned (below the axis) = active last week, silent this week."
        :option="lifecycleOption"
        :loading="pending"
        :height="300"
      />
      <ChartCard
        title="First launches: did they stick?"
        hint="Installs by the week of their first start beacon, split by whether they ever went on to send a periodic events beacon. Launch-only installs start and die before the first events beacon fires."
        :option="funnelOption"
        :loading="pending"
        :height="300"
      >
        <p class="hint">
          Across all time, <strong>{{ fmtPct(k?.activationRate) }}</strong> of first
          launches became an active install
          ({{ fmtInt(k?.everActive) }} of
          {{ fmtInt((k?.everActive ?? 0) + (k?.launchOnly ?? 0)) }}).
        </p>
      </ChartCard>
    </div>
  </div>
</template>
