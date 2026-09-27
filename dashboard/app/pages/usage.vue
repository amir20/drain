<script setup lang="ts">
import { baseOptions, fmtInt, fmtPct, ordinalRamp } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Usage · Dozzle analytics' })

const { data: raw, pending } = useRangedFetch<any>('/api/usage')
const { theme, version } = useChartTheme()
// useRangedFetch cannot infer the response shape, so read it untyped in one place.
const data = computed<any>(() => raw.value)

const installs = computed<number>(() => data.value?.installs ?? 0)
const empty = computed(() => !pending.value && installs.value === 0)

/**
 * Horizontal bars in one hue. `value` is what the bar measures; the tooltip carries the
 * other number so a share never hides its count, or a count its share.
 */
function barsOption(
  rows: { label: string; value: number; note: string }[],
  opts: { percent?: boolean; colour?: string } = {},
) {
  void version.value
  const t = theme.value
  if (!rows.length) return null
  const sorted = [...rows].reverse()
  return {
    ...baseOptions(t, { legend: false }),
    grid: { left: 8, right: 72, top: 8, bottom: 4, containLabel: true },
    xAxis: {
      type: 'value',
      ...(opts.percent ? { max: 100 } : {}),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: t.muted,
        fontSize: 11,
        formatter: (v: number) => (opts.percent ? `${v}%` : fmtInt(v)),
      },
      splitLine: { lineStyle: { color: t.grid } },
    },
    yAxis: {
      type: 'category',
      data: sorted.map((r) => r.label),
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.secondary, fontSize: 12 },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) => `<b>${p[0].name}</b><br>${sorted[p[0].dataIndex]!.note}`,
    },
    series: [
      {
        type: 'bar',
        data: sorted.map((r) => r.value),
        itemStyle: { color: opts.colour ?? t.series[0], borderRadius: [0, 4, 4, 0] },
        label: {
          show: true,
          position: 'right',
          color: t.secondary,
          fontSize: 11.5,
          formatter: (p: any) => (opts.percent ? `${p.value}%` : fmtInt(p.value)),
        },
      },
    ],
  }
}

const pct = (n: number) => (installs.value ? Number(((100 * n) / installs.value).toFixed(1)) : 0)

/** Share of reporting installs that used each feature group at least once. */
const groupsOption = computed(() =>
  barsOption(
    (data.value?.groups ?? []).map((g: any) => ({
      label: g.label,
      value: pct(g.installs),
      note: `${fmtInt(g.installs)} installs used it<br>${fmtInt(g.total)} times in total`,
    })),
    { percent: true },
  ),
)

/** One group's counters, share of reporting installs per counter. */
const detail = ref<string>('views')
const detailGroup = computed(() => (data.value?.groups ?? []).find((g: any) => g.key === detail.value))
const detailOption = computed(() =>
  barsOption(
    (detailGroup.value?.counters ?? []).map((c: any) => ({
      label: c.key,
      value: pct(c.installs),
      note: `${fmtInt(c.installs)} installs<br>${fmtInt(c.total)} times in total`,
    })),
    { percent: true },
  ),
)

/** Funnels are counts of events, with the installs behind them in the tooltip. */
function funnelOption(rows: any[] | undefined, colour?: string) {
  if (!rows?.some((r) => r.total > 0)) return null
  return barsOption(
    rows.map((r) => ({
      label: r.label,
      value: r.total,
      note: `${fmtInt(r.total)} times<br>${fmtInt(r.installs)} installs`,
    })),
    { colour },
  )
}
const addHostOption = computed(() => funnelOption(data.value?.addHost))
const wizardOption = computed(() => funnelOption(data.value?.wizard))

const cloud = computed(() => {
  const rows = data.value?.cloud ?? []
  const welcome = rows.find((r: any) => r.key === 'cloud.welcome')
  const connect = rows.find((r: any) => r.key === 'cloud.connect')
  return { welcome, connect }
})

/** Per install per usage day, so a bigger base does not read as a flakier one. */
const perInstallDay = (key: string) => {
  const r = (data.value?.reliability ?? []).find((x: any) => x.key === key)
  const days = data.value?.usageDays ?? 0
  return days ? (r?.total ?? 0) / days : null
}

/** Active minutes per reported day. Ordered buckets, so the ordinal ramp. */
const activeOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = data.value?.active ?? []
  const total = rows.reduce((n: number, r: any) => n + r.days, 0)
  if (!total) return null
  const colours = ordinalRamp(t, rows.length)
  return {
    ...baseOptions(t, { legend: false }),
    grid: { left: 8, right: 16, top: 16, bottom: 4, containLabel: true },
    xAxis: {
      type: 'category',
      data: rows.map((r: any) => `${r.bucket} min`),
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.secondary, fontSize: 12 },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: t.muted, fontSize: 11, formatter: (v: number) => `${v}%` },
      splitLine: { lineStyle: { color: t.grid } },
    },
    tooltip: {
      ...baseOptions(t).tooltip,
      formatter: (p: any) =>
        `<b>${p[0].name}</b><br>${p[0].value}% of usage days<br><span style="opacity:.7">${fmtInt(rows[p[0].dataIndex].days)} days</span>`,
    },
    series: [
      {
        type: 'bar',
        data: rows.map((r: any, i: number) => ({
          value: Number(((100 * r.days) / total).toFixed(1)),
          itemStyle: { color: colours[i], borderRadius: [4, 4, 0, 0] },
        })),
      },
    ],
  }
})

const localeOption = computed(() =>
  barsOption(
    (data.value?.locales ?? []).slice(0, 12).map((l: any) => ({
      label: l.locale,
      value: pct(l.installs),
      note: `${fmtInt(l.installs)} installs<br>${fmtInt(l.sessions)} sessions`,
    })),
    { percent: true },
  ),
)
</script>

<template>
  <div>
    <p class="section-title">What people do in Dozzle</p>

    <p v-if="empty" class="hint muted">
      No usage beacons in this range yet. Dozzle sends one per install per day from the
      release that adds them.
    </p>

    <template v-else>
      <div class="grid cols-4">
        <StatTile label="Reporting installs" :value="fmtInt(installs)" hint="sent a usage beacon" />
        <StatTile
          label="Cloud connect rate"
          :value="
            cloud.welcome?.total ? fmtPct((100 * (cloud.connect?.total ?? 0)) / cloud.welcome.total) : '—'
          "
          :hint="`${fmtInt(cloud.connect?.total ?? 0)} of ${fmtInt(cloud.welcome?.total ?? 0)} welcomes`"
        />
        <StatTile
          label="Stream reconnects"
          :value="perInstallDay('stream.reconnect')?.toFixed(2) ?? '—'"
          hint="per install per day"
        />
        <StatTile
          label="Agent disconnects"
          :value="perInstallDay('agent.disconnect')?.toFixed(2) ?? '—'"
          hint="per install per day"
        />
      </div>

      <div class="grid cols-2" style="margin-top: 16px">
        <ChartCard
          title="Features used"
          hint="Share of reporting installs that used each group at least once in this range. Hover for how often."
          :option="groupsOption"
          :loading="pending"
          :height="300"
        />
        <ChartCard
          :title="`Inside ${detailGroup?.label ?? 'a group'}`"
          hint="Share of reporting installs that used each counter at least once."
          :option="detailOption"
          :loading="pending"
          :height="300"
        >
          <p class="hint">
            <label>
              Group
              <select v-model="detail">
                <option v-for="g in data?.groups ?? []" :key="g.key" :value="g.key">{{ g.label }}</option>
              </select>
            </label>
          </p>
        </ChartCard>
      </div>

      <div class="grid cols-2" style="margin-top: 16px">
        <ChartCard
          title="Adding a host"
          hint="Outcomes of Add host in the UI. Anything but Added is a user who wanted a second host and did not get one."
          :option="addHostOption"
          :loading="pending"
          :height="260"
        />
        <ChartCard
          title="Setup wizard"
          hint="How often the wizard was shown and finished, and which steps people skipped."
          :option="wizardOption"
          :loading="pending"
          :height="260"
        />
      </div>

      <div class="grid cols-2" style="margin-top: 16px">
        <ChartCard
          title="Time in Dozzle"
          hint="Minutes someone had Dozzle open, per install per reported day. Bucketed by Dozzle before it is sent."
          :option="activeOption"
          :loading="pending"
          :height="260"
        />
        <ChartCard
          title="UI language"
          hint="Share of reporting installs with at least one session in each language. Tells you which translations are read."
          :option="localeOption"
          :loading="pending"
          :height="260"
        />
      </div>
    </template>
  </div>
</template>
