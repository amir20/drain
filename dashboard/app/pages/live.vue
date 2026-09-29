<script setup lang="ts">
import type { LiveBeacon, LiveTick } from '~~/server/utils/live'
import { barSeries, baseOptions, fmtInt } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Live · Dozzle analytics' })

const FEED_MAX = 100
/** Beacon names in a fixed colour order; anything newer folds into "other". */
const NAMES = ['events', 'start', 'usage'] as const

const { theme, version } = useChartTheme()

const tick = shallowRef<LiveTick | null>(null)
const feed = shallowRef<LiveBeacon[]>([])
const status = ref<'connecting' | 'live' | 'reconnecting'>('connecting')
const paused = ref(false)
/** Beacons that arrived while paused, shown on resume. */
let held: LiveBeacon[] = []

let source: EventSource | undefined

onMounted(() => {
  // Same origin, so the session cookie rides along and require-auth.ts sees it.
  source = new EventSource('/api/live')
  source.addEventListener('open', () => (status.value = 'live'))
  // EventSource retries on its own - including after the server's 15-minute close - so
  // an error is only ever "reconnecting" from here.
  source.addEventListener('error', () => (status.value = 'reconnecting'))
  source.addEventListener('tick', (e) => {
    status.value = 'live'
    const t = JSON.parse((e as MessageEvent).data) as LiveTick
    tick.value = t
    if (!t.beacons.length) return
    if (paused.value) {
      held = [...t.beacons, ...held].slice(0, FEED_MAX)
      return
    }
    // A reconnect replays the server's recent buffer; skip rows already on screen.
    const have = new Set(feed.value.map((b) => b.key))
    feed.value = [...t.beacons.filter((b) => !have.has(b.key)), ...feed.value].slice(0, FEED_MAX)
  })
})

onBeforeUnmount(() => source?.close())

function togglePause() {
  paused.value = !paused.value
  if (!paused.value && held.length) {
    const have = new Set(feed.value.map((b) => b.key))
    feed.value = [...held.filter((b) => !have.has(b.key)), ...feed.value].slice(0, FEED_MAX)
    held = []
  }
}

const stats = computed(() => tick.value?.stats)
const loading = computed(() => !tick.value)

/** Stacked per-minute bars. The newest minute is still filling, so it is drawn faded. */
const minutesOption = computed(() => {
  void version.value
  const t = theme.value
  const rows = tick.value?.minutes ?? []
  if (!rows.length) return null
  const minutes = [...new Set(rows.map((r) => r.minute))].sort()
  const lastMinute = minutes[minutes.length - 1]
  const byName = new Map<string, Map<string, number>>()
  for (const r of rows) {
    const name = (NAMES as readonly string[]).includes(r.name) ? r.name : 'other'
    const m = byName.get(name) ?? new Map<string, number>()
    m.set(r.minute, (m.get(r.minute) ?? 0) + r.beacons)
    byName.set(name, m)
  }
  const names = [...NAMES, 'other'].filter((n) => byName.has(n))
  const base = baseOptions(t)
  return {
    ...base,
    xAxis: {
      ...base.xAxis,
      type: 'category',
      data: minutes,
      axisLabel: {
        ...base.xAxis.axisLabel,
        formatter: (v: string) =>
          new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      },
    },
    series: names.map((name, i) => {
      const color = name === 'other' ? t.ordinalNone : t.series[i]!
      return barSeries(
        name,
        color,
        minutes.map((m) => ({
          value: byName.get(name)!.get(m) ?? 0,
          itemStyle: m === lastMinute ? { opacity: 0.45 } : undefined,
        })) as any,
        {
          stack: 'beacons',
          barCategoryGap: '20%',
          itemStyle: { color, borderColor: t.surface, borderWidth: 1 },
        },
      )
    }),
  }
})

function ago(iso: string): string {
  const s = Math.max(0, Math.round((now.value - Date.parse(iso)) / 1000))
  if (s < 60) return `${s}s ago`
  return `${Math.floor(s / 60)}m ago`
}

// Drives the "Ns ago" column without re-rendering on every tick of the stream.
const now = ref(Date.now())
let clock: ReturnType<typeof setInterval> | undefined
onMounted(() => (clock = setInterval(() => (now.value = Date.now()), 1000)))
onBeforeUnmount(() => clearInterval(clock))
</script>

<template>
  <div>
    <div class="head">
      <p class="section-title">Right now</p>
      <span class="status" :class="status">
        <span class="dot" aria-hidden="true" />
        {{ status === 'live' ? 'Live' : status === 'connecting' ? 'Connecting…' : 'Reconnecting…' }}
        <span v-if="tick" class="muted">· updated {{ ago(tick.at) }}</span>
      </span>
    </div>

    <div class="grid cols-3">
      <StatTile
        label="Beacons, last minute"
        :value="fmtInt(stats?.beaconsLastMinute)"
        hint="every beacon type"
      />
      <StatTile
        label="Installs, last 5 min"
        :value="fmtInt(stats?.installsLast5m)"
        hint="distinct installs reporting"
      />
      <StatTile
        label="Installs, last hour"
        :value="fmtInt(stats?.installsLastHour)"
        hint="distinct installs reporting"
      />
    </div>

    <div class="grid split" style="margin-top: 16px">
      <ChartCard
        title="Beacons per minute"
        hint="The trailing hour, straight from the beacon table rather than the hourly aggregates. The faded bar is the current minute, still filling."
        :option="minutesOption"
        :loading="loading"
        :height="280"
      />
      <section class="card">
        <header>
          <h2>Versions in the last 15 minutes</h2>
          <p class="hint">Distinct installs reporting each version.</p>
        </header>
        <p v-if="loading" class="muted small">Loading…</p>
        <p v-else-if="!tick?.versions.length" class="muted small">Nothing reported yet.</p>
        <table v-else class="data">
          <thead>
            <tr><th>Version</th><th>Installs</th></tr>
          </thead>
          <tbody>
            <tr v-for="v in tick?.versions" :key="v.version">
              <td>{{ v.version }}</td>
              <td>{{ fmtInt(v.installs) }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>

    <section class="card" style="margin-top: 16px">
      <header class="feed-head">
        <div>
          <h2>Incoming beacons</h2>
          <p class="hint">
            The newest {{ FEED_MAX }}, as drain writes them. Installs are shown by the first
            eight characters of their id; the hashed client address is never sent to the page.
          </p>
        </div>
        <button class="btn" @click="togglePause">{{ paused ? 'Resume' : 'Pause' }}</button>
      </header>
      <div class="scroll-x feed-scroll">
        <table class="data feed">
          <thead>
            <tr>
              <th>When</th>
              <th>Beacon</th>
              <th>Install</th>
              <th>Version</th>
              <th>Mode</th>
              <th>Auth</th>
              <th>Browser</th>
              <th>OS</th>
              <th>Containers</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!feed.length">
              <td colspan="9" class="muted">
                {{ loading ? 'Loading…' : 'Waiting for the next beacon…' }}
              </td>
            </tr>
            <tr v-for="b in feed" :key="b.key">
              <td :title="new Date(b.time).toLocaleString()">{{ ago(b.time) }}</td>
              <td><span class="pill" :class="b.name">{{ b.name }}</span></td>
              <td class="mono">{{ b.install || '—' }}</td>
              <td>{{ b.version ?? '—' }}</td>
              <td>{{ b.mode ?? '—' }}</td>
              <td>{{ b.auth ?? '—' }}</td>
              <td>{{ b.browser ?? '—' }}</td>
              <td>{{ b.os ?? '—' }}</td>
              <td>{{ fmtInt(b.containers) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>

<style scoped>
.head { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; }
.status { display: inline-flex; align-items: center; gap: 6px; font-size: 12.5px; color: var(--text-secondary); }
.dot { width: 8px; height: 8px; border-radius: 50%; background: var(--text-muted); }
.status.live .dot { background: var(--good); animation: pulse 2s ease-in-out infinite; }
.status.reconnecting .dot { background: var(--critical); }
@keyframes pulse { 50% { opacity: 0.35; } }
@media (prefers-reduced-motion: reduce) { .status.live .dot { animation: none; } }

.grid.split { grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); }
@media (max-width: 1080px) { .grid.split { grid-template-columns: minmax(0, 1fr); } }

.small { font-size: 13px; }
.feed-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.feed td { font-size: 13px; }
/* The header stays put while the rows scroll, so a full feed does not stretch the page. */
.feed-scroll { max-height: 560px; overflow-y: auto; margin-bottom: 12px; }
.feed thead th { position: sticky; top: 0; background: var(--surface); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.pill {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 500;
  background: var(--hover);
}
.pill.events { color: var(--series-1); }
.pill.start { color: var(--series-2); }
.pill.usage { color: var(--series-3); }
</style>
