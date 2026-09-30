<script setup lang="ts">
import type { LiveBeacon, LiveMessage, LiveSummary } from '~~/server/utils/live'
import { barSeries, baseOptions, fmtInt } from '~/composables/useChartOptions'

definePageMeta({ middleware: 'auth' })
useHead({ title: 'Live · Dozzle analytics' })

const FEED_MAX = 100

/**
 * What each beacon is, in words. The names are Dozzle's wire names, which say little on
 * their own - "events" is the beacon the UI's event stream sends, i.e. someone opened it.
 */
const KINDS = [
  {
    name: 'events',
    label: 'UI opened',
    about: 'Someone opened Dozzle in a browser. At most one every 5 minutes per install.',
  },
  { name: 'start', label: 'Server started', about: 'A Dozzle server or agent booted.' },
  {
    name: 'usage',
    label: 'Usage report',
    about:
      'Counters of what was used since the last report. One per install per day, sent 24 hours after the server starts, from v11.1.3.',
  },
] as const
const kindOf = (name: string) => KINDS.find((k) => k.name === name)

const { theme, version } = useChartTheme()

const summary = shallowRef<LiveSummary | null>(null)
const feed = shallowRef<LiveBeacon[]>([])
const status = ref<'connecting' | 'live' | 'reconnecting'>('connecting')
const paused = ref(false)
/** Beacons that arrived while paused, shown on resume. */
const held = shallowRef<LiveBeacon[]>([])
/** Empty means every kind. */
const only = ref<string | null>(null)
/** Rows that arrived after the page opened, so they can be picked out as they land. */
const fresh = new Set<string>()

let source: EventSource | undefined

function merge(into: LiveBeacon[], incoming: LiveBeacon[]): LiveBeacon[] {
  const have = new Set(into.map((b) => b.key))
  return [...incoming.filter((b) => !have.has(b.key)), ...into].slice(0, FEED_MAX)
}

onMounted(() => {
  // Same origin, so the session cookie rides along and require-auth.ts sees it.
  source = new EventSource('/api/live')
  // EventSource retries on its own - including after the server's 15-minute close - so
  // an error is only ever "reconnecting" from here.
  source.addEventListener('error', () => (status.value = 'reconnecting'))
  const on = (type: LiveMessage['type'], fn: (m: any) => void) =>
    source!.addEventListener(type, (e) => {
      status.value = 'live'
      fn(JSON.parse((e as MessageEvent).data))
    })

  on('snapshot', (m: Extract<LiveMessage, { type: 'snapshot' }>) => {
    summary.value = m.summary
    // A reconnect replays the recent buffer; merge keeps rows already on screen.
    feed.value = merge(feed.value, m.beacons)
  })
  on('summary', (m: Extract<LiveMessage, { type: 'summary' }>) => (summary.value = m.summary))
  on('beacons', (m: Extract<LiveMessage, { type: 'beacons' }>) => {
    for (const b of m.beacons) fresh.add(b.key)
    if (paused.value) held.value = merge(held.value, m.beacons)
    else feed.value = merge(feed.value, m.beacons)
    // Only rows still on screen need remembering.
    if (fresh.size > 4 * FEED_MAX) {
      const keep = new Set([...feed.value, ...held.value].map((b) => b.key))
      for (const k of fresh) if (!keep.has(k)) fresh.delete(k)
    }
  })
})

onBeforeUnmount(() => source?.close())

function togglePause() {
  paused.value = !paused.value
  if (!paused.value && held.value.length) {
    feed.value = merge(feed.value, held.value)
    held.value = []
  }
}

const loading = computed(() => !summary.value)
const dbDown = computed(() => summary.value && !summary.value.connected)
const shown = computed(() => (only.value ? feed.value.filter((b) => b.name === only.value) : feed.value))

/** Stacked per-minute bars. The newest minute is still filling, so it is drawn faded. */
const minutesOption = computed(() => {
  void version.value
  const t = theme.value
  const m = summary.value?.minutes
  const width = m ? Math.max(0, ...Object.values(m.counts).map((c) => c.length)) : 0
  if (!m || !width) return null
  const start = Date.parse(m.start)
  const minutes = Array.from({ length: width }, (_, i) => new Date(start + i * 60_000).toISOString())
  const other = Object.keys(m.counts).filter((n) => !kindOf(n))
  const base = baseOptions(t)
  const series = KINDS.filter((k) => m.counts[k.name]).map((k, i) => ({
    label: k.label,
    color: t.series[KINDS.indexOf(k)] ?? t.series[i]!,
    values: m.counts[k.name]!,
  }))
  if (other.length) {
    series.push({
      label: 'Other',
      color: t.ordinalNone,
      values: minutes.map((_, i) => other.reduce((n, name) => n + (m.counts[name]![i] ?? 0), 0)),
    })
  }
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
    series: series.map((s) =>
      barSeries(
        s.label,
        s.color,
        s.values.map((value, i) => ({
          value,
          itemStyle: i === width - 1 ? { opacity: 0.45 } : undefined,
        })) as any,
        {
          stack: 'beacons',
          barCategoryGap: '20%',
          itemStyle: { color: s.color, borderColor: t.surface, borderWidth: 1 },
        },
      ),
    ),
  }
})

/** One line saying what this particular beacon tells us. */
function details(b: LiveBeacon): string {
  const parts: string[] = []
  if (b.name === 'events') {
    if (b.browser && b.os) parts.push(`${b.browser} on ${b.os}`)
    if (b.containers !== null) parts.push(`${fmtInt(b.containers)} running containers`)
  } else if (b.name === 'start') {
    if (b.mode) parts.push(`${b.mode} mode`)
  } else if (b.name === 'usage') {
    if (b.activeMinutes) parts.push(`open ${b.activeMinutes} min that day`)
  }
  if (b.auth && b.auth !== 'none') parts.push(`${b.auth} auth`)
  return parts.join(' · ') || '—'
}

function ago(iso: string): string {
  const s = Math.max(0, Math.round((now.value - Date.parse(iso)) / 1000))
  if (s < 60) return `${s}s ago`
  return `${Math.floor(s / 60)}m ago`
}

// Drives the "Ns ago" column without re-rendering on every message of the stream.
const now = ref(Date.now())
let clock: ReturnType<typeof setInterval> | undefined
onMounted(() => (clock = setInterval(() => (now.value = Date.now()), 1000)))
onBeforeUnmount(() => clearInterval(clock))
</script>

<template>
  <div>
    <div class="head">
      <p class="section-title">Right now</p>
      <span class="status" :class="dbDown ? 'reconnecting' : status">
        <span class="dot" aria-hidden="true" />
        <template v-if="dbDown">Lost the database, retrying…</template>
        <template v-else>
          {{ status === 'live' ? 'Live' : status === 'connecting' ? 'Connecting…' : 'Reconnecting…' }}
        </template>
      </span>
    </div>

    <div class="grid cols-4">
      <StatTile
        label="Installs, last hour"
        :value="fmtInt(summary?.installsLastHour)"
        :hint="`${fmtInt(summary?.installsLast5m)} in the last 5 minutes`"
      />
      <StatTile
        v-for="k in KINDS"
        :key="k.name"
        :label="`${k.label}, last hour`"
        :value="fmtInt(summary ? (summary.lastHour[k.name] ?? 0) : undefined)"
        :hint="k.about"
      />
    </div>

    <div class="grid split" style="margin-top: 16px">
      <ChartCard
        title="Beacons per minute"
        hint="Every beacon drain has written in the last hour, as it arrives. The faded bar is the current minute, still filling."
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
        <p v-else-if="!summary?.versions.length" class="muted small">Nothing reported yet.</p>
        <table v-else class="data">
          <thead>
            <tr><th>Version</th><th>Installs</th></tr>
          </thead>
          <tbody>
            <tr v-for="v in summary?.versions" :key="v.version">
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
            Each row is one beacon, pushed the moment drain writes it. Installs are shown by
            the first eight characters of their id; the hashed client address never leaves
            the server.
          </p>
        </div>
        <button class="btn" @click="togglePause">
          {{ paused ? `Resume${held.length ? ` (${held.length} new)` : ''}` : 'Pause' }}
        </button>
      </header>
      <div class="filters" role="group" aria-label="Show">
        <button class="chip" :class="{ on: !only }" @click="only = null">All</button>
        <button
          v-for="k in KINDS"
          :key="k.name"
          class="chip"
          :class="[k.name, { on: only === k.name }]"
          :title="k.about"
          @click="only = only === k.name ? null : k.name"
        >
          {{ k.label }}
        </button>
      </div>
      <div class="scroll-x feed-scroll">
        <table class="data feed">
          <thead>
            <tr>
              <th>When</th>
              <th>What</th>
              <th>Install</th>
              <th>Version</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!shown.length">
              <td colspan="5" class="muted">
                {{ loading ? 'Loading…' : 'Waiting for the next beacon…' }}
              </td>
            </tr>
            <tr v-for="b in shown" :key="b.key" :class="{ fresh: fresh.has(b.key) }">
              <td :title="new Date(b.time).toLocaleString()">{{ ago(b.time) }}</td>
              <td>
                <span class="pill" :class="b.name" :title="kindOf(b.name)?.about">
                  {{ kindOf(b.name)?.label ?? b.name }}
                </span>
              </td>
              <td class="mono">{{ b.install || '—' }}</td>
              <td>{{ b.version ?? '—' }}</td>
              <td class="details">{{ details(b) }}</td>
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

.grid.split { grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); }
@media (max-width: 1080px) { .grid.split { grid-template-columns: minmax(0, 1fr); } }

.small { font-size: 13px; }
.feed-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.filters { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 12px; }
.chip {
  font: inherit;
  font-size: 12.5px;
  padding: 3px 10px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
}
.chip:hover { background: var(--hover); }
.chip.on { background: var(--hover); color: var(--text-primary); }
.chip.events.on { color: var(--series-1); }
.chip.start.on { color: var(--series-2); }
.chip.usage.on { color: var(--series-3); }

.feed td { font-size: 13px; }
/* Words, not figures: read left to right like any other text. */
.feed th, .feed td { text-align: left; }
.feed td.details { color: var(--text-secondary); white-space: nowrap; }
/* The header stays put while the rows scroll, so a full feed does not stretch the page. */
.feed-scroll { max-height: 560px; overflow-y: auto; margin-bottom: 12px; }
.feed thead th { position: sticky; top: 0; background: var(--surface); }
/* A row that just landed glows briefly, so new arrivals are easy to follow. */
.feed tr.fresh td { animation: arrive 2.4s ease-out; }
@keyframes arrive { from { background: var(--hover); } to { background: transparent; } }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.pill {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 500;
  white-space: nowrap;
  background: var(--hover);
}
.pill.events { color: var(--series-1); }
.pill.start { color: var(--series-2); }
.pill.usage { color: var(--series-3); }

@media (prefers-reduced-motion: reduce) {
  .status.live .dot { animation: none; }
  .feed tr.fresh td { animation: none; }
}
</style>
