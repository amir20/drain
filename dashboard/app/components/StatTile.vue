<script setup lang="ts">
const props = defineProps<{
  label: string
  value: string
  previous?: number | null
  current?: number | null
  hint?: string
  /** For metrics where down is the good direction (churn). */
  invert?: boolean
}>()

const delta = computed(() => {
  if (props.previous === null || props.previous === undefined) return null
  if (props.current === null || props.current === undefined) return null
  if (props.previous === 0) return null
  return ((props.current - props.previous) / props.previous) * 100
})

const tone = computed(() => {
  if (delta.value === null) return 'flat'
  const up = delta.value > 0
  if (Math.abs(delta.value) < 0.5) return 'flat'
  return (props.invert ? !up : up) ? 'up' : 'down'
})
</script>

<template>
  <div class="tile">
    <p class="label">{{ label }}</p>
    <p class="value">{{ value }}</p>
    <p class="foot">
      <!-- Direction is spelled out with an arrow glyph and a sign, never colour alone. -->
      <span v-if="delta !== null" class="delta" :class="tone">
        {{ delta > 0 ? '▲' : delta < 0 ? '▼' : '▬' }}
        {{ Math.abs(delta).toFixed(1) }}%
      </span>
      <span v-if="hint" class="muted">{{ hint }}</span>
    </p>
  </div>
</template>

<style scoped>
.tile {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px 16px 12px;
}
.label {
  margin: 0 0 6px;
  font-size: 11.5px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--text-muted);
}
.value { margin: 0; font-size: 27px; font-weight: 600; letter-spacing: -0.02em; line-height: 1.1; }
.foot { margin: 6px 0 0; display: flex; gap: 8px; align-items: baseline; font-size: 12px; }
.delta { font-weight: 600; font-variant-numeric: tabular-nums; }
.delta.up { color: var(--delta-up); }
.delta.down { color: var(--critical); }
.delta.flat { color: var(--text-muted); }
</style>
