<script setup lang="ts">
const { active, setPreset, setCustom } = useRange()
const route = useRoute()

const open = ref(false)
const from = ref(String(route.query.from ?? ''))
const to = ref(String(route.query.to ?? ''))
const today = new Date().toISOString().slice(0, 10)

function apply() {
  if (!from.value || !to.value) return
  if (from.value > to.value) return
  setCustom(from.value, to.value)
  open.value = false
}
</script>

<template>
  <div class="range">
    <div class="presets" role="group" aria-label="Time range">
      <button
        v-for="p in PRESETS"
        :key="p.key"
        class="preset"
        :class="{ on: active === p.key }"
        :aria-pressed="active === p.key"
        @click="setPreset(p.key)"
      >
        {{ p.label }}
      </button>
      <button class="preset" :class="{ on: active === 'custom' }" @click="open = !open">
        Custom{{ active === 'custom' ? ` · ${from} → ${to}` : '' }}
      </button>
    </div>

    <form v-if="open" class="custom" @submit.prevent="apply">
      <label>From <input v-model="from" type="date" :max="today" required></label>
      <label>To <input v-model="to" type="date" :max="today" required></label>
      <button class="btn" type="submit">Apply</button>
    </form>
  </div>
</template>

<style scoped>
.range { position: relative; }
/* Same treatment as the nav: scroll rather than wrap, so a narrow screen keeps the
   presets on one line instead of stacking them or clipping the last one. */
.presets { display: flex; gap: 2px; overflow-x: auto; scrollbar-width: none; }
.presets::-webkit-scrollbar { display: none; }
.preset {
  font: inherit;
  font-size: 13px;
  padding: 5px 11px;
  border: 1px solid transparent;
  border-radius: 7px;
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
  white-space: nowrap;
}
.preset:hover { background: var(--hover); color: var(--text-primary); }
.preset.on {
  background: var(--surface);
  border-color: var(--border);
  color: var(--text-primary);
  font-weight: 600;
}
.custom {
  position: absolute;
  right: 0;
  max-width: calc(100vw - 32px);
  flex-wrap: wrap;
  top: calc(100% + 8px);
  display: flex;
  gap: 10px;
  align-items: end;
  padding: 12px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  box-shadow: 0 8px 28px rgba(0, 0, 0, 0.14);
  z-index: 30;
}
.custom label { display: grid; gap: 4px; font-size: 11.5px; color: var(--text-muted); }
.custom input {
  font: inherit;
  padding: 5px 8px;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--page);
  color: var(--text-primary);
  color-scheme: light dark;
}
</style>
