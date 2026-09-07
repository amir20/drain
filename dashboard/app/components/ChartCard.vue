<script setup lang="ts">
import VChart from 'vue-echarts'

defineProps<{
  title: string
  hint?: string
  height?: number
  option: Record<string, unknown> | null
  loading?: boolean
  empty?: boolean
}>()
</script>

<template>
  <section class="card">
    <header>
      <h2>{{ title }}</h2>
      <p v-if="hint" class="hint">{{ hint }}</p>
    </header>
    <div class="plot" :style="{ height: `${height ?? 280}px` }">
      <p v-if="loading" class="state muted">Loading…</p>
      <p v-else-if="empty || !option" class="state muted">No data in this range.</p>
      <ClientOnly v-else>
        <VChart :option="option" autoresize />
      </ClientOnly>
    </div>
    <slot />
  </section>
</template>

<style scoped>
.plot { position: relative; margin-bottom: 12px; }
.plot > :deep(.echarts) { width: 100%; height: 100%; }
.state {
  display: grid;
  place-items: center;
  height: 100%;
  margin: 0;
  font-size: 13px;
}
</style>
