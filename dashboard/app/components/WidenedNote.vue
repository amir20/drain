<script setup lang="ts">
/** Mirrors MIN_WEEKS in server/utils/range.ts. */
const MIN_WEEKS = 8
defineProps<{ widened?: boolean }>()
</script>

<template>
  <p v-if="widened" class="note">
    These measures are per calendar week, and the selected range holds fewer than two
    complete weeks — showing the last {{ MIN_WEEKS }} complete weeks instead.
  </p>
</template>

<style scoped>
.note {
  margin: 0 0 14px;
  padding: 8px 11px;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text-secondary);
  font-size: 12.5px;
}
</style>
