<script setup lang="ts">
const { loggedIn, user, clear } = useUserSession()
// The chosen range lives only in the query string, so a bare path would reset every
// page switch to the 30-day default.
const route = useRoute()

async function signOut() {
  await $fetch('/auth/logout', { method: 'POST' })
  await clear()
  await navigateTo('/login')
}
</script>

<template>
  <div>
    <header class="topbar">
      <p class="brand">Dozzle <span>analytics</span></p>
      <nav class="nav">
        <NuxtLink :to="{ path: '/', query: route.query }">Overview</NuxtLink>
        <NuxtLink :to="{ path: '/retention', query: route.query }">Retention</NuxtLink>
        <NuxtLink :to="{ path: '/engagement', query: route.query }">Engagement</NuxtLink>
        <NuxtLink :to="{ path: '/features', query: route.query }">Features</NuxtLink>
        <NuxtLink :to="{ path: '/environment', query: route.query }">Environment</NuxtLink>
      </nav>
      <span class="spacer" />
      <RangePicker />
      <button v-if="loggedIn" class="who" @click="signOut">
        <img v-if="user?.avatar" :src="user.avatar" alt="" width="22" height="22">
        <span>{{ user?.login }}</span>
      </button>
    </header>
    <main class="shell">
      <slot />
    </main>
  </div>
</template>

<style scoped>
.who {
  display: flex;
  align-items: center;
  gap: 7px;
  font: inherit;
  font-size: 13px;
  padding: 4px 10px 4px 4px;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: var(--surface);
  color: var(--text-secondary);
  cursor: pointer;
}
.who:hover { color: var(--text-primary); }
.who img { border-radius: 50%; }
</style>
