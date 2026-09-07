<script setup lang="ts">
const { loggedIn, user, clear } = useUserSession()

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
        <NuxtLink to="/">Overview</NuxtLink>
        <NuxtLink to="/retention">Retention</NuxtLink>
        <NuxtLink to="/engagement">Engagement</NuxtLink>
        <NuxtLink to="/features">Features</NuxtLink>
        <NuxtLink to="/environment">Environment</NuxtLink>
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
