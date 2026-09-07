<script setup lang="ts">
definePageMeta({ layout: false })
useHead({ title: 'Sign in · Dozzle analytics' })

const route = useRoute()
const { loggedIn } = useUserSession()

const message = computed(() => {
  if (route.query.error === 'forbidden') return 'That GitHub account is not on the allowlist.'
  if (route.query.error === 'oauth') return 'GitHub sign-in failed. Try again.'
  return null
})

watchEffect(() => {
  if (loggedIn.value) navigateTo('/')
})
</script>

<template>
  <main class="wrap">
    <div class="box">
      <p class="brand">Dozzle <span>analytics</span></p>
      <p class="blurb">Product analytics over the Dozzle beacon. Access is by invitation.</p>
      <p v-if="message" class="error" role="alert">{{ message }}</p>
      <a class="gh" href="/auth/github">
        <svg viewBox="0 0 16 16" width="17" height="17" aria-hidden="true" fill="currentColor">
          <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z" />
        </svg>
        Continue with GitHub
      </a>
    </div>
  </main>
</template>

<style scoped>
.wrap { display: grid; place-items: center; min-height: 100dvh; padding: 24px; }
.box {
  width: min(380px, 100%);
  padding: 30px 28px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 14px;
  text-align: center;
}
.brand { margin: 0; font-size: 19px; font-weight: 650; letter-spacing: -0.01em; }
.brand span { color: var(--text-muted); font-weight: 450; }
.blurb { margin: 8px 0 22px; color: var(--text-secondary); font-size: 13px; }
.error {
  margin: 0 0 16px;
  padding: 8px 10px;
  border-radius: 8px;
  font-size: 12.5px;
  color: var(--critical);
  background: color-mix(in srgb, var(--critical) 10%, transparent);
}
.gh {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 9px;
  padding: 10px;
  border-radius: 9px;
  background: var(--text-primary);
  color: var(--surface);
  text-decoration: none;
  font-weight: 600;
  font-size: 14px;
}
.gh:hover { opacity: 0.88; }
</style>
