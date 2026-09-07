# syntax=docker/dockerfile:1

FROM oven/bun:1-alpine AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM oven/bun:1-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production NITRO_PORT=3000 NITRO_HOST=0.0.0.0
COPY --from=build /app/.output ./.output
EXPOSE 3000
USER bun
CMD ["bun", ".output/server/index.mjs"]
