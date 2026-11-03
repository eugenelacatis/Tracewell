# Build from the repo root:
#   docker build -f web/Dockerfile -t tracewell/web .
# Only source files are copied, so a host node_modules/ or .next/ never leaks into the image.
FROM node:24-alpine AS build
WORKDIR /app
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/next.config.ts web/tsconfig.json web/eslint.config.mjs ./
COPY web/app ./app
COPY web/components ./components
COPY web/lib ./lib
RUN npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production PORT=3000 HOSTNAME=0.0.0.0
# WEB_QUERY_URL and WEB_API_KEY are read at request time, so set them on the container, not here.
COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static
USER node
EXPOSE 3000
CMD ["node", "server.js"]
