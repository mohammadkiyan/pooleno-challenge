# Build the exact backend used by the application, alongside the viewer assets.
FROM node:26.13.0-alpine3.24 AS backend
WORKDIR /app
COPY package.json package-lock.json tsconfig.json tsconfig.build.json tsconfig.mock.json ./
RUN npm ci
COPY src ./src
COPY mock-api ./mock-api
RUN npm run build

FROM node:26.13.0-alpine3.24 AS frontend
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY extras/package.json extras/package-lock.json ./extras/
RUN cd extras && npm ci
COPY extras ./extras
WORKDIR /app/extras
RUN npm run build

FROM node:26.13.0-alpine3.24
ENV NODE_ENV=production VIEWER_HOST=0.0.0.0
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=backend /app/dist ./dist
COPY migrations ./migrations
COPY --from=frontend /app/extras/dist ./extras/dist
COPY extras/server ./extras/server
COPY extras/shared ./extras/shared
USER node
EXPOSE 4173
CMD ["node", "extras/server/index.ts"]
