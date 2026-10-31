FROM node:26.13.0-alpine3.24 AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json tsconfig.build.json tsconfig.mock.json ./
RUN npm ci
COPY src ./src
COPY mock-api ./mock-api
RUN npm run build

FROM node:26.13.0-alpine3.24 AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist
COPY migrations ./migrations
# A new named volume takes this ownership, which lets the mock API write as `node`.
RUN mkdir -p /var/lib/mock-api && chown node:node /var/lib/mock-api
USER node
CMD ["node", "dist/entrypoints/api.js"]
