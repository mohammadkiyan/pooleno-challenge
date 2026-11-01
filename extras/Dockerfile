# The production build of the viewer, so that ./review.sh can serve it with only Docker on the host.
FROM node:26.13.0-alpine3.24
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build
EXPOSE 4173
CMD ["node_modules/.bin/vite", "preview", "--host", "0.0.0.0", "--port", "4173", "--strictPort"]
