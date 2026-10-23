FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY core.mjs server.mjs api.mjs ./
USER node
ENV PORT=3000
EXPOSE 3000
CMD ["node", "server.mjs"]
