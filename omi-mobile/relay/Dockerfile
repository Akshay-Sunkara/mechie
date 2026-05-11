FROM node:22-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY relay.js ./

ENV PORT=8765
EXPOSE 8765

CMD ["node", "relay.js"]
