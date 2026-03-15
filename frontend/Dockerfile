FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 5173

CMD ["sh", "-c", "cp \"${FRONTEND_ENV_FILE:-.env}\" .env && npm run dev -- --host 0.0.0.0 --port 5173"]
