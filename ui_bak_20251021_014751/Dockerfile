FROM node:20-alpine

WORKDIR /app

# deps install
COPY package*.json /app/
RUN npm ci

# app code
COPY . /app

# Vite dev server port (compose run karega)
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host"]
