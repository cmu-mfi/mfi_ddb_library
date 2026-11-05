# Stage 1: build the React/Vite app
FROM node:22-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Stage 2: serve it with nginx and proxy /api to the backend
FROM nginx:stable-alpine

# Where /api/* is forwarded. Override in compose if the backend has another name.
ENV BACKEND_URL=http://mfi-metadata-form-backend:8000

COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
