# MFI DDB Retrieval App - frontend for the Retrieval API (RWS)
#
# Stage 1: build the React/Vite app
FROM node:22-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Stage 2: serve it with nginx and proxy /api to the Retrieval API
FROM nginx:stable-alpine

# Where the Retrieval API (RWS) is reachable from this container.
ENV RWS_URL=http://mfi-rws-app:8000

COPY nginx.conf.template /etc/nginx/templates/default.conf.template
# Runs before the template is rendered: names only in /etc/hosts -> their IP.
COPY rws-hosts.envsh /docker-entrypoint.d/18-rws-hosts.envsh
RUN sed -i 's/\r$//' /docker-entrypoint.d/18-rws-hosts.envsh && chmod 755 /docker-entrypoint.d/18-rws-hosts.envsh
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
