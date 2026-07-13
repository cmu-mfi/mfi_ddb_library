# Stage 1: Build the React/Vite application
FROM node:22-alpine AS builder

WORKDIR /app

# Copy package files first to take advantage of Docker layer caching
COPY package*.json ./
RUN npm ci

# Copy the rest of the frontend source code
COPY . .

# Build the static assets (generates the /app/dist directory)
RUN npm run build

# Stage 2: Serve the static files using Nginx
FROM nginx:stable-alpine

# Copy our custom Nginx routing rules over the default ones
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy the compiled production assets from the builder stage to Nginx's web root
COPY --from=builder /app/dist /usr/share/nginx/html

# Expose port 80 (Nginx default web hosting port)
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]

# not using npm run dev because it is not suitable for production. Instead, we build the app and serve it with Nginx.
# npm run dev will download the entire node runtime which will bloat the image size.