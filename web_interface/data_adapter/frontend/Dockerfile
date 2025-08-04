# web_interface/data_adapter/frontend/Dockerfile
FROM node:18

# 1. Set working dir
WORKDIR /app

# 2. Copy package manifests
COPY package.json package-lock.json* ./

# 3. Install all dependencies (including react-scripts)
#    Use legacy-peer-deps if you hit peer conflict errors
RUN npm install --legacy-peer-deps

# 4. Copy the rest of your source code
COPY . .

# 5. Expose dev port and start
EXPOSE 3000
CMD ["npm", "start"]
