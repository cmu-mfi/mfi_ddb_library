import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// `npm run dev` proxies /api to the Retrieval API (RWS) on localhost:8000.
// /api/<x> -> <RWS>/mfi-ddb/<x>, /api/health -> <RWS>/
const target = process.env.VITE_RWS_URL || 'http://localhost:8000'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/api/health': { target, changeOrigin: true, rewrite: () => '/' },
      '/api': {
        target,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/mfi-ddb'),
      },
    },
  },
})
