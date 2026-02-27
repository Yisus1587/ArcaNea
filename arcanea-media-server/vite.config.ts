import fs from 'fs';
import path from 'path';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, '.', '');

    const certDir = path.resolve(__dirname, 'certs');
    const certPath = path.join(certDir, 'arcanea-cert.pem');
    const keyPath = path.join(certDir, 'arcanea-key.pem');

    let https: any = false;
    try {
      if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
        https = {
          cert: fs.readFileSync(certPath),
          key: fs.readFileSync(keyPath),
        };
      }
    } catch (e) {
      https = false;
    }

    return {
      server: {
        // Dev port (keep it >9000 to avoid well-known/common ports).
        port: 9587,
        host: '0.0.0.0',
        https,
        proxy: {
          '/api': {
            target: 'http://127.0.0.1:9800',
            changeOrigin: true,
            secure: false,
            rewrite: (path: string) => path,
          }
        }
      },
      plugins: [react()],
      build: {
        sourcemap: false,
      },
      define: {
        'process.env.API_KEY': JSON.stringify(env.GEMINI_API_KEY),
        'process.env.GEMINI_API_KEY': JSON.stringify(env.GEMINI_API_KEY)
      },
      resolve: {
        alias: {
          '@': path.resolve(__dirname, '.'),
        }
      }
    };
});
