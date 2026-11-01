import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true, proxy: { "/live": { target: process.env.VIEWER_URL ?? "http://127.0.0.1:8084" } } },
  test: { include: ["tests/**/*.test.ts"] },
});
