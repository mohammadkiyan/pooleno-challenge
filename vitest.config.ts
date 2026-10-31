import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['tests/unit/**/*.test.ts'],
        },
      },
      {
        test: {
          name: 'database',
          include: ['tests/integration/**/*.test.ts', 'tests/e2e/**/*.test.ts'],
          globalSetup: ['tests/support/global-setup.ts'],
          // These files share one PostgreSQL database and reset it between tests.
          fileParallelism: false,
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
    ],
  },
});
