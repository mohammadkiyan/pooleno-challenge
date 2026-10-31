import { defineConfig } from 'vitest/config';

// Resolve CommonJS packages exactly as Node does in production, rather than through
// Vitest's own interop, which would make `sorted-btree` behave differently under test.
const deps = { interopDefault: false };

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['tests/unit/**/*.test.ts'],
          deps,
        },
      },
      {
        test: {
          name: 'database',
          include: ['tests/integration/**/*.test.ts', 'tests/e2e/**/*.test.ts'],
          globalSetup: ['tests/support/global-setup.ts'],
          deps,
          // These files share one PostgreSQL database and reset it between tests.
          fileParallelism: false,
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
    ],
  },
});
