import { defineConfig, globalIgnores } from 'eslint/config';
import tseslint from 'typescript-eslint';

export default defineConfig(
  globalIgnores(['dist/', 'extras/']),
  tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: { allowDefaultProject: ['eslint.config.js'] },
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    rules: {
      // Numbers read naturally in messages; bigint and sequence values are strings at JSON boundaries.
      '@typescript-eslint/restrict-template-expressions': ['error', { allowNumber: true }],
    },
  },
  {
    files: ['tests/**'],
    rules: {
      // Vitest's asymmetric matchers, such as expect.any(String), are typed as `any`.
      '@typescript-eslint/no-unsafe-assignment': 'off',
    },
  },
);
