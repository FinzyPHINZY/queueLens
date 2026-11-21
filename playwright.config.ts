import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  use: {
    browserName: 'chromium',
    launchOptions: process.env.QUEUELENS_CHROMIUM_PATH
      ? { executablePath: process.env.QUEUELENS_CHROMIUM_PATH }
      : undefined,
  },
});
