import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  use: {
    browserName: 'chromium',
    launchOptions:
      (process.env.QUEUEHARBOR_CHROMIUM_PATH ?? process.env.QUEUELENS_CHROMIUM_PATH)
        ? {
            executablePath:
              process.env.QUEUEHARBOR_CHROMIUM_PATH ?? process.env.QUEUELENS_CHROMIUM_PATH,
          }
        : undefined,
  },
});
