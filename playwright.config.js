const { defineConfig } = require('@playwright/test');
module.exports = defineConfig({
  testDir: './tests',
  testMatch: '*.spec.js',
  fullyParallel: true,
  use: { baseURL: 'http://127.0.0.1:8080' },
  webServer: {
    command: process.env.DFS_WEB_COMMAND || 'node server/index.mjs',
    url: 'http://127.0.0.1:8080',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
});
