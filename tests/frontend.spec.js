const { test, expect } = require('@playwright/test');
const job = {
  job_id: 'f0aafbd6-89ba-41d6-bdc8-124012384ddd',
  filename: 'sales.csv',
  status: 'completed',
  rows: 4,
  progress: 100,
  total_chunks: 1,
  completed_chunks: 1,
  created_at: '2026-01-01T00:00:00Z',
};
test('upload queues a file, progress refreshes, and regional totals display', async ({
  page,
}) => {
  let jobs = [];
  await page.route('**/api/jobs', async (route) => {
    if (route.request().method() === 'GET')
      return route.fulfill({ json: jobs });
    expect(route.request().postDataBuffer().toString()).toContain('sample.csv');
    jobs = [{ ...job, status: 'queued', completed_chunks: 0, progress: 0 }];
    await route.fulfill({ status: 202, json: { job_id: job.job_id } });
  });
  await page.route('**/api/jobs/*/results', (route) =>
    route.fulfill({
      json: [
        { region: 'North', total_cents: '20000' },
        { region: 'South', total_cents: '8000' },
      ],
    }),
  );
  await page.goto('/');
  await expect(page.getByText('No files yet.', { exact: false })).toBeVisible();
  await page.locator('#file').setInputFiles('public/sample.csv');
  await page.getByRole('button', { name: 'Start processing' }).click();
  await expect(page.getByText('File queued.', { exact: false })).toBeVisible();
  await expect(page.getByText('0% · 0/1 chunks')).toBeVisible();
  jobs = [job];
  await page.getByRole('button', { name: 'Refresh' }).click();
  await page
    .getByRole('button', { name: 'View results for sales.csv' })
    .click();
  await expect(
    page.getByRole('cell', { name: '200.00', exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole('cell', { name: '80.00', exact: true }),
  ).toBeVisible();
  await expect(page.getByText('280.00', { exact: true })).toBeVisible();
});
test('server and upload failures are recoverable', async ({ page }) => {
  await page.route('**/api/jobs', (route) =>
    route.fulfill(
      route.request().method() === 'GET'
        ? { status: 503, json: { error: 'Could not load jobs' } }
        : { status: 400, json: { error: 'Invalid CSV header' } },
    ),
  );
  await page.goto('/');
  await expect(page.getByText('Connection unavailable')).toBeVisible();
  await page.locator('#file').setInputFiles('public/sample.csv');
  await page.getByRole('button', { name: 'Start processing' }).click();
  await expect(page.getByText('Invalid CSV header')).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Start processing' }),
  ).toBeEnabled();
});
test('filenames and regions render as text and mobile layout fits', async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route('**/api/jobs', (route) =>
    route.fulfill({
      json: [{ ...job, filename: '<img src=x onerror=alert(1)>.csv' }],
    }),
  );
  await page.goto('/');
  await expect(
    page.getByText('<img src=x onerror=alert(1)>.csv', { exact: true }),
  ).toBeVisible();
  expect(await page.locator('#jobs img').count()).toBe(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({ path: 'test-results/mobile.png', fullPage: true });
});
test('desktop working surface has no browser errors', async ({ page }) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/api/jobs', (route) => route.fulfill({ json: [job] }));
  await page.goto('/');
  await expect(
    page.getByRole('heading', { name: 'Your files. In motion.' }),
  ).toBeVisible();
  await page.screenshot({ path: 'test-results/desktop.png', fullPage: true });
  expect(errors).toEqual([]);
});

test('large cent values retain precision in results', async ({ page }) => {
  await page.route('**/api/jobs', (route) => route.fulfill({ json: [job] }));
  await page.route('**/api/jobs/*/results', (route) =>
    route.fulfill({
      json: [{ region: 'North', total_cents: '9007199254740993' }],
    }),
  );
  await page.goto('/');
  await page
    .getByRole('button', { name: 'View results for sales.csv' })
    .click();
  await expect(
    page.getByRole('cell', { name: '90,071,992,547,409.93' }),
  ).toBeVisible();
});
