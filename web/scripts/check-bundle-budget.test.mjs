import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { afterEach, expect, it } from 'vitest';
import { bundleReport, checkBudgets, importClosure } from './check-bundle-budget.mjs';

let directory;
afterEach(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

it('counts static imports once, skips dynamic routes and source maps, and accounts for route dependencies', async () => {
  directory = await mkdtemp(join(tmpdir(), 'loom-bundle-test-'));
  await mkdir(join(directory, '.vite'));
  const manifest = {
    entry: { file: 'entry.js', isEntry: true, imports: ['shared'], dynamicImports: ['route'] },
    shared: { file: 'shared.js', imports: ['entry'] },
    route: { file: 'route.js', isDynamicEntry: true, imports: ['shared', 'extra'] },
    extra: { file: 'extra.js' },
  };
  await writeFile(join(directory, '.vite/manifest.json'), JSON.stringify(manifest));
  for (const file of ['entry.js', 'shared.js', 'route.js', 'extra.js']) await writeFile(join(directory, file), 'x'.repeat(100));
  await writeFile(join(directory, 'entry.js.map'), 'x'.repeat(600_000));
  const report = await bundleReport(directory);
  expect(report.initial.bytes).toBe(200);
  expect(report.initial.gzipBytes).toBe(2 * gzipSync('x'.repeat(100)).length);
  expect(report.routes.route.files).toEqual(['extra.js', 'route.js']);
  expect(report.coldEntries.route.bytes).toBe(400);
  expect(report.coldEntries.route.files).toEqual(['entry.js', 'extra.js', 'route.js', 'shared.js']);
  expect(Object.keys(report.chunks)).toHaveLength(4);
  expect(() => importClosure(manifest, ['missing'])).toThrow('Missing manifest');
});

const report = () => ({ initial: { bytes: 100_000, gzipBytes: 30_000 }, routes: { page: { bytes: 10_000, gzipBytes: 3_000 } }, chunks: { 'page.js': { bytes: 10_000 } } });
it('rejects hard caps, gzip-only breaches, missing baselines, and unapproved growth', () => {
  const baseline = report();
  expect(checkBudgets(report(), baseline)).toEqual([]);
  const growth = report(); growth.routes.page.gzipBytes = 3_301;
  expect(checkBudgets(growth, baseline).join()).toContain('10%');
  delete baseline.routes.page;
  expect(checkBudgets(report(), baseline).join()).toContain('missing or invalid baseline');
  const oversized = report(); oversized.initial.bytes = 260_001; oversized.initial.gzipBytes = 85_001;
  oversized.routes.page.bytes = 300_001; oversized.routes.page.gzipBytes = 90_001;
  oversized.chunks['page.js'].bytes = 500_001;
  expect(checkBudgets(oversized)).toHaveLength(5);
});
