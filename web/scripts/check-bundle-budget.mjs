#!/usr/bin/env node
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';

export const BUDGETS = {
  initial: { bytes: 260_000, gzipBytes: 85_000 },
  route: { bytes: 300_000, gzipBytes: 90_000 },
  chunk: 500_000,
};

export function importClosure(manifest, roots) {
  const seen = new Set();
  function visit(key) {
    if (seen.has(key)) return;
    if (!manifest[key]) throw new Error(`Missing manifest dependency: ${key}`);
    seen.add(key);
    for (const dependency of manifest[key].imports ?? []) visit(dependency);
  }
  roots.forEach(visit);
  return [...seen];
}

export async function bundleReport(directory = 'dist') {
  const manifest = JSON.parse(await readFile(resolve(directory, '.vite/manifest.json'), 'utf8'));
  const entries = Object.keys(manifest).filter(key => manifest[key].isEntry);
  if (!entries.length) throw new Error('Bundle manifest has no entry');
  const chunks = {};
  // Walk emitted JS as well as the manifest, so an unreferenced oversized chunk
  // cannot escape the warning budget. Source maps are deliberately excluded.
  for (const file of await readdir(directory, { recursive: true })) {
    if (!/\.(m?js)$/.test(file)) continue;
    const buffer = await readFile(resolve(directory, file));
    chunks[file] = { bytes: buffer.length, gzipBytes: gzipSync(buffer).length };
  }
  function measure(keys) {
    const files = [...new Set(keys.map(key => manifest[key].file))].filter(file => /\.(m?js)$/.test(file));
    return {
      bytes: files.reduce((n, file) => n + chunks[file].bytes, 0),
      gzipBytes: files.reduce((n, file) => n + chunks[file].gzipBytes, 0),
      files: files.sort(),
    };
  }
  const initialKeys = importClosure(manifest, entries);
  const initialSet = new Set(initialKeys);
  const routes = Object.fromEntries(Object.keys(manifest)
    .filter(key => manifest[key].isDynamicEntry)
    .sort().map(key => [key, measure(importClosure(manifest, [key]).filter(k => !initialSet.has(k)))]));
  // Explicit cold module closures prevent confusing the shared shell with a
  // complete first page. Interaction-triggered children are measured separately;
  // browser request evidence covers children mounted by the default view.
  const coldEntries = Object.fromEntries(Object.keys(routes).map(key =>
    [key, measure(importClosure(manifest, [...entries, key]))]));
  return { initial: measure(initialKeys), routes, coldEntries, chunks };
}

export function checkBudgets(report, baseline) {
  const errors = [];
  for (const [name, value, cap, previous] of [
    ['initial', report.initial, BUDGETS.initial, baseline?.initial],
    ...Object.entries(report.routes).map(([key, value]) => [key, value, BUDGETS.route, baseline?.routes?.[key]]),
  ]) {
    for (const metric of ['bytes', 'gzipBytes']) {
      if (value[metric] > cap[metric]) errors.push(`${name} ${metric}: ${value[metric]} exceeds ${cap[metric]}`);
      if (baseline && (!previous || !Number.isFinite(previous[metric]) || previous[metric] < 0)) {
        errors.push(`${name}: missing or invalid baseline ${metric}`);
      } else if (previous && value[metric] > previous[metric] * 1.1) {
        errors.push(`${name} ${metric}: more than 10% above baseline ${previous[metric]}`);
      }
    }
  }
  for (const [file, size] of Object.entries(report.chunks)) {
    if (size.bytes > BUDGETS.chunk) errors.push(`${file}: exceeds Vite's 500 kB warning limit`);
  }
  return errors;
}

export async function verifyBundleBudget({ directory = 'dist', baselinePath = 'bundle-baseline.json', update = false } = {}) {
  const report = await bundleReport(directory);
  // Persist the exact asset list for CI/browser evidence; baseline keys stay
  // source-based, independent of content hashes and deployment build metadata.
  await writeFile(resolve(directory, 'bundle-report.json'), JSON.stringify(report, null, 2) + '\n');
  const baseline = update ? null : JSON.parse(await readFile(baselinePath, 'utf8'));
  const errors = checkBudgets(report, baseline);
  if (errors.length) throw new Error(errors.join('\n'));
  if (update) {
    const size = ({ bytes, gzipBytes }) => ({ bytes, gzipBytes });
    await writeFile(baselinePath, JSON.stringify({
      initial: size(report.initial),
      routes: Object.fromEntries(Object.entries(report.routes).map(([key, value]) => [key, size(value)])),
    }, null, 2) + '\n');
  }
  console.log(`Bundle budget passed: initial ${(report.initial.bytes / 1000).toFixed(2)} kB / gzip ${(report.initial.gzipBytes / 1000).toFixed(2)} kB; ${Object.keys(report.routes).length} lazy entries`);
  return report;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  verifyBundleBudget({ update: process.argv.includes('--write-baseline') }).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
