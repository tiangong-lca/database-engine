#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
if (args.length === 1 && args[0] === '--help') {
  process.stdout.write('Usage: node scripts/benchmark_hybrid_versions.mjs --local-container supabase_db_database-engine-600-<isolated-suffix>\nRuns rollback-only synthetic top-10/20 and natural executor-role HNSW comparisons. Reset only that isolated stack before and after; this is not production relevance evidence.\n');
  process.exit(0);
}
if (args.length !== 2 || args[0] !== '--local-container' ||
    !/^supabase_db_database-engine-600-[a-z0-9-]+$/u.test(args[1])) {
  throw new Error('An explicitly named isolated Database #600 local container is required');
}
const container = args[1];
const name = execFileSync('docker', ['inspect', '--format', '{{.Name}}', container], { encoding: 'utf8' }).trim();
if (name !== '/' + container) throw new Error('Container identity mismatch');
const fixture = readFileSync(resolve(root, 'supabase/tests/20260902_portal_version_search_v2.sql'), 'utf8');
const marker = '\ncreate temporary table version_results(';
if (fixture.split(marker).length !== 2) throw new Error('Version fixture boundary changed');
const setup = fixture.slice(0, fixture.indexOf(marker));
const benchmark = readFileSync(resolve(root, 'supabase/tests/benchmarks/20260902_hybrid_version_comparison.sql'), 'utf8');
const output = execFileSync('docker', [
  'exec', '-i', container, 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'postgres',
], { input: setup + '\n' + benchmark, encoding: 'utf8', timeout: 180000, maxBuffer: 8 * 1024 * 1024 });
const records = output.split('\n').filter(line => line.startsWith('{'));
if (records.length !== 2) throw new Error('Both Process and Flow benchmark records are required');
for (const record of records) {
  const value = JSON.parse(record);
  if (value.benchmark !== 'hybrid-version-comparison.v1' || value.profile !== 'isolated-synthetic') {
    throw new Error('Unexpected benchmark envelope');
  }
  if (value.kind === 'flow' && (
    value.v2Plan?.executorRole !== 'portal_public_executor' ||
    !value.v2Plan?.indexNames?.includes('flows_embedding_ft_hnsw_idx') ||
    value.v2UnfilteredPlan?.executorRole !== 'portal_public_executor' ||
    !value.v2UnfilteredPlan?.indexNames?.includes('flows_embedding_ft_hnsw_idx')
  )) {
    throw new Error('Broad and unfiltered Flow V2 must naturally use source HNSW as portal_public_executor');
  }
  if (value.kind === 'process' && (
    value.v2Plan?.executorRole !== 'portal_public_executor' ||
    !value.v2Plan?.indexNames?.includes('processes_embedding_ft_hnsw_idx') ||
    value.v2UnfilteredPlan?.executorRole !== 'portal_public_executor' ||
    !value.v2UnfilteredPlan?.indexNames?.includes('processes_embedding_ft_hnsw_idx')
  )) {
    throw new Error('Broad and unfiltered Process V2 must naturally use source HNSW as portal_public_executor');
  }
  if (value.kind === 'process') {
    const expectedCases = {
      zero: ['exact', 0],
      selective: ['exact', 6],
      boundary_2000: ['exact', 2000],
      overflow_2001: ['hnsw', 2001],
      broad: ['hnsw', 5020],
      unfiltered: ['hnsw', null],
    };
    for (const [caseName, [expectedRoute, expectedPopulation]] of Object.entries(expectedCases)) {
      const measured = value.adaptiveRouteTimings?.[caseName];
      if (!measured || measured.expectedRoute !== expectedRoute ||
          measured.observedRoute !== expectedRoute ||
          measured.candidatePopulation !== expectedPopulation ||
          !Number.isFinite(measured.firstMs) || !Number.isFinite(measured.repeatP50Ms) ||
          !Number.isFinite(measured.repeatMaxMs)) {
        throw new Error(`Invalid Process adaptive-route evidence for ${caseName}`);
      }
    }
    if (value.adaptiveRouteTimings.zero.resultCount !== 0 ||
        value.adaptiveRouteTimings.selective.resultCount !== 6 ||
        value.adaptiveRouteTimings.boundary_2000.resultCount !== 200) {
      throw new Error('Process exact-route result bounds drifted');
    }
  }
  process.stdout.write(JSON.stringify(value) + '\n');
}
