#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);

if (args.length === 1 && args[0] === '--help') {
  process.stdout.write(
    'Usage: node scripts/benchmark_next_hybrid_v2.mjs --local-container ' +
      'supabase_db_database-engine-624[-<isolated-suffix>]\n' +
      'Runs a rollback-only synthetic segmented benchmark against an explicitly ' +
      'named local Database #624 container. It never accepts a remote URL.\n',
  );
  process.exit(0);
}

if (
  args.length !== 2 ||
  args[0] !== '--local-container' ||
  !/^supabase_db_database-engine-624(?:-[a-z0-9-]+)?$/u.test(args[1])
) {
  throw new Error(
    'An explicitly named isolated Database #624 local container is required',
  );
}

const container = args[1];
const inspectedName = execFileSync(
  'docker',
  ['inspect', '--format', '{{.Name}}', container],
  { encoding: 'utf8' },
).trim();
if (inspectedName !== `/${container}`) {
  throw new Error('Container identity mismatch');
}

const fixture = readFileSync(
  resolve(root, 'supabase/tests/20260905_next_hybrid_filter_adaptive_v2.sql'),
  'utf8',
);
const marker = '\nselect extensions.ok(';
if (fixture.split(marker).length < 2) {
  throw new Error('Next Hybrid V2 fixture boundary changed');
}
const setup = fixture.slice(0, fixture.indexOf(marker));
const benchmark = readFileSync(
  resolve(root, 'supabase/tests/benchmarks/20260905_next_hybrid_v2_segmented.sql'),
  'utf8',
);

const output = execFileSync(
  'docker',
  [
    'exec',
    '-i',
    container,
    'psql',
    '-X',
    '-qAt',
    '-v',
    'ON_ERROR_STOP=1',
    '-U',
    'postgres',
    '-d',
    'postgres',
  ],
  {
    input: `${setup}\n${benchmark}`,
    encoding: 'utf8',
    timeout: 300_000,
    maxBuffer: 8 * 1024 * 1024,
  },
);

const records = output
  .split('\n')
  .filter((line) => line.startsWith('{'))
  .map((line) => JSON.parse(line));

if (records.length !== 2) {
  throw new Error('Both Process and Flow segmented benchmark records are required');
}

for (const record of records) {
  if (
    record.benchmark !== 'next-hybrid-v2-segmented.v1' ||
    record.profile !== 'isolated-synthetic'
  ) {
    throw new Error('Unexpected benchmark envelope');
  }

  const expected = {
    zero: ['exact', 0],
    small: ['exact', 1],
    boundary_2000: ['exact', 2000],
    overflow_2001: ['hnsw', 2001],
    broad: ['hnsw', 2001],
    unfiltered: ['hnsw', null],
  };
  for (const [caseName, [route, population]] of Object.entries(expected)) {
    const measured = record.routes?.[caseName];
    if (
      !measured ||
      measured.expectedRoute !== route ||
      measured.observedRoute !== route ||
      measured.candidatePopulation !== population ||
      !Number.isFinite(measured.firstObservedMs) ||
      !Number.isFinite(measured.repeatP50Ms) ||
      !/^[0-9a-f]{64}$/u.test(measured.resultSha256 ?? '')
    ) {
      throw new Error(
        `${record.kind} ${caseName} route evidence is invalid: ${JSON.stringify(measured)}`,
      );
    }
  }

  const expectedActualPopulations = {
    zero: 0,
    small: 1,
    boundary_2000: 2000,
    overflow_2001: 2001,
    broad: record.kind === 'process' ? 2503 : 4003,
  };
  for (const [caseName, population] of Object.entries(expectedActualPopulations)) {
    if (record.routes?.[caseName]?.actualPopulation !== population) {
      throw new Error(`${record.kind} ${caseName} actual population drifted`);
    }
  }

  const expectedIndexes =
    record.kind === 'process'
      ? [
          'next_hybrid_public_candidate_type_v2_idx',
          'next_hybrid_public_candidate_team_v2_idx',
          'processes_search_text_pgroonga',
        ]
      : [
          'next_hybrid_public_candidate_classification_v2_idx',
          'next_hybrid_public_candidate_team_v2_idx',
          'flows_search_text_pgroonga',
        ];
  for (const indexName of expectedIndexes) {
    if (!record.planIndexNames?.includes(indexName)) {
      throw new Error(
        `${record.kind} plan did not naturally use ${indexName}: ` +
          JSON.stringify({
            indexNames: record.planIndexNames,
            nodeTypes: record.planNodeTypes,
          }),
      );
    }
  }

  const expectedHnsw =
    record.kind === 'process'
      ? 'processes_embedding_ft_tg_hnsw_idx'
      : 'flows_embedding_ft_hnsw_idx';
  const hnswIndexes = record.planIndexesByShape?.hnsw ?? [];
  const hnswNodeTypes = record.planNodeTypesByShape?.hnsw ?? [];
  if (
    !hnswIndexes.includes(expectedHnsw) &&
    !hnswNodeTypes.includes('Seq Scan')
  ) {
    throw new Error(
      `${record.kind} broad semantic plan is neither natural HNSW nor an eligible small-universe sequential scan`,
    );
  }

  process.stdout.write(`${JSON.stringify(record)}\n`);
}
