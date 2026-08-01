import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const configUrl = new URL('../config.toml', import.meta.url);
const migrationUrl = new URL(
  '../migrations/20260801023000_api_private_boundary_poc.sql',
  import.meta.url,
);
const devWorkflowUrl = new URL('../../.github/workflows/supabase-dev.yml', import.meta.url);

test('api is the only application custom schema exposed by config', async () => {
  const config = await readFile(configUrl, 'utf8');
  const schemasMatch = config.match(/^schemas\s*=\s*\[([^\]]+)\]/m);
  assert.ok(schemasMatch, 'config.toml must declare api.schemas');
  const schemas = [...schemasMatch[1].matchAll(/"([^"]+)"/g)].map((match) => match[1]);
  assert.deepEqual(schemas, ['api', 'public', 'graphql_public']);
  assert.ok(!schemas.includes('private'));
  assert.ok(!schemas.includes('util'));
  assert.ok(!schemas.includes('archive'));

  const searchPathMatch = config.match(/^extra_search_path\s*=\s*\[([^\]]+)\]/m);
  assert.ok(searchPathMatch, 'config.toml must declare api.extra_search_path');
  assert.doesNotMatch(searchPathMatch[1], /private|util|archive/);
});

test('migration keeps the contract additive and reloads PostgREST', async () => {
  const migration = await readFile(migrationUrl, 'utf8');
  assert.match(migration, /with \(security_invoker = true\)/);
  assert.match(migration, /security invoker/);
  assert.match(migration, /notify pgrst, 'reload schema'/);
  assert.doesNotMatch(migration, /drop\s+(table|view|function|schema)/i);
});

test('persistent dev deploys migrations before pushing Data API configuration', async () => {
  const workflow = await readFile(devWorkflowUrl, 'utf8');
  const migrationPush = workflow.indexOf('supabase db push --include-all');
  const configPush = workflow.indexOf(
    'supabase config push --project-ref "$SUPABASE_PROJECT_ID"',
  );

  assert.notEqual(migrationPush, -1, 'persistent dev workflow must push migrations');
  assert.notEqual(configPush, -1, 'persistent dev workflow must push config');
  assert.ok(migrationPush < configPush, 'schema migrations must precede exposed-schema config');
});
