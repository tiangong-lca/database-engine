#!/usr/bin/env node

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RUNNER = path.join(HERE, 'protected_flow_identity_rest_e2e.mjs');
const REF = 'aaaaaaaaaaaaaaa';

async function runRunner(args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [RUNNER, ...args], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const stdout = [];
    const stderr = [];
    child.stdout.on('data', (chunk) => stdout.push(chunk));
    child.stderr.on('data', (chunk) => stderr.push(chunk));
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({
      code,
      signal,
      stdout: Buffer.concat(stdout).toString('utf8'),
      stderr: Buffer.concat(stderr).toString('utf8'),
    }));
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), 'flow-identity-transport-contract-'));
  try {
    const binDir = path.join(tempDir, 'bin');
    const invocationLog = path.join(tempDir, 'npx-invocations.log');
    const fakeNpx = path.join(binDir, 'npx');
    await mkdir(binDir, { mode: 0o700 });
    await writeFile(fakeNpx, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/npx-invocations.log"
case "$*" in
  *"--yes supabase@2.109.1 --log-level error test db --db-url "*) ;;
  *) exit 41 ;;
esac
sql_file=
for arg do
  sql_file="$arg"
done
grep -qi '^begin read only;' "$sql_file"
grep -qi '^rollback;' "$sql_file"
printf '%s\\n' 'TAP version 13' '1..1' 'ok 1 - fake authenticated read-only transport'
`, { mode: 0o700, flag: 'wx' });

    const runnerEnv = {
      PREVIEW_ENVIRONMENT: 'preview',
      PREVIEW_PROJECT_REF: REF,
      PREVIEW_SUPABASE_URL: `https://${REF}.supabase.co`,
      PREVIEW_SUPABASE_ANON_KEY: 'anon-key-at-least-20-characters',
      PREVIEW_SUPABASE_SERVICE_ROLE_KEY: 'service-key-at-least-20-characters',
      PREVIEW_DB_URL: `postgresql://postgres.${REF}:dummy@pooler.example.invalid:6543/postgres`,
      PATH: `${binDir}:${process.env.PATH ?? ''}`,
      HOME: tempDir,
      TMPDIR: tempDir,
      LANG: 'C',
      NPM_CONFIG_CACHE: path.join(tempDir, 'npm-cache'),
    };
    const result = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], runnerEnv);

    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.signal, null);
    assert.equal(result.stderr, '');
    const evidence = JSON.parse(result.stdout);
    assert.equal(evidence.schema_version, 'protected-flow-identity-transport-preflight-evidence.v1');
    assert.equal(evidence.status, 'passed');
    assert.equal(evidence.environment, 'preview');
    assert.equal(evidence.disposable_actor_count, 0);
    assert.equal(evidence.primary_write_count, 0);
    assert.match(evidence.transport_proof.sql_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.stdout_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.stderr_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.transport_target_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.transport_binding_sha256, /^[0-9a-f]{64}$/);

    await writeFile(fakeNpx, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/npx-invocations.log"
printf '%s\\n' 'failed to connect to fake transport' >&2
exit 31
`, { mode: 0o700 });
    const failed = await runRunner(['--expected-preview-ref', REF], runnerEnv);
    assert.equal(failed.code, 1);
    assert.equal(failed.signal, null);
    assert.equal(failed.stdout, '');
    const failure = JSON.parse(failed.stderr);
    assert.equal(failure.schema_version, 'protected-flow-identity-rest-e2e-evidence.v1');
    assert.equal(failure.status, 'failed');
    assert.equal(failure.code, 'DB_TEST_FAILED');
    assert.equal(failure.details.stage, 'transport_preflight');
    assert.equal(failure.details.cleanup.fixture_cleanup_ran, false);
    assert.equal(failure.details.cleanup.owner_session_revoked, false);
    assert.equal(failure.details.cleanup.foreign_session_revoked, false);
    assert.equal(failure.details.cleanup.owner_deleted, false);
    assert.equal(failure.details.cleanup.foreign_deleted, false);
    assert.equal(failure.details.cleanup.actors_retained_for_cleanup, false);
    assert.equal(failure.details.cleanup_failure_code, null);

    const invocations = (await readFile(invocationLog, 'utf8')).trim().split('\n');
    assert.equal(invocations.length, 2);
    assert.match(invocations[0], /supabase@2\.109\.1/);
    assert.match(invocations[0], /test db/);
    process.stdout.write(`${JSON.stringify({
      status: 'passed',
      fake_npx_invocations: 2,
      actor_before_transport_failure: 0,
    })}\n`);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error.message}\n`);
  process.exitCode = 1;
});
