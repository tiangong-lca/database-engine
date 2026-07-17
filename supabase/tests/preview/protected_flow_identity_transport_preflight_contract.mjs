#!/usr/bin/env node

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RUNNER = path.join(HERE, 'protected_flow_identity_rest_e2e.mjs');
const REF = 'aaaaaaaaaaaaaaa';

async function fileSha256(filePath) {
  return createHash('sha256').update(await readFile(filePath)).digest('hex');
}

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
  const tempDirRaw = await mkdtemp(path.join(os.tmpdir(), 'flow-identity-transport-contract-'));
  const tempDir = await realpath(tempDirRaw);
  try {
    const binDir = path.join(tempDir, 'bin');
    const exactDir = path.join(tempDir, 'exact');
    const invocationLog = path.join(tempDir, 'supabase-cli-invocations.log');
    const decoyInvocationLog = path.join(tempDir, 'decoy-invocations.log');
    const fakeSupabaseCli = path.join(exactDir, 'supabase');
    await mkdir(binDir, { mode: 0o700 });
    await mkdir(exactDir, { mode: 0o700 });
    for (const name of ['supabase', 'npx']) {
      await writeFile(path.join(binDir, name), `#!/bin/sh
set -eu
printf '%s\\n' '${name} $*' >> "$HOME/decoy-invocations.log"
exit 97
`, { mode: 0o700, flag: 'wx' });
    }
    await writeFile(fakeSupabaseCli, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/supabase-cli-invocations.log"
case "$*" in
  *"--log-level error test db --db-url "*) ;;
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
      PREVIEW_SUPABASE_CLI_PATH: fakeSupabaseCli,
      PREVIEW_SUPABASE_CLI_SHA256: await fileSha256(fakeSupabaseCli),
      PATH: `${binDir}:${process.env.PATH ?? ''}`,
      HOME: tempDir,
      TMPDIR: tempDir,
      LANG: 'C',
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
    assert.equal(evidence.transport_proof.executable_path_sha256,
      createHash('sha256').update(fakeSupabaseCli).digest('hex'));
    assert.equal(evidence.transport_proof.executable_file_sha256,
      await fileSha256(fakeSupabaseCli));
    assert.match(evidence.transport_proof.child_env_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.working_directory_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.transport_target_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.transport_binding_sha256, /^[0-9a-f]{64}$/);

    const rebound = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], { ...runnerEnv, LANG: 'C.UTF-8' });
    assert.equal(rebound.code, 0, rebound.stderr);
    const reboundEvidence = JSON.parse(rebound.stdout);
    assert.notEqual(
      reboundEvidence.transport_proof.child_env_sha256,
      evidence.transport_proof.child_env_sha256,
    );
    assert.notEqual(
      reboundEvidence.transport_proof.transport_binding_sha256,
      evidence.transport_proof.transport_binding_sha256,
    );

    await writeFile(fakeSupabaseCli, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/supabase-cli-invocations.log"
printf '%s\\n' 'failed to connect to fake transport' >&2
exit 31
`, { mode: 0o700 });
    runnerEnv.PREVIEW_SUPABASE_CLI_SHA256 = await fileSha256(fakeSupabaseCli);
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

    runnerEnv.PREVIEW_SUPABASE_CLI_SHA256 = '0'.repeat(64);
    const drifted = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], runnerEnv);
    assert.equal(drifted.code, 1);
    assert.equal(drifted.signal, null);
    assert.equal(drifted.stdout, '');
    const driftFailure = JSON.parse(drifted.stderr);
    assert.equal(driftFailure.status, 'failed');
    assert.equal(driftFailure.code, 'PREVIEW_SUPABASE_CLI_SHA256_MISMATCH');
    assert.equal(driftFailure.details.stage, 'transport_preflight');

    const symlinkCli = path.join(tempDir, 'supabase-symlink');
    await symlink(fakeSupabaseCli, symlinkCli);
    const symlinked = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], {
      ...runnerEnv,
      PREVIEW_SUPABASE_CLI_PATH: symlinkCli,
      PREVIEW_SUPABASE_CLI_SHA256: await fileSha256(fakeSupabaseCli),
    });
    assert.equal(symlinked.code, 1);
    assert.equal(symlinked.signal, null);
    assert.equal(symlinked.stdout, '');
    const symlinkFailure = JSON.parse(symlinked.stderr);
    assert.equal(symlinkFailure.status, 'failed');
    assert.equal(symlinkFailure.code, 'PREVIEW_SUPABASE_CLI_FILE_INVALID');
    assert.equal(symlinkFailure.details.stage, 'transport_preflight');

    const invocations = (await readFile(invocationLog, 'utf8')).trim().split('\n');
    assert.equal(invocations.length, 3);
    for (const invocation of invocations) {
      assert.doesNotMatch(invocation, /npx|--yes|supabase@/);
      assert.match(invocation, /test db/);
    }
    let decoyUsed = false;
    try {
      await readFile(decoyInvocationLog, 'utf8');
      decoyUsed = true;
    } catch (error) {
      assert.equal(error.code, 'ENOENT');
    }
    assert.equal(decoyUsed, false);
    process.stdout.write(`${JSON.stringify({
      status: 'passed',
      fake_supabase_cli_invocations: 3,
      child_env_rebindings: 1,
      cli_sha_drift_rejections: 1,
      symlink_rejections: 1,
      path_decoy_invocations: 0,
      actor_before_transport_failure: 0,
    })}\n`);
  } finally {
    await rm(tempDirRaw, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error.message}\n`);
  process.exitCode = 1;
});
