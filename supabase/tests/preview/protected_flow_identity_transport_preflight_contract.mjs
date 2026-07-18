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
const SCENARIO_NAMESPACE = 'fie2e-hosted-0123456789abcdef01234567';
const REQUEST_ID = '11111111-1111-4111-8111-111111111111';
const PG_PROVE_IMAGE_REF = 'public.ecr.aws/supabase/pg_prove:3.36';
const PG_PROVE_IMAGE_ID = `sha256:${'1'.repeat(64)}`;
const PG_PROVE_IMAGE_REPO_DIGEST = `public.ecr.aws/supabase/pg_prove@${PG_PROVE_IMAGE_ID}`;
const PG_PROVE_IMAGE_PLATFORM = 'linux/arm64';

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
    const dockerDir = path.join(tempDir, 'docker-exact');
    const invocationLog = path.join(tempDir, 'supabase-cli-invocations.log');
    const dockerInvocationLog = path.join(tempDir, 'docker-cli-invocations.log');
    const decoyInvocationLog = path.join(tempDir, 'decoy-invocations.log');
    const fakeSupabaseCli = path.join(exactDir, 'supabase');
    const fakeDockerCli = path.join(dockerDir, 'docker');
    await mkdir(binDir, { mode: 0o700 });
    await mkdir(exactDir, { mode: 0o700 });
    await mkdir(dockerDir, { mode: 0o700 });
    for (const name of ['supabase', 'npx', 'docker']) {
      await writeFile(path.join(binDir, name), `#!/bin/sh
set -eu
printf '%s\\n' '${name} $*' >> "$HOME/decoy-invocations.log"
exit 97
`, { mode: 0o700, flag: 'wx' });
    }
    await writeFile(fakeDockerCli, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/docker-cli-invocations.log"
if [ "$1" = 'image' ] && [ "$2" = 'inspect' ]; then
  [ "$3" = '${PG_PROVE_IMAGE_REF}' ]
  [ "$4" = '--format' ]
  [ -n "$5" ]
  if [ -f "$HOME/docker-image-missing" ]; then
    printf '%s\\n' 'No such image' >&2
    exit 44
  fi
  printf '%s\\n' '${JSON.stringify({
    id: PG_PROVE_IMAGE_ID,
    repo_digests: [PG_PROVE_IMAGE_REPO_DIGEST],
    architecture: 'arm64',
    os: 'linux',
  })}'
elif [ "$*" = 'version --format nested-runtime-ok' ]; then
  printf '%s\\n' '29.4.0'
else
  exit 43
fi
`, { mode: 0o700, flag: 'wx' });
    await writeFile(fakeSupabaseCli, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/supabase-cli-invocations.log"
docker version --format nested-runtime-ok >/dev/null
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
grep -q "^set application_name = 'fi269-transport-preflight';" "$sql_file"
grep -q "current_setting('application_name')" "$sql_file"
grep -q 'DB_APPLICATION_NAME_BINDING_FAILED' "$sql_file"
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
      PREVIEW_DOCKER_CLI_PATH: fakeDockerCli,
      PREVIEW_DOCKER_CLI_SHA256: await fileSha256(fakeDockerCli),
      PREVIEW_PG_PROVE_IMAGE_REF: PG_PROVE_IMAGE_REF,
      PREVIEW_PG_PROVE_IMAGE_ID: PG_PROVE_IMAGE_ID,
      PREVIEW_PG_PROVE_IMAGE_REPO_DIGEST: PG_PROVE_IMAGE_REPO_DIGEST,
      PREVIEW_PG_PROVE_IMAGE_PLATFORM: PG_PROVE_IMAGE_PLATFORM,
      PATH: `${dockerDir}:${binDir}:${process.env.PATH ?? ''}`,
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
    assert.equal(evidence.schema_version, 'protected-flow-identity-transport-preflight-evidence.v2');
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
    assert.equal(evidence.transport_proof.docker_executable_path_sha256,
      createHash('sha256').update(fakeDockerCli).digest('hex'));
    assert.equal(evidence.transport_proof.docker_executable_file_sha256,
      await fileSha256(fakeDockerCli));
    assert.equal(evidence.transport_proof.pg_prove_image_ref_sha256,
      createHash('sha256').update(PG_PROVE_IMAGE_REF).digest('hex'));
    assert.equal(evidence.transport_proof.pg_prove_image_id_sha256,
      createHash('sha256').update(PG_PROVE_IMAGE_ID).digest('hex'));
    assert.equal(evidence.transport_proof.pg_prove_image_repo_digest_sha256,
      createHash('sha256').update(PG_PROVE_IMAGE_REPO_DIGEST).digest('hex'));
    assert.equal(evidence.transport_proof.pg_prove_image_platform_sha256,
      createHash('sha256').update(PG_PROVE_IMAGE_PLATFORM).digest('hex'));
    assert.match(evidence.transport_proof.pg_prove_image_inspect_argv_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.pg_prove_image_inspect_stdout_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.pg_prove_image_inspect_stderr_sha256, /^[0-9a-f]{64}$/);
    assert.match(evidence.transport_proof.child_env_sha256, /^[0-9a-f]{64}$/);
    assert.equal(
      evidence.transport_proof.application_name_sha256,
      createHash('sha256').update('fi269-transport-preflight').digest('hex'),
    );
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

    const pathMisbound = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], { ...runnerEnv, PATH: `${binDir}:${dockerDir}:${process.env.PATH ?? ''}` });
    assert.equal(pathMisbound.code, 1);
    assert.equal(pathMisbound.signal, null);
    assert.equal(pathMisbound.stdout, '');
    const pathMisboundFailure = JSON.parse(pathMisbound.stderr);
    assert.equal(pathMisboundFailure.status, 'failed');
    assert.equal(pathMisboundFailure.code, 'PREVIEW_DOCKER_CLI_NOT_FIRST_IN_PATH');

    const mismatchedImageId = `sha256:${'2'.repeat(64)}`;
    const imageMismatched = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], {
      ...runnerEnv,
      PREVIEW_PG_PROVE_IMAGE_ID: mismatchedImageId,
      PREVIEW_PG_PROVE_IMAGE_REPO_DIGEST:
        `public.ecr.aws/supabase/pg_prove@${mismatchedImageId}`,
    });
    assert.equal(imageMismatched.code, 1);
    assert.equal(imageMismatched.signal, null);
    assert.equal(imageMismatched.stdout, '');
    const imageMismatchFailure = JSON.parse(imageMismatched.stderr);
    assert.equal(imageMismatchFailure.status, 'failed');
    assert.equal(imageMismatchFailure.code, 'PREVIEW_PG_PROVE_IMAGE_IDENTITY_MISMATCH');
    assert.equal(imageMismatchFailure.details.stage, 'transport_preflight');

    const missingImageMarker = path.join(tempDir, 'docker-image-missing');
    await writeFile(missingImageMarker, '', { mode: 0o600, flag: 'wx' });
    const imageMissing = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], runnerEnv);
    await rm(missingImageMarker);
    assert.equal(imageMissing.code, 1);
    assert.equal(imageMissing.signal, null);
    assert.equal(imageMissing.stdout, '');
    const imageMissingFailure = JSON.parse(imageMissing.stderr);
    assert.equal(imageMissingFailure.status, 'failed');
    assert.equal(imageMissingFailure.code, 'PREVIEW_PG_PROVE_IMAGE_INSPECT_FAILED');
    assert.equal(imageMissingFailure.details.stage, 'transport_preflight');

    await writeFile(fakeSupabaseCli, `#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$HOME/supabase-cli-invocations.log"
docker version --format nested-runtime-ok >/dev/null
printf '%s\\n' 'failed to connect to fake transport' >&2
exit 31
`, { mode: 0o700 });
    runnerEnv.PREVIEW_SUPABASE_CLI_SHA256 = await fileSha256(fakeSupabaseCli);
    const failed = await runRunner([
      '--expected-preview-ref', REF,
      '--scenario-namespace', SCENARIO_NAMESPACE,
      '--request-id', REQUEST_ID,
    ], runnerEnv);
    assert.equal(failed.code, 1);
    assert.equal(failed.signal, null);
    assert.equal(failed.stdout, '');
    const failure = JSON.parse(failed.stderr);
    assert.equal(failure.schema_version, 'protected-flow-identity-rest-e2e-evidence.v2');
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

    runnerEnv.PREVIEW_SUPABASE_CLI_SHA256 = await fileSha256(fakeSupabaseCli);
    runnerEnv.PREVIEW_DOCKER_CLI_SHA256 = '0'.repeat(64);
    const dockerDrifted = await runRunner([
      '--transport-preflight-only',
      '--expected-preview-ref', REF,
    ], runnerEnv);
    assert.equal(dockerDrifted.code, 1);
    assert.equal(dockerDrifted.signal, null);
    assert.equal(dockerDrifted.stdout, '');
    const dockerDriftFailure = JSON.parse(dockerDrifted.stderr);
    assert.equal(dockerDriftFailure.status, 'failed');
    assert.equal(dockerDriftFailure.code, 'PREVIEW_DOCKER_CLI_SHA256_MISMATCH');
    assert.equal(dockerDriftFailure.details.stage, 'transport_preflight');
    runnerEnv.PREVIEW_DOCKER_CLI_SHA256 = await fileSha256(fakeDockerCli);

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
    const dockerInvocations = (await readFile(dockerInvocationLog, 'utf8')).trim().split('\n');
    assert.equal(dockerInvocations.length, 8);
    assert.equal(
      dockerInvocations.filter((invocation) => invocation.startsWith('image inspect ')).length,
      5,
    );
    assert.equal(
      dockerInvocations.filter((invocation) => invocation === 'version --format nested-runtime-ok').length,
      3,
    );
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
      fake_docker_cli_invocations: 8,
      pg_prove_image_inspections: 5,
      pg_prove_image_identity_rejections: 1,
      pg_prove_image_missing_rejections: 1,
      child_env_rebindings: 1,
      cli_sha_drift_rejections: 1,
      docker_sha_drift_rejections: 1,
      docker_path_order_rejections: 1,
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
