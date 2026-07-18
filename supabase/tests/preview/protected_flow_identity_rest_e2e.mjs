#!/usr/bin/env node

/**
 * Hosted Preview E2E for guarded Step 3 owner-draft flow-identity rewrites.
 *
 * No dotenv or third-party runtime dependency is used. Raw execution permits
 * stay in memory and cross the race-worker boundary only over stdin. Rendered
 * SQL containing branch credentials is held in a private mode-0600 temporary
 * file for the pinned Supabase CLI and removed in finally; the DB URL is a
 * bounded child argv value. The outer one-shot wrapper must durably freeze the
 * request/namespace CLI selectors before launch so crash recovery never relies
 * on child memory or output. Emitted evidence contains hashes and counts only.
 * The matching SQL cleanup is mandatory even after a failure.
 */

import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import {
  lstat, mkdtemp, readFile, readdir, realpath, rm, writeFile,
} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../../..');
const SCRIPT_PATH = fileURLToPath(import.meta.url);
const TRANSPORT_PREFLIGHT = path.join(HERE, 'protected_flow_identity_transport_preflight.sql');
const FIXTURE_TEMPLATE = path.join(HERE, 'protected_flow_identity_fixture.sql');
const CLEANUP_TEMPLATE = path.join(HERE, 'protected_flow_identity_cleanup.sql');
const RESIDUE_READBACK_TEMPLATE = path.join(
  HERE,
  'protected_flow_identity_residue_readback.sql',
);

const FIXTURE_COMMAND = 'preview_e2e_flow_identity_fixture';
const FIXTURE_TARGET_TABLE = 'preview_e2e_flow_identity';
const FIXTURE_TARGET_VERSION = '00.00.001';
const MANIFEST_SCHEMA = 'protected-flow-identity-preview-fixture.v1';
const EVIDENCE_SCHEMA = 'protected-flow-identity-rest-e2e-evidence.v3';
const TRANSPORT_EVIDENCE_SCHEMA = 'protected-flow-identity-transport-preflight-evidence.v2';
const SUPABASE_CLI_VERSION = '2.109.1';
const PG_PROVE_IMAGE_REPOSITORY = 'public.ecr.aws/supabase/pg_prove';
const PG_PROVE_IMAGE_REF = `${PG_PROVE_IMAGE_REPOSITORY}:3.36`;
const PG_PROVE_IMAGE_PLATFORM = 'linux/arm64';
const PG_PROVE_INSPECT_TEMPLATE = '{"id":{{json .Id}},"repo_digests":{{json .RepoDigests}},"architecture":{{json .Architecture}},"os":{{json .Os}}}';
const DOCKER_INSPECT_TIMEOUT_MS = 15_000;
const PRODUCTION_REF = 'qgzvkongdjqiiamzbbts';
const TRANSPORT_APPLICATION_NAME = 'fi269-transport-preflight';
const DB_TEST_CHILD_ENV_NAMES = Object.freeze([
  'PATH', 'HOME', 'TMPDIR', 'TMP', 'TEMP', 'LANG', 'LC_ALL',
  'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy',
  'no_proxy',
]);
const AUTH_USER_LIST_PAGE_SIZE = 200;
const AUTH_USER_LIST_MAX_PAGES = 50;
const AUTH_USER_LIST_MAX_TOTAL =
  AUTH_USER_LIST_PAGE_SIZE * AUTH_USER_LIST_MAX_PAGES;
const RECOVERY_CHECKPOINT_SCHEMA =
  'protected-flow-identity-recovery-checkpoint.v1';
const RECOVERY_CHECKPOINT_ACK_SCHEMA =
  'protected-flow-identity-recovery-checkpoint-ack.v1';
const RECOVERY_CHECKPOINT_MAX_BYTES = 16 * 1024;
const RECOVERY_CHECKPOINT_TIMEOUT_MS = 10_000;
const RECOVERY_CHECKPOINT_STAGES = new Set([
  'runner_ready',
  'owner_actor_registered',
  'owner_actor_create_requested',
  'owner_actor_bound',
  'owner_actor_bound_cleanup',
  'foreign_actor_registered',
  'foreign_actor_create_requested',
  'foreign_actor_bound',
  'foreign_actor_bound_cleanup',
  'actors_bound',
  'fixture_committed',
  'fixture_manifest_bound',
  'capture_receipt_bound',
  'scope_bound',
  'process_1_committed',
  'process_2_committed',
  'finalize_selectors_bound',
  'cleanup_sql_committed',
]);
const RECOVERY_CHECKPOINT_ACTOR_KEYS = Object.freeze([
  'actor_bound_checkpoint_acknowledged',
  'create_requested',
  'email',
  'fixture_sha256',
  'locator_sha256',
  'namespace',
  'request_id',
  'role',
  'user_id',
]);
const RECOVERY_SELECTOR_UUID_KEYS = Object.freeze([
  'actor_user_ids',
  'derivative_batch_ids',
  'derivative_request_ids',
  'process_ids',
  'receipt_ids',
  'scope_ids',
  'support_entity_ids',
  'wrapper_invocation_ids',
]);
const RECOVERY_SELECTOR_DECIMAL_KEYS = Object.freeze([
  'audit_ids',
  'derivative_proposal_ids',
  'embedding_pending_job_ids',
  'embedding_queue_msg_ids',
  'fixture_backend_pids',
  'http_request_ids',
]);
const RECOVERY_SELECTOR_KEYS = Object.freeze([
  'schema_version',
  'request_id',
  'namespace',
  'operation_id',
  ...RECOVERY_SELECTOR_UUID_KEYS,
  ...RECOVERY_SELECTOR_DECIMAL_KEYS,
]);

const EXPECTED = Object.freeze({
  source_count: 305,
  mapping_count: 1,
  support_count: 2,
  process_count: 2,
  rewrite_count: 2,
  pending_occurrence_count: 2,
  orphan_count: 303,
});

const PLACEHOLDERS = Object.freeze([
  'ACTOR_UUID_SQL',
  'ACTOR_EMAIL_SQL',
  'FOREIGN_UUID_SQL',
  'FOREIGN_EMAIL_SQL',
  'SCENARIO_NAMESPACE_SQL',
  'REQUEST_ID_SQL',
  'PREVIEW_REF_SQL',
  'PREVIEW_URL_SQL',
  'SERVICE_ROLE_KEY_SQL',
]);
const RESIDUE_PLACEHOLDERS = Object.freeze([
  'ACTOR_UUID_SQL',
  'FOREIGN_UUID_SQL',
  'SCENARIO_NAMESPACE_SQL',
  'REQUEST_ID_SQL',
  'PREVIEW_REF_SQL',
  'OPERATION_ID_SQL',
  'CAPTURE_RECEIPT_IDS_JSON_SQL',
  'SCOPE_IDS_JSON_SQL',
  'DERIVATIVE_REQUEST_IDS_JSON_SQL',
  'DERIVATIVE_BATCH_IDS_JSON_SQL',
  'HTTP_REQUEST_IDS_JSON_SQL',
  'FIXTURE_BACKEND_PIDS_JSON_SQL',
]);
const RESIDUE_COUNT_NAMES = Object.freeze([
  'auth_users', 'auth_identities', 'auth_sessions', 'auth_refresh_tokens',
  'capture_receipts', 'capture_source_guards', 'capture_target_guards',
  'capture_support_guards', 'capture_mapping_guards',
  'capture_process_intents', 'flow_identity_scopes',
  'flow_identity_mappings', 'flow_identity_process_ledger',
  'flow_identity_mutation_permits', 'flow_identity_wrapper_invocations',
  'derivative_requests', 'derivative_proposals', 'derivative_permits',
  'dataset_extraction_queue', 'dataset_extraction_archive',
  'dataset_extraction_failures', 'embedding_queue', 'embedding_archive',
  'pending_embedding_jobs', 'embedding_failures', 'pg_net_request_queue',
  'pg_net_responses', 'fixture_manifest_audits', 'scenario_command_audits',
  'processes', 'flows', 'flowproperties', 'unitgroups', 'vault_project_url',
  'vault_project_secret_key', 'fault_trigger', 'fault_function',
  'active_fixture_sessions', 'retained_fixture_sessions',
]);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
const IMAGE_ID_RE = /^sha256:[0-9a-f]{64}$/;
const REF_RE = /^[a-z0-9]{15,64}$/;
const NAMESPACE_RE = /^fie2e-hosted-[0-9a-f]{24}$/;
const SAFE_CODE_RE = /^[A-Z][A-Z0-9_]{2,127}$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PERMIT_SCHEMA = 'dataset-flow-identity-execution-permit.v1';
const ALLOWED_RACE_REJECTIONS = new Set([
  'FLOW_IDENTITY_WRAPPER_PERMIT_REQUIRED',
  'FLOW_IDENTITY_PROCESS_SCOPE_BUSY',
  'FLOW_IDENTITY_PROCESS_LOCK_BUSY',
]);

class SafeError extends Error {
  constructor(code, details = {}) {
    super(code);
    this.name = 'SafeError';
    this.code = code;
    this.details = sanitizeEvidence(details);
  }
}

function fail(code, details = {}) {
  throw new SafeError(code, details);
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function assertCondition(condition, code, details = {}) {
  if (!condition) fail(code, details);
}

function requireString(value, code, pattern = null) {
  assertCondition(typeof value === 'string' && value.length > 0, code);
  if (pattern) assertCondition(pattern.test(value), code);
  return value;
}

function requireUuid(value, code) {
  return requireString(value, code, UUID_RE).toLowerCase();
}

function requireSha(value, code) {
  return requireString(value, code, SHA256_RE);
}

function requireInteger(value, expected, code) {
  assertCondition(Number.isSafeInteger(value) && value === expected, code, {
    expected,
    observed: Number.isSafeInteger(value) ? value : null,
  });
}

function requireDecimalId(value, code) {
  return requireString(value, code, /^(?:0|[1-9][0-9]*)$/);
}

function optionalDecimalId(value, code) {
  if (value === null || value === undefined) return null;
  return requireDecimalId(value, code);
}

function sha256(value) {
  const input = Buffer.isBuffer(value)
    ? value
    : typeof value === 'string'
      ? value
      : canonicalJson(value);
  return createHash('sha256').update(input).digest('hex');
}

function isArrayIndexKey(key) {
  return /^(0|[1-9][0-9]{0,9})$/.test(key)
    && Number(key) <= 4_294_967_294;
}

function canonicalKeyCompare(left, right) {
  const leftIndex = isArrayIndexKey(left);
  const rightIndex = isArrayIndexKey(right);
  if (leftIndex && rightIndex) return Number(left) - Number(right);
  if (leftIndex) return -1;
  if (rightIndex) return 1;
  return left < right ? -1 : left > right ? 1 : 0;
}

function assertRestrictedSafeJson(value, pathName = '$') {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') {
    return;
  }
  if (typeof value === 'number') {
    assertCondition(Number.isSafeInteger(value), 'RESTRICTED_JSON_NUMBER_INVALID', {
      path_sha256: sha256(pathName),
    });
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertRestrictedSafeJson(item, `${pathName}.${index}`));
    return;
  }
  assertCondition(isPlainObject(value), 'RESTRICTED_JSON_VALUE_INVALID', {
    path_sha256: sha256(pathName),
  });
  for (const [key, item] of Object.entries(value)) {
    assertRestrictedSafeJson(item, `${pathName}.${key}`);
  }
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (isPlainObject(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort(canonicalKeyCompare)
        .map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}

function canonicalJson(value) {
  assertRestrictedSafeJson(value);
  return JSON.stringify(canonicalize(value));
}

function safeRemoteCode(value) {
  return typeof value === 'string' && SAFE_CODE_RE.test(value) ? value : null;
}

function sanitizeEvidence(value) {
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    if (
      SHA256_RE.test(value)
      || SAFE_CODE_RE.test(value)
      || /^(passed|failed|preview|completed|pending|sealed|running|true|false)$/.test(value)
      || /^[a-z0-9_.-]{1,80}$/.test(value)
    ) return value;
    return `sha256:${sha256(value)}`;
  }
  if (Array.isArray(value)) return value.map(sanitizeEvidence);
  if (isPlainObject(value)) {
    const safe = {};
    for (const [key, item] of Object.entries(value)) {
      if (/email|password|token|secret|authorization|apikey|url|connection|permit/i.test(key)) {
        continue;
      }
      safe[key] = sanitizeEvidence(item);
    }
    return safe;
  }
  return null;
}

function createRecoverySelectors(context) {
  return {
    schema_version: 'protected-flow-identity-recovery-selectors.v2',
    request_id: context.requestId,
    namespace: context.namespace,
    operation_id: `preview-flow-identity-${context.requestId}`,
    actor_user_ids: [],
    support_entity_ids: [],
    process_ids: [],
    receipt_ids: [],
    scope_ids: [],
    wrapper_invocation_ids: [],
    derivative_batch_ids: [],
    derivative_request_ids: [],
    derivative_proposal_ids: [],
    http_request_ids: [],
    embedding_pending_job_ids: [],
    embedding_queue_msg_ids: [],
    audit_ids: [],
    fixture_backend_pids: [],
  };
}

function addSelector(selectors, key, value) {
  if (value === null || value === undefined) return;
  assertCondition(Array.isArray(selectors[key]), 'RECOVERY_SELECTOR_KEY_INVALID');
  if (!selectors[key].includes(value)) selectors[key].push(value);
}

function normalizedRecoverySelectors(selectors) {
  return Object.fromEntries(Object.entries(selectors).map(([key, value]) => [
    key,
    Array.isArray(value) ? [...new Set(value)].sort() : value,
  ]));
}

async function validatePrivateTempDir(privateTempDir) {
  requireString(privateTempDir, 'PRIVATE_TEMP_DIR_INVALID');
  assertCondition(path.isAbsolute(privateTempDir), 'PRIVATE_TEMP_DIR_NOT_ABSOLUTE');
  assertCondition(path.normalize(privateTempDir) === privateTempDir, 'PRIVATE_TEMP_DIR_NOT_CANONICAL');
  let stat;
  let resolved;
  let entries;
  try {
    stat = await lstat(privateTempDir);
    resolved = await realpath(privateTempDir);
    entries = await readdir(privateTempDir);
  } catch {
    fail('PRIVATE_TEMP_DIR_READ_FAILED');
  }
  assertCondition(
    stat.isDirectory() && !stat.isSymbolicLink(),
    'PRIVATE_TEMP_DIR_TYPE_INVALID',
  );
  assertCondition(resolved === privateTempDir, 'PRIVATE_TEMP_DIR_NOT_CANONICAL');
  assertCondition((stat.mode & 0o777) === 0o700, 'PRIVATE_TEMP_DIR_MODE_INVALID');
  if (typeof process.getuid === 'function') {
    assertCondition(stat.uid === process.getuid(), 'PRIVATE_TEMP_DIR_OWNER_INVALID');
  }
  assertCondition(entries.length === 0, 'PRIVATE_TEMP_DIR_NOT_EMPTY');
  return privateTempDir;
}

function assertExactKeys(value, expectedKeys, code) {
  assertCondition(isPlainObject(value), code);
  const observed = Object.keys(value).sort();
  const expected = [...expectedKeys].sort();
  assertCondition(
    observed.length === expected.length
      && observed.every((key, index) => key === expected[index]),
    code,
    { key_set_sha256: sha256(observed) },
  );
}

function validateRecoveryCheckpointPayload(payload, requestId, namespace) {
  assertExactKeys(
    payload,
    ['actors', 'recovery_selectors'],
    'RECOVERY_CHECKPOINT_PAYLOAD_SCHEMA_INVALID',
  );
  assertCondition(
    Array.isArray(payload.actors) && payload.actors.length <= 2,
    'RECOVERY_CHECKPOINT_ACTOR_SET_INVALID',
  );
  const expectedRoles = ['owner', 'foreign'];
  for (const [index, actor] of payload.actors.entries()) {
    assertExactKeys(
      actor,
      RECOVERY_CHECKPOINT_ACTOR_KEYS,
      'RECOVERY_CHECKPOINT_ACTOR_SCHEMA_INVALID',
    );
    const expectedRole = expectedRoles[index];
    assertCondition(actor.role === expectedRole, 'RECOVERY_CHECKPOINT_ACTOR_ROLE_INVALID');
    const expectedEmail = `flow-identity-${expectedRole}-${namespace.slice('fie2e-hosted-'.length)}@example.invalid`;
    assertCondition(
      typeof actor.email === 'string'
        && actor.email.length <= 320
        && EMAIL_RE.test(actor.email)
        && actor.email === expectedEmail,
      'RECOVERY_CHECKPOINT_ACTOR_EMAIL_INVALID',
    );
    assertCondition(
      actor.fixture_sha256 === sha256('database-engine#269'),
      'RECOVERY_CHECKPOINT_ACTOR_FIXTURE_INVALID',
    );
    assertCondition(
      actor.request_id === requestId && actor.namespace === namespace,
      'RECOVERY_CHECKPOINT_ACTOR_BINDING_INVALID',
    );
    assertCondition(
      actor.user_id === null || (typeof actor.user_id === 'string' && UUID_RE.test(actor.user_id)),
      'RECOVERY_CHECKPOINT_ACTOR_ID_INVALID',
    );
    assertCondition(
      typeof actor.create_requested === 'boolean'
        && typeof actor.actor_bound_checkpoint_acknowledged === 'boolean',
      'RECOVERY_CHECKPOINT_ACTOR_STATE_INVALID',
    );
    assertCondition(
      (actor.user_id === null || actor.create_requested)
        && (!actor.actor_bound_checkpoint_acknowledged || actor.user_id !== null),
      'RECOVERY_CHECKPOINT_ACTOR_STATE_INVALID',
    );
    assertCondition(
      actor.locator_sha256 === sha256(actor.email),
      'RECOVERY_CHECKPOINT_ACTOR_LOCATOR_INVALID',
    );
  }

  const selectors = payload.recovery_selectors;
  assertExactKeys(
    selectors,
    RECOVERY_SELECTOR_KEYS,
    'RECOVERY_CHECKPOINT_SELECTOR_SCHEMA_INVALID',
  );
  assertCondition(
    selectors.schema_version === 'protected-flow-identity-recovery-selectors.v2'
      && selectors.request_id === requestId
      && selectors.namespace === namespace
      && selectors.operation_id === `preview-flow-identity-${requestId}`,
    'RECOVERY_CHECKPOINT_SELECTOR_BINDING_INVALID',
  );
  for (const key of RECOVERY_SELECTOR_UUID_KEYS) {
    const values = selectors[key];
    assertCondition(
      Array.isArray(values)
        && values.length <= 200
        && values.every((value) => typeof value === 'string' && UUID_RE.test(value))
        && values.every((value, index) => index === 0 || values[index - 1] < value),
      'RECOVERY_CHECKPOINT_SELECTOR_UUID_SET_INVALID',
      { selector_key_sha256: sha256(key) },
    );
  }
  for (const key of RECOVERY_SELECTOR_DECIMAL_KEYS) {
    const values = selectors[key];
    assertCondition(
      Array.isArray(values)
        && values.length <= 200
        && values.every((value) => typeof value === 'string' && /^(?:0|[1-9][0-9]*)$/.test(value))
        && values.every((value, index) => index === 0 || values[index - 1] < value),
      'RECOVERY_CHECKPOINT_SELECTOR_DECIMAL_SET_INVALID',
      { selector_key_sha256: sha256(key) },
    );
  }
  return payload;
}

function createRecoveryCheckpointClient({
  requestId,
  namespace,
  ipc = process,
  timeoutMs = RECOVERY_CHECKPOINT_TIMEOUT_MS,
} = {}) {
  requireUuid(requestId, 'RECOVERY_CHECKPOINT_REQUEST_INVALID');
  requireString(namespace, 'RECOVERY_CHECKPOINT_NAMESPACE_INVALID', NAMESPACE_RE);
  assertCondition(
    ipc?.connected === true
      && typeof ipc.send === 'function'
      && typeof ipc.on === 'function'
      && typeof ipc.off === 'function',
    'RECOVERY_CHECKPOINT_IPC_UNAVAILABLE',
  );
  assertCondition(
    Number.isInteger(timeoutMs) && timeoutMs >= 100 && timeoutMs <= 60_000,
    'RECOVERY_CHECKPOINT_TIMEOUT_INVALID',
  );
  let sequence = 0;
  let previousSha256 = sha256('protected-flow-identity-recovery-checkpoint-genesis.v1');
  let inFlight = false;

  const checkpoint = async (stage, payload) => {
    requireString(stage, 'RECOVERY_CHECKPOINT_STAGE_INVALID', /^[a-z][a-z0-9_]{2,63}$/);
    assertCondition(
      RECOVERY_CHECKPOINT_STAGES.has(stage),
      'RECOVERY_CHECKPOINT_STAGE_FORBIDDEN',
    );
    assertCondition(!inFlight, 'RECOVERY_CHECKPOINT_CONCURRENT');
    validateRecoveryCheckpointPayload(payload, requestId, namespace);
    const nextSequence = sequence + 1;
    const framePayload = {
      schema_version: RECOVERY_CHECKPOINT_SCHEMA,
      sequence: nextSequence,
      previous_sha256: previousSha256,
      request_id: requestId,
      namespace,
      stage,
      payload,
    };
    const frame = {
      ...framePayload,
      frame_sha256: sha256(canonicalJson(framePayload)),
    };
    const message = {
      type: 'protected-flow-identity-recovery-checkpoint',
      frame,
    };
    assertCondition(
      Buffer.byteLength(canonicalJson(message)) <= RECOVERY_CHECKPOINT_MAX_BYTES,
      'RECOVERY_CHECKPOINT_TOO_LARGE',
    );
    inFlight = true;
    try {
      await new Promise((resolve, reject) => {
        let settled = false;
        const finish = (error = null) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          ipc.off('message', onMessage);
          ipc.off('disconnect', onDisconnect);
          if (error) reject(error);
          else resolve();
        };
        const onMessage = (ack) => {
          if (ack?.type !== 'protected-flow-identity-recovery-checkpoint-ack') return;
          try {
            assertExactKeys(
              ack,
              ['type', 'schema_version', 'sequence', 'frame_sha256'],
              'RECOVERY_CHECKPOINT_ACK_INVALID',
            );
          } catch {
            finish(new SafeError('RECOVERY_CHECKPOINT_ACK_INVALID'));
            return;
          }
          if (
            ack.schema_version !== RECOVERY_CHECKPOINT_ACK_SCHEMA
              || ack.sequence !== nextSequence
              || ack.frame_sha256 !== frame.frame_sha256
          ) {
            finish(new SafeError('RECOVERY_CHECKPOINT_ACK_INVALID'));
            return;
          }
          finish();
        };
        const onDisconnect = () => finish(
          new SafeError('RECOVERY_CHECKPOINT_IPC_DISCONNECTED'),
        );
        const timer = setTimeout(
          () => finish(new SafeError('RECOVERY_CHECKPOINT_ACK_TIMEOUT')),
          timeoutMs,
        );
        timer.unref();
        ipc.on('message', onMessage);
        ipc.on('disconnect', onDisconnect);
        try {
          ipc.send(message, (error) => {
            if (error) finish(new SafeError('RECOVERY_CHECKPOINT_SEND_FAILED'));
          });
        } catch {
          finish(new SafeError('RECOVERY_CHECKPOINT_SEND_FAILED'));
        }
      });
      sequence = nextSequence;
      previousSha256 = frame.frame_sha256;
      return frame;
    } finally {
      inFlight = false;
    }
  };

  return Object.freeze({
    checkpoint,
    summary: () => ({
      checkpoint_count: sequence,
      last_checkpoint_sha256: previousSha256,
    }),
  });
}

function parseBoundedInteger(value, fallback, minimum, maximum, code) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  assertCondition(Number.isInteger(parsed) && parsed >= minimum && parsed <= maximum, code);
  return parsed;
}

function parseArgs(argv) {
  if (argv.length === 1 && argv[0] === '--race-worker') {
    return {
      raceWorker: true, transportPreflightOnly: false, help: false,
      expectedPreviewRef: null, scenarioNamespace: null, requestId: null,
      privateTempDir: null, recoveryIpc: false,
    };
  }
  if (argv.includes('--help') || argv.includes('-h')) {
    return {
      raceWorker: false, transportPreflightOnly: false, help: true,
      expectedPreviewRef: null, scenarioNamespace: null, requestId: null,
      privateTempDir: null, recoveryIpc: false,
    };
  }
  let expectedPreviewRef = null;
  let transportPreflightOnly = false;
  let scenarioNamespace = null;
  let requestId = null;
  let privateTempDir = null;
  let recoveryIpc = false;
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === '--expected-preview-ref' && index + 1 < argv.length) {
      expectedPreviewRef = argv[index + 1];
      index += 1;
    } else if (argv[index] === '--transport-preflight-only') {
      assertCondition(!transportPreflightOnly, 'ARG_TRANSPORT_PREFLIGHT_DUPLICATE');
      transportPreflightOnly = true;
    } else if (argv[index] === '--scenario-namespace' && index + 1 < argv.length) {
      assertCondition(scenarioNamespace === null, 'ARG_SCENARIO_NAMESPACE_DUPLICATE');
      scenarioNamespace = argv[index + 1];
      index += 1;
    } else if (argv[index] === '--request-id' && index + 1 < argv.length) {
      assertCondition(requestId === null, 'ARG_REQUEST_ID_DUPLICATE');
      requestId = argv[index + 1];
      index += 1;
    } else if (argv[index] === '--private-temp-dir' && index + 1 < argv.length) {
      assertCondition(privateTempDir === null, 'ARG_PRIVATE_TEMP_DIR_DUPLICATE');
      privateTempDir = argv[index + 1];
      index += 1;
    } else if (argv[index] === '--recovery-ipc') {
      assertCondition(!recoveryIpc, 'ARG_RECOVERY_IPC_DUPLICATE');
      recoveryIpc = true;
    } else {
      fail('ARG_UNKNOWN');
    }
  }
  requireString(expectedPreviewRef, 'ARG_EXPECTED_PREVIEW_REF_REQUIRED', REF_RE);
  if (transportPreflightOnly) {
    assertCondition(
      scenarioNamespace === null && requestId === null
        && privateTempDir === null && recoveryIpc === false,
      'ARG_TRANSPORT_PREFLIGHT_SCOPE_INVALID',
    );
  } else {
    requireString(
      scenarioNamespace,
      'ARG_SCENARIO_NAMESPACE_REQUIRED',
      NAMESPACE_RE,
    );
    requireUuid(requestId, 'ARG_REQUEST_ID_REQUIRED');
    requireString(privateTempDir, 'ARG_PRIVATE_TEMP_DIR_REQUIRED');
    assertCondition(path.isAbsolute(privateTempDir), 'ARG_PRIVATE_TEMP_DIR_NOT_ABSOLUTE');
    assertCondition(recoveryIpc, 'ARG_RECOVERY_IPC_REQUIRED');
  }
  return {
    raceWorker: false,
    transportPreflightOnly,
    help: false,
    expectedPreviewRef,
    scenarioNamespace,
    requestId,
    privateTempDir,
    recoveryIpc,
  };
}

function helpText() {
  return [
    'Usage: node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --expected-preview-ref <ref> --scenario-namespace <fie2e-hosted-24hex> --request-id <uuid> --private-temp-dir <absolute-0700-empty-dir> --recovery-ipc',
    '       node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --transport-preflight-only --expected-preview-ref <ref>',
    '',
    'Required environment:',
    '  PREVIEW_ENVIRONMENT=preview',
    '  PREVIEW_PROJECT_REF=<same exact ref>',
    '  PREVIEW_SUPABASE_URL=https://<ref>.supabase.co',
    '  PREVIEW_SUPABASE_ANON_KEY=<Preview anon/publishable key>',
    '  PREVIEW_SUPABASE_SERVICE_ROLE_KEY=<Preview service role/secret key>',
    '  PREVIEW_DB_URL=<percent-encoded Preview PostgreSQL URL>',
    '  PREVIEW_SUPABASE_CLI_PATH=<exact absolute native Supabase CLI path>',
    '  PREVIEW_SUPABASE_CLI_SHA256=<exact native Supabase CLI file SHA-256>',
    '  PREVIEW_DOCKER_CLI_PATH=<exact absolute regular docker executable path>',
    '  PREVIEW_DOCKER_CLI_SHA256=<exact docker executable file SHA-256>',
    `  PREVIEW_PG_PROVE_IMAGE_REF=${PG_PROVE_IMAGE_REF}`,
    '  PREVIEW_PG_PROVE_IMAGE_ID=<exact cached sha256: image ID>',
    '  PREVIEW_PG_PROVE_IMAGE_REPO_DIGEST=<exact cached repository@sha256: digest>',
    `  PREVIEW_PG_PROVE_IMAGE_PLATFORM=${PG_PROVE_IMAGE_PLATFORM}`,
    '',
    'Optional bounded timings:',
    '  PREVIEW_RPC_TIMEOUT_MS (default 90000)',
    '  PREVIEW_DB_TEST_TIMEOUT_MS (default 180000)',
    '  PREVIEW_RACE_TIMEOUT_MS (default 120000)',
  ].join('\n');
}

function envRequired(name) {
  const value = process.env[name];
  assertCondition(typeof value === 'string' && value.length > 0, `ENV_${name}_REQUIRED`);
  return value;
}

function urlContainsExactRef(databaseUrl, expectedRef) {
  try {
    const parsed = new URL(databaseUrl);
    if (!['postgres:', 'postgresql:'].includes(parsed.protocol)) return false;
    const username = decodeURIComponent(parsed.username).toLowerCase();
    return parsed.hostname.toLowerCase().split('.').includes(expectedRef)
      || username.split('.').includes(expectedRef);
  } catch {
    return false;
  }
}

function loadConfig(expectedPreviewRef) {
  const environment = envRequired('PREVIEW_ENVIRONMENT');
  const projectRef = envRequired('PREVIEW_PROJECT_REF');
  const urlText = envRequired('PREVIEW_SUPABASE_URL');
  const anonKey = envRequired('PREVIEW_SUPABASE_ANON_KEY');
  const serviceRoleKey = envRequired('PREVIEW_SUPABASE_SERVICE_ROLE_KEY');
  const dbUrl = envRequired('PREVIEW_DB_URL');
  const supabaseCliPath = envRequired('PREVIEW_SUPABASE_CLI_PATH');
  const supabaseCliSha256 = requireSha(
    envRequired('PREVIEW_SUPABASE_CLI_SHA256'),
    'PREVIEW_SUPABASE_CLI_SHA256_INVALID',
  );
  const dockerCliPath = envRequired('PREVIEW_DOCKER_CLI_PATH');
  const dockerCliSha256 = requireSha(
    envRequired('PREVIEW_DOCKER_CLI_SHA256'),
    'PREVIEW_DOCKER_CLI_SHA256_INVALID',
  );
  const pgProveImageRef = envRequired('PREVIEW_PG_PROVE_IMAGE_REF');
  const pgProveImageId = requireString(
    envRequired('PREVIEW_PG_PROVE_IMAGE_ID'),
    'PREVIEW_PG_PROVE_IMAGE_ID_INVALID',
    IMAGE_ID_RE,
  );
  const pgProveImageRepoDigest = envRequired('PREVIEW_PG_PROVE_IMAGE_REPO_DIGEST');
  const pgProveImagePlatform = envRequired('PREVIEW_PG_PROVE_IMAGE_PLATFORM');
  const childPath = envRequired('PATH');

  assertCondition(environment === 'preview', 'PREVIEW_ENVIRONMENT_MISMATCH');
  assertCondition(projectRef === expectedPreviewRef, 'PREVIEW_REF_ENV_ARG_MISMATCH');
  assertCondition(projectRef !== PRODUCTION_REF, 'PRODUCTION_REF_FORBIDDEN');
  assertCondition(REF_RE.test(projectRef), 'PREVIEW_REF_INVALID');
  assertCondition(anonKey !== serviceRoleKey, 'PREVIEW_KEYS_MUST_DIFFER');
  assertCondition(anonKey.length >= 20 && serviceRoleKey.length >= 20, 'PREVIEW_KEY_INVALID');

  let parsed;
  try {
    parsed = new URL(urlText);
  } catch {
    fail('PREVIEW_SUPABASE_URL_INVALID');
  }
  assertCondition(parsed.protocol === 'https:', 'PREVIEW_SUPABASE_URL_NOT_HTTPS');
  assertCondition(parsed.hostname === `${projectRef}.supabase.co`, 'PREVIEW_SUPABASE_URL_REF_MISMATCH');
  assertCondition(parsed.pathname === '/' && !parsed.search && !parsed.hash, 'PREVIEW_SUPABASE_URL_PATH_INVALID');
  assertCondition(urlContainsExactRef(dbUrl, projectRef), 'PREVIEW_DB_URL_REF_MISMATCH');
  assertCondition(path.isAbsolute(supabaseCliPath), 'PREVIEW_SUPABASE_CLI_PATH_NOT_ABSOLUTE');
  assertCondition(path.isAbsolute(dockerCliPath), 'PREVIEW_DOCKER_CLI_PATH_NOT_ABSOLUTE');
  assertCondition(path.basename(dockerCliPath) === 'docker', 'PREVIEW_DOCKER_CLI_BASENAME_INVALID');
  const childPathEntries = childPath.split(path.delimiter);
  assertCondition(
    childPathEntries.length > 0
      && childPathEntries.every((entry) => entry.length > 0 && path.isAbsolute(entry)),
    'PREVIEW_CHILD_PATH_INVALID',
  );
  assertCondition(
    childPathEntries[0] === path.dirname(dockerCliPath),
    'PREVIEW_DOCKER_CLI_NOT_FIRST_IN_PATH',
  );
  assertCondition(dockerCliPath !== supabaseCliPath, 'PREVIEW_EXECUTABLE_PATHS_MUST_DIFFER');
  assertCondition(pgProveImageRef === PG_PROVE_IMAGE_REF, 'PREVIEW_PG_PROVE_IMAGE_REF_MISMATCH');
  assertCondition(
    pgProveImageRepoDigest === `${PG_PROVE_IMAGE_REPOSITORY}@${pgProveImageId}`,
    'PREVIEW_PG_PROVE_IMAGE_REPO_DIGEST_INVALID',
  );
  assertCondition(
    pgProveImagePlatform === PG_PROVE_IMAGE_PLATFORM,
    'PREVIEW_PG_PROVE_IMAGE_PLATFORM_MISMATCH',
  );

  return Object.freeze({
    environment,
    projectRef,
    supabaseUrl: parsed.origin,
    anonKey,
    serviceRoleKey,
    dbUrl,
    supabaseCliPath,
    supabaseCliSha256,
    dockerCliPath,
    dockerCliSha256,
    pgProveImageRef,
    pgProveImageId,
    pgProveImageRepoDigest,
    pgProveImagePlatform,
    rpcTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_RPC_TIMEOUT_MS, 90_000, 5_000, 180_000,
      'PREVIEW_RPC_TIMEOUT_INVALID',
    ),
    dbTestTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_DB_TEST_TIMEOUT_MS, 180_000, 10_000, 300_000,
      'PREVIEW_DB_TEST_TIMEOUT_INVALID',
    ),
    raceTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_RACE_TIMEOUT_MS, 120_000, 5_000, 180_000,
      'PREVIEW_RACE_TIMEOUT_INVALID',
    ),
  });
}

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function validateTemplateValue(name, value) {
  if (['ACTOR_UUID_SQL', 'FOREIGN_UUID_SQL', 'REQUEST_ID_SQL'].includes(name)) {
    requireUuid(value, `TEMPLATE_${name}_INVALID`);
  } else if (['ACTOR_EMAIL_SQL', 'FOREIGN_EMAIL_SQL'].includes(name)) {
    assertCondition(
      typeof value === 'string' && value.length <= 320 && EMAIL_RE.test(value)
        && !/[\u0000-\u001f\u007f]/.test(value),
      `TEMPLATE_${name}_INVALID`,
    );
  } else if (name === 'SCENARIO_NAMESPACE_SQL') {
    requireString(value, `TEMPLATE_${name}_INVALID`, NAMESPACE_RE);
  } else if (name === 'PREVIEW_REF_SQL') {
    requireString(value, `TEMPLATE_${name}_INVALID`, REF_RE);
    assertCondition(value !== PRODUCTION_REF, `TEMPLATE_${name}_INVALID`);
  } else if (name === 'PREVIEW_URL_SQL') {
    let parsed;
    try { parsed = new URL(value); } catch { fail(`TEMPLATE_${name}_INVALID`); }
    assertCondition(
      parsed.protocol === 'https:' && parsed.pathname === '/'
        && !parsed.search && !parsed.hash
        && /^[a-z0-9]{15,64}\.supabase\.co$/.test(parsed.hostname),
      `TEMPLATE_${name}_INVALID`,
    );
  } else if (name === 'SERVICE_ROLE_KEY_SQL') {
    assertCondition(
      typeof value === 'string' && value.length >= 20 && value.length <= 16_384
        && !/\s/.test(value),
      `TEMPLATE_${name}_INVALID`,
    );
  } else {
    fail('TEMPLATE_UNKNOWN_PLACEHOLDER');
  }
}

async function renderSqlTemplate(templatePath, values) {
  let source;
  try { source = await readFile(templatePath, 'utf8'); } catch {
    fail('SQL_TEMPLATE_READ_FAILED', { template_sha256: sha256(path.basename(templatePath)) });
  }
  let rendered = source;
  for (const placeholder of PLACEHOLDERS) {
    const marker = `{{${placeholder}}}`;
    assertCondition(rendered.includes(marker), 'SQL_TEMPLATE_PLACEHOLDER_MISSING', {
      placeholder_sha256: sha256(placeholder),
    });
    validateTemplateValue(placeholder, values[placeholder]);
    rendered = rendered.replaceAll(marker, sqlLiteral(values[placeholder]));
  }
  assertCondition(!/{{[A-Z0-9_]+}}/.test(rendered), 'SQL_TEMPLATE_UNRESOLVED_PLACEHOLDER');
  return {
    sql: rendered,
    templateSha256: sha256(source),
    renderedSha256: sha256(rendered),
  };
}

function validateResidueJsonSelector(name, value, maximum, pattern) {
  let parsed;
  try { parsed = JSON.parse(value); } catch { fail(`RESIDUE_${name}_JSON_INVALID`); }
  assertCondition(
    Array.isArray(parsed)
      && parsed.length <= maximum
      && new Set(parsed).size === parsed.length
      && parsed.every((item) => typeof item === 'string' && pattern.test(item)),
    `RESIDUE_${name}_INVALID`,
  );
}

function validateResidueTemplateValue(name, value, context) {
  if (['ACTOR_UUID_SQL', 'FOREIGN_UUID_SQL', 'REQUEST_ID_SQL'].includes(name)) {
    requireUuid(value, `RESIDUE_${name}_INVALID`);
  } else if (name === 'SCENARIO_NAMESPACE_SQL') {
    requireString(value, `RESIDUE_${name}_INVALID`, NAMESPACE_RE);
  } else if (name === 'PREVIEW_REF_SQL') {
    requireString(value, `RESIDUE_${name}_INVALID`, REF_RE);
    assertCondition(value !== PRODUCTION_REF, `RESIDUE_${name}_INVALID`);
  } else if (name === 'OPERATION_ID_SQL') {
    assertCondition(
      value === `preview-flow-identity-${context.requestId}`,
      'RESIDUE_OPERATION_ID_INVALID',
    );
  } else if (name === 'CAPTURE_RECEIPT_IDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 1, UUID_RE);
  } else if (name === 'SCOPE_IDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 1, UUID_RE);
  } else if (name === 'DERIVATIVE_REQUEST_IDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 2, UUID_RE);
  } else if (name === 'DERIVATIVE_BATCH_IDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 2, UUID_RE);
  } else if (name === 'HTTP_REQUEST_IDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 2, /^[1-9][0-9]{0,18}$/);
  } else if (name === 'FIXTURE_BACKEND_PIDS_JSON_SQL') {
    validateResidueJsonSelector(name, value, 64, /^[1-9][0-9]{0,9}$/);
  } else {
    fail('RESIDUE_TEMPLATE_UNKNOWN_PLACEHOLDER');
  }
}

async function renderResidueReadback(context, selectors) {
  const normalized = normalizedRecoverySelectors(selectors);
  const values = {
    ACTOR_UUID_SQL: context.owner.userId,
    FOREIGN_UUID_SQL: context.foreign.userId,
    SCENARIO_NAMESPACE_SQL: context.namespace,
    REQUEST_ID_SQL: context.requestId,
    PREVIEW_REF_SQL: context.config.projectRef,
    OPERATION_ID_SQL: normalized.operation_id,
    CAPTURE_RECEIPT_IDS_JSON_SQL: canonicalJson(normalized.receipt_ids),
    SCOPE_IDS_JSON_SQL: canonicalJson(normalized.scope_ids),
    DERIVATIVE_REQUEST_IDS_JSON_SQL: canonicalJson(
      normalized.derivative_request_ids,
    ),
    DERIVATIVE_BATCH_IDS_JSON_SQL: canonicalJson(normalized.derivative_batch_ids),
    HTTP_REQUEST_IDS_JSON_SQL: canonicalJson(normalized.http_request_ids),
    FIXTURE_BACKEND_PIDS_JSON_SQL: canonicalJson(
      normalized.fixture_backend_pids ?? [],
    ),
  };
  let source;
  try { source = await readFile(RESIDUE_READBACK_TEMPLATE, 'utf8'); } catch {
    fail('RESIDUE_TEMPLATE_READ_FAILED', {
      template_path_sha256: sha256(path.basename(RESIDUE_READBACK_TEMPLATE)),
    });
  }
  let rendered = source;
  for (const placeholder of RESIDUE_PLACEHOLDERS) {
    const marker = `{{${placeholder}}}`;
    assertCondition(rendered.includes(marker), 'RESIDUE_TEMPLATE_PLACEHOLDER_MISSING', {
      placeholder_sha256: sha256(placeholder),
    });
    validateResidueTemplateValue(placeholder, values[placeholder], context);
    rendered = rendered.replaceAll(marker, sqlLiteral(values[placeholder]));
  }
  assertCondition(
    !/{{[A-Z0-9_]+}}/.test(rendered),
    'RESIDUE_TEMPLATE_UNRESOLVED_PLACEHOLDER',
  );
  return {
    sql: rendered,
    templateSha256: sha256(source),
    renderedSha256: sha256(rendered),
  };
}

function parseResidueCounts(stdoutText) {
  const matches = [
    ...stdoutText.matchAll(/^# residue_counts=(\{[^\r\n]+\})\r?$/gm),
  ];
  assertCondition(matches.length === 1, 'RESIDUE_COUNT_OUTPUT_INVALID', {
    match_count: matches.length,
  });
  let counts;
  try { counts = JSON.parse(matches[0][1]); } catch { fail('RESIDUE_COUNT_JSON_INVALID'); }
  assertCondition(isPlainObject(counts), 'RESIDUE_COUNT_JSON_INVALID');
  const names = Object.keys(counts).sort();
  assertCondition(
    canonicalJson(names) === canonicalJson([...RESIDUE_COUNT_NAMES].sort()),
    'RESIDUE_COUNT_NAME_SET_INVALID',
    { observed_name_set_sha256: sha256(names) },
  );
  for (const name of RESIDUE_COUNT_NAMES) {
    assertCondition(
      Number.isSafeInteger(counts[name]) && counts[name] === 0,
      'RESIDUE_COUNT_NONZERO',
      { count_name_sha256: sha256(name), observed: counts[name] },
    );
  }
  return counts;
}

function summarizeDbProof(proof) {
  return {
    sql_sha256: proof.sql_sha256,
    stdout_sha256: proof.stdout_sha256,
    stderr_sha256: proof.stderr_sha256,
    executable_path_sha256: proof.executable_path_sha256,
    executable_file_sha256: proof.executable_file_sha256,
    docker_executable_path_sha256: proof.docker_executable_path_sha256,
    docker_executable_file_sha256: proof.docker_executable_file_sha256,
    pg_prove_image_ref_sha256: proof.pg_prove_image_ref_sha256,
    pg_prove_image_id_sha256: proof.pg_prove_image_id_sha256,
    pg_prove_image_repo_digest_sha256: proof.pg_prove_image_repo_digest_sha256,
    pg_prove_image_platform_sha256: proof.pg_prove_image_platform_sha256,
    pg_prove_image_inspect_argv_sha256: proof.pg_prove_image_inspect_argv_sha256,
    pg_prove_image_inspect_stdout_sha256:
      proof.pg_prove_image_inspect_stdout_sha256,
    pg_prove_image_inspect_stderr_sha256:
      proof.pg_prove_image_inspect_stderr_sha256,
    child_env_sha256: proof.child_env_sha256,
    application_name_sha256: proof.application_name_sha256,
    working_directory_sha256: proof.working_directory_sha256,
    ...(proof.transport_target_sha256
      ? { transport_target_sha256: proof.transport_target_sha256 }
      : {}),
    ...(proof.transport_binding_sha256
      ? { transport_binding_sha256: proof.transport_binding_sha256 }
      : {}),
  };
}

function summarizeDbProofs(proofs) {
  return Object.fromEntries(
    Object.entries(proofs).map(([name, proof]) => [name, summarizeDbProof(proof)]),
  );
}

function dbDiagnosticCodes(stdoutText, stderrText, config) {
  let diagnostic = `${stdoutText}\n${stderrText}`;
  for (const sensitive of [
    config.dbUrl, config.supabaseUrl, config.anonKey,
    config.serviceRoleKey, config.projectRef, config.supabaseCliPath, config.dockerCliPath,
  ]) diagnostic = diagnostic.replaceAll(sensitive, ' REDACTED ');
  diagnostic = diagnostic
    .replace(/postgres(?:ql)?:\/\/\S+/gi, ' REDACTED ')
    .replace(/\beyJ[A-Za-z0-9._-]{20,}\b/g, ' REDACTED ')
    .replace(/\bsb_(?:publishable|secret)_[A-Za-z0-9_-]{10,}\b/g, ' REDACTED ')
    .replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, ' EMAIL ')
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi, ' UUID ')
    .replace(/'(?:''|[^'])*'/g, ' VALUE ')
    .replace(/"(?:""|[^"])*"/g, ' IDENT ');
  return [...new Set(diagnostic.split(/\r?\n/)
    .filter((line) => /(?:error|fatal|panic|not ok|failed|bail out|psql:)/i.test(line))
    .map((line) => `DB_DIAG_${line}`.toUpperCase()
      .replace(/\b\d+\b/g, 'N').replace(/[^A-Z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '').slice(0, 127))
    .filter((code) => SAFE_CODE_RE.test(code)))].slice(-12);
}

async function verifyBoundExecutable(filePath, expectedSha256, codePrefix) {
  const executablePathSha256 = sha256(filePath);
  let statBefore;
  let realPathBefore;
  try {
    statBefore = await lstat(filePath);
    realPathBefore = await realpath(filePath);
  } catch {
    fail(`${codePrefix}_READ_FAILED`, { executable_path_sha256: executablePathSha256 });
  }
  assertCondition(
    statBefore.isFile() && !statBefore.isSymbolicLink() && (statBefore.mode & 0o111) !== 0,
    `${codePrefix}_FILE_INVALID`,
    { executable_path_sha256: executablePathSha256 },
  );
  assertCondition(
    realPathBefore === filePath,
    `${codePrefix}_PATH_NOT_CANONICAL`,
    { executable_path_sha256: executablePathSha256 },
  );
  let executableBytes;
  try {
    executableBytes = await readFile(filePath);
  } catch {
    fail(`${codePrefix}_READ_FAILED`, { executable_path_sha256: executablePathSha256 });
  }
  const executableFileSha256 = sha256(executableBytes);
  assertCondition(
    executableFileSha256 === expectedSha256,
    `${codePrefix}_SHA256_MISMATCH`,
    {
      executable_path_sha256: executablePathSha256,
      expected_sha256: expectedSha256,
      observed_sha256: executableFileSha256,
    },
  );
  let statAfter;
  let realPathAfter;
  try {
    statAfter = await lstat(filePath);
    realPathAfter = await realpath(filePath);
  } catch {
    fail(`${codePrefix}_CHANGED_BEFORE_SPAWN`, {
      executable_path_sha256: executablePathSha256,
    });
  }
  assertCondition(
    realPathAfter === realPathBefore
      && statAfter.isFile()
      && !statAfter.isSymbolicLink()
      && statAfter.dev === statBefore.dev
      && statAfter.ino === statBefore.ino
      && statAfter.mode === statBefore.mode
      && statAfter.size === statBefore.size
      && statAfter.mtimeMs === statBefore.mtimeMs
      && statAfter.ctimeMs === statBefore.ctimeMs,
    `${codePrefix}_CHANGED_BEFORE_SPAWN`,
    { executable_path_sha256: executablePathSha256 },
  );
  return { executablePathSha256, executableFileSha256 };
}

async function inspectBoundPgProveImage(config, childEnv) {
  const args = [
    'image', 'inspect', config.pgProveImageRef,
    '--format', PG_PROVE_INSPECT_TEMPLATE,
  ];
  const stdoutHash = createHash('sha256');
  const stderrHash = createHash('sha256');
  const stdoutChunks = [];
  const stderrChunks = [];
  let stdoutBytes = 0;
  let stderrBytes = 0;
  let timedOut = false;
  let outputTooLarge = false;
  let spawnFailed = false;

  const result = await new Promise((resolve) => {
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const child = spawn(config.dockerCliPath, args, {
      cwd: REPO_ROOT, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'], shell: false,
    });
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      setTimeout(() => child.kill('SIGKILL'), 2_000).unref();
    }, DOCKER_INSPECT_TIMEOUT_MS);
    timeout.unref();
    const collect = (hash, chunks, kind) => (chunk) => {
      hash.update(chunk);
      if (kind === 'stdout') stdoutBytes += chunk.length;
      else stderrBytes += chunk.length;
      const retained = chunks.reduce((total, item) => total + item.length, 0);
      if (retained < 64 * 1024) chunks.push(chunk.subarray(0, 64 * 1024 - retained));
      if (stdoutBytes + stderrBytes > 64 * 1024 && !outputTooLarge) {
        outputTooLarge = true;
        child.kill('SIGTERM');
      }
    };
    child.stdout.on('data', collect(stdoutHash, stdoutChunks, 'stdout'));
    child.stderr.on('data', collect(stderrHash, stderrChunks, 'stderr'));
    child.once('error', () => {
      clearTimeout(timeout);
      spawnFailed = true;
      finish({ exitCode: null, signal: null });
    });
    child.once('close', (exitCode, signal) => {
      clearTimeout(timeout);
      finish({ exitCode, signal: signal ?? null });
    });
  });

  const stdout = Buffer.concat(stdoutChunks).toString('utf8');
  const stderr = Buffer.concat(stderrChunks).toString('utf8');
  const proof = {
    pg_prove_image_ref_sha256: sha256(config.pgProveImageRef),
    pg_prove_image_id_sha256: sha256(config.pgProveImageId),
    pg_prove_image_repo_digest_sha256: sha256(config.pgProveImageRepoDigest),
    pg_prove_image_platform_sha256: sha256(config.pgProveImagePlatform),
    pg_prove_image_inspect_argv_sha256: sha256(args),
    pg_prove_image_inspect_stdout_sha256: stdoutHash.digest('hex'),
    pg_prove_image_inspect_stderr_sha256: stderrHash.digest('hex'),
    pg_prove_image_inspect_stdout_bytes: stdoutBytes,
    pg_prove_image_inspect_stderr_bytes: stderrBytes,
  };
  if (
    spawnFailed || timedOut || outputTooLarge
    || result.exitCode !== 0 || result.signal !== null
  ) {
    fail('PREVIEW_PG_PROVE_IMAGE_INSPECT_FAILED', {
      ...proof,
      inspect_exit_code: result.exitCode,
      inspect_signal: result.signal,
      inspect_spawn_failed: spawnFailed,
      inspect_timed_out: timedOut,
      inspect_output_too_large: outputTooLarge,
    });
  }

  let inspected;
  try {
    inspected = JSON.parse(stdout.trim());
  } catch {
    fail('PREVIEW_PG_PROVE_IMAGE_IDENTITY_MISMATCH', {
      ...proof,
      inspect_output_json: false,
    });
  }
  const observedId = typeof inspected?.id === 'string' ? inspected.id : '<invalid>';
  const observedRepoDigests = Array.isArray(inspected?.repo_digests)
    ? inspected.repo_digests
    : ['<invalid>'];
  const observedPlatform = typeof inspected?.os === 'string'
    && typeof inspected?.architecture === 'string'
    ? `${inspected.os}/${inspected.architecture}`
    : '<invalid>';
  const identityMatches = isPlainObject(inspected)
    && observedId === config.pgProveImageId
    && observedRepoDigests.includes(config.pgProveImageRepoDigest)
    && observedPlatform === config.pgProveImagePlatform;
  if (!identityMatches) {
    fail('PREVIEW_PG_PROVE_IMAGE_IDENTITY_MISMATCH', {
      ...proof,
      observed_image_id_sha256: sha256(observedId),
      observed_repo_digest_set_sha256: sha256(observedRepoDigests),
      observed_platform_sha256: sha256(observedPlatform),
    });
  }
  assertCondition(stderr.length === 0, 'PREVIEW_PG_PROVE_IMAGE_INSPECT_STDERR', proof);
  return proof;
}

function bindDbApplicationName(sql, applicationName) {
  if (applicationName !== null) {
    requireString(
      applicationName,
      'DB_APPLICATION_NAME_INVALID',
      /^fi269-(?:transport-preflight|fie2e-hosted-[0-9a-f]{24})$/,
    );
  }
  if (applicationName === null) return sql;
  const applicationNameSql = sqlLiteral(applicationName);
  return `\\set ON_ERROR_STOP on
set application_name = ${applicationNameSql};
do $assert_fixture_application_name$
begin
  if current_setting('application_name') is distinct from ${applicationNameSql} then
    raise exception 'DB_APPLICATION_NAME_BINDING_FAILED';
  end if;
end
$assert_fixture_application_name$;
${sql}`;
}

async function runDbTest(config, tempDir, name, sql, options = {}) {
  const applicationName = options.applicationName ?? null;
  const captureStdout = options.captureStdout === true;
  const effectiveSql = bindDbApplicationName(sql, applicationName);
  const filePath = path.join(tempDir, `${name}.sql`);
  await writeFile(filePath, effectiveSql, { mode: 0o600, flag: 'wx' });
  const args = [
    '--log-level', 'error',
    'test', 'db', '--db-url', config.dbUrl, filePath,
  ];
  const stdoutHash = createHash('sha256');
  const stderrHash = createHash('sha256');
  const stdoutChunks = [];
  const stderrChunks = [];
  let stdoutBytes = 0;
  let stderrBytes = 0;
  let timedOut = false;
  let outputTooLarge = false;
  const childEnv = {};
  for (const name of DB_TEST_CHILD_ENV_NAMES) {
    if (typeof process.env[name] === 'string') childEnv[name] = process.env[name];
  }
  const childEnvNames = Object.keys(childEnv).sort();
  const childEnvSha256 = sha256(Object.fromEntries(
    childEnvNames.map((name) => [name, sha256(childEnv[name])]),
  ));
  const workingDirectorySha256 = sha256(REPO_ROOT);
  const dockerExecutable = await verifyBoundExecutable(
    config.dockerCliPath,
    config.dockerCliSha256,
    'PREVIEW_DOCKER_CLI',
  );
  const supabaseExecutable = await verifyBoundExecutable(
    config.supabaseCliPath,
    config.supabaseCliSha256,
    'PREVIEW_SUPABASE_CLI',
  );
  const pgProveImage = await inspectBoundPgProveImage(config, childEnv);

  const result = await new Promise((resolve, reject) => {
    const child = spawn(config.supabaseCliPath, args, {
      cwd: REPO_ROOT, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'], shell: false,
    });
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      setTimeout(() => child.kill('SIGKILL'), 2_000).unref();
    }, config.dbTestTimeoutMs);
    timeout.unref();
    const collect = (hash, chunks, kind) => (chunk) => {
      hash.update(chunk);
      if (kind === 'stdout') stdoutBytes += chunk.length;
      else stderrBytes += chunk.length;
      const retained = chunks.reduce((total, item) => total + item.length, 0);
      if (retained < 64 * 1024) chunks.push(chunk.subarray(0, 64 * 1024 - retained));
      if (stdoutBytes + stderrBytes > 4 * 1024 * 1024 && !outputTooLarge) {
        outputTooLarge = true;
        child.kill('SIGTERM');
      }
    };
    child.stdout.on('data', collect(stdoutHash, stdoutChunks, 'stdout'));
    child.stderr.on('data', collect(stderrHash, stderrChunks, 'stderr'));
    child.once('error', () => { clearTimeout(timeout); reject(new SafeError('DB_TEST_SPAWN_FAILED')); });
    child.once('close', (exitCode, signal) => {
      clearTimeout(timeout);
      resolve({ exitCode, signal: signal ?? null });
    });
  });
  const evidence = {
    sql_sha256: sha256(effectiveSql),
    stdout_sha256: stdoutHash.digest('hex'),
    stderr_sha256: stderrHash.digest('hex'),
    stdout_bytes: stdoutBytes,
    stderr_bytes: stderrBytes,
    exit_code: result.exitCode,
    signal: result.signal,
    executable_path_sha256: supabaseExecutable.executablePathSha256,
    executable_file_sha256: supabaseExecutable.executableFileSha256,
    docker_executable_path_sha256: dockerExecutable.executablePathSha256,
    docker_executable_file_sha256: dockerExecutable.executableFileSha256,
    ...pgProveImage,
    child_env_sha256: childEnvSha256,
    child_env_names: childEnvNames,
    application_name_sha256: applicationName === null ? null : sha256(applicationName),
    working_directory_sha256: workingDirectorySha256,
  };
  if (result.exitCode !== 0 || timedOut || outputTooLarge) {
    evidence.diagnostic_codes = dbDiagnosticCodes(
      Buffer.concat(stdoutChunks).toString('utf8'),
      Buffer.concat(stderrChunks).toString('utf8'),
      config,
    );
  }
  if (timedOut) fail('DB_TEST_TIMEOUT', evidence);
  if (outputTooLarge) fail('DB_TEST_OUTPUT_TOO_LARGE', evidence);
  if (result.exitCode !== 0) fail('DB_TEST_FAILED', evidence);
  if (captureStdout) {
    evidence.stdout_text = Buffer.concat(stdoutChunks).toString('utf8');
  }
  return evidence;
}

async function runTransportPreflight(config, tempDir) {
  let sql;
  try {
    sql = await readFile(TRANSPORT_PREFLIGHT, 'utf8');
  } catch {
    fail('TRANSPORT_PREFLIGHT_READ_FAILED', {
      preflight_path_sha256: sha256(path.basename(TRANSPORT_PREFLIGHT)),
    });
  }
  try {
    const proof = await runDbTest(
      config,
      tempDir,
      'transport-preflight',
      sql,
      { applicationName: TRANSPORT_APPLICATION_NAME },
    );
    const transportTargetSha256 = sha256(config.dbUrl);
    return {
      ...proof,
      transport_target_sha256: transportTargetSha256,
      transport_binding_sha256: sha256({
        argv_template: [
          '--log-level', 'error',
          'test', 'db', '--db-url', '<transport-target>', '<private-sql-file>',
        ],
        child_env_names: proof.child_env_names,
        child_env_sha256: proof.child_env_sha256,
        application_name_sha256: proof.application_name_sha256,
        docker_executable_file_sha256: proof.docker_executable_file_sha256,
        docker_executable_path_sha256: proof.docker_executable_path_sha256,
        executable_file_sha256: proof.executable_file_sha256,
        executable_path_sha256: proof.executable_path_sha256,
        pg_prove_image_id_sha256: proof.pg_prove_image_id_sha256,
        pg_prove_image_inspect_argv_sha256: proof.pg_prove_image_inspect_argv_sha256,
        pg_prove_image_inspect_stderr_sha256: proof.pg_prove_image_inspect_stderr_sha256,
        pg_prove_image_inspect_stdout_sha256: proof.pg_prove_image_inspect_stdout_sha256,
        pg_prove_image_platform_sha256: proof.pg_prove_image_platform_sha256,
        pg_prove_image_ref_sha256: proof.pg_prove_image_ref_sha256,
        pg_prove_image_repo_digest_sha256: proof.pg_prove_image_repo_digest_sha256,
        supabase_cli_version: SUPABASE_CLI_VERSION,
        transport_target_sha256: transportTargetSha256,
        working_directory_sha256: proof.working_directory_sha256,
      }),
    };
  } catch (error) {
    const safe = error instanceof SafeError ? error : new SafeError('TRANSPORT_PREFLIGHT_FAILED');
    safe.details = sanitizeEvidence({ ...safe.details, stage: 'transport_preflight' });
    throw safe;
  }
}

async function executeTransportPreflightOnly(config) {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), 'flow-identity-preview-transport-'));
  try {
    const proof = await runTransportPreflight(config, tempDir);
    return {
      schema_version: TRANSPORT_EVIDENCE_SCHEMA,
      status: 'passed',
      environment: 'preview',
      project_ref_sha256: sha256(config.projectRef),
      transport_proof: {
        sql_sha256: proof.sql_sha256,
        stdout_sha256: proof.stdout_sha256,
        stderr_sha256: proof.stderr_sha256,
        executable_path_sha256: proof.executable_path_sha256,
        executable_file_sha256: proof.executable_file_sha256,
        docker_executable_path_sha256: proof.docker_executable_path_sha256,
        docker_executable_file_sha256: proof.docker_executable_file_sha256,
        pg_prove_image_ref_sha256: proof.pg_prove_image_ref_sha256,
        pg_prove_image_id_sha256: proof.pg_prove_image_id_sha256,
        pg_prove_image_repo_digest_sha256: proof.pg_prove_image_repo_digest_sha256,
        pg_prove_image_platform_sha256: proof.pg_prove_image_platform_sha256,
        pg_prove_image_inspect_argv_sha256: proof.pg_prove_image_inspect_argv_sha256,
        pg_prove_image_inspect_stdout_sha256: proof.pg_prove_image_inspect_stdout_sha256,
        pg_prove_image_inspect_stderr_sha256: proof.pg_prove_image_inspect_stderr_sha256,
        child_env_sha256: proof.child_env_sha256,
        application_name_sha256: proof.application_name_sha256,
        working_directory_sha256: proof.working_directory_sha256,
        transport_target_sha256: proof.transport_target_sha256,
        transport_binding_sha256: proof.transport_binding_sha256,
      },
      disposable_actor_count: 0,
      primary_write_count: 0,
    };
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function readResponseTextBounded(
  response,
  maximumBytes,
  controller,
  tooLargeCode,
) {
  if (typeof response.body?.getReader !== 'function') {
    const text = await response.text();
    if (Buffer.byteLength(text) > maximumBytes) {
      controller.abort();
      fail(tooLargeCode);
    }
    return text;
  }
  const reader = response.body.getReader();
  const chunks = [];
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      assertCondition(value instanceof Uint8Array, 'HTTP_RESPONSE_CHUNK_INVALID');
      bytes += value.byteLength;
      if (bytes > maximumBytes) {
        controller.abort();
        await reader.cancel().catch(() => {});
        fail(tooLargeCode);
      }
      chunks.push(Buffer.from(value));
    }
  } finally {
    reader.releaseLock();
  }
  return Buffer.concat(chunks, bytes).toString('utf8');
}

async function fetchJson(url, options, timeoutMs, expectedStatuses = [200]) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  timeout.unref();
  let response;
  try {
    response = await fetch(url, { ...options, redirect: 'error', signal: controller.signal });
  } catch {
    clearTimeout(timeout);
    fail('HTTP_TRANSPORT_FAILED');
  }
  let textBody;
  try {
    textBody = await readResponseTextBounded(
      response,
      16 * 1024 * 1024,
      controller,
      'HTTP_RESPONSE_TOO_LARGE',
    );
  } catch (error) {
    clearTimeout(timeout);
    if (error instanceof SafeError) throw error;
    fail('HTTP_RESPONSE_READ_FAILED', { http_status: response.status });
  }
  clearTimeout(timeout);
  let body = null;
  if (textBody) {
    try { body = JSON.parse(textBody); } catch {
      fail('HTTP_RESPONSE_NOT_JSON', { http_status: response.status, body_sha256: sha256(textBody) });
    }
  }
  if (!expectedStatuses.includes(response.status)) {
    fail('HTTP_UNEXPECTED_STATUS', {
      http_status: response.status,
      body_sha256: sha256(textBody),
      remote_code: safeRemoteCode(body?.code ?? body?.error_code),
    });
  }
  return {
    body,
    status: response.status,
    bodySha256: sha256(textBody),
    pagination: {
      link: response.headers?.get?.('link') ?? null,
      totalCount: response.headers?.get?.('x-total-count') ?? null,
    },
  };
}

function serviceHeaders(config, json = true) {
  return {
    apikey: config.serviceRoleKey,
    authorization: `Bearer ${config.serviceRoleKey}`,
    accept: 'application/json',
    ...(json ? { 'content-type': 'application/json' } : {}),
  };
}

function actorHeaders(config, actor) {
  return {
    apikey: config.anonKey,
    authorization: `Bearer ${actor.accessToken}`,
    accept: 'application/json',
    'content-type': 'application/json',
  };
}

function actorRecoverySummary(candidate) {
  return {
    role: candidate.role,
    user_id: candidate.userId,
    request_id: candidate.requestId,
    namespace: candidate.namespace,
    locator_sha256: sha256(candidate.email),
    create_requested: candidate.createRequested,
    create_acknowledged: candidate.createAcknowledged,
    pre_create_absence_readback_attempted:
      candidate.preCreateAbsenceReadbackAttempted,
    pre_create_absence_confirmed: candidate.preCreateAbsenceConfirmed,
    lookup_attempted: candidate.lookupAttempted,
    lookup_match_found: candidate.userId !== null,
    actor_bound_checkpoint_acknowledged:
      candidate.actorBoundCheckpointAcknowledged,
    sign_in_requested: candidate.signInRequested,
    sign_in_complete: candidate.signInComplete,
    cleanup_sign_in_attempted: candidate.cleanupSignInAttempted,
    cleanup_sign_in_complete: candidate.cleanupSignInComplete,
    session_revoke_attempted: candidate.sessionRevokeAttempted,
    session_revoked: candidate.sessionRevoked,
    delete_attempted: candidate.deleteAttempted,
    delete_acknowledged: candidate.deleteAcknowledged,
    delete_http_status: candidate.deleteHttpStatus,
    delete_already_absent: candidate.deleteAlreadyAbsent,
    absence_readback_attempted: candidate.absenceReadbackAttempted,
    selector_absence_readback_attempted:
      candidate.selectorAbsenceReadbackAttempted,
    selector_absence_confirmed: candidate.selectorAbsenceConfirmed,
    absence_confirmed: candidate.absenceConfirmed,
    retained: !candidate.absenceConfirmed,
    cleanup_failure_codes: candidate.cleanupFailureCodes,
  };
}

function recoveryCheckpointPayload(context) {
  return {
    actors: context.actorCandidates.map((candidate) => ({
      role: candidate.role,
      email: candidate.email,
      fixture_sha256: candidate.fixtureSha256,
      request_id: candidate.requestId,
      namespace: candidate.namespace,
      user_id: candidate.userId,
      create_requested: candidate.createRequested,
      actor_bound_checkpoint_acknowledged:
        candidate.actorBoundCheckpointAcknowledged,
      locator_sha256: sha256(candidate.email),
    })),
    recovery_selectors: normalizedRecoverySelectors(
      context.recoverySelectors,
    ),
  };
}

async function checkpointRecovery(context, stage) {
  return context.recoveryCheckpointClient.checkpoint(
    stage,
    recoveryCheckpointPayload(context),
  );
}

function attachCleanupDetails(error, cleanup, actorCandidates) {
  const safe = error instanceof SafeError ? error : new SafeError('HOSTED_PREVIEW_E2E_FAILED');
  safe.details = sanitizeEvidence({
    ...safe.details,
    cleanup,
    cleanup_failure_code: cleanup.cleanup_failure_code ?? null,
    cleanup_failure_codes: cleanup.cleanup_failure_codes ?? [],
    recovery_actors: actorCandidates.map(actorRecoverySummary),
  });
  return safe;
}

function nextAuthUserListPage(
  config,
  result,
  page,
  observedCount,
  expectedFilter,
) {
  const totalText = result.pagination?.totalCount;
  assertCondition(
    typeof totalText === 'string' && /^(?:0|[1-9][0-9]{0,8})$/.test(totalText),
    'AUTH_USER_LIST_TOTAL_INVALID',
  );
  const total = Number(totalText);
  assertCondition(total >= observedCount && total <= AUTH_USER_LIST_MAX_TOTAL,
    'AUTH_USER_LIST_TOTAL_INVALID');

  const link = result.pagination?.link;
  if (link === null || link === '') {
    assertCondition(
      observedCount === total
        && result.body.users.length <= AUTH_USER_LIST_PAGE_SIZE,
      'AUTH_USER_LIST_PAGINATION_INCOMPLETE',
    );
    return null;
  }
  assertCondition(typeof link === 'string' && link.length <= 16 * 1024,
    'AUTH_USER_LIST_LINK_INVALID');
  let nextPage = null;
  for (const part of link.split(',')) {
    const match = part.match(/^\s*<([^>]+)>\s*;\s*rel="(first|prev|next|last)"\s*$/);
    assertCondition(match !== null, 'AUTH_USER_LIST_LINK_INVALID');
    if (match[2] !== 'next') continue;
    assertCondition(nextPage === null, 'AUTH_USER_LIST_LINK_INVALID');
    let nextUrl;
    try { nextUrl = new URL(match[1], config.supabaseUrl); } catch {
      fail('AUTH_USER_LIST_LINK_INVALID');
    }
    const expectedBase = new URL('/auth/v1/admin/users', config.supabaseUrl);
    assertCondition(
      nextUrl.origin === expectedBase.origin
        && [expectedBase.pathname, '/admin/users'].includes(nextUrl.pathname)
        && nextUrl.searchParams.get('per_page')
          === String(AUTH_USER_LIST_PAGE_SIZE)
        && nextUrl.searchParams.get('filter') === expectedFilter
        && [...nextUrl.searchParams.keys()].every((key) =>
          key === 'page' || key === 'per_page' || key === 'filter'),
      'AUTH_USER_LIST_LINK_INVALID',
    );
    const parsed = Number(nextUrl.searchParams.get('page'));
    assertCondition(
      Number.isSafeInteger(parsed)
        && parsed === page + 1
        && parsed <= AUTH_USER_LIST_MAX_PAGES,
      parsed > AUTH_USER_LIST_MAX_PAGES
        ? 'AUTH_USER_LIST_PAGE_LIMIT'
        : 'AUTH_USER_LIST_PAGE_INVALID',
    );
    nextPage = parsed;
  }
  if (nextPage === null) {
    assertCondition(observedCount === total, 'AUTH_USER_LIST_PAGINATION_INCOMPLETE');
  } else {
    assertCondition(
      result.body.users.length === AUTH_USER_LIST_PAGE_SIZE
        && observedCount === page * AUTH_USER_LIST_PAGE_SIZE
        && observedCount < total,
      'AUTH_USER_LIST_PAGINATION_INVALID',
    );
  }
  return nextPage;
}

function disposableActorMatches(user, candidate) {
  return user?.email === candidate.email
    && user?.app_metadata?.preview_e2e === true
    && user?.app_metadata?.fixture_sha256 === candidate.fixtureSha256
    && user?.app_metadata?.preview_e2e_request_id === candidate.requestId
    && user?.app_metadata?.preview_e2e_namespace === candidate.namespace
    && user?.app_metadata?.preview_e2e_role === candidate.role;
}

async function listExactDisposableActors(config, candidate) {
  candidate.lookupAttempted = true;
  const matches = [];
  let observedCount = 0;
  let expectedTotal = null;
  let page = 1;
  for (;;) {
    const url = new URL('/auth/v1/admin/users', config.supabaseUrl);
    url.searchParams.set('page', String(page));
    url.searchParams.set('per_page', String(AUTH_USER_LIST_PAGE_SIZE));
    url.searchParams.set('filter', candidate.email);
    const result = await fetchJson(
      url,
      { method: 'GET', headers: serviceHeaders(config, false) },
      config.rpcTimeoutMs,
    );
    assertCondition(Array.isArray(result.body?.users), 'AUTH_USER_LIST_RESPONSE_INVALID');
    assertCondition(
      result.body.users.length <= AUTH_USER_LIST_PAGE_SIZE,
      'AUTH_USER_LIST_RESPONSE_INVALID',
    );
    observedCount += result.body.users.length;
    const totalText = result.pagination?.totalCount;
    assertCondition(
      typeof totalText === 'string' && /^(?:0|[1-9][0-9]{0,8})$/.test(totalText),
      'AUTH_USER_LIST_TOTAL_INVALID',
    );
    const pageTotal = Number(totalText);
    if (expectedTotal === null) expectedTotal = pageTotal;
    assertCondition(
      pageTotal === expectedTotal && pageTotal <= AUTH_USER_LIST_MAX_TOTAL,
      'AUTH_USER_LIST_TOTAL_DRIFT',
    );
    for (const user of result.body.users) {
      if (disposableActorMatches(user, candidate)) matches.push(user);
    }
    const nextPage = nextAuthUserListPage(
      config,
      result,
      page,
      observedCount,
      candidate.email,
    );
    if (nextPage === null) break;
    page = nextPage;
  }
  candidate.lastUserListTotal = expectedTotal;
  return matches;
}

async function findDisposableActor(config, candidate) {
  const matches = await listExactDisposableActors(config, candidate);
  assertCondition(matches.length <= 1, 'AUTH_ACTOR_RECOVERY_AMBIGUOUS', {
    match_count: matches.length,
    locator_sha256: sha256(candidate.email),
  });
  if (matches.length === 0) return null;
  return requireUuid(matches[0]?.id, 'AUTH_ACTOR_RECOVERY_ID_INVALID');
}

async function verifyDisposableActorById(config, candidate, userId) {
  const result = await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(userId)}`,
    { method: 'GET', headers: serviceHeaders(config, false) },
    config.rpcTimeoutMs,
    [200, 404],
  );
  const user = result.body?.user ?? result.body;
  assertCondition(
    result.status === 200
      && disposableActorMatches(user, candidate)
      && requireUuid(user?.id, 'AUTH_CREATE_RESPONSE_ACTOR_MISMATCH') === userId,
    'AUTH_CREATE_RESPONSE_ACTOR_MISMATCH',
  );
  return userId;
}

async function createDisposableActor(config, roleLabel, context) {
  const suffix = context.namespace.slice('fie2e-hosted-'.length);
  const email = `flow-identity-${roleLabel}-${suffix}@example.invalid`;
  const password = randomBytes(36).toString('base64url');
  const candidate = {
    role: roleLabel,
    email,
    password,
    fixtureSha256: sha256('database-engine#269'),
    requestId: context.requestId,
    namespace: context.namespace,
    userId: null,
    accessToken: null,
    createRequested: false,
    createAcknowledged: false,
    preCreateAbsenceReadbackAttempted: false,
    preCreateAbsenceConfirmed: false,
    lookupAttempted: false,
    lastUserListTotal: null,
    actorBoundCheckpointAcknowledged: false,
    signInRequested: false,
    signInComplete: false,
    cleanupSignInAttempted: false,
    cleanupSignInComplete: false,
    sessionRevokeAttempted: false,
    sessionRevoked: false,
    deleteAttempted: false,
    deleteAcknowledged: false,
    deleteHttpStatus: null,
    deleteAlreadyAbsent: false,
    absenceReadbackAttempted: false,
    selectorAbsenceReadbackAttempted: false,
    selectorAbsenceConfirmed: false,
    absenceConfirmed: false,
    cleanupFailureCodes: [],
    recoveryContext: context,
  };
  context.actorCandidates.push(candidate);
  await checkpointRecovery(context, `${roleLabel}_actor_registered`);
  candidate.preCreateAbsenceReadbackAttempted = true;
  const preexisting = await listExactDisposableActors(config, candidate);
  assertCondition(preexisting.length === 0, 'AUTH_ACTOR_PREEXISTING', {
    match_count: preexisting.length,
    locator_sha256: sha256(candidate.email),
  });
  candidate.preCreateAbsenceConfirmed = true;
  candidate.createRequested = true;
  await checkpointRecovery(context, `${roleLabel}_actor_create_requested`);
  const created = await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users`,
    {
      method: 'POST', headers: serviceHeaders(config),
      body: JSON.stringify({
        email, password, email_confirm: true,
        app_metadata: {
          preview_e2e: true,
          fixture_sha256: candidate.fixtureSha256,
          preview_e2e_request_id: context.requestId,
          preview_e2e_namespace: context.namespace,
          preview_e2e_role: roleLabel,
        },
      }),
    },
    config.rpcTimeoutMs,
    [200, 201],
  );
  candidate.createAcknowledged = true;
  const responseId = created.body?.id ?? created.body?.user?.id;
  const responseUserId = typeof responseId === 'string' && UUID_RE.test(responseId)
    ? responseId.toLowerCase()
    : null;
  candidate.userId = responseUserId === null
    ? await findDisposableActor(config, candidate)
    : await verifyDisposableActorById(config, candidate, responseUserId);
  assertCondition(candidate.userId !== null, 'AUTH_CREATE_RESPONSE_INVALID', {
    locator_sha256: sha256(candidate.email),
  });
  await checkpointRecovery(context, `${roleLabel}_actor_bound`);
  candidate.actorBoundCheckpointAcknowledged = true;
  candidate.signInRequested = true;
  const signedIn = await fetchJson(
    `${config.supabaseUrl}/auth/v1/token?grant_type=password`,
    {
      method: 'POST',
      headers: { apikey: config.anonKey, accept: 'application/json', 'content-type': 'application/json' },
      body: JSON.stringify({ email, password }),
    },
    config.rpcTimeoutMs,
  );
  assertCondition(
    requireUuid(signedIn.body?.user?.id, 'AUTH_SIGN_IN_RESPONSE_INVALID') === candidate.userId,
    'AUTH_SIGN_IN_ACTOR_MISMATCH',
  );
  candidate.accessToken = requireString(signedIn.body?.access_token, 'AUTH_ACCESS_TOKEN_MISSING');
  candidate.signInComplete = true;
  return {
    userId: candidate.userId,
    email: candidate.email,
    accessToken: candidate.accessToken,
    candidate,
  };
}

async function revokeActorSession(config, actor) {
  if (!actor?.accessToken) return;
  await fetchJson(
    `${config.supabaseUrl}/auth/v1/logout?scope=global`,
    { method: 'POST', headers: { apikey: config.anonKey, authorization: `Bearer ${actor.accessToken}` } },
    config.rpcTimeoutMs,
    [200, 204],
  );
}

async function reacquireActorSessionForCleanup(config, candidate) {
  candidate.cleanupSignInAttempted = true;
  const signedIn = await fetchJson(
    `${config.supabaseUrl}/auth/v1/token?grant_type=password`,
    {
      method: 'POST',
      headers: {
        apikey: config.anonKey,
        accept: 'application/json',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ email: candidate.email, password: candidate.password }),
    },
    config.rpcTimeoutMs,
  );
  assertCondition(
    requireUuid(signedIn.body?.user?.id, 'AUTH_CLEANUP_SIGN_IN_RESPONSE_INVALID')
      === candidate.userId,
    'AUTH_CLEANUP_SIGN_IN_ACTOR_MISMATCH',
  );
  candidate.accessToken = requireString(
    signedIn.body?.access_token,
    'AUTH_CLEANUP_ACCESS_TOKEN_MISSING',
  );
  candidate.cleanupSignInComplete = true;
}

async function deleteDisposableActor(config, userId) {
  return fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(userId)}`,
    {
      method: 'DELETE',
      headers: serviceHeaders(config),
      body: JSON.stringify({ should_soft_delete: false }),
    },
    config.rpcTimeoutMs,
    [200, 204, 404],
  );
}

async function confirmDisposableActorAbsent(config, userId) {
  const result = await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(userId)}`,
    { method: 'GET', headers: serviceHeaders(config, false) },
    config.rpcTimeoutMs,
    [200, 404],
  );
  assertCondition(result.status === 404, 'AUTH_ACTOR_DELETE_READBACK_FAILED', {
    user_id: userId,
    http_status: result.status,
  });
}

async function confirmDisposableActorSelectorAbsent(config, candidate) {
  candidate.selectorAbsenceReadbackAttempted = true;
  const matches = await listExactDisposableActors(config, candidate);
  assertCondition(matches.length === 0, 'AUTH_ACTOR_SELECTOR_RESIDUE', {
    match_count: matches.length,
    locator_sha256: sha256(candidate.email),
  });
  candidate.selectorAbsenceConfirmed = true;
}

async function cleanupActorCandidate(
  config,
  candidate,
  deleteAllowed = true,
  recoveryContext = null,
) {
  const checkpointContext = recoveryContext ?? candidate.recoveryContext ?? null;
  const failures = [];
  const recordFailure = (error, fallbackCode) => {
    const cause = error instanceof SafeError ? error : new SafeError('UNHANDLED_FAILURE');
    const safe = new SafeError(fallbackCode, {
      cause_code: cause.code,
      ...cause.details,
    });
    failures.push(safe);
    candidate.cleanupFailureCodes.push(fallbackCode);
    if (cause.code !== fallbackCode) candidate.cleanupFailureCodes.push(cause.code);
  };
  if (!deleteAllowed) {
    if (candidate.accessToken !== null) {
      candidate.sessionRevokeAttempted = true;
      try {
        await revokeActorSession(config, candidate);
        candidate.sessionRevoked = true;
      } catch (error) {
        recordFailure(error, 'AUTH_SESSION_REVOKE_FAILED');
      }
    }
    if (failures.length > 0) {
      failures[0].details = sanitizeEvidence({
        ...failures[0].details,
        cleanup_failure_codes: candidate.cleanupFailureCodes,
      });
      throw failures[0];
    }
    return;
  }
  if (candidate.userId === null && candidate.createRequested) {
    try {
      candidate.userId = await findDisposableActor(config, candidate);
    } catch (error) {
      recordFailure(error, 'AUTH_ACTOR_RECOVERY_FAILED');
    }
  }
  if (
    candidate.userId !== null
      && !candidate.actorBoundCheckpointAcknowledged
      && failures.length === 0
  ) {
    try {
      assertCondition(
        checkpointContext !== null,
        'AUTH_ACTOR_BOUND_CHECKPOINT_REQUIRED',
      );
      await checkpointRecovery(
        checkpointContext,
        `${candidate.role}_actor_bound_cleanup`,
      );
      candidate.actorBoundCheckpointAcknowledged = true;
    } catch (error) {
      recordFailure(error, 'AUTH_ACTOR_BOUND_CHECKPOINT_FAILED');
    }
  }
  if (candidate.lookupAttempted && candidate.userId === null && failures.length === 0) {
    candidate.selectorAbsenceReadbackAttempted = true;
    candidate.selectorAbsenceConfirmed = true;
    recordFailure(
      new SafeError('AUTH_ACTOR_RECOVERY_UNRESOLVED'),
      'AUTH_ACTOR_RECOVERY_FAILED',
    );
  } else if (candidate.userId !== null && failures.length === 0) {
    if (candidate.accessToken === null) {
      try {
        await reacquireActorSessionForCleanup(config, candidate);
      } catch (error) {
        recordFailure(error, 'AUTH_CLEANUP_SIGN_IN_FAILED');
      }
    }
    if (candidate.accessToken !== null && failures.length === 0) {
      candidate.sessionRevokeAttempted = true;
      try {
        await revokeActorSession(config, candidate);
        candidate.sessionRevoked = true;
      } catch (error) {
        recordFailure(error, 'AUTH_SESSION_REVOKE_FAILED');
      }
    }
    if (!candidate.sessionRevoked && failures.length === 0) {
      recordFailure(
        new SafeError('AUTH_SESSION_REVOKE_REQUIRED'),
        'AUTH_SESSION_REVOKE_FAILED',
      );
    }
  }
  if (failures.length === 0 && candidate.userId !== null) {
    candidate.deleteAttempted = true;
    try {
      const deletion = await deleteDisposableActor(config, candidate.userId);
      candidate.deleteHttpStatus = deletion.status;
      candidate.deleteAlreadyAbsent = deletion.status === 404;
      candidate.deleteAcknowledged = true;
    } catch (error) {
      recordFailure(error, 'AUTH_ACTOR_DELETE_FAILED');
    }
    candidate.absenceReadbackAttempted = true;
    try {
      await confirmDisposableActorAbsent(config, candidate.userId);
    } catch (error) {
      recordFailure(error, 'AUTH_ACTOR_DELETE_READBACK_FAILED');
    }
    try {
      await confirmDisposableActorSelectorAbsent(config, candidate);
    } catch (error) {
      recordFailure(error, 'AUTH_ACTOR_SELECTOR_READBACK_FAILED');
    }
    candidate.absenceConfirmed = candidate.deleteAcknowledged
      && candidate.selectorAbsenceConfirmed
      && !candidate.cleanupFailureCodes.includes('AUTH_ACTOR_DELETE_READBACK_FAILED');
  }
  if (failures.length > 0) {
    failures[0].details = sanitizeEvidence({
      ...failures[0].details,
      cleanup_failure_codes: candidate.cleanupFailureCodes,
    });
    throw failures[0];
  }
}

async function cleanupActorCandidates(
  config,
  candidates,
  deleteAllowed = true,
  recoveryContext = null,
) {
  const failures = [];
  for (const candidate of candidates) {
    try {
      await cleanupActorCandidate(
        config,
        candidate,
        deleteAllowed,
        recoveryContext,
      );
    } catch (error) {
      failures.push(error instanceof SafeError
        ? error
        : new SafeError('AUTH_ACTOR_CLEANUP_FAILED'));
    }
  }
  return failures;
}

async function rpc(config, actor, functionName, parameters) {
  const result = await fetchJson(
    `${config.supabaseUrl}/rest/v1/rpc/${functionName}`,
    { method: 'POST', headers: actorHeaders(config, actor), body: JSON.stringify(parameters) },
    config.rpcTimeoutMs,
  );
  assertCondition(isPlainObject(result.body), 'RPC_RESPONSE_INVALID', {
    function_sha256: sha256(functionName), body_sha256: result.bodySha256,
  });
  return result.body;
}

async function rpcThrown(config, actor, functionName, parameters) {
  return fetchJson(
    `${config.supabaseUrl}/rest/v1/rpc/${functionName}`,
    { method: 'POST', headers: actorHeaders(config, actor), body: JSON.stringify(parameters) },
    config.rpcTimeoutMs,
    [400, 409, 500],
  );
}

async function readFixtureManifest(config, actorUserId, requestId) {
  const url = new URL('/rest/v1/command_audit_log', config.supabaseUrl);
  url.searchParams.set('select', 'payload');
  url.searchParams.set('command', `eq.${FIXTURE_COMMAND}`);
  url.searchParams.set('actor_user_id', `eq.${actorUserId}`);
  url.searchParams.set('target_table', `eq.${FIXTURE_TARGET_TABLE}`);
  url.searchParams.set('target_id', `eq.${requestId}`);
  url.searchParams.set('target_version', `eq.${FIXTURE_TARGET_VERSION}`);
  url.searchParams.set('limit', '2');
  const result = await fetchJson(
    url, { method: 'GET', headers: serviceHeaders(config, false) }, config.rpcTimeoutMs,
  );
  assertCondition(Array.isArray(result.body), 'FIXTURE_MANIFEST_QUERY_INVALID');
  return result.body;
}

function validateHashes(value, pathName = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateHashes(item, `${pathName}.${index}`));
    return;
  }
  if (!isPlainObject(value)) return;
  for (const [key, item] of Object.entries(value)) {
    if (key.endsWith('_sha256')) {
      requireSha(item, 'FIXTURE_HASH_INVALID');
    } else {
      validateHashes(item, `${pathName}.${key}`);
    }
  }
}

function validateManifest(payload, context) {
  assertCondition(isPlainObject(payload), 'FIXTURE_MANIFEST_INVALID');
  assertCondition(payload.schema_version === MANIFEST_SCHEMA, 'FIXTURE_MANIFEST_SCHEMA_INVALID');
  assertCondition(payload.scenario_id === context.namespace, 'FIXTURE_NAMESPACE_MISMATCH');
  assertCondition(payload.request_id === context.requestId, 'FIXTURE_REQUEST_ID_MISMATCH');
  assertCondition(payload.preview_ref === context.config.projectRef, 'FIXTURE_PREVIEW_REF_MISMATCH');
  assertCondition(payload.actor?.user_id === context.owner.userId, 'FIXTURE_OWNER_MISMATCH');
  assertCondition(payload.actor?.email === context.owner.email, 'FIXTURE_OWNER_MISMATCH');
  assertCondition(payload.foreign_actor?.user_id === context.foreign.userId, 'FIXTURE_FOREIGN_MISMATCH');
  assertCondition(payload.foreign_actor?.email === context.foreign.email, 'FIXTURE_FOREIGN_MISMATCH');
  assertCondition(isPlainObject(payload.expected), 'FIXTURE_EXPECTED_INVALID');
  for (const [key, expected] of Object.entries(EXPECTED)) {
    requireInteger(payload.expected[key], expected, 'FIXTURE_EXPECTED_MISMATCH');
  }
  const entities = payload.entities;
  assertCondition(isPlainObject(entities), 'FIXTURE_ENTITIES_INVALID');
  for (const key of [
    'unitgroup_id', 'flowproperty_id', 'source_flow_id',
    'target_flow_id', 'pending_flow_id',
  ]) requireUuid(entities[key], 'FIXTURE_ENTITY_ID_INVALID');
  assertCondition(Array.isArray(entities.process_ids) && entities.process_ids.length === 2, 'FIXTURE_PROCESS_IDS_INVALID');
  entities.process_ids.forEach((id) => requireUuid(id, 'FIXTURE_PROCESS_IDS_INVALID'));
  assertCondition(isPlainObject(payload.capture_request), 'FIXTURE_CAPTURE_REQUEST_INVALID');
  assertCondition(payload.capture_request.actor?.user_id === context.owner.userId, 'FIXTURE_CAPTURE_ACTOR_MISMATCH');
  assertCondition(payload.capture_request.project_ref === context.config.projectRef, 'FIXTURE_CAPTURE_REF_MISMATCH');
  assertCondition(payload.capture_request.operation_id === payload.operation_id, 'FIXTURE_OPERATION_MISMATCH');
  requireUuid(payload.capture_request.request_id, 'FIXTURE_CAPTURE_ID_INVALID');
  assertCondition(payload.capture_request.request_id === context.requestId, 'FIXTURE_CAPTURE_ID_MISMATCH');
  assertCondition(payload.capture_request.protected_closure?.orphans?.length === 303, 'FIXTURE_ORPHANS_INVALID');
  assertCondition(payload.capture_request.process_intents?.length === 2, 'FIXTURE_PROCESS_INTENTS_INVALID');
  assertCondition(payload.capture_request.mappings?.length === 1, 'FIXTURE_MAPPINGS_INVALID');
  validateHashes(payload);
  return payload;
}

function validateCapture(result, replay = false) {
  assertCondition(result.ok === true, 'CAPTURE_FAILED', { remote_code: safeRemoteCode(result.code) });
  assertCondition(result.replay === replay, 'CAPTURE_REPLAY_FLAG_INVALID');
  requireUuid(result.receipt_id, 'CAPTURE_RECEIPT_ID_INVALID');
  requireSha(result.receipt_proof_sha256, 'CAPTURE_RECEIPT_PROOF_INVALID');
  for (const [key, expected] of Object.entries({
    source_count: 305, mapping_count: 1, support_count: 2,
    process_count: 2, rewrite_count: 2,
  })) requireInteger(result[key], expected, 'CAPTURE_COUNT_MISMATCH');
  return result;
}

async function captureDerivativeRecoverySelectors(
  context,
  scopeRead,
  selectors,
  expectedCompletedCount,
) {
  assertCondition(
    Array.isArray(scopeRead.processes) && scopeRead.processes.length === 2,
    'RECOVERY_SELECTOR_PROCESS_SET_INVALID',
  );
  assertCondition(
    scopeRead.processes.filter((process) => process.status === 'completed').length
      === expectedCompletedCount,
    'RECOVERY_SELECTOR_COMPLETED_COUNT_INVALID',
  );
  for (const process of scopeRead.processes) {
    const processId = requireUuid(process.id, 'RECOVERY_SELECTOR_PROCESS_ID_INVALID');
    const processVersion = requireString(
      process.version,
      'RECOVERY_SELECTOR_PROCESS_VERSION_INVALID',
    );
    addSelector(selectors, 'process_ids', processId);
    if (process.status !== 'completed') continue;
    const derivativeBatchId = requireUuid(
      process.derivative_batch_id,
      'RECOVERY_SELECTOR_DERIVATIVE_BATCH_INVALID',
    );
    const derivativeRequestId = requireUuid(
      process.derivative_request_id,
      'RECOVERY_SELECTOR_DERIVATIVE_REQUEST_INVALID',
    );
    addSelector(selectors, 'derivative_batch_ids', derivativeBatchId);
    addSelector(selectors, 'derivative_request_ids', derivativeRequestId);
    addSelector(
      selectors,
      'audit_ids',
      requireDecimalId(process.audit_id, 'RECOVERY_SELECTOR_PROCESS_AUDIT_INVALID'),
    );

    const derivative = await rpc(
      context.config,
      context.owner,
      'cmd_dataset_derivative_rebuild_read',
      { p_request_id: derivativeRequestId },
    );
    assertCondition(
      derivative.ok === true
        && derivative.schema_version === 'dataset-derivative-rebuild-status.v1'
        && derivative.request_id === derivativeRequestId
        && derivative.table === 'processes'
        && derivative.id === processId
        && derivative.version === processVersion,
      'RECOVERY_SELECTOR_DERIVATIVE_READ_INVALID',
      { remote_code: safeRemoteCode(derivative.code) },
    );
    addSelector(
      selectors,
      'audit_ids',
      requireDecimalId(
        derivative.database_audit_id,
        'RECOVERY_SELECTOR_DERIVATIVE_AUDIT_INVALID',
      ),
    );
    addSelector(
      selectors,
      'audit_ids',
      requireDecimalId(
        derivative.summary_audit_id,
        'RECOVERY_SELECTOR_DERIVATIVE_SUMMARY_AUDIT_INVALID',
      ),
    );
    addSelector(
      selectors,
      'http_request_ids',
      optionalDecimalId(
        derivative.markdown?.request_id,
        'RECOVERY_SELECTOR_HTTP_REQUEST_INVALID',
      ),
    );
    addSelector(
      selectors,
      'derivative_proposal_ids',
      optionalDecimalId(
        derivative.markdown?.proposal_id,
        'RECOVERY_SELECTOR_MARKDOWN_PROPOSAL_INVALID',
      ),
    );
    addSelector(
      selectors,
      'embedding_pending_job_ids',
      optionalDecimalId(
        derivative.embedding?.pending_job_id,
        'RECOVERY_SELECTOR_PENDING_EMBEDDING_INVALID',
      ),
    );
    addSelector(
      selectors,
      'embedding_queue_msg_ids',
      optionalDecimalId(
        derivative.embedding?.queue_msg_id,
        'RECOVERY_SELECTOR_EMBEDDING_MESSAGE_INVALID',
      ),
    );
    addSelector(
      selectors,
      'derivative_proposal_ids',
      optionalDecimalId(
        derivative.embedding?.proposal_id,
        'RECOVERY_SELECTOR_EMBEDDING_PROPOSAL_INVALID',
      ),
    );
  }
}

function buildPreflightRequest(manifest, capture) {
  const hashes = manifest.preflight_hashes;
  return {
    schema_version: 'dataset-flow-identity-scope-preflight.v2',
    request_id: randomUUID(),
    receipt_id: capture.receipt_id,
    receipt_proof_sha256: capture.receipt_proof_sha256,
    environment: 'preview',
    project_ref: manifest.preview_ref,
    actor: manifest.actor,
    target_visibility: 'owner_draft',
    user_state_claim: 'authenticated_actor_state_100_plus_own_state_0',
    operation_id: manifest.operation_id,
    plan_sha256: hashes.plan_sha256,
    freeze_sha256: hashes.freeze_sha256,
    policy_approval_text_sha256:
      manifest.capture_request.compatibility_policy.approval_text_sha256,
    execution_approval_request_sha256: hashes.execution_approval_request_sha256,
    execution_approval_text_sha256: hashes.execution_approval_text_sha256,
    execution_approval_identity_sha256: hashes.execution_approval_identity_sha256,
    toolchain_evidence_sha256:
      manifest.capture_request.artifact_evidence.toolchain_evidence_sha256,
    maximum_wrapper_invocations: 1,
    maximum_process_posts: 2,
    maximum_finalize_posts: 1,
    maximum_cli_apply_spawns: 1,
    approval_reusable: false,
    automatic_retry: false,
  };
}

function validatePermit(value, generation, invocationId = null) {
  assertCondition(isPlainObject(value), 'EXECUTION_PERMIT_MISSING');
  assertCondition(value.schema_version === PERMIT_SCHEMA, 'EXECUTION_PERMIT_SCHEMA_INVALID');
  const observedInvocation = requireUuid(value.invocation_id, 'EXECUTION_PERMIT_INVOCATION_INVALID');
  if (invocationId) assertCondition(observedInvocation === invocationId, 'EXECUTION_PERMIT_INVOCATION_CHANGED');
  requireInteger(value.generation, generation, 'EXECUTION_PERMIT_GENERATION_INVALID');
  requireString(value.token, 'EXECUTION_PERMIT_TOKEN_INVALID', SHA256_RE);
  return value;
}

function processRequests(scopeRead, scopeProof) {
  assertCondition(Array.isArray(scopeRead.processes) && scopeRead.processes.length === 2, 'SCOPE_READ_PROCESSES_INVALID');
  return scopeRead.processes.map((process, index) => {
    requireInteger(process.ordinal, index + 1, 'SCOPE_PROCESS_ORDINAL_INVALID');
    const body = {
      schema_version: 'dataset-flow-identity-process-rewrite.v2',
      request_id: randomUUID(),
      scope_proof_sha256: requireSha(scopeProof, 'SCOPE_PROOF_INVALID'),
      ordinal: index + 1,
      process_intent_proof_sha256: requireSha(
        process.process_intent_proof_sha256,
        'PROCESS_INTENT_PROOF_INVALID',
      ),
    };
    return { ...body, process_request_sha256: sha256(body) };
  });
}

function buildFinalizeRequest(scopeProof) {
  return {
    schema_version: 'dataset-flow-identity-scope-finalize.v2',
    request_id: randomUUID(),
    scope_proof_sha256: requireSha(scopeProof, 'SCOPE_PROOF_INVALID'),
    expected: {
      process_count: 2,
      rewrite_count: 2,
      completed_process_count: 2,
    },
  };
}

function contextSqlGate(config) {
  return `
  if util.dataset_alias_execution_server_context()->>'environment'
      is distinct from 'preview'
    or util.dataset_alias_execution_server_context()->>'project_ref'
      is distinct from ${sqlLiteral(config.projectRef)} then
    raise exception 'HOSTED_PREVIEW_CONTEXT_MISMATCH';
  end if;`;
}

function driftSql(context, manifest) {
  const sourceId = manifest.entities.source_flow_id;
  return `begin;
set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
do $proof$
declare affected integer;
begin
${contextSqlGate(context.config)}
  set local session_replication_role = replica;
  update public.flows
  set modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz
    + interval '1 microsecond'
  where id = ${sqlLiteral(sourceId)}::uuid and version = '01.00.000'
    and user_id = ${sqlLiteral(context.owner.userId)}::uuid and state_code = 0
    and modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz;
  get diagnostics affected = row_count;
  set local session_replication_role = origin;
  if affected <> 1 then raise exception 'DRIFT_SOURCE_CARDINALITY_MISMATCH'; end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - exact mapped source modified_at drift is committed')
) as t(tap);
commit;`;
}

function driftRejectedProofSql(context, manifest) {
  return `begin;
set local search_path = extensions, public, auth;
do $proof$
begin
${contextSqlGate(context.config)}
  if (select count(*) from util.dataset_flow_identity_capture_receipts
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and request_id = ${sqlLiteral(context.requestId)}::uuid) <> 1
    or exists (select 1 from util.dataset_flow_identity_scopes
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and operation_id = ${sqlLiteral(manifest.operation_id)})
    or exists (select 1 from util.dataset_flow_identity_wrapper_invocations
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid)
    or exists (select 1 from public.command_audit_log
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
        and payload->>'operation_id' = ${sqlLiteral(manifest.operation_id)})
    or (select modified_at from public.flows
        where id = ${sqlLiteral(manifest.entities.source_flow_id)}::uuid
          and version = '01.00.000') is distinct from
        ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz
          + interval '1 microsecond'
    or exists (select 1 from util.dataset_derivative_rebuild_requests
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and target_id in (${manifest.entities.process_ids.map((id) => `${sqlLiteral(id)}::uuid`).join(',')})) then
    raise exception 'LIVE_DRIFT_REJECTION_WROTE_STATE';
  end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - live-drift rejection wrote no scope, permit, audit, or derivative state')
) as t(tap);
commit;`;
}

function restoreSql(context, manifest) {
  return `begin;
set local search_path = extensions, public, auth;
do $proof$
declare affected integer;
begin
${contextSqlGate(context.config)}
  set local session_replication_role = replica;
  update public.flows
  set modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz
  where id = ${sqlLiteral(manifest.entities.source_flow_id)}::uuid
    and version = '01.00.000'
    and user_id = ${sqlLiteral(context.owner.userId)}::uuid
    and modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz
      + interval '1 microsecond';
  get diagnostics affected = row_count;
  set local session_replication_role = origin;
  if affected <> 1 then raise exception 'RESTORE_SOURCE_CARDINALITY_MISMATCH'; end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - exact mapped source baseline is restored without trigger side effects')
) as t(tap);
commit;`;
}

function faultInstallSql(context, manifest, scopeId) {
  const faultMarker = `database-engine#269 hosted Preview fault scenario=${context.namespace} request=${context.requestId}`;
  return `begin;
set local search_path = extensions, public, auth;
do $gate$
begin
${contextSqlGate(context.config)}
  if to_regprocedure('private.preview_flow_identity_post_primary_fault_v1()') is not null
    or exists (select 1 from pg_trigger
      where tgname = 'preview_flow_identity_post_primary_fault_v1'
        and tgrelid = 'public.command_audit_log'::regclass
        and not tgisinternal) then
    raise exception 'FAULT_HOOK_ALREADY_EXISTS';
  end if;
end $gate$;
create function private.preview_flow_identity_post_primary_fault_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $fault$
begin
  -- ${faultMarker}
  if old.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    and new.command = old.command
    and old.actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
    and old.target_table = 'processes'
    and old.target_id = ${sqlLiteral(manifest.entities.process_ids[0])}::uuid
    and old.payload->>'scope_id' = ${sqlLiteral(scopeId)}
    and old.payload->>'schema_version' = 'dataset-flow-identity-process-rewrite.v1'
    and new.payload->>'schema_version' = 'dataset-flow-identity-process-rewrite.v2' then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PREVIEW_POST_PRIMARY_FAULT';
  end if;
  return new;
end
$fault$;
revoke all on function private.preview_flow_identity_post_primary_fault_v1()
  from public, anon, authenticated, service_role;
create trigger preview_flow_identity_post_primary_fault_v1
before update on public.command_audit_log
for each row execute function private.preview_flow_identity_post_primary_fault_v1();
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - exact actor, scope, and ordinal-1 audit-promotion fault is installed')
) as t(tap);
commit;`;
}

function rollbackProofSql(context, manifest, scopeId, invocationId) {
  const processIds = manifest.entities.process_ids;
  return `begin;
set local search_path = extensions, public, auth;
do $proof$
begin
${contextSqlGate(context.config)}
  if (select count(*) from util.dataset_flow_identity_process_ledger
      where scope_id = ${sqlLiteral(scopeId)}::uuid
        and status = 'pending' and active) <> 2
    or (select jsonb_build_object(
          'generation', generation,
          'successful_process_posts', successful_process_posts,
          'status', status)
        from util.dataset_flow_identity_wrapper_invocations
        where id = ${sqlLiteral(invocationId)}::uuid)
      is distinct from jsonb_build_object(
        'generation', 0, 'successful_process_posts', 0, 'status', 'active')
    or (select status from util.dataset_flow_identity_scopes
        where id = ${sqlLiteral(scopeId)}::uuid) is distinct from 'sealed'
    or exists (select 1 from util.dataset_flow_identity_mutation_permits
      where scope_id = ${sqlLiteral(scopeId)}::uuid)
    or exists (select 1 from public.command_audit_log
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
        and payload->>'scope_id' = ${sqlLiteral(scopeId)})
    or exists (select 1 from util.dataset_derivative_rebuild_requests
      where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and target_id in (${processIds.map((id) => `${sqlLiteral(id)}::uuid`).join(',')}))
    or exists (select 1 from public.processes
      where id in (${processIds.map((id) => `${sqlLiteral(id)}::uuid`).join(',')})
        and version = '01.00.000'
        and json_ordered::jsonb #>>
          '{processDataSet,exchanges,exchange,0,referenceToFlowDataSet,@refObjectId}'
          is distinct from ${sqlLiteral(manifest.entities.source_flow_id)}) then
    raise exception 'POST_PRIMARY_FAULT_DID_NOT_ROLL_BACK_EXACTLY';
  end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - post-primary fault rolled back JSON, ledger, audit, derivative, and permit state')
) as t(tap);
commit;`;
}

function faultRemoveSql(context) {
  const faultMarker = `database-engine#269 hosted Preview fault scenario=${context.namespace} request=${context.requestId}`;
  return `begin;
set local search_path = extensions, public, auth;
do $gate$
declare source text;
begin
${contextSqlGate(context.config)}
  if to_regprocedure(
      'private.preview_flow_identity_post_primary_fault_v1()'
    ) is null
    or (select count(*)
        from pg_trigger as fault_trigger
        where fault_trigger.tgname =
            'preview_flow_identity_post_primary_fault_v1'
          and fault_trigger.tgrelid = 'public.command_audit_log'::regclass
          and not fault_trigger.tgisinternal
          and fault_trigger.tgenabled = 'O'
          and fault_trigger.tgfoid = to_regprocedure(
            'private.preview_flow_identity_post_primary_fault_v1()'
          )::oid) <> 1 then
    raise exception 'FAULT_HOOK_BINDING_MISMATCH';
  end if;
  select procedure.prosrc into source
  from pg_proc as procedure
  join pg_namespace as namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'private'
    and procedure.proname = 'preview_flow_identity_post_primary_fault_v1'
    and procedure.pronargs = 0;
  if source is null
    or position(${sqlLiteral(faultMarker)} in source) = 0 then
    raise exception 'FAULT_HOOK_MARKER_MISMATCH';
  end if;
end $gate$;
drop trigger preview_flow_identity_post_primary_fault_v1
  on public.command_audit_log;
drop function private.preview_flow_identity_post_primary_fault_v1();
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - exact post-primary fault hook is removed')
) as t(tap);
commit;`;
}

function raceProofSql(context, manifest, scopeId, invocationId, tokenSha256) {
  const [process1, process2] = manifest.entities.process_ids;
  return `begin;
set local search_path = extensions, public, auth;
do $proof$
begin
${contextSqlGate(context.config)}
  if (select jsonb_build_object(
        'generation', generation,
        'successful_process_posts', successful_process_posts,
        'status', status,
        'token_sha256', token_sha256)
      from util.dataset_flow_identity_wrapper_invocations
      where id = ${sqlLiteral(invocationId)}::uuid)
      is distinct from jsonb_build_object(
        'generation', 1,
        'successful_process_posts', 1,
        'status', 'active',
        'token_sha256', ${sqlLiteral(tokenSha256)})
    or (select count(*) from util.dataset_flow_identity_process_ledger
        where scope_id = ${sqlLiteral(scopeId)}::uuid
          and ordinal = 1 and status = 'completed'
          and wrapper_invocation_id = ${sqlLiteral(invocationId)}::uuid
          and permit_generation_before = 0
          and audit_id is not null and derivative_batch_id is not null) <> 1
    or (select count(*) from util.dataset_flow_identity_process_ledger
        where scope_id = ${sqlLiteral(scopeId)}::uuid
          and ordinal = 2 and status = 'pending'
          and audit_id is null and derivative_batch_id is null) <> 1
    or exists (select 1 from util.dataset_flow_identity_mutation_permits
      where scope_id = ${sqlLiteral(scopeId)}::uuid)
    or (select count(*) from public.command_audit_log
        where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
          and command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
          and payload->>'scope_id' = ${sqlLiteral(scopeId)}) <> 1
    or (select count(*) from util.dataset_derivative_rebuild_requests as child
        join util.dataset_flow_identity_process_ledger as ledger
          on ledger.derivative_batch_id = child.batch_id
          and ledger.process_id = child.target_id
          and ledger.process_version = child.target_version
        where ledger.scope_id = ${sqlLiteral(scopeId)}::uuid) <> 1
    or (select json_ordered::jsonb #>>
          '{processDataSet,exchanges,exchange,0,referenceToFlowDataSet,@refObjectId}'
        from public.processes where id = ${sqlLiteral(process1)}::uuid
          and version = '01.00.000')
      is distinct from ${sqlLiteral(manifest.entities.target_flow_id)}
    or (select json_ordered::jsonb #>>
          '{processDataSet,exchanges,exchange,0,referenceToFlowDataSet,@refObjectId}'
        from public.processes where id = ${sqlLiteral(process2)}::uuid
          and version = '01.00.000')
      is distinct from ${sqlLiteral(manifest.entities.source_flow_id)}
    or exists (select 1 from public.processes
      where id in (${sqlLiteral(process1)}::uuid, ${sqlLiteral(process2)}::uuid)
        and version = '01.00.000'
        and (
          jsonb_array_length(json_ordered::jsonb #>
            '{processDataSet,exchanges,exchange}') <> 3
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,0,meanAmount}' <> '5'
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,0,resultingAmount}' <> '5'
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,0,generalComment,#text}'
              <> 'preserve me'
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,1,meanAmount}' <> '7'
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,2,meanAmount}' <> '11'
          or json_ordered::jsonb #>>
            '{processDataSet,exchanges,exchange,2,referenceToFlowDataSet,@refObjectId}'
              <> ${sqlLiteral(manifest.entities.pending_flow_id)}
        ))
    or (select count(*) from public.flows
        where user_id = ${sqlLiteral(context.owner.userId)}::uuid
          and state_code = 0
          and json #>>
            '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
            = 'Elementary flow') <> 305
    or (select jsonb_build_object('user_id', user_id, 'state_code', state_code,
          'modified_at', modified_at)
        from public.flows where id = ${sqlLiteral(manifest.entities.source_flow_id)}::uuid
          and version = '01.00.000')
      is distinct from jsonb_build_object(
        'user_id', ${sqlLiteral(context.owner.userId)}::uuid,
        'state_code', 0,
        'modified_at', ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz)
    or (select jsonb_build_object('user_id', user_id, 'state_code', state_code,
          'modified_at', modified_at)
        from public.flows where id = ${sqlLiteral(manifest.entities.target_flow_id)}::uuid
          and version = '01.00.000')
      is distinct from jsonb_build_object(
        'user_id', ${sqlLiteral(context.foreign.userId)}::uuid,
        'state_code', 100,
        'modified_at', ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz)
    or (select count(*) from public.flowproperties
        where id = ${sqlLiteral(manifest.entities.flowproperty_id)}::uuid
          and version = '01.00.000'
          and user_id = ${sqlLiteral(context.foreign.userId)}::uuid
          and state_code = 100
          and modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz) <> 1
    or (select count(*) from public.unitgroups
        where id = ${sqlLiteral(manifest.entities.unitgroup_id)}::uuid
          and version = '01.00.000'
          and user_id = ${sqlLiteral(context.foreign.userId)}::uuid
          and state_code = 100
          and modified_at = ${sqlLiteral(manifest.fixture_modified_at)}::timestamptz) <> 1 then
    raise exception 'RACE_READBACK_PROOF_MISMATCH';
  end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - exactly one race mutation, audit, derivative child, and permit rotation committed')
) as t(tap);
commit;`;
}

function lifecycleProofSql(
  context,
  manifest,
  scopeId,
  invocationId,
  finalizeStatus,
  permit2TokenSha256,
  finalizeTokenSha256,
) {
  const [process1, process2] = manifest.entities.process_ids;
  const invocationStatus = finalizeStatus === 'completed' ? 'completed' : 'active';
  return `begin;
set local search_path = extensions, public, auth;
do $proof$
begin
${contextSqlGate(context.config)}
  if (select jsonb_build_object(
        'generation', generation,
        'successful_process_posts', successful_process_posts,
        'successful_finalize_posts', successful_finalize_posts,
        'status', status)
      from util.dataset_flow_identity_wrapper_invocations
      where id = ${sqlLiteral(invocationId)}::uuid
        and actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
        and scope_id = ${sqlLiteral(scopeId)}::uuid)
      is distinct from jsonb_build_object(
        'generation', 3,
        'successful_process_posts', 2,
        'successful_finalize_posts', 1,
        'status', ${sqlLiteral(invocationStatus)})
    or (select token_sha256
        from util.dataset_flow_identity_wrapper_invocations
        where id = ${sqlLiteral(invocationId)}::uuid)
      = ${sqlLiteral(permit2TokenSha256)}
    ${finalizeTokenSha256 === null ? '' : `or (select token_sha256
        from util.dataset_flow_identity_wrapper_invocations
        where id = ${sqlLiteral(invocationId)}::uuid)
      is distinct from ${sqlLiteral(finalizeTokenSha256)}`}
    or (select count(*) from util.dataset_flow_identity_process_ledger
        where scope_id = ${sqlLiteral(scopeId)}::uuid
          and status = 'completed'
          and wrapper_invocation_id = ${sqlLiteral(invocationId)}::uuid
          and audit_id is not null
          and derivative_batch_id is not null) <> 2
    or exists (select 1 from util.dataset_flow_identity_mutation_permits
        where scope_id = ${sqlLiteral(scopeId)}::uuid)
    or (select count(*) from public.command_audit_log
        where actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid
          and command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
          and payload->>'scope_id' = ${sqlLiteral(scopeId)}) <> 2
    or (select count(*) from util.dataset_derivative_rebuild_requests as child
        join util.dataset_flow_identity_process_ledger as ledger
          on ledger.derivative_batch_id = child.batch_id
          and ledger.process_id = child.target_id
          and ledger.process_version = child.target_version
        where ledger.scope_id = ${sqlLiteral(scopeId)}::uuid) <> 2
    or (select status from util.dataset_flow_identity_scopes
        where id = ${sqlLiteral(scopeId)}::uuid
          and actor_user_id = ${sqlLiteral(context.owner.userId)}::uuid)
      is distinct from ${sqlLiteral(finalizeStatus)}
    or exists (select 1 from public.processes
        where id in (${sqlLiteral(process1)}::uuid, ${sqlLiteral(process2)}::uuid)
          and version = '01.00.000'
          and (
            user_id is distinct from ${sqlLiteral(context.owner.userId)}::uuid
            or state_code is distinct from 0
            or jsonb_array_length(json_ordered::jsonb #>
              '{processDataSet,exchanges,exchange}') <> 3
            or json_ordered::jsonb #>>
              '{processDataSet,exchanges,exchange,0,referenceToFlowDataSet,@refObjectId}'
                is distinct from ${sqlLiteral(manifest.entities.target_flow_id)}
            or json_ordered::jsonb #>>
              '{processDataSet,exchanges,exchange,0,meanAmount}' <> '5'
            or json_ordered::jsonb #>>
              '{processDataSet,exchanges,exchange,0,resultingAmount}' <> '5'
            or json_ordered::jsonb #>>
              '{processDataSet,exchanges,exchange,0,generalComment,#text}'
                <> 'preserve me'
          )) then
    raise exception 'FULL_LIFECYCLE_READBACK_PROOF_MISMATCH';
  end if;
end $proof$;
select tap from (values
  ('TAP version 13'), ('1..1'),
  ('ok 1 - both process rewrites and one finalize post have exact durable closure')
) as t(tap);
commit;`;
}

function templateValues(context) {
  return {
    ACTOR_UUID_SQL: context.owner.userId,
    ACTOR_EMAIL_SQL: context.owner.email,
    FOREIGN_UUID_SQL: context.foreign.userId,
    FOREIGN_EMAIL_SQL: context.foreign.email,
    SCENARIO_NAMESPACE_SQL: context.namespace,
    REQUEST_ID_SQL: context.requestId,
    PREVIEW_REF_SQL: context.config.projectRef,
    PREVIEW_URL_SQL: context.config.supabaseUrl,
    SERVICE_ROLE_KEY_SQL: context.config.serviceRoleKey,
  };
}

async function renderCleanupTemplate(context) {
  return renderSqlTemplate(CLEANUP_TEMPLATE, templateValues(context));
}

async function raceWorkerMain() {
  assertCondition(typeof fetch === 'function', 'NODE_FETCH_UNAVAILABLE');
  process.stdout.write('READY\n');
  let input = '';
  for await (const chunk of process.stdin) {
    input += chunk.toString('utf8');
    if (input.length > 2 * 1024 * 1024) fail('RACE_WORKER_INPUT_TOO_LARGE');
    if (input.includes('\n')) break;
  }
  const line = input.slice(0, input.indexOf('\n'));
  let payload;
  try { payload = JSON.parse(line); } catch { fail('RACE_WORKER_INPUT_INVALID'); }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), payload.timeout_ms);
  timeout.unref();
  let response;
  try {
    response = await fetch(`${payload.url}/rest/v1/rpc/cmd_dataset_flow_identity_process_rewrite_guarded`, {
      method: 'POST',
      redirect: 'error',
      signal: controller.signal,
      headers: {
        apikey: payload.anon_key,
        authorization: `Bearer ${payload.access_token}`,
        accept: 'application/json',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        p_scope_id: payload.scope_id,
        p_request: payload.process_request,
        p_authorization: payload.execution_permit,
      }),
    });
  } catch {
    clearTimeout(timeout);
    fail('RACE_WORKER_TRANSPORT_FAILED');
  }
  let text;
  try {
    text = await readResponseTextBounded(
      response,
      1024 * 1024,
      controller,
      'RACE_WORKER_RESPONSE_TOO_LARGE',
    );
  } catch (error) {
    clearTimeout(timeout);
    if (error instanceof SafeError) throw error;
    fail('RACE_WORKER_RESPONSE_READ_FAILED');
  }
  clearTimeout(timeout);
  let body;
  try { body = JSON.parse(text); } catch { fail('RACE_WORKER_RESPONSE_INVALID'); }
  process.stdout.write(`${JSON.stringify({ status: response.status, body })}\n`);
}

function createRaceWorker(timeoutMs) {
  const child = spawn(process.execPath, [SCRIPT_PATH, '--race-worker'], {
    env: {}, stdio: ['pipe', 'pipe', 'pipe'], shell: false,
  });
  const lines = readline.createInterface({ input: child.stdout });
  let readyResolve;
  let readyReject;
  let resultResolve;
  let resultReject;
  const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  const result = new Promise((resolve, reject) => { resultResolve = resolve; resultReject = reject; });
  let sawReady = false;
  let sawResult = false;
  let stderrBytes = 0;
  const stderrHash = createHash('sha256');
  child.stderr.on('data', (chunk) => {
    stderrBytes += chunk.length;
    stderrHash.update(chunk);
    if (stderrBytes > 64 * 1024) child.kill('SIGTERM');
  });
  lines.on('line', (line) => {
    if (!sawReady) {
      if (line !== 'READY') {
        const error = new SafeError('RACE_WORKER_READY_INVALID');
        readyReject(error); resultReject(error); child.kill('SIGTERM'); return;
      }
      sawReady = true; readyResolve(); return;
    }
    if (sawResult) {
      resultReject(new SafeError('RACE_WORKER_MULTIPLE_RESULTS')); child.kill('SIGTERM'); return;
    }
    try {
      const parsed = JSON.parse(line);
      assertCondition(isPlainObject(parsed) && isPlainObject(parsed.body), 'RACE_WORKER_RESULT_INVALID');
      sawResult = true;
      resultResolve(parsed);
    } catch (error) {
      resultReject(error instanceof SafeError ? error : new SafeError('RACE_WORKER_RESULT_INVALID'));
    }
  });
  child.once('error', () => {
    const error = new SafeError('RACE_WORKER_SPAWN_FAILED');
    readyReject(error); resultReject(error);
  });
  child.once('close', (code) => {
    if (!sawReady) readyReject(new SafeError('RACE_WORKER_EXIT_BEFORE_READY', { exit_code: code }));
    if (!sawResult) resultReject(new SafeError('RACE_WORKER_EXIT_BEFORE_RESULT', {
      exit_code: code, stderr_bytes: stderrBytes, stderr_sha256: stderrHash.digest('hex'),
    }));
  });
  const timer = setTimeout(() => {
    child.kill('SIGTERM');
    const error = new SafeError('RACE_WORKER_TIMEOUT');
    readyReject(error); resultReject(error);
  }, timeoutMs);
  timer.unref();
  result.finally(() => clearTimeout(timer)).catch(() => {});
  return { child, ready, result };
}

async function concurrentProcessPosts(context, scopeId, processRequest, permit) {
  const workers = [
    createRaceWorker(context.config.raceTimeoutMs),
    createRaceWorker(context.config.raceTimeoutMs),
  ];
  try {
    await Promise.all(workers.map((worker) => worker.ready));
    const payload = JSON.stringify({
      url: context.config.supabaseUrl,
      anon_key: context.config.anonKey,
      access_token: context.owner.accessToken,
      timeout_ms: context.config.rpcTimeoutMs,
      scope_id: scopeId,
      process_request: processRequest,
      execution_permit: permit,
    });
    for (const worker of workers) worker.child.stdin.end(`${payload}\n`);
    const results = await Promise.all(workers.map((worker) => worker.result));
    const responseSummary = (item) => ({
      status: item.status,
      code: item.body.code ?? null,
      ok: typeof item.body.ok === 'boolean' ? item.body.ok : null,
    });
    const successes = results.filter((item) => item.status === 200 && item.body.ok === true);
    const rejections = results.filter((item) => item.body.ok === false);
    assertCondition(successes.length === 1 && rejections.length === 1, 'RACE_CARDINALITY_INVALID', {
      success_count: successes.length, rejection_count: rejections.length,
      response_set_sha256: sha256(results.map(responseSummary)),
    });
    const rejectionCode = requireString(rejections[0].body.code, 'RACE_REJECTION_CODE_MISSING', SAFE_CODE_RE);
    assertCondition(ALLOWED_RACE_REJECTIONS.has(rejectionCode), 'RACE_REJECTION_CODE_INVALID', {
      remote_code: rejectionCode,
    });
    return {
      success: successes[0].body,
      rejectionCode,
      responseSetSha256: sha256(results.map(responseSummary)),
    };
  } finally {
    for (const worker of workers) {
      if (!worker.child.killed) worker.child.kill('SIGTERM');
    }
  }
}

async function execute(config, args) {
  const context = {
    config,
    namespace: args.scenarioNamespace,
    requestId: args.requestId,
    applicationName: null,
    owner: null,
    foreign: null,
    actorCandidates: [],
    recoverySelectors: null,
    recoveryCheckpointClient: null,
  };
  context.applicationName = `fi269-${context.namespace}`;
  context.recoveryCheckpointClient = createRecoveryCheckpointClient({
    requestId: context.requestId,
    namespace: context.namespace,
  });
  const dbOptions = Object.freeze({ applicationName: context.applicationName });
  const recoverySelectors = createRecoverySelectors(context);
  context.recoverySelectors = recoverySelectors;
  let tempDir = null;
  let fixtureAttempted = false;
  let cleanupFailure = null;
  const cleanupFailures = [];
  let primaryError = null;
  let resultEvidence = null;
  const dbProofs = {};
  const cleanup = {
    fixture_cleanup_ran: false,
    manifest_rows_after_cleanup: null,
    owner_session_revoked: false,
    foreign_session_revoked: false,
    owner_deleted: false,
    foreign_deleted: false,
    owner_absence_confirmed: false,
    foreign_absence_confirmed: false,
    owner_retained: false,
    foreign_retained: false,
    actors_retained_for_cleanup: false,
    recovery_actor_count: 0,
    recovery_actors: [],
    cleanup_closed: false,
    cleanup_failure_codes: [],
    cleanup_checkpoint_acknowledged: false,
    recovery_selectors: null,
    recovery_selector_set_sha256: null,
    residue_readback_ran: false,
    residue_readback_passed: false,
    residue_count: null,
    residue_count_set_sha256: null,
    temp_dir_removed: false,
  };
  const recordCleanupFailure = (error, fallbackCode) => {
    const safe = error instanceof SafeError ? error : new SafeError(fallbackCode);
    cleanupFailures.push(safe);
    cleanupFailure ??= safe;
  };

  try {
    tempDir = await validatePrivateTempDir(args.privateTempDir);
    await checkpointRecovery(context, 'runner_ready');
    dbProofs.transport_preflight = await runTransportPreflight(config, tempDir);
    context.owner = await createDisposableActor(config, 'owner', context);
    context.foreign = await createDisposableActor(config, 'foreign', context);
    addSelector(recoverySelectors, 'actor_user_ids', context.owner.userId);
    addSelector(recoverySelectors, 'actor_user_ids', context.foreign.userId);
    await checkpointRecovery(context, 'actors_bound');
    const fixture = await renderSqlTemplate(FIXTURE_TEMPLATE, templateValues(context));
    fixtureAttempted = true;
    dbProofs.fixture = await runDbTest(
      config,
      tempDir,
      'fixture',
      fixture.sql,
      dbOptions,
    );
    await checkpointRecovery(context, 'fixture_committed');

    const manifestRows = await readFixtureManifest(config, context.owner.userId, context.requestId);
    assertCondition(manifestRows.length === 1, 'FIXTURE_MANIFEST_CARDINALITY_INVALID', { observed: manifestRows.length });
    const manifest = validateManifest(manifestRows[0].payload, context);
    recoverySelectors.operation_id = requireString(
      manifest.operation_id,
      'RECOVERY_SELECTOR_OPERATION_INVALID',
    );
    for (const key of [
      'unitgroup_id', 'flowproperty_id', 'source_flow_id',
      'target_flow_id', 'pending_flow_id',
    ]) addSelector(recoverySelectors, 'support_entity_ids', manifest.entities[key]);
    for (const processId of manifest.entities.process_ids) {
      addSelector(recoverySelectors, 'process_ids', processId);
    }
    await checkpointRecovery(context, 'fixture_manifest_bound');

    const foreignResult = await rpc(
      config, context.foreign,
      'cmd_dataset_flow_identity_capture_attest_guarded',
      { p_request: manifest.capture_request },
    );
    assertCondition(
      foreignResult.ok === false
        && foreignResult.code === 'FLOW_IDENTITY_CAPTURE_INVALID_REQUEST',
      'FOREIGN_ACTOR_ACL_PROOF_FAILED',
      { remote_code: safeRemoteCode(foreignResult.code) },
    );

    const capture = validateCapture(await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_capture_attest_guarded',
      { p_request: manifest.capture_request },
    ));
    addSelector(recoverySelectors, 'receipt_ids', capture.receipt_id);
    await checkpointRecovery(context, 'capture_receipt_bound');

    const preflightRequest = buildPreflightRequest(manifest, capture);
    dbProofs.drift = await runDbTest(
      config,
      tempDir,
      'drift',
      driftSql(context, manifest),
      dbOptions,
    );
    const driftRejected = await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_scope_preflight_guarded',
      { p_request: preflightRequest },
    );
    assertCondition(
      driftRejected.ok === false
        && driftRejected.code === 'FLOW_IDENTITY_PREFLIGHT_LIVE_DRIFT',
      'LIVE_DRIFT_NOT_REJECTED',
      { remote_code: safeRemoteCode(driftRejected.code) },
    );
    dbProofs.driftRejected = await runDbTest(
      config, tempDir, 'drift-rejected-proof',
      driftRejectedProofSql(context, manifest),
      dbOptions,
    );
    dbProofs.restore = await runDbTest(
      config,
      tempDir,
      'restore',
      restoreSql(context, manifest),
      dbOptions,
    );

    const preflight = await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_scope_preflight_guarded',
      { p_request: preflightRequest },
    );
    assertCondition(preflight.ok === true && preflight.replay === false && preflight.status === 'sealed', 'PREFLIGHT_FAILED', {
      remote_code: safeRemoteCode(preflight.code),
    });
    requireInteger(preflight.process_count, 2, 'PREFLIGHT_PROCESS_COUNT_INVALID');
    requireInteger(preflight.source_universe_count, 305, 'PREFLIGHT_SOURCE_COUNT_INVALID');
    const scopeId = requireUuid(preflight.scope_id, 'PREFLIGHT_SCOPE_ID_INVALID');
    const scopeProof = requireSha(preflight.scope_proof_sha256, 'PREFLIGHT_SCOPE_PROOF_INVALID');
    const permit0 = validatePermit(preflight.execution_permit, 0);
    const invocationId = permit0.invocation_id;
    addSelector(recoverySelectors, 'scope_ids', scopeId);
    addSelector(recoverySelectors, 'wrapper_invocation_ids', invocationId);
    await checkpointRecovery(context, 'scope_bound');

    const scopeRead = await rpc(
      config, context.owner, 'cmd_dataset_flow_identity_scope_read',
      { p_scope_id: scopeId },
    );
    assertCondition(scopeRead.ok === true && scopeRead.status === 'sealed', 'SEALED_SCOPE_READ_INVALID');
    const requests = processRequests(scopeRead, scopeProof);
    const finalizeRequest = buildFinalizeRequest(scopeProof);

    const foreignScopeRead = await rpc(
      config, context.foreign, 'cmd_dataset_flow_identity_scope_read',
      { p_scope_id: scopeId },
    );
    assertCondition(
      foreignScopeRead.ok === false
        && foreignScopeRead.code === 'FLOW_IDENTITY_SCOPE_NOT_FOUND',
      'FOREIGN_SCOPE_READ_FENCE_FAILED',
      { remote_code: safeRemoteCode(foreignScopeRead.code) },
    );
    const foreignProcess = await rpc(
      config, context.foreign,
      'cmd_dataset_flow_identity_process_rewrite_guarded',
      { p_scope_id: scopeId, p_request: requests[0], p_authorization: permit0 },
    );
    assertCondition(
      foreignProcess.ok === false
        && foreignProcess.code === 'FLOW_IDENTITY_SCOPE_PROOF_MISMATCH',
      'FOREIGN_PROCESS_FENCE_FAILED',
      { remote_code: safeRemoteCode(foreignProcess.code) },
    );
    const foreignFinalize = await rpc(
      config, context.foreign,
      'cmd_dataset_flow_identity_scope_finalize_guarded',
      {
        p_scope_id: scopeId,
        p_request: finalizeRequest,
        p_authorization: permit0,
      },
    );
    assertCondition(
      foreignFinalize.ok === false
        && foreignFinalize.code === 'FLOW_IDENTITY_FINALIZE_SCOPE_PROOF_MISMATCH',
      'FOREIGN_FINALIZE_FENCE_FAILED',
      { remote_code: safeRemoteCode(foreignFinalize.code) },
    );

    dbProofs.faultInstall = await runDbTest(
      config, tempDir, 'fault-install', faultInstallSql(context, manifest, scopeId),
      dbOptions,
    );
    const faultResponse = await rpcThrown(
      config, context.owner,
      'cmd_dataset_flow_identity_process_rewrite_guarded',
      { p_scope_id: scopeId, p_request: requests[0], p_authorization: permit0 },
    );
    assertCondition(
      faultResponse.body?.code === 'P0001'
        && faultResponse.body?.message === 'FLOW_IDENTITY_PREVIEW_POST_PRIMARY_FAULT',
      'POST_PRIMARY_FAULT_NOT_OBSERVED',
      { http_status: faultResponse.status, remote_code: safeRemoteCode(faultResponse.body?.code) },
    );
    dbProofs.rollback = await runDbTest(
      config, tempDir, 'rollback-proof',
      rollbackProofSql(context, manifest, scopeId, invocationId),
      dbOptions,
    );
    dbProofs.faultRemove = await runDbTest(
      config, tempDir, 'fault-remove', faultRemoveSql(context),
      dbOptions,
    );

    const race = await concurrentProcessPosts(context, scopeId, requests[0], permit0);
    assertCondition(race.success.ordinal === 1 && race.success.replay === false, 'RACE_SUCCESS_INVALID');
    const permit1 = validatePermit(race.success.execution_permit, 1, invocationId);
    assertCondition(permit1.token !== permit0.token, 'RACE_PERMIT_NOT_ROTATED');

    const postFirstMutationRead = await rpc(
      config, context.owner, 'cmd_dataset_flow_identity_scope_read',
      { p_scope_id: scopeId },
    );
    assertCondition(
      postFirstMutationRead.ok === true
        && postFirstMutationRead.completed_process_count === 1
        && postFirstMutationRead.pending_process_count === 1,
      'POST_FIRST_MUTATION_SCOPE_READ_INVALID',
      { remote_code: safeRemoteCode(postFirstMutationRead.code) },
    );
    await captureDerivativeRecoverySelectors(
      context,
      postFirstMutationRead,
      recoverySelectors,
      1,
    );
    await checkpointRecovery(context, 'process_1_committed');

    const stale = await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_process_rewrite_guarded',
      { p_scope_id: scopeId, p_request: requests[1], p_authorization: permit0 },
    );
    assertCondition(
      stale.ok === false && stale.code === 'FLOW_IDENTITY_WRAPPER_PERMIT_REQUIRED',
      'STALE_PERMIT_NOT_REJECTED',
      { remote_code: safeRemoteCode(stale.code) },
    );

    const tokenSha256 = sha256(permit1.token);
    dbProofs.race = await runDbTest(
      config, tempDir, 'race-proof',
      raceProofSql(context, manifest, scopeId, invocationId, tokenSha256),
      dbOptions,
    );

    const secondProcess = await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_process_rewrite_guarded',
      { p_scope_id: scopeId, p_request: requests[1], p_authorization: permit1 },
    );
    assertCondition(
      secondProcess.ok === true
        && secondProcess.ordinal === 2
        && secondProcess.replay === false,
      'SECOND_PROCESS_REWRITE_FAILED',
      { remote_code: safeRemoteCode(secondProcess.code) },
    );
    const permit2 = validatePermit(secondProcess.execution_permit, 2, invocationId);
    assertCondition(permit2.token !== permit1.token, 'SECOND_PROCESS_PERMIT_NOT_ROTATED');

    const preFinalizeRead = await rpc(
      config, context.owner, 'cmd_dataset_flow_identity_scope_read',
      { p_scope_id: scopeId },
    );
    assertCondition(
      preFinalizeRead.ok === true
        && preFinalizeRead.completed_process_count === 2
        && preFinalizeRead.pending_process_count === 0
        && preFinalizeRead.processes?.[0]?.status === 'completed'
        && preFinalizeRead.processes?.[1]?.status === 'completed',
      'PRE_FINALIZE_SCOPE_READ_INVALID',
      { remote_code: safeRemoteCode(preFinalizeRead.code) },
    );
    await captureDerivativeRecoverySelectors(
      context,
      preFinalizeRead,
      recoverySelectors,
      2,
    );
    await checkpointRecovery(context, 'process_2_committed');

    const finalize = await rpc(
      config, context.owner,
      'cmd_dataset_flow_identity_scope_finalize_guarded',
      {
        p_scope_id: scopeId,
        p_request: finalizeRequest,
        p_authorization: permit2,
      },
    );
    assertCondition(
      finalize.ok === true
        && ['derivatives_pending', 'completed'].includes(finalize.status)
        && finalize.process_count === 2
        && finalize.completed_process_count === 2
        && finalize.rewrite_count === 2
        && finalize.invocation_id === invocationId
        && finalize.permit_generation_before === 2
        && finalize.replay === false,
      'FINALIZE_RESULT_INVALID',
      { remote_code: safeRemoteCode(finalize.code) },
    );
    let finalizeTokenSha256 = null;
    if (finalize.status === 'derivatives_pending') {
      const permit3 = validatePermit(finalize.execution_permit, 3, invocationId);
      assertCondition(permit3.token !== permit2.token, 'FINALIZE_PERMIT_NOT_ROTATED');
      finalizeTokenSha256 = sha256(permit3.token);
    } else {
      assertCondition(finalize.execution_permit === null, 'TERMINAL_FINALIZE_PERMIT_NOT_CONSUMED');
    }
    addSelector(
      recoverySelectors,
      'audit_ids',
      optionalDecimalId(finalize.audit_id, 'RECOVERY_SELECTOR_FINALIZE_AUDIT_INVALID'),
    );
    const postFinalizeRead = await rpc(
      config, context.owner, 'cmd_dataset_flow_identity_scope_read',
      { p_scope_id: scopeId },
    );
    assertCondition(
      postFinalizeRead.ok === true
        && postFinalizeRead.completed_process_count === 2
        && postFinalizeRead.pending_process_count === 0,
      'POST_FINALIZE_SCOPE_READ_INVALID',
      { remote_code: safeRemoteCode(postFinalizeRead.code) },
    );
    await captureDerivativeRecoverySelectors(
      context,
      postFinalizeRead,
      recoverySelectors,
      2,
    );
    await checkpointRecovery(context, 'finalize_selectors_bound');

    dbProofs.lifecycle = await runDbTest(
      config,
      tempDir,
      'lifecycle-proof',
      lifecycleProofSql(
        context,
        manifest,
        scopeId,
        invocationId,
        finalize.status,
        sha256(permit2.token),
        finalizeTokenSha256,
      ),
      dbOptions,
    );

    resultEvidence = {
      schema_version: EVIDENCE_SCHEMA,
      status: 'passed',
      environment: 'preview',
      project_ref_sha256: sha256(config.projectRef),
      fixture_template_sha256: fixture.templateSha256,
      fixture_rendered_sha256: fixture.renderedSha256,
      manifest_sha256: sha256(manifest),
      capture_proof_sha256: sha256(capture),
      scope_proof_sha256: scopeProof,
      exact_counts: EXPECTED,
      foreign_actor_rejection: foreignResult.code,
      foreign_scope_read_rejection: foreignScopeRead.code,
      foreign_process_rejection: foreignProcess.code,
      foreign_finalize_rejection: foreignFinalize.code,
      live_drift_rejection: driftRejected.code,
      post_primary_fault_sqlstate: faultResponse.body.code,
      race_rejection: race.rejectionCode,
      race_response_set_sha256: race.responseSetSha256,
      stale_permit_rejection: stale.code,
      invocation_generation_after: 3,
      successful_process_posts: 2,
      successful_finalize_posts: 1,
      finalize_status: finalize.status,
      completed_process_count: preFinalizeRead.completed_process_count,
      pending_process_count: preFinalizeRead.pending_process_count,
      final_read_sha256: sha256(preFinalizeRead),
      finalize_result_sha256: sha256(finalize),
      recovery_selectors: normalizedRecoverySelectors(recoverySelectors),
      recovery_selector_set_sha256: sha256(
        normalizedRecoverySelectors(recoverySelectors),
      ),
      db_proofs: summarizeDbProofs(dbProofs),
    };
  } catch (error) {
    primaryError = error instanceof SafeError ? error : new SafeError('HOSTED_PREVIEW_E2E_FAILED');
  } finally {
    cleanup.recovery_selectors = normalizedRecoverySelectors(recoverySelectors);
    cleanup.recovery_selector_set_sha256 = sha256(cleanup.recovery_selectors);
    if (context.owner && context.foreign && fixtureAttempted && tempDir) {
      try {
        const cleanupTemplate = await renderCleanupTemplate(context);
        const proof = await runDbTest(
          config,
          tempDir,
          'cleanup',
          cleanupTemplate.sql,
          dbOptions,
        );
        dbProofs.cleanup = proof;
        cleanup.fixture_cleanup_ran = true;
        cleanup.cleanup_template_sha256 = cleanupTemplate.templateSha256;
        cleanup.cleanup_rendered_sha256 = cleanupTemplate.renderedSha256;
        cleanup.cleanup_stdout_sha256 = proof.stdout_sha256;
        cleanup.cleanup_stderr_sha256 = proof.stderr_sha256;
        const remaining = await readFixtureManifest(config, context.owner.userId, context.requestId);
        cleanup.manifest_rows_after_cleanup = remaining.length;
        assertCondition(remaining.length === 0, 'CLEANUP_MANIFEST_RESIDUE');
        await checkpointRecovery(context, 'cleanup_sql_committed');
        cleanup.cleanup_checkpoint_acknowledged = true;
      } catch (error) {
        recordCleanupFailure(error, 'FIXTURE_CLEANUP_FAILED');
      }
    }

    const fixtureCleanupClosed = !fixtureAttempted || (
      cleanup.fixture_cleanup_ran
      && cleanup.manifest_rows_after_cleanup === 0
      && cleanup.cleanup_checkpoint_acknowledged
    );
    const actorCleanupFailures = await cleanupActorCandidates(
      config,
      context.actorCandidates,
      fixtureCleanupClosed,
      context,
    );
    for (const error of actorCleanupFailures) {
      recordCleanupFailure(error, 'AUTH_ACTOR_CLEANUP_FAILED');
    }
    for (const candidate of context.actorCandidates) {
      cleanup[`${candidate.role}_session_revoked`] = candidate.sessionRevoked;
      cleanup[`${candidate.role}_deleted`] = candidate.deleteAcknowledged
        && candidate.absenceConfirmed;
      cleanup[`${candidate.role}_absence_confirmed`] = candidate.absenceConfirmed;
      cleanup[`${candidate.role}_retained`] = !candidate.absenceConfirmed;
    }
    cleanup.recovery_actor_count = context.actorCandidates.length;
    cleanup.recovery_actors = context.actorCandidates.map(actorRecoverySummary);
    cleanup.actors_retained_for_cleanup = context.actorCandidates.some(
      (candidate) => !candidate.absenceConfirmed,
    );
    if (context.owner && context.foreign && fixtureAttempted && tempDir) {
      try {
        const residueTemplate = await renderResidueReadback(
          context,
          recoverySelectors,
        );
        cleanup.residue_readback_ran = true;
        const proof = await runDbTest(
          config,
          tempDir,
          'residue-readback',
          residueTemplate.sql,
          { ...dbOptions, captureStdout: true },
        );
        dbProofs.residue_readback = proof;
        const counts = parseResidueCounts(proof.stdout_text);
        cleanup.residue_template_sha256 = residueTemplate.templateSha256;
        cleanup.residue_rendered_sha256 = residueTemplate.renderedSha256;
        cleanup.residue_stdout_sha256 = proof.stdout_sha256;
        cleanup.residue_stderr_sha256 = proof.stderr_sha256;
        cleanup.residue_count = Object.keys(counts).length;
        cleanup.residue_count_set_sha256 = sha256(counts);
        cleanup.residue_readback_passed = true;
      } catch (error) {
        recordCleanupFailure(error, 'RESIDUE_READBACK_FAILED');
      }
    }
    if (tempDir) {
      try {
        await rm(tempDir, { recursive: true, force: true });
        cleanup.temp_dir_removed = true;
      } catch {
        recordCleanupFailure(new SafeError('TEMP_DIR_REMOVE_FAILED', {
          temp_dir_path_sha256: sha256(tempDir),
        }), 'TEMP_DIR_REMOVE_FAILED');
      }
    }
    cleanup.cleanup_failure_codes = [...new Set([
      ...cleanupFailures.map((error) => error.code),
      ...context.actorCandidates.flatMap((candidate) => candidate.cleanupFailureCodes),
    ])];
    const checkpointSummary = context.recoveryCheckpointClient.summary();
    cleanup.recovery_checkpoint_count = checkpointSummary.checkpoint_count;
    cleanup.last_recovery_checkpoint_sha256 =
      checkpointSummary.last_checkpoint_sha256;
    cleanup.cleanup_closed = fixtureCleanupClosed
      && cleanupFailure === null
      && context.actorCandidates.every((candidate) => candidate.absenceConfirmed)
      && (!fixtureAttempted || cleanup.residue_readback_passed)
      && cleanup.temp_dir_removed;
  }

  if (primaryError) {
    throw attachCleanupDetails(primaryError, {
      ...cleanup,
      cleanup_failure_code: cleanupFailure?.code ?? null,
    }, context.actorCandidates);
  }
  if (cleanupFailure) {
    throw attachCleanupDetails(cleanupFailure, {
      ...cleanup,
      cleanup_failure_code: cleanupFailure.code,
    }, context.actorCandidates);
  }
  assertCondition(
    cleanup.fixture_cleanup_ran
      && cleanup.manifest_rows_after_cleanup === 0
      && cleanup.owner_session_revoked
      && cleanup.foreign_session_revoked
      && cleanup.owner_deleted
      && cleanup.foreign_deleted
      && cleanup.owner_absence_confirmed
      && cleanup.foreign_absence_confirmed
      && cleanup.residue_readback_ran
      && cleanup.residue_readback_passed
      && cleanup.residue_count === RESIDUE_COUNT_NAMES.length
      && cleanup.cleanup_closed,
    'CLEANUP_PROOF_INCOMPLETE',
    cleanup,
  );
  resultEvidence.db_proofs = summarizeDbProofs(dbProofs);
  resultEvidence.cleanup = cleanup;
  return resultEvidence;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.raceWorker) {
    await raceWorkerMain();
    return;
  }
  if (args.help) {
    process.stdout.write(`${helpText()}\n`);
    return;
  }
  assertCondition(typeof fetch === 'function', 'NODE_FETCH_UNAVAILABLE');
  const config = loadConfig(args.expectedPreviewRef);
  const startedAt = new Date().toISOString();
  if (args.transportPreflightOnly) {
    const evidence = await executeTransportPreflightOnly(config);
    process.stdout.write(`${JSON.stringify(sanitizeEvidence({
      ...evidence,
      started_at_sha256: sha256(startedAt),
      completed_at_sha256: sha256(new Date().toISOString()),
      cli_version_sha256: sha256(SUPABASE_CLI_VERSION),
    }))}\n`);
    return;
  }
  const evidence = await execute(config, args);
  process.stdout.write(`${JSON.stringify(sanitizeEvidence({
    ...evidence,
    started_at_sha256: sha256(startedAt),
    completed_at_sha256: sha256(new Date().toISOString()),
    cli_version_sha256: sha256(SUPABASE_CLI_VERSION),
  }))}\n`);
}

export const __test = Object.freeze({
  RESIDUE_COUNT_NAMES,
  SafeError,
  actorRecoverySummary,
  attachCleanupDetails,
  bindDbApplicationName,
  cleanupActorCandidate,
  cleanupActorCandidates,
  createRecoveryCheckpointClient,
  confirmDisposableActorAbsent,
  confirmDisposableActorSelectorAbsent,
  createRecoverySelectors,
  createDisposableActor,
  findDisposableActor,
  listExactDisposableActors,
  normalizedRecoverySelectors,
  parseResidueCounts,
  readResponseTextBounded,
  reacquireActorSessionForCleanup,
  renderCleanupTemplate,
  renderResidueReadback,
  validatePrivateTempDir,
});

const IS_MAIN = typeof process.argv[1] === 'string'
  && path.resolve(process.argv[1]) === path.resolve(SCRIPT_PATH);

if (IS_MAIN) {
  main().catch((error) => {
    const safe = error instanceof SafeError ? error : new SafeError('UNHANDLED_FAILURE');
    process.stderr.write(`${JSON.stringify(sanitizeEvidence({
      schema_version: EVIDENCE_SCHEMA,
      status: 'failed',
      code: safe.code,
      details: safe.details,
    }))}\n`);
    process.exitCode = 1;
  });
}
