#!/usr/bin/env node

/**
 * Hosted Preview E2E for guarded Step 3 owner-draft flow-identity rewrites.
 *
 * No dotenv or third-party runtime dependency is used. Raw execution permits
 * stay in memory and cross the race-worker boundary only over stdin. Rendered
 * SQL containing branch credentials is held in a private mode-0600 temporary
 * file for the pinned Supabase CLI and removed in finally; the DB URL is a
 * bounded child argv value. Emitted evidence contains hashes and counts only.
 * The matching SQL cleanup is mandatory even after a failure.
 */

import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
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

const FIXTURE_COMMAND = 'preview_e2e_flow_identity_fixture';
const FIXTURE_TARGET_TABLE = 'preview_e2e_flow_identity';
const FIXTURE_TARGET_VERSION = '00.00.001';
const MANIFEST_SCHEMA = 'protected-flow-identity-preview-fixture.v1';
const EVIDENCE_SCHEMA = 'protected-flow-identity-rest-e2e-evidence.v1';
const TRANSPORT_EVIDENCE_SCHEMA = 'protected-flow-identity-transport-preflight-evidence.v1';
const SUPABASE_CLI_VERSION = '2.109.1';
const PRODUCTION_REF = 'qgzvkongdjqiiamzbbts';
const DB_TEST_CHILD_ENV_NAMES = Object.freeze([
  'PATH', 'HOME', 'TMPDIR', 'TMP', 'TEMP', 'LANG', 'LC_ALL',
  'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy',
  'no_proxy', 'NPM_CONFIG_CACHE', 'npm_config_cache',
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

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
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

function parseBoundedInteger(value, fallback, minimum, maximum, code) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  assertCondition(Number.isInteger(parsed) && parsed >= minimum && parsed <= maximum, code);
  return parsed;
}

function parseArgs(argv) {
  if (argv.length === 1 && argv[0] === '--race-worker') {
    return {
      raceWorker: true, transportPreflightOnly: false, help: false, expectedPreviewRef: null,
    };
  }
  if (argv.includes('--help') || argv.includes('-h')) {
    return {
      raceWorker: false, transportPreflightOnly: false, help: true, expectedPreviewRef: null,
    };
  }
  let expectedPreviewRef = null;
  let transportPreflightOnly = false;
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === '--expected-preview-ref' && index + 1 < argv.length) {
      expectedPreviewRef = argv[index + 1];
      index += 1;
    } else if (argv[index] === '--transport-preflight-only') {
      assertCondition(!transportPreflightOnly, 'ARG_TRANSPORT_PREFLIGHT_DUPLICATE');
      transportPreflightOnly = true;
    } else {
      fail('ARG_UNKNOWN');
    }
  }
  requireString(expectedPreviewRef, 'ARG_EXPECTED_PREVIEW_REF_REQUIRED', REF_RE);
  return { raceWorker: false, transportPreflightOnly, help: false, expectedPreviewRef };
}

function helpText() {
  return [
    'Usage: node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --expected-preview-ref <ref>',
    '       node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --transport-preflight-only --expected-preview-ref <ref>',
    '',
    'Required environment:',
    '  PREVIEW_ENVIRONMENT=preview',
    '  PREVIEW_PROJECT_REF=<same exact ref>',
    '  PREVIEW_SUPABASE_URL=https://<ref>.supabase.co',
    '  PREVIEW_SUPABASE_ANON_KEY=<Preview anon/publishable key>',
    '  PREVIEW_SUPABASE_SERVICE_ROLE_KEY=<Preview service role/secret key>',
    '  PREVIEW_DB_URL=<percent-encoded Preview PostgreSQL URL>',
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

  return Object.freeze({
    environment,
    projectRef,
    supabaseUrl: parsed.origin,
    anonKey,
    serviceRoleKey,
    dbUrl,
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

function dbDiagnosticCodes(stdoutText, stderrText, config) {
  let diagnostic = `${stdoutText}\n${stderrText}`;
  for (const sensitive of [
    config.dbUrl, config.supabaseUrl, config.anonKey,
    config.serviceRoleKey, config.projectRef,
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

async function runDbTest(config, tempDir, name, sql) {
  const filePath = path.join(tempDir, `${name}.sql`);
  await writeFile(filePath, sql, { mode: 0o600, flag: 'wx' });
  const args = [
    '--yes', `supabase@${SUPABASE_CLI_VERSION}`, '--log-level', 'error',
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

  const result = await new Promise((resolve, reject) => {
    const child = spawn('npx', args, {
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
    sql_sha256: sha256(sql),
    stdout_sha256: stdoutHash.digest('hex'),
    stderr_sha256: stderrHash.digest('hex'),
    stdout_bytes: stdoutBytes,
    stderr_bytes: stderrBytes,
    exit_code: result.exitCode,
    signal: result.signal,
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
    const proof = await runDbTest(config, tempDir, 'transport-preflight', sql);
    const transportTargetSha256 = sha256(config.dbUrl);
    return {
      ...proof,
      transport_target_sha256: transportTargetSha256,
      transport_binding_sha256: sha256({
        argv_template: [
          '--yes', `supabase@${SUPABASE_CLI_VERSION}`, '--log-level', 'error',
          'test', 'db', '--db-url', '<transport-target>', '<private-sql-file>',
        ],
        child_env_names: DB_TEST_CHILD_ENV_NAMES
          .filter((name) => typeof process.env[name] === 'string')
          .sort(),
        executable: 'npx',
        repo_root: REPO_ROOT,
        transport_target_sha256: transportTargetSha256,
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
        transport_target_sha256: proof.transport_target_sha256,
        transport_binding_sha256: proof.transport_binding_sha256,
      },
      disposable_actor_count: 0,
      primary_write_count: 0,
    };
  } finally {
    await rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
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
  clearTimeout(timeout);
  let textBody;
  try { textBody = await response.text(); } catch {
    fail('HTTP_RESPONSE_READ_FAILED', { http_status: response.status });
  }
  assertCondition(Buffer.byteLength(textBody) <= 16 * 1024 * 1024, 'HTTP_RESPONSE_TOO_LARGE');
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
  return { body, status: response.status, bodySha256: sha256(textBody) };
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

async function createDisposableActor(config, roleLabel) {
  const suffix = randomBytes(18).toString('hex');
  const email = `flow-identity-${roleLabel}-${suffix}@example.invalid`;
  const password = randomBytes(36).toString('base64url');
  const created = await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users`,
    {
      method: 'POST', headers: serviceHeaders(config),
      body: JSON.stringify({
        email, password, email_confirm: true,
        app_metadata: { preview_e2e: true, fixture_sha256: sha256('database-engine#269') },
      }),
    },
    config.rpcTimeoutMs,
    [200, 201],
  );
  const userId = requireUuid(created.body?.id ?? created.body?.user?.id, 'AUTH_CREATE_RESPONSE_INVALID');
  try {
    const signedIn = await fetchJson(
      `${config.supabaseUrl}/auth/v1/token?grant_type=password`,
      {
        method: 'POST',
        headers: { apikey: config.anonKey, accept: 'application/json', 'content-type': 'application/json' },
        body: JSON.stringify({ email, password }),
      },
      config.rpcTimeoutMs,
    );
    assertCondition(requireUuid(signedIn.body?.user?.id, 'AUTH_SIGN_IN_RESPONSE_INVALID') === userId, 'AUTH_SIGN_IN_ACTOR_MISMATCH');
    return {
      userId, email,
      accessToken: requireString(signedIn.body?.access_token, 'AUTH_ACCESS_TOKEN_MISSING'),
    };
  } catch (error) {
    await deleteDisposableActor(config, userId).catch(() => {});
    throw error;
  }
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

async function deleteDisposableActor(config, userId) {
  await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(userId)}`,
    { method: 'DELETE', headers: serviceHeaders(config, false) },
    config.rpcTimeoutMs,
    [200, 204],
  );
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
  clearTimeout(timeout);
  const text = await response.text();
  assertCondition(Buffer.byteLength(text) <= 1024 * 1024, 'RACE_WORKER_RESPONSE_TOO_LARGE');
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

async function execute(config) {
  const context = {
    config,
    namespace: `fie2e-hosted-${randomBytes(12).toString('hex')}`,
    requestId: randomUUID(),
    owner: null,
    foreign: null,
  };
  let tempDir = null;
  let fixtureAttempted = false;
  let cleanupFailure = null;
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
    actors_retained_for_cleanup: false,
  };

  try {
    tempDir = await mkdtemp(path.join(os.tmpdir(), 'flow-identity-preview-'));
    dbProofs.transport_preflight = await runTransportPreflight(config, tempDir);
    context.owner = await createDisposableActor(config, 'owner');
    context.foreign = await createDisposableActor(config, 'foreign');
    const fixture = await renderSqlTemplate(FIXTURE_TEMPLATE, templateValues(context));
    fixtureAttempted = true;
    dbProofs.fixture = await runDbTest(config, tempDir, 'fixture', fixture.sql);

    const manifestRows = await readFixtureManifest(config, context.owner.userId, context.requestId);
    assertCondition(manifestRows.length === 1, 'FIXTURE_MANIFEST_CARDINALITY_INVALID', { observed: manifestRows.length });
    const manifest = validateManifest(manifestRows[0].payload, context);

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

    const preflightRequest = buildPreflightRequest(manifest, capture);
    dbProofs.drift = await runDbTest(config, tempDir, 'drift', driftSql(context, manifest));
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
    );
    dbProofs.restore = await runDbTest(config, tempDir, 'restore', restoreSql(context, manifest));

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
    );
    dbProofs.faultRemove = await runDbTest(
      config, tempDir, 'fault-remove', faultRemoveSql(context),
    );

    const race = await concurrentProcessPosts(context, scopeId, requests[0], permit0);
    assertCondition(race.success.ordinal === 1 && race.success.replay === false, 'RACE_SUCCESS_INVALID');
    const permit1 = validatePermit(race.success.execution_permit, 1, invocationId);
    assertCondition(permit1.token !== permit0.token, 'RACE_PERMIT_NOT_ROTATED');

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
      db_proofs: Object.fromEntries(Object.entries(dbProofs).map(([name, proof]) => [name, {
        sql_sha256: proof.sql_sha256,
        stdout_sha256: proof.stdout_sha256,
        stderr_sha256: proof.stderr_sha256,
        ...(proof.transport_target_sha256
          ? { transport_target_sha256: proof.transport_target_sha256 }
          : {}),
        ...(proof.transport_binding_sha256
          ? { transport_binding_sha256: proof.transport_binding_sha256 }
          : {}),
      }])),
    };
  } catch (error) {
    primaryError = error instanceof SafeError ? error : new SafeError('HOSTED_PREVIEW_E2E_FAILED');
  } finally {
    if (context.owner && context.foreign && fixtureAttempted && tempDir) {
      try {
        const cleanupTemplate = await renderSqlTemplate(CLEANUP_TEMPLATE, templateValues(context));
        const proof = await runDbTest(config, tempDir, 'cleanup', cleanupTemplate.sql);
        cleanup.fixture_cleanup_ran = true;
        cleanup.cleanup_template_sha256 = cleanupTemplate.templateSha256;
        cleanup.cleanup_rendered_sha256 = cleanupTemplate.renderedSha256;
        cleanup.cleanup_stdout_sha256 = proof.stdout_sha256;
        cleanup.cleanup_stderr_sha256 = proof.stderr_sha256;
        const remaining = await readFixtureManifest(config, context.owner.userId, context.requestId);
        cleanup.manifest_rows_after_cleanup = remaining.length;
        assertCondition(remaining.length === 0, 'CLEANUP_MANIFEST_RESIDUE');
      } catch (error) {
        cleanupFailure = error instanceof SafeError ? error : new SafeError('FIXTURE_CLEANUP_FAILED');
      }
    }

    for (const [label, actor] of [['owner', context.owner], ['foreign', context.foreign]]) {
      if (!actor) continue;
      try {
        await revokeActorSession(config, actor);
        cleanup[`${label}_session_revoked`] = true;
      } catch (error) {
        cleanupFailure ??= error instanceof SafeError ? error : new SafeError('AUTH_SESSION_REVOKE_FAILED');
      }
    }

    const cleanupClosed = !fixtureAttempted || (
      cleanupFailure === null
      && cleanup.fixture_cleanup_ran
      && cleanup.manifest_rows_after_cleanup === 0
    );
    if (cleanupClosed) {
      for (const [label, actor] of [['owner', context.owner], ['foreign', context.foreign]]) {
        if (!actor) continue;
        try {
          await deleteDisposableActor(config, actor.userId);
          cleanup[`${label}_deleted`] = true;
        } catch (error) {
          cleanup.actors_retained_for_cleanup = true;
          cleanupFailure ??= error instanceof SafeError ? error : new SafeError('AUTH_ACTOR_DELETE_FAILED');
        }
      }
    } else if (context.owner || context.foreign) {
      cleanup.actors_retained_for_cleanup = true;
    }
    if (tempDir) await rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }

  if (primaryError) {
    primaryError.details = sanitizeEvidence({
      ...primaryError.details,
      cleanup,
      cleanup_failure_code: cleanupFailure?.code ?? null,
    });
    throw primaryError;
  }
  if (cleanupFailure) throw cleanupFailure;
  assertCondition(
    cleanup.fixture_cleanup_ran
      && cleanup.manifest_rows_after_cleanup === 0
      && cleanup.owner_session_revoked
      && cleanup.foreign_session_revoked
      && cleanup.owner_deleted
      && cleanup.foreign_deleted,
    'CLEANUP_PROOF_INCOMPLETE',
    cleanup,
  );
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
  const evidence = await execute(config);
  process.stdout.write(`${JSON.stringify(sanitizeEvidence({
    ...evidence,
    started_at_sha256: sha256(startedAt),
    completed_at_sha256: sha256(new Date().toISOString()),
    cli_version_sha256: sha256(SUPABASE_CLI_VERSION),
  }))}\n`);
}

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
