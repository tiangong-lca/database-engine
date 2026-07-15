#!/usr/bin/env node

/**
 * Hosted Preview proof for protected, one-shot dataset alias execution.
 *
 * This runner deliberately has no dotenv support and no third-party runtime
 * dependencies. Secrets are accepted from the process environment only. The
 * only stdout/stderr payload emitted by an execution is one redacted JSON
 * evidence document; remote bodies, URLs, e-mail addresses, tokens, and DB
 * connection strings are never printed.
 */

import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../../..');
const FIXTURE_TEMPLATE = path.join(HERE, 'protected_alias_fixture.sql');
const CLEANUP_TEMPLATE = path.join(HERE, 'protected_alias_cleanup.sql');

const FIXTURE_COMMAND = 'preview_e2e_protected_alias_fixture';
const FIXTURE_TARGET_TABLE = 'preview_e2e_protected_alias';
const FIXTURE_TARGET_VERSION = '00.00.001';
const SUPABASE_CLI_VERSION = '2.109.1';
const MANIFEST_SCHEMA = 'protected-alias-preview-fixture.v1';
const EVIDENCE_SCHEMA = 'protected-alias-rest-e2e-evidence.v1';

const SCENARIOS = Object.freeze([
  'success',
  'business_rollback',
  'concurrent_duplicate',
  'lost_response',
  'http_timeout',
]);

const GATE_NAMES = Object.freeze([
  'primary_support_plan',
  'execution_unused',
  'derivative_quiescence',
]);

const EXPECTED = Object.freeze({
  action_count: 52,
  batch_count: 2,
  exchange_count: 59,
  amount_field_count: 118,
  unrelated_exchange_count: 309,
  audit_count: 55,
  flowproperty_count: 2,
  flow_count: 23,
  process_count: 27,
  derivative_target_count: 50,
});

const PLACEHOLDERS = Object.freeze([
  'ACTOR_UUID_SQL',
  'ACTOR_EMAIL_SQL',
  'SCENARIO_NAMESPACE_SQL',
  'REQUEST_ID_SQL',
  'PREVIEW_REF_SQL',
  'PREVIEW_URL_SQL',
  'SERVICE_ROLE_KEY_SQL',
  'SCENARIO_KIND_SQL',
]);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
const VERSION_RE = /^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$/;
const REF_RE = /^[a-z0-9]{15,64}$/;
const NAMESPACE_RE = /^pae2e-[a-z_]+-[0-9a-f]{24}$/;
const SAFE_CODE_RE = /^[A-Z][A-Z0-9_]{2,127}$/;
const ACTION_TABLES = new Set(['flowproperties', 'flows', 'processes']);
const SUPPORT_TABLES = new Set(['flowproperties', 'unitgroups']);

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

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (isPlainObject(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}

function canonicalJson(value) {
  return JSON.stringify(canonicalize(value));
}

function sha256(value) {
  const input = Buffer.isBuffer(value)
    ? value
    : typeof value === 'string'
      ? value
      : canonicalJson(value);
  return createHash('sha256').update(input).digest('hex');
}

function safeRemoteCode(value) {
  return typeof value === 'string' && SAFE_CODE_RE.test(value)
    ? value
    : null;
}

function sanitizeEvidence(value) {
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    if (
      SHA256_RE.test(value)
      || SAFE_CODE_RE.test(value)
      || /^(passed|failed|pending|indeterminate|completed|dispatched|derivatives_pending|not_started|status_only|direct|barrier|drop_response|delay_response)$/.test(value)
      || /^[a-z0-9_.-]{1,80}$/.test(value)
    ) {
      return value;
    }
    return `sha256:${sha256(value)}`;
  }
  if (Array.isArray(value)) return value.map(sanitizeEvidence);
  if (isPlainObject(value)) {
    const safe = {};
    for (const [key, item] of Object.entries(value)) {
      if (/email|password|token|secret|authorization|apikey|url|connection/i.test(key)) {
        continue;
      }
      safe[key] = sanitizeEvidence(item);
    }
    return safe;
  }
  return null;
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
  assertCondition(Number.isInteger(value) && value === expected, code, {
    expected,
    observed: Number.isInteger(value) ? value : null,
  });
}

function parseBoundedInteger(value, fallback, minimum, maximum, code) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  assertCondition(Number.isInteger(parsed) && parsed >= minimum && parsed <= maximum, code);
  return parsed;
}

function exactObjectKeys(value, keys, code) {
  assertCondition(isPlainObject(value), code);
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  assertCondition(canonicalJson(actual) === canonicalJson(expected), code, {
    actual_key_count: actual.length,
    expected_key_count: expected.length,
  });
}

function parseArgs(argv) {
  if (argv.includes('--help') || argv.includes('-h')) {
    return { help: true };
  }
  let expectedPreviewRef = null;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--expected-preview-ref') {
      assertCondition(index + 1 < argv.length, 'ARG_EXPECTED_PREVIEW_REF_REQUIRED');
      expectedPreviewRef = argv[index + 1];
      index += 1;
      continue;
    }
    fail('ARG_UNKNOWN');
  }
  requireString(expectedPreviewRef, 'ARG_EXPECTED_PREVIEW_REF_REQUIRED', REF_RE);
  return { help: false, expectedPreviewRef };
}

function helpText() {
  return [
    'Usage: node supabase/tests/preview/protected_alias_rest_e2e.mjs --expected-preview-ref <ref>',
    '',
    'Required environment variables:',
    '  PREVIEW_ENVIRONMENT=preview',
    '  PREVIEW_PROJECT_REF=<same exact ref>',
    '  PREVIEW_SUPABASE_URL=https://<ref>.supabase.co',
    '  PREVIEW_SUPABASE_ANON_KEY=<Preview anon/publishable key>',
    '  PREVIEW_SUPABASE_SERVICE_ROLE_KEY=<Preview service role/secret key>',
    '  PREVIEW_DB_URL=<percent-encoded Preview PostgreSQL URL>',
    '',
    'Optional bounded timings:',
    '  PREVIEW_RPC_TIMEOUT_MS (default 90000)',
    '  PREVIEW_POLL_TIMEOUT_MS (default 150000)',
    '  PREVIEW_POLL_INTERVAL_MS (default 1000)',
    '  PREVIEW_HTTP_TIMEOUT_MS (default 500)',
    '  PREVIEW_DB_TEST_TIMEOUT_MS (default 120000)',
  ].join('\n');
}

function envRequired(name) {
  const value = process.env[name];
  assertCondition(typeof value === 'string' && value.length > 0, `ENV_${name}_REQUIRED`);
  return value;
}

function urlContainsExactRef(databaseUrl, expectedRef) {
  let parsed;
  try {
    parsed = new URL(databaseUrl);
  } catch {
    return false;
  }
  if (!['postgres:', 'postgresql:'].includes(parsed.protocol)) return false;
  let username;
  try {
    username = decodeURIComponent(parsed.username);
  } catch {
    return false;
  }
  const hostLabels = parsed.hostname.toLowerCase().split('.');
  const userParts = username.toLowerCase().split('.');
  return hostLabels.includes(expectedRef) || userParts.includes(expectedRef);
}

function loadConfig(expectedPreviewRef) {
  const environment = envRequired('PREVIEW_ENVIRONMENT');
  const projectRef = envRequired('PREVIEW_PROJECT_REF');
  const supabaseUrlText = envRequired('PREVIEW_SUPABASE_URL');
  const anonKey = envRequired('PREVIEW_SUPABASE_ANON_KEY');
  const serviceRoleKey = envRequired('PREVIEW_SUPABASE_SERVICE_ROLE_KEY');
  const dbUrl = envRequired('PREVIEW_DB_URL');

  assertCondition(environment === 'preview', 'PREVIEW_ENVIRONMENT_MISMATCH');
  assertCondition(projectRef === expectedPreviewRef, 'PREVIEW_REF_ENV_ARG_MISMATCH');
  assertCondition(REF_RE.test(projectRef), 'PREVIEW_REF_INVALID');
  assertCondition(anonKey !== serviceRoleKey, 'PREVIEW_KEYS_MUST_DIFFER');
  assertCondition(anonKey.length >= 20 && serviceRoleKey.length >= 20, 'PREVIEW_KEY_INVALID');

  let supabaseUrl;
  try {
    supabaseUrl = new URL(supabaseUrlText);
  } catch {
    fail('PREVIEW_SUPABASE_URL_INVALID');
  }
  assertCondition(supabaseUrl.protocol === 'https:', 'PREVIEW_SUPABASE_URL_NOT_HTTPS');
  assertCondition(
    supabaseUrl.hostname.toLowerCase() === `${projectRef}.supabase.co`,
    'PREVIEW_SUPABASE_URL_REF_MISMATCH',
  );
  assertCondition(
    supabaseUrl.pathname === '/' || supabaseUrl.pathname === '',
    'PREVIEW_SUPABASE_URL_PATH_INVALID',
  );
  assertCondition(urlContainsExactRef(dbUrl, projectRef), 'PREVIEW_DB_URL_REF_MISMATCH');

  return Object.freeze({
    environment,
    projectRef,
    supabaseUrl: supabaseUrl.origin,
    anonKey,
    serviceRoleKey,
    dbUrl,
    rpcTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_RPC_TIMEOUT_MS,
      90_000,
      5_000,
      180_000,
      'PREVIEW_RPC_TIMEOUT_INVALID',
    ),
    pollTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_POLL_TIMEOUT_MS,
      150_000,
      10_000,
      300_000,
      'PREVIEW_POLL_TIMEOUT_INVALID',
    ),
    pollIntervalMs: parseBoundedInteger(
      process.env.PREVIEW_POLL_INTERVAL_MS,
      1_000,
      250,
      10_000,
      'PREVIEW_POLL_INTERVAL_INVALID',
    ),
    httpTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_HTTP_TIMEOUT_MS,
      500,
      100,
      5_000,
      'PREVIEW_HTTP_TIMEOUT_INVALID',
    ),
    dbTestTimeoutMs: parseBoundedInteger(
      process.env.PREVIEW_DB_TEST_TIMEOUT_MS,
      120_000,
      10_000,
      300_000,
      'PREVIEW_DB_TEST_TIMEOUT_INVALID',
    ),
  });
}

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function validateTemplateValue(name, value) {
  switch (name) {
    case 'ACTOR_UUID_SQL':
    case 'REQUEST_ID_SQL':
      requireUuid(value, `TEMPLATE_${name}_INVALID`);
      break;
    case 'ACTOR_EMAIL_SQL':
      assertCondition(
        typeof value === 'string'
          && value.length <= 320
          && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
          && !/[\u0000-\u001f\u007f]/.test(value),
        `TEMPLATE_${name}_INVALID`,
      );
      break;
    case 'SCENARIO_NAMESPACE_SQL':
      requireString(value, `TEMPLATE_${name}_INVALID`, NAMESPACE_RE);
      break;
    case 'PREVIEW_REF_SQL':
      requireString(value, `TEMPLATE_${name}_INVALID`, REF_RE);
      break;
    case 'PREVIEW_URL_SQL': {
      let previewUrl;
      try {
        previewUrl = new URL(value);
      } catch {
        fail(`TEMPLATE_${name}_INVALID`);
      }
      assertCondition(
        previewUrl.protocol === 'https:'
          && previewUrl.pathname === '/'
          && previewUrl.search === ''
          && previewUrl.hash === ''
          && /^[a-z0-9]{15,64}\.supabase\.co$/.test(previewUrl.hostname),
        `TEMPLATE_${name}_INVALID`,
      );
      break;
    }
    case 'SERVICE_ROLE_KEY_SQL':
      assertCondition(
        typeof value === 'string'
          && value.length >= 20
          && value.length <= 4096
          && !/\s/.test(value),
        `TEMPLATE_${name}_INVALID`,
      );
      break;
    case 'SCENARIO_KIND_SQL':
      assertCondition(SCENARIOS.includes(value), `TEMPLATE_${name}_INVALID`);
      break;
    default:
      fail('TEMPLATE_UNKNOWN_PLACEHOLDER');
  }
}

async function renderSqlTemplate(templatePath, values) {
  let source;
  try {
    source = await readFile(templatePath, 'utf8');
  } catch {
    fail('SQL_TEMPLATE_READ_FAILED', { template_sha256: sha256(path.basename(templatePath)) });
  }

  let rendered = source;
  for (const placeholder of PLACEHOLDERS) {
    const marker = `{{${placeholder}}}`;
    assertCondition(rendered.includes(marker), 'SQL_TEMPLATE_PLACEHOLDER_MISSING', {
      placeholder_sha256: sha256(placeholder),
      template_sha256: sha256(source),
    });
    validateTemplateValue(placeholder, values[placeholder]);
    rendered = rendered.replaceAll(marker, sqlLiteral(values[placeholder]));
  }
  assertCondition(!/{{[A-Z0-9_]+}}/.test(rendered), 'SQL_TEMPLATE_UNRESOLVED_PLACEHOLDER', {
    template_sha256: sha256(source),
  });
  return {
    sql: rendered,
    template_sha256: sha256(source),
    rendered_sha256: sha256(rendered),
  };
}

function dbDiagnosticCodes(stdoutText, stderrText, config) {
  let diagnostic = `${stdoutText}\n${stderrText}`;
  for (const sensitive of [
    config.dbUrl,
    config.supabaseUrl,
    config.anonKey,
    config.serviceRoleKey,
    config.projectRef,
  ]) {
    diagnostic = diagnostic.replaceAll(sensitive, ' REDACTED ');
  }
  diagnostic = diagnostic
    .replace(/postgres(?:ql)?:\/\/\S+/gi, ' REDACTED ')
    .replace(/\beyJ[A-Za-z0-9._-]{20,}\b/g, ' REDACTED ')
    .replace(/\bsb_(?:publishable|secret)_[A-Za-z0-9_-]{10,}\b/g, ' REDACTED ')
    .replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, ' EMAIL ')
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi, ' UUID ')
    .replace(/'(?:''|[^'])*'/g, ' VALUE ')
    .replace(/"(?:""|[^"])*"/g, ' IDENT ');

  const codes = diagnostic
    .split(/\r?\n/)
    .filter((line) => /(?:error|fatal|panic|not ok|failed|bail out|psql:)/i.test(line))
    .map((line) => line.match(/(?:error|fatal|panic|not ok|failed|bail out|psql:).*$/i)?.[0] ?? line)
    .map((line) => `DB_DIAG_${line}`
      .toUpperCase()
      .replace(/\b\d+\b/g, 'N')
      .replace(/[^A-Z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '')
      .slice(0, 127))
    .filter((code) => /^[A-Z][A-Z0-9_]{2,127}$/.test(code));
  return [...new Set(codes)].slice(-12);
}

async function runDbTest(config, tempDir, name, sql) {
  const filePath = path.join(tempDir, `${name}.sql`);
  await writeFile(filePath, sql, { mode: 0o600, flag: 'wx' });

  const args = [
    '--yes',
    `supabase@${SUPABASE_CLI_VERSION}`,
    '--log-level',
    'error',
    'test',
    'db',
    '--db-url',
    config.dbUrl,
    filePath,
  ];

  const stdoutHash = createHash('sha256');
  const stderrHash = createHash('sha256');
  let stdoutBytes = 0;
  let stderrBytes = 0;
  let outputTooLarge = false;
  let timedOut = false;
  const stdoutChunks = [];
  const stderrChunks = [];
  let stdoutCapturedBytes = 0;
  let stderrCapturedBytes = 0;

  const childEnv = {};
  for (const name of [
    'PATH',
    'HOME',
    'TMPDIR',
    'TMP',
    'TEMP',
    'LANG',
    'LC_ALL',
    'HTTP_PROXY',
    'HTTPS_PROXY',
    'NO_PROXY',
    'http_proxy',
    'https_proxy',
    'no_proxy',
    'NPM_CONFIG_CACHE',
    'npm_config_cache',
  ]) {
    if (typeof process.env[name] === 'string') childEnv[name] = process.env[name];
  }
  // PREVIEW_* credentials are intentionally absent. The CLI receives only
  // the one DB connection argument its `test db` surface requires.

  const result = await new Promise((resolve, reject) => {
    const child = spawn('npx', args, {
      cwd: REPO_ROOT,
      env: childEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: false,
    });

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      const hardKill = setTimeout(() => child.kill('SIGKILL'), 2_000);
      hardKill.unref();
    }, config.dbTestTimeoutMs);
    timeout.unref();

    const collect = (hash, kind) => (chunk) => {
      hash.update(chunk);
      if (kind === 'stdout') {
        stdoutBytes += chunk.length;
        if (stdoutCapturedBytes < 64 * 1024) {
          const retained = chunk.subarray(0, 64 * 1024 - stdoutCapturedBytes);
          stdoutChunks.push(retained);
          stdoutCapturedBytes += retained.length;
        }
      } else {
        stderrBytes += chunk.length;
        if (stderrCapturedBytes < 64 * 1024) {
          const retained = chunk.subarray(0, 64 * 1024 - stderrCapturedBytes);
          stderrChunks.push(retained);
          stderrCapturedBytes += retained.length;
        }
      }
      if (stdoutBytes + stderrBytes > 4 * 1024 * 1024 && !outputTooLarge) {
        outputTooLarge = true;
        child.kill('SIGTERM');
      }
    };
    child.stdout.on('data', collect(stdoutHash, 'stdout'));
    child.stderr.on('data', collect(stderrHash, 'stderr'));
    child.once('error', () => {
      clearTimeout(timeout);
      reject(new SafeError('DB_TEST_SPAWN_FAILED'));
    });
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

async function fetchJson(url, options, timeoutMs, expectedStatuses = [200]) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  timeout.unref();
  let response;
  try {
    response = await fetch(url, {
      ...options,
      redirect: 'error',
      signal: controller.signal,
    });
  } catch {
    clearTimeout(timeout);
    fail('HTTP_TRANSPORT_FAILED');
  }
  clearTimeout(timeout);

  let textBody;
  try {
    textBody = await response.text();
  } catch {
    fail('HTTP_RESPONSE_READ_FAILED', { http_status: response.status });
  }
  assertCondition(Buffer.byteLength(textBody) <= 16 * 1024 * 1024, 'HTTP_RESPONSE_TOO_LARGE', {
    http_status: response.status,
  });

  let body = null;
  if (textBody.length > 0) {
    try {
      body = JSON.parse(textBody);
    } catch {
      fail('HTTP_RESPONSE_NOT_JSON', {
        http_status: response.status,
        body_sha256: sha256(textBody),
      });
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

async function createDisposableActor(config, scenario) {
  const suffix = randomBytes(18).toString('hex');
  const email = `protected-alias-${suffix}@example.invalid`;
  const password = randomBytes(36).toString('base64url');

  const created = await fetchJson(
    `${config.supabaseUrl}/auth/v1/admin/users`,
    {
      method: 'POST',
      headers: serviceHeaders(config),
      body: JSON.stringify({
        email,
        password,
        email_confirm: true,
        app_metadata: { preview_e2e: true, scenario_sha256: sha256(scenario) },
      }),
    },
    config.rpcTimeoutMs,
    [200, 201],
  );
  const userId = requireUuid(created.body?.id ?? created.body?.user?.id, 'AUTH_CREATE_RESPONSE_INVALID');

  let signedIn;
  try {
    signedIn = await fetchJson(
      `${config.supabaseUrl}/auth/v1/token?grant_type=password`,
      {
        method: 'POST',
        headers: {
          apikey: config.anonKey,
          'content-type': 'application/json',
          accept: 'application/json',
        },
        body: JSON.stringify({ email, password }),
      },
      config.rpcTimeoutMs,
      [200],
    );
  } catch (error) {
    await deleteDisposableActor(config, userId).catch(() => {});
    throw error;
  }

  assertCondition(
    requireUuid(signedIn.body?.user?.id, 'AUTH_SIGN_IN_RESPONSE_INVALID') === userId,
    'AUTH_SIGN_IN_ACTOR_MISMATCH',
  );
  const accessToken = requireString(
    signedIn.body?.access_token,
    'AUTH_ACCESS_TOKEN_MISSING',
  );
  return { userId, email, accessToken };
}

async function revokeActorSession(config, actor) {
  if (!actor?.accessToken) return;
  await fetchJson(
    `${config.supabaseUrl}/auth/v1/logout?scope=global`,
    {
      method: 'POST',
      headers: {
        apikey: config.anonKey,
        authorization: `Bearer ${actor.accessToken}`,
      },
    },
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

function actorRpcHeaders(config, actor) {
  return {
    apikey: config.anonKey,
    authorization: `Bearer ${actor.accessToken}`,
    accept: 'application/json',
    'content-type': 'application/json',
  };
}

async function rpc(config, actor, functionName, parameters, baseUrl = config.supabaseUrl) {
  const result = await fetchJson(
    `${baseUrl}/rest/v1/rpc/${functionName}`,
    {
      method: 'POST',
      headers: actorRpcHeaders(config, actor),
      body: JSON.stringify(parameters),
    },
    config.rpcTimeoutMs,
    [200],
  );
  assertCondition(isPlainObject(result.body), 'RPC_RESPONSE_INVALID', {
    function_sha256: sha256(functionName),
    body_sha256: result.bodySha256,
  });
  return result.body;
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
    url,
    { method: 'GET', headers: serviceHeaders(config, false) },
    config.rpcTimeoutMs,
    [200],
  );
  assertCondition(Array.isArray(result.body), 'FIXTURE_MANIFEST_QUERY_INVALID');
  return result.body;
}

async function readActorRows(config, actor, table, keys) {
  const url = new URL(`/rest/v1/${table}`, config.supabaseUrl);
  url.searchParams.set('select', 'id,version,user_id,state_code,json_ordered');
  url.searchParams.set('user_id', `eq.${actor.userId}`);
  url.searchParams.set('state_code', 'eq.0');
  url.searchParams.set('id', `in.(${keys.map((key) => key.id).join(',')})`);
  url.searchParams.set('limit', String(keys.length));
  const result = await fetchJson(
    url,
    {
      method: 'GET',
      headers: {
        apikey: config.anonKey,
        authorization: `Bearer ${actor.accessToken}`,
        accept: 'application/json',
      },
    },
    config.rpcTimeoutMs,
    [200],
  );
  assertCondition(Array.isArray(result.body), 'ROLLBACK_ROW_READ_INVALID', {
    table_sha256: sha256(table),
    response_sha256: result.bodySha256,
  });
  return result.body;
}

function normalizeEntityKey(item, code, allowedTables = ACTION_TABLES) {
  assertCondition(isPlainObject(item), code);
  const table = requireString(item.table, code);
  assertCondition(allowedTables.has(table), code);
  const id = requireUuid(item.id, code);
  const version = requireString(item.version, code, VERSION_RE);
  return { table, id, version };
}

function entityKeyString(item) {
  return `${item.table}:${item.id}:${item.version}`;
}

function validateUniqueKeys(items, expectedLength, code) {
  assertCondition(Array.isArray(items) && items.length === expectedLength, code, {
    expected: expectedLength,
    observed: Array.isArray(items) ? items.length : null,
  });
  const normalized = items.map((item) => normalizeEntityKey(item, code));
  assertCondition(new Set(normalized.map(entityKeyString)).size === expectedLength, code);
  return normalized;
}

function countTables(keys) {
  const counts = { flowproperties: 0, flows: 0, processes: 0 };
  for (const key of keys) counts[key.table] += 1;
  return counts;
}

function assertKeyParity(left, right, code) {
  const leftKeys = left.map(entityKeyString).sort();
  const rightKeys = right.map(entityKeyString).sort();
  assertCondition(canonicalJson(leftKeys) === canonicalJson(rightKeys), code, {
    left_sha256: sha256(leftKeys),
    right_sha256: sha256(rightKeys),
  });
}

function validateHashFields(value, code, currentPath = 'manifest') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateHashFields(item, code, `${currentPath}.${index}`));
    return;
  }
  if (!isPlainObject(value)) return;
  for (const [key, item] of Object.entries(value)) {
    if (key.endsWith('_sha256')) {
      assertCondition(typeof item === 'string' && SHA256_RE.test(item), code, {
        path_sha256: sha256(`${currentPath}.${key}`),
      });
    } else {
      validateHashFields(item, code, `${currentPath}.${key}`);
    }
  }
}

function validateExpected(expected) {
  exactObjectKeys(expected, Object.keys(EXPECTED), 'FIXTURE_EXPECTED_SHAPE_INVALID');
  for (const [key, value] of Object.entries(EXPECTED)) {
    requireInteger(expected[key], value, 'FIXTURE_EXPECTED_COUNT_MISMATCH');
  }
}

function planActionKeys(plan) {
  assertCondition(isPlainObject(plan), 'FIXTURE_PLAN_INVALID');
  assertCondition(plan.schema_version === 'dataset-alias-plan.v1', 'FIXTURE_PLAN_INVALID');
  assertCondition(plan.target_visibility === 'owner_draft', 'FIXTURE_PLAN_INVALID');
  assertCondition(Array.isArray(plan.batches) && plan.batches.length === 2, 'FIXTURE_PLAN_INVALID');
  const dimensions = plan.batches.map((batch) => batch?.dimension);
  assertCondition(
    canonicalJson(dimensions) === canonicalJson(['time', 'length_time']),
    'FIXTURE_PLAN_BATCH_ORDER_INVALID',
  );
  const actions = plan.batches.flatMap((batch) => {
    assertCondition(isPlainObject(batch) && Array.isArray(batch.actions), 'FIXTURE_PLAN_INVALID');
    return batch.actions;
  });
  return validateUniqueKeys(actions, EXPECTED.action_count, 'FIXTURE_PLAN_ACTIONS_INVALID');
}

function validateSupportKeys(supportKeys) {
  assertCondition(Array.isArray(supportKeys) && supportKeys.length === 6, 'FIXTURE_SUPPORT_KEYS_INVALID');
  const occurrences = new Set();
  for (const item of supportKeys) {
    const key = normalizeEntityKey(item, 'FIXTURE_SUPPORT_KEYS_INVALID', SUPPORT_TABLES);
    assertCondition(['time', 'length_time'].includes(item.dimension), 'FIXTURE_SUPPORT_KEYS_INVALID');
    assertCondition(
      ['flowproperty', 'unitgroup', 'source_unitgroup'].includes(item.role),
      'FIXTURE_SUPPORT_KEYS_INVALID',
    );
    assertCondition(
      (item.role === 'flowproperty' && key.table === 'flowproperties')
        || (item.role !== 'flowproperty' && key.table === 'unitgroups'),
      'FIXTURE_SUPPORT_KEYS_INVALID',
    );
    occurrences.add(`${item.dimension}:${item.role}`);
  }
  assertCondition(occurrences.size === 6, 'FIXTURE_SUPPORT_KEYS_INVALID');
}

function validateManifest(payload, context) {
  assertCondition(isPlainObject(payload), 'FIXTURE_MANIFEST_INVALID');
  assertCondition(payload.schema_version === MANIFEST_SCHEMA, 'FIXTURE_MANIFEST_SCHEMA_INVALID');
  assertCondition(payload.scenario_id === context.namespace, 'FIXTURE_SCENARIO_ID_MISMATCH');
  assertCondition(payload.scenario_kind === context.scenario, 'FIXTURE_SCENARIO_KIND_MISMATCH');
  assertCondition(requireUuid(payload.request_id, 'FIXTURE_REQUEST_ID_INVALID') === context.requestId, 'FIXTURE_REQUEST_ID_MISMATCH');

  validateExpected(payload.expected);
  requireInteger(
    payload.unrelated_exchange_count,
    EXPECTED.unrelated_exchange_count,
    'FIXTURE_UNRELATED_EXCHANGE_COUNT_MISMATCH',
  );
  requireSha(payload.before_hash_set_sha256, 'FIXTURE_BEFORE_HASH_INVALID');
  requireSha(payload.desired_hash_set_sha256, 'FIXTURE_DESIRED_HASH_INVALID');
  requireSha(payload.unrelated_exchange_set_sha256, 'FIXTURE_UNRELATED_HASH_INVALID');

  const actionKeys = validateUniqueKeys(
    payload.action_keys,
    EXPECTED.action_count,
    'FIXTURE_ACTION_KEYS_INVALID',
  );
  const actionCounts = countTables(actionKeys);
  requireInteger(actionCounts.flowproperties, 2, 'FIXTURE_ACTION_TABLE_COUNT_INVALID');
  requireInteger(actionCounts.flows, 23, 'FIXTURE_ACTION_TABLE_COUNT_INVALID');
  requireInteger(actionCounts.processes, 27, 'FIXTURE_ACTION_TABLE_COUNT_INVALID');

  const targetKeys = validateUniqueKeys(
    payload.target_keys,
    EXPECTED.derivative_target_count,
    'FIXTURE_TARGET_KEYS_INVALID',
  );
  const targetCounts = countTables(targetKeys);
  requireInteger(targetCounts.flowproperties, 0, 'FIXTURE_TARGET_TABLE_COUNT_INVALID');
  requireInteger(targetCounts.flows, 23, 'FIXTURE_TARGET_TABLE_COUNT_INVALID');
  requireInteger(targetCounts.processes, 27, 'FIXTURE_TARGET_TABLE_COUNT_INVALID');
  assertCondition(
    targetKeys.every((key) => actionKeys.some((action) => entityKeyString(action) === entityKeyString(key))),
    'FIXTURE_TARGET_NOT_ACTION',
  );

  validateSupportKeys(payload.support_keys);

  assertCondition(
    Array.isArray(payload.baseline_hashes)
      && payload.baseline_hashes.length === EXPECTED.derivative_target_count,
    'FIXTURE_BASELINE_HASHES_INVALID',
  );
  const baselineKeys = validateUniqueKeys(
    payload.baseline_hashes,
    EXPECTED.derivative_target_count,
    'FIXTURE_BASELINE_HASHES_INVALID',
  );
  payload.baseline_hashes.forEach((entry) => {
    requireSha(entry.baseline_snapshot_sha256, 'FIXTURE_BASELINE_HASH_INVALID');
  });
  assertKeyParity(baselineKeys, targetKeys, 'FIXTURE_BASELINE_TARGET_PARITY_INVALID');

  const preflight = payload.preflight_request;
  exactObjectKeys(
    preflight,
    [
      'schema_version',
      'request_id',
      'environment',
      'project_ref',
      'actor',
      'target_visibility',
      'plan',
      'freeze',
      'approval',
      'bindings',
      'expected',
      'derivative_targets',
    ],
    'FIXTURE_PREFLIGHT_SHAPE_INVALID',
  );
  assertCondition(preflight.schema_version === 'dataset-alias-execution-preflight.v1', 'FIXTURE_PREFLIGHT_INVALID');
  assertCondition(requireUuid(preflight.request_id, 'FIXTURE_PREFLIGHT_INVALID') === context.requestId, 'FIXTURE_PREFLIGHT_INVALID');
  assertCondition(preflight.environment === 'preview', 'FIXTURE_PREFLIGHT_ENV_INVALID');
  assertCondition(preflight.project_ref === context.config.projectRef, 'FIXTURE_PREFLIGHT_REF_INVALID');
  assertCondition(preflight.target_visibility === 'owner_draft', 'FIXTURE_PREFLIGHT_VISIBILITY_INVALID');
  assertCondition(
    isPlainObject(preflight.actor)
      && requireUuid(preflight.actor.user_id, 'FIXTURE_PREFLIGHT_ACTOR_INVALID') === context.actor.userId
      && preflight.actor.email === context.actor.email,
    'FIXTURE_PREFLIGHT_ACTOR_INVALID',
  );
  validateExpected(preflight.expected);

  const preflightActions = planActionKeys(preflight.plan);
  assertKeyParity(preflightActions, actionKeys, 'FIXTURE_PLAN_ACTION_PARITY_INVALID');
  const derivativeTargets = validateUniqueKeys(
    preflight.derivative_targets,
    EXPECTED.derivative_target_count,
    'FIXTURE_DERIVATIVE_TARGETS_INVALID',
  );
  assertKeyParity(derivativeTargets, targetKeys, 'FIXTURE_DERIVATIVE_TARGET_PARITY_INVALID');
  for (const target of preflight.derivative_targets) {
    assertCondition(
      requireUuid(target.user_id, 'FIXTURE_DERIVATIVE_TARGET_INVALID') === context.actor.userId
        && target.state_code === 0,
      'FIXTURE_DERIVATIVE_TARGET_INVALID',
    );
    requireSha(target.baseline_snapshot_sha256, 'FIXTURE_DERIVATIVE_BASELINE_INVALID');
    const baseline = payload.baseline_hashes.find(
      (entry) => entityKeyString(normalizeEntityKey(entry, 'FIXTURE_BASELINE_HASHES_INVALID'))
        === entityKeyString(normalizeEntityKey(target, 'FIXTURE_DERIVATIVE_TARGET_INVALID')),
    );
    assertCondition(
      baseline?.baseline_snapshot_sha256 === target.baseline_snapshot_sha256,
      'FIXTURE_DERIVATIVE_BASELINE_PARITY_INVALID',
    );
  }

  if (context.scenario === 'business_rollback') {
    const fault = normalizeEntityKey(
      payload.business_rollback_fault,
      'FIXTURE_BUSINESS_FAULT_INVALID',
    );
    assertCondition(fault.table === 'flowproperties', 'FIXTURE_BUSINESS_FAULT_INVALID');
    assertCondition(
      actionKeys.some((key) => entityKeyString(key) === entityKeyString(fault)),
      'FIXTURE_BUSINESS_FAULT_NOT_ACTION',
    );
  }

  validateHashFields(payload, 'FIXTURE_HASH_FIELD_INVALID');
  return {
    payload,
    actionKeys,
    targetKeys,
    manifestSha256: sha256(payload),
  };
}

async function verifyBusinessRollbackRows(config, actor, manifest) {
  const expectedActions = manifest.payload.preflight_request.plan.batches.flatMap(
    (batch) => batch.actions,
  );
  const expectedByKey = new Map();
  for (const action of expectedActions) {
    const key = normalizeEntityKey(action, 'ROLLBACK_EXPECTED_ACTION_INVALID');
    assertCondition(isPlainObject(action.expected_json_ordered), 'ROLLBACK_EXPECTED_ACTION_INVALID');
    expectedByKey.set(entityKeyString(key), action.expected_json_ordered);
  }
  assertCondition(expectedByKey.size === 52, 'ROLLBACK_EXPECTED_ACTION_INVALID');

  const observed = [];
  for (const table of ['flowproperties', 'flows', 'processes']) {
    const keys = manifest.actionKeys.filter((key) => key.table === table);
    const rows = await readActorRows(config, actor, table, keys);
    assertCondition(rows.length === keys.length, 'ROLLBACK_ROW_CARDINALITY_INVALID', {
      table_sha256: sha256(table),
      expected: keys.length,
      observed: rows.length,
    });
    for (const row of rows) {
      const key = normalizeEntityKey({
        table,
        id: row?.id,
        version: row?.version,
      }, 'ROLLBACK_ROW_KEY_INVALID');
      assertCondition(
        requireUuid(row?.user_id, 'ROLLBACK_ROW_OWNER_INVALID') === actor.userId
          && row?.state_code === 0,
        'ROLLBACK_ROW_OWNER_INVALID',
      );
      const expectedJson = expectedByKey.get(entityKeyString(key));
      assertCondition(expectedJson !== undefined, 'ROLLBACK_UNEXPECTED_ROW');
      assertCondition(
        canonicalJson(row?.json_ordered) === canonicalJson(expectedJson),
        'ROLLBACK_BUSINESS_ROW_CHANGED',
        {
          key_sha256: sha256(entityKeyString(key)),
          expected_sha256: sha256(expectedJson),
          observed_sha256: sha256(row?.json_ordered),
        },
      );
      observed.push({
        ...key,
        json_ordered_sha256: sha256(row.json_ordered),
      });
    }
  }
  assertCondition(observed.length === 52, 'ROLLBACK_ROW_CARDINALITY_INVALID');

  const expectedSupport = new Map();
  for (const batch of manifest.payload.preflight_request.plan.batches) {
    for (const role of ['flowproperty', 'unitgroup', 'source_unitgroup']) {
      const snapshot = batch.target?.[role];
      const table = role === 'flowproperty' ? 'flowproperties' : 'unitgroups';
      const key = normalizeEntityKey(
        { table, id: snapshot?.id, version: snapshot?.version },
        'ROLLBACK_EXPECTED_SUPPORT_INVALID',
        SUPPORT_TABLES,
      );
      assertCondition(isPlainObject(snapshot?.expected_json_ordered), 'ROLLBACK_EXPECTED_SUPPORT_INVALID');
      expectedSupport.set(entityKeyString(key), snapshot.expected_json_ordered);
    }
  }
  assertCondition(expectedSupport.size === 6, 'ROLLBACK_EXPECTED_SUPPORT_INVALID');

  const observedSupport = [];
  for (const table of ['flowproperties', 'unitgroups']) {
    const keys = manifest.payload.support_keys
      .map((item) => normalizeEntityKey(item, 'ROLLBACK_EXPECTED_SUPPORT_INVALID', SUPPORT_TABLES))
      .filter((key) => key.table === table);
    const rows = await readActorRows(config, actor, table, keys);
    assertCondition(rows.length === keys.length, 'ROLLBACK_SUPPORT_CARDINALITY_INVALID', {
      table_sha256: sha256(table),
      expected: keys.length,
      observed: rows.length,
    });
    for (const row of rows) {
      const key = normalizeEntityKey(
        { table, id: row?.id, version: row?.version },
        'ROLLBACK_SUPPORT_KEY_INVALID',
        SUPPORT_TABLES,
      );
      assertCondition(
        requireUuid(row?.user_id, 'ROLLBACK_SUPPORT_OWNER_INVALID') === actor.userId
          && row?.state_code === 0,
        'ROLLBACK_SUPPORT_OWNER_INVALID',
      );
      const expectedJson = expectedSupport.get(entityKeyString(key));
      assertCondition(expectedJson !== undefined, 'ROLLBACK_UNEXPECTED_SUPPORT_ROW');
      assertCondition(
        canonicalJson(row?.json_ordered) === canonicalJson(expectedJson),
        'ROLLBACK_SUPPORT_ROW_CHANGED',
        {
          key_sha256: sha256(entityKeyString(key)),
          expected_sha256: sha256(expectedJson),
          observed_sha256: sha256(row?.json_ordered),
        },
      );
      observedSupport.push({ ...key, json_ordered_sha256: sha256(row.json_ordered) });
    }
  }
  assertCondition(observedSupport.length === 6, 'ROLLBACK_SUPPORT_CARDINALITY_INVALID');
  return {
    unchanged_business_row_count: observed.length,
    unchanged_business_row_set_sha256: sha256(
      observed.sort((left, right) => entityKeyString(left).localeCompare(entityKeyString(right))),
    ),
    unchanged_support_row_count: observedSupport.length,
    unchanged_support_row_set_sha256: sha256(
      observedSupport.sort((left, right) => entityKeyString(left).localeCompare(entityKeyString(right))),
    ),
  };
}

function preflightCounts(result) {
  return {
    rows: result?.simulation?.plan_rows,
    exchanges: result?.simulation?.plan_exchanges,
    audits: result?.simulation?.alias_audits,
    derivative_targets: result?.simulation?.derivative_targets,
  };
}

async function performPreflightAndGates(config, actor, preflightRequest) {
  const preflight = await rpc(
    config,
    actor,
    'cmd_dataset_alias_execution_preflight_guarded',
    { p_request: preflightRequest },
  );
  assertCondition(preflight.ok === true, 'PREFLIGHT_REJECTED', {
    remote_code: safeRemoteCode(preflight.code),
    response_sha256: sha256(preflight),
  });
  const simulation = preflightCounts(preflight);
  requireInteger(simulation.rows, 52, 'PREFLIGHT_SIMULATION_COUNT_INVALID');
  requireInteger(simulation.exchanges, 59, 'PREFLIGHT_SIMULATION_COUNT_INVALID');
  requireInteger(simulation.audits, 55, 'PREFLIGHT_SIMULATION_COUNT_INVALID');
  requireInteger(simulation.derivative_targets, 50, 'PREFLIGHT_SIMULATION_COUNT_INVALID');
  assertCondition(preflight.simulation?.rolled_back === true, 'PREFLIGHT_NOT_ROLLED_BACK');
  const preflightToken = requireSha(preflight.preflight_token, 'PREFLIGHT_TOKEN_INVALID');
  const proofSha256 = requireSha(preflight.preflight_proof_sha256, 'PREFLIGHT_PROOF_INVALID');

  const gates = {};
  for (const gateName of GATE_NAMES) {
    const result = await rpc(
      config,
      actor,
      'cmd_dataset_alias_execution_gate_guarded',
      {
        p_request_id: preflightRequest.request_id,
        p_preflight_token: preflightToken,
        p_gate_name: gateName,
      },
    );
    assertCondition(result.ok === true && result.status === 'passed', 'GATE_REJECTED', {
      gate_name: gateName,
      remote_code: safeRemoteCode(result.code),
      response_sha256: sha256(result),
    });
    const expectedSha256 = requireSha(result.expected_sha256, 'GATE_EXPECTED_HASH_INVALID');
    const observedSha256 = requireSha(result.observed_sha256, 'GATE_OBSERVED_HASH_INVALID');
    assertCondition(expectedSha256 === observedSha256, 'GATE_HASH_MISMATCH', {
      gate_name: gateName,
      expected_sha256: expectedSha256,
      observed_sha256: observedSha256,
    });
    requireSha(result.receipt_sha256, 'GATE_RECEIPT_HASH_INVALID');
    requireString(result.captured_at, 'GATE_CAPTURED_AT_INVALID');
    gates[gateName] = {
      expected_sha256: expectedSha256,
      observed_sha256: observedSha256,
      status: 'passed',
      captured_at: result.captured_at,
    };
  }

  return {
    preflight,
    admissionRequest: {
      schema_version: 'dataset-alias-execution-admit.v1',
      request_id: preflightRequest.request_id,
      preflight_token: preflightToken,
      preflight_proof_sha256: proofSha256,
      gate_results: gates,
    },
  };
}

function businessRollbackFaultSql(config, context, manifest) {
  const fault = normalizeEntityKey(
    manifest.payload.business_rollback_fault,
    'FIXTURE_BUSINESS_FAULT_INVALID',
  );
  const table = fault.table;
  assertCondition(table === 'flowproperties', 'FIXTURE_BUSINESS_FAULT_INVALID');
  return `begin;
set local search_path = extensions, public, auth;
set local statement_timeout = '30s';
do $business_rollback_fault$
declare
  server_context jsonb := util.dataset_alias_execution_server_context();
  changed_count integer;
begin
  if server_context->>'environment' is distinct from 'preview'
    or server_context->>'project_ref' is distinct from ${sqlLiteral(config.projectRef)} then
    raise exception 'business rollback fault requires the exact trusted Preview context';
  end if;

  update public.${table}
  set modified_at = modified_at + interval '1 microsecond'
  where id = ${sqlLiteral(fault.id)}::uuid
    and version = ${sqlLiteral(fault.version)}
    and user_id = ${sqlLiteral(context.actor.userId)}::uuid
    and state_code = 0
    and exists (
      select 1
      from public.command_audit_log as fixture
      where fixture.command = ${sqlLiteral(FIXTURE_COMMAND)}
        and fixture.actor_user_id = ${sqlLiteral(context.actor.userId)}::uuid
        and fixture.target_table = ${sqlLiteral(FIXTURE_TARGET_TABLE)}
        and fixture.target_id = ${sqlLiteral(context.requestId)}::uuid
        and fixture.target_version = ${sqlLiteral(FIXTURE_TARGET_VERSION)}
        and fixture.payload->>'schema_version' is not distinct from ${sqlLiteral(MANIFEST_SCHEMA)}
        and fixture.payload->>'scenario_id' is not distinct from ${sqlLiteral(context.namespace)}
        and fixture.payload->>'scenario_kind' is not distinct from 'business_rollback'
    );
  get diagnostics changed_count = row_count;
  if changed_count is distinct from 1 then
    raise exception 'business rollback fault must drift exactly one actor-owned flowproperty';
  end if;
end;
$business_rollback_fault$;

select tap
from (values
  ('TAP version 13'),
  ('1..3'),
  ('ok 1 - fault injection is restricted to Preview'),
  ('ok 2 - fault injection is restricted to the exact Preview ref'),
  ('ok 3 - exactly one actor-owned flowproperty is drifted after all gates')
) as business_rollback_fault_tap(tap);
commit;
`;
}

function admissionParameters(admissionRequest) {
  return { p_request: admissionRequest };
}

async function directAdmission(config, actor, admissionRequest) {
  const result = await rpc(
    config,
    actor,
    'cmd_dataset_alias_execution_admit_guarded',
    admissionParameters(admissionRequest),
  );
  assertCondition(result.ok === true, 'ADMISSION_REJECTED', {
    remote_code: safeRemoteCode(result.code),
    response_sha256: sha256(result),
  });
  assertAdmissionAccepted(result);
  return result;
}

function assertAdmissionAccepted(result) {
  assertCondition(
    result.ok === true
      && result.status === 'dispatched'
      && result.attempt_count === 1
      && result.dispatch_count === 1
      && result.attempt_consumed === true
      && result.retry_allowed === false,
    'ADMISSION_RESPONSE_INVALID',
    { response_sha256: sha256(result) },
  );
  requireSha(result.admission_request_sha256, 'ADMISSION_HASH_INVALID');
  requireSha(result.gate_results_sha256, 'ADMISSION_GATE_HASH_INVALID');
}

function stripHopByHopHeaders(headers) {
  const cleaned = { ...headers };
  for (const name of [
    'connection',
    'host',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailers',
    'transfer-encoding',
    'upgrade',
  ]) {
    delete cleaned[name];
  }
  return cleaned;
}

function collectIncomingRequest(request, limit = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    request.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new SafeError('PROXY_REQUEST_TOO_LARGE'));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.once('end', () => resolve(Buffer.concat(chunks)));
    request.once('error', () => reject(new SafeError('PROXY_REQUEST_READ_FAILED')));
  });
}

function upstreamRequest(target, request, body) {
  const headers = stripHopByHopHeaders(request.headers);
  headers['content-length'] = String(body.length);
  return new Promise((resolve, reject) => {
    const client = target.protocol === 'https:' ? https : http;
    const upstream = client.request(
      target,
      {
        method: request.method,
        headers,
        timeout: 120_000,
      },
      (response) => {
        const chunks = [];
        let size = 0;
        response.on('data', (chunk) => {
          size += chunk.length;
          if (size > 16 * 1024 * 1024) {
            response.destroy();
            reject(new SafeError('PROXY_RESPONSE_TOO_LARGE'));
            return;
          }
          chunks.push(chunk);
        });
        response.once('end', () => resolve({
          statusCode: response.statusCode ?? 502,
          headers: stripHopByHopHeaders(response.headers),
          body: Buffer.concat(chunks),
        }));
        response.once('error', () => reject(new SafeError('PROXY_UPSTREAM_RESPONSE_FAILED')));
      },
    );
    upstream.once('timeout', () => upstream.destroy(new Error('timeout')));
    upstream.once('error', () => reject(new SafeError('PROXY_UPSTREAM_FAILED')));
    upstream.end(body);
  });
}

async function listenLocal(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assertCondition(isPlainObject(address) && Number.isInteger(address.port), 'PROXY_LISTEN_FAILED');
  return `http://127.0.0.1:${address.port}`;
}

async function closeLocal(server) {
  if (!server.listening) return;
  if (typeof server.closeAllConnections === 'function') server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}

async function concurrentAdmissions(config, actor, admissionRequest) {
  const pending = [];
  const server = http.createServer(async (request, response) => {
    try {
      const body = await collectIncomingRequest(request);
      pending.push({ request, response, body });
      if (pending.length > 2) {
        response.writeHead(409, { 'content-type': 'application/json' });
        response.end('{"code":"LOCAL_BARRIER_EXCESS_REQUEST"}');
        return;
      }
      if (pending.length === 2) {
        const pair = [...pending];
        queueMicrotask(() => {
          for (const item of pair) {
            const target = new URL(item.request.url, config.supabaseUrl);
            upstreamRequest(target, item.request, item.body)
              .then((upstream) => {
                item.response.writeHead(upstream.statusCode, upstream.headers);
                item.response.end(upstream.body);
              })
              .catch(() => {
                if (!item.response.destroyed) {
                  item.response.writeHead(502, { 'content-type': 'application/json' });
                  item.response.end('{"code":"LOCAL_BARRIER_UPSTREAM_FAILED"}');
                }
              });
          }
        });
      }
    } catch {
      if (!response.destroyed) response.destroy();
    }
  });
  server.on('clientError', (_error, socket) => socket.destroy());
  const localBase = await listenLocal(server);
  let results;
  try {
    results = await Promise.all([
      rpc(
        config,
        actor,
        'cmd_dataset_alias_execution_admit_guarded',
        admissionParameters(admissionRequest),
        localBase,
      ),
      rpc(
        config,
        actor,
        'cmd_dataset_alias_execution_admit_guarded',
        admissionParameters(admissionRequest),
        localBase,
      ),
    ]);
  } finally {
    await closeLocal(server);
  }

  const accepted = results.filter((result) => result.ok === true);
  const rejected = results.filter(
    (result) => result.ok === false
      && result.code === 'ALIAS_EXECUTION_ATTEMPT_ALREADY_CONSUMED',
  );
  assertCondition(accepted.length === 1 && rejected.length === 1, 'DUPLICATE_ADMISSION_RESULT_INVALID', {
    response_set_sha256: sha256(results),
    accepted_count: accepted.length,
    rejected_count: rejected.length,
  });
  assertAdmissionAccepted(accepted[0]);
  return {
    accepted: accepted[0],
    responseSetSha256: sha256(results),
    rejectionCode: rejected[0].code,
  };
}

async function faultedAdmission(config, actor, admissionRequest, mode) {
  let upstreamOutcomeResolve;
  const upstreamOutcome = new Promise((resolve) => {
    upstreamOutcomeResolve = resolve;
  });
  let requestCount = 0;
  const delayedTimers = new Set();
  const server = http.createServer(async (request, response) => {
    requestCount += 1;
    if (requestCount !== 1) {
      response.writeHead(409, { 'content-type': 'application/json' });
      response.end('{"code":"LOCAL_PROXY_ONE_REQUEST_ONLY"}');
      return;
    }
    try {
      const body = await collectIncomingRequest(request);
      const target = new URL(request.url, config.supabaseUrl);
      const upstream = await upstreamRequest(target, request, body);
      upstreamOutcomeResolve({
        http_status: upstream.statusCode,
        response_sha256: sha256(upstream.body),
      });
      if (mode === 'drop_response') {
        response.destroy();
        return;
      }
      const timer = setTimeout(() => {
        delayedTimers.delete(timer);
        if (!response.destroyed) {
          response.writeHead(upstream.statusCode, upstream.headers);
          response.end(upstream.body);
        }
      }, config.httpTimeoutMs + 1_000);
      delayedTimers.add(timer);
    } catch {
      upstreamOutcomeResolve({ http_status: null, response_sha256: sha256('proxy-upstream-failed') });
      if (!response.destroyed) response.destroy();
    }
  });
  server.on('clientError', (_error, socket) => socket.destroy());
  const localBase = await listenLocal(server);
  const localUrl = `${localBase}/rest/v1/rpc/cmd_dataset_alias_execution_admit_guarded`;
  const controller = new AbortController();
  let timeout = null;
  if (mode === 'delay_response') {
    timeout = setTimeout(() => controller.abort(), config.httpTimeoutMs);
    timeout.unref();
  }

  let transportFailed = false;
  let settled = null;
  try {
    try {
      await fetch(localUrl, {
        method: 'POST',
        headers: actorRpcHeaders(config, actor),
        body: JSON.stringify(admissionParameters(admissionRequest)),
        redirect: 'error',
        signal: controller.signal,
      });
    } catch {
      transportFailed = true;
    } finally {
      if (timeout) clearTimeout(timeout);
    }
    assertCondition(transportFailed, 'TRANSPORT_FAULT_NOT_OBSERVED', { mode });

    settled = await Promise.race([
      upstreamOutcome,
      new Promise((_, reject) => {
        const timer = setTimeout(
          () => reject(new SafeError('PROXY_UPSTREAM_SETTLE_TIMEOUT')),
          config.rpcTimeoutMs,
        );
        timer.unref();
      }),
    ]);
  } finally {
    for (const timer of delayedTimers) clearTimeout(timer);
    await closeLocal(server);
  }
  assertCondition(requestCount === 1, 'TRANSPORT_ADMISSION_POST_COUNT_INVALID');
  assertCondition(settled?.http_status === 200, 'TRANSPORT_UPSTREAM_HTTP_STATUS_INVALID', {
    http_status: settled?.http_status ?? null,
    response_sha256: settled?.response_sha256 ?? null,
  });
  return settled;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function readCounts(result) {
  return {
    rows: result?.primary_readback?.row_count ?? null,
    exchanges: result?.primary_readback?.exchange_count ?? null,
    audits: result?.primary_readback?.alias_audit_count ?? null,
    derivative_targets: result?.derivative_readback?.target_count ?? null,
    derivative_flows: result?.derivative_readback?.flow_count ?? null,
    derivative_processes: result?.derivative_readback?.process_count ?? null,
  };
}

function isReadRetry(result) {
  return result?.ok === false && [
    'ALIAS_EXECUTION_READ_STATE_CHANGED',
    'ALIAS_EXECUTION_READ_LOCK_BUSY',
  ].includes(result.code);
}

function isPrimarySuccess(result) {
  const counts = readCounts(result);
  return result?.ok === true
    && result.attempt_count === 1
    && result.dispatch_count === 1
    && result.retry_allowed === false
    && ['derivatives_pending', 'completed'].includes(result.execution_status)
    && counts.rows === 52
    && counts.exchanges === 59
    && counts.audits === 55
    && result.primary_readback?.live_closure_proof === true
    && counts.derivative_targets === 50
    && counts.derivative_flows === 23
    && counts.derivative_processes === 27;
}

function isBusinessRollback(result) {
  const counts = readCounts(result);
  return result?.ok === true
    && result.execution_status === 'failed'
    && result.attempt_count === 1
    && result.dispatch_count === 1
    && result.retry_allowed === false
    && counts.rows === null
    && counts.exchanges === null
    && counts.audits === 0
    && counts.derivative_targets === 0
    && counts.derivative_flows === 0
    && counts.derivative_processes === 0;
}

async function pollRead(config, actor, requestId, expectation) {
  const deadline = Date.now() + config.pollTimeoutMs;
  let readCount = 0;
  let transportReadFailures = 0;
  let lastResult = null;
  while (Date.now() < deadline) {
    try {
      lastResult = await rpc(
        config,
        actor,
        'cmd_dataset_alias_execution_read',
        { p_request_id: requestId },
      );
      readCount += 1;
    } catch (error) {
      if (error instanceof SafeError && error.code === 'HTTP_TRANSPORT_FAILED') {
        transportReadFailures += 1;
        await sleep(config.pollIntervalMs);
        continue;
      }
      throw error;
    }

    if (isReadRetry(lastResult)) {
      await sleep(config.pollIntervalMs);
      continue;
    }
    if (expectation === 'business_rollback' && isBusinessRollback(lastResult)) {
      return { result: lastResult, readCount, transportReadFailures };
    }
    if (expectation !== 'business_rollback' && isPrimarySuccess(lastResult)) {
      return { result: lastResult, readCount, transportReadFailures };
    }
    if (
      lastResult?.ok === false
      || ['failed', 'indeterminate'].includes(lastResult?.execution_status)
    ) {
      fail('READBACK_TERMINAL_FAILURE', {
        remote_code: safeRemoteCode(lastResult?.code ?? lastResult?.error?.code),
        execution_status: lastResult?.execution_status,
        response_sha256: sha256(lastResult),
      });
    }
    await sleep(config.pollIntervalMs);
  }
  fail('READBACK_TIMEOUT', {
    read_count: readCount,
    transport_read_failures: transportReadFailures,
    last_response_sha256: lastResult ? sha256(lastResult) : null,
    execution_status: lastResult?.execution_status ?? null,
  });
}

function scenarioNamespace(scenario) {
  return `pae2e-${scenario}-${randomBytes(12).toString('hex')}`;
}

function templateValues(config, scenario, namespace, requestId, actor) {
  return {
    ACTOR_UUID_SQL: actor.userId,
    ACTOR_EMAIL_SQL: actor.email,
    SCENARIO_NAMESPACE_SQL: namespace,
    REQUEST_ID_SQL: requestId,
    PREVIEW_REF_SQL: config.projectRef,
    PREVIEW_URL_SQL: config.supabaseUrl,
    SERVICE_ROLE_KEY_SQL: config.serviceRoleKey,
    SCENARIO_KIND_SQL: scenario,
  };
}

function scenarioEvidence(context, fixtureProof, preflightAndGates, admission, readback, transport) {
  const final = readback.result;
  const counts = readCounts(final);
  const remoteCodes = [];
  if (admission.rejectionCode) remoteCodes.push(admission.rejectionCode);
  const errorCode = safeRemoteCode(final?.error?.code);
  if (errorCode) remoteCodes.push(errorCode);
  return {
    scenario: context.scenario,
    status: 'passed',
    actor_sha256: sha256(context.actor.userId),
    request_sha256: sha256(context.requestId),
    namespace_sha256: sha256(context.namespace),
    manifest_sha256: fixtureProof.manifestSha256,
    preflight_proof_sha256: preflightAndGates.preflight.preflight_proof_sha256,
    admission_request_sha256:
      admission.accepted?.admission_request_sha256
      ?? final.admission_request_sha256
      ?? null,
    transport_proof_sha256:
      admission.responseSetSha256
      ?? admission.proxyOutcome?.response_sha256
      ?? null,
    transport_http_status: admission.proxyOutcome?.http_status ?? null,
    transport,
    admission_posts: admission.posts,
    recovery_mode: admission.recoveryMode,
    read_posts: readback.readCount,
    transport_read_failures: readback.transportReadFailures,
    attempt_count: final.attempt_count,
    dispatch_count: final.dispatch_count,
    execution_status: final.execution_status,
    counts,
    codes: remoteCodes.sort(),
    final_read_sha256: sha256(final),
  };
}

async function executeScenario(config, scenario) {
  const context = {
    config,
    scenario,
    namespace: scenarioNamespace(scenario),
    requestId: randomUUID(),
    actor: null,
  };
  let tempDir = null;
  let fixtureAttempted = false;
  let primaryError = null;
  let evidence = null;
  const cleanup = {
    fixture_cleanup_ran: false,
    manifest_rows_after_cleanup: null,
    session_revoked: false,
    actor_deleted: false,
    actor_retained_for_cleanup: false,
  };
  let cleanupFailure = null;

  try {
    tempDir = await mkdtemp(path.join(os.tmpdir(), 'protected-alias-preview-'));
    context.actor = await createDisposableActor(config, scenario);
    const values = templateValues(
      config,
      scenario,
      context.namespace,
      context.requestId,
      context.actor,
    );
    const fixture = await renderSqlTemplate(FIXTURE_TEMPLATE, values);
    fixtureAttempted = true;
    const fixtureDbProof = await runDbTest(config, tempDir, 'fixture', fixture.sql);

    const manifestRows = await readFixtureManifest(
      config,
      context.actor.userId,
      context.requestId,
    );
    assertCondition(manifestRows.length === 1, 'FIXTURE_MANIFEST_CARDINALITY_INVALID', {
      observed: manifestRows.length,
    });
    const fixtureProof = validateManifest(manifestRows[0]?.payload, context);

    const preflightAndGates = await performPreflightAndGates(
      config,
      context.actor,
      fixtureProof.payload.preflight_request,
    );

    if (scenario === 'business_rollback') {
      const faultSql = businessRollbackFaultSql(config, context, fixtureProof);
      await runDbTest(config, tempDir, 'business-rollback-fault', faultSql);
    }

    let admission;
    let transport;
    if (scenario === 'concurrent_duplicate') {
      const duplicate = await concurrentAdmissions(
        config,
        context.actor,
        preflightAndGates.admissionRequest,
      );
      admission = {
        accepted: duplicate.accepted,
        posts: 2,
        recoveryMode: 'status_only',
        rejectionCode: duplicate.rejectionCode,
        responseSetSha256: duplicate.responseSetSha256,
      };
      transport = 'barrier';
    } else if (scenario === 'lost_response' || scenario === 'http_timeout') {
      const mode = scenario === 'lost_response' ? 'drop_response' : 'delay_response';
      const proxyOutcome = await faultedAdmission(
        config,
        context.actor,
        preflightAndGates.admissionRequest,
        mode,
      );
      admission = {
        accepted: null,
        posts: 1,
        recoveryMode: 'status_only',
        rejectionCode: null,
        proxyOutcome,
      };
      transport = mode;
    } else {
      const accepted = await directAdmission(
        config,
        context.actor,
        preflightAndGates.admissionRequest,
      );
      admission = {
        accepted,
        posts: 1,
        recoveryMode: 'status_only',
        rejectionCode: null,
      };
      transport = 'direct';
    }

    const readback = await pollRead(
      config,
      context.actor,
      context.requestId,
      scenario,
    );
    const finalAdmissionSha256 = requireSha(
      readback.result.admission_request_sha256,
      'READBACK_ADMISSION_HASH_INVALID',
    );
    requireSha(readback.result.gate_results_sha256, 'READBACK_GATE_HASH_INVALID');
    assertCondition(
      readback.result.preflight_proof_sha256
        === preflightAndGates.preflight.preflight_proof_sha256,
      'READBACK_PREFLIGHT_PROOF_MISMATCH',
    );
    if (admission.accepted) {
      assertCondition(
        admission.accepted.admission_request_sha256 === finalAdmissionSha256,
        'READBACK_ADMISSION_HASH_MISMATCH',
      );
    }
    const rollbackProof = scenario === 'business_rollback'
      ? await verifyBusinessRollbackRows(config, context.actor, fixtureProof)
      : null;
    assertCondition(admission.posts === (scenario === 'concurrent_duplicate' ? 2 : 1), 'ADMISSION_POST_COUNT_INVALID');
    assertCondition(admission.recoveryMode === 'status_only', 'RECOVERY_MODE_INVALID');

    evidence = scenarioEvidence(
      context,
      fixtureProof,
      preflightAndGates,
      admission,
      readback,
      transport,
    );
    if (rollbackProof) evidence.rollback = rollbackProof;
    evidence.fixture = {
      template_sha256: fixture.template_sha256,
      rendered_sha256: fixture.rendered_sha256,
      db_test_stdout_sha256: fixtureDbProof.stdout_sha256,
      db_test_stderr_sha256: fixtureDbProof.stderr_sha256,
    };
  } catch (error) {
    primaryError = error instanceof SafeError ? error : new SafeError('SCENARIO_FAILED');
  } finally {
    if (context.actor && fixtureAttempted && tempDir) {
      try {
        const values = templateValues(
          config,
          scenario,
          context.namespace,
          context.requestId,
          context.actor,
        );
        const cleanupTemplate = await renderSqlTemplate(CLEANUP_TEMPLATE, values);
        const cleanupDbProof = await runDbTest(config, tempDir, 'cleanup', cleanupTemplate.sql);
        cleanup.fixture_cleanup_ran = true;
        cleanup.cleanup_template_sha256 = cleanupTemplate.template_sha256;
        cleanup.cleanup_rendered_sha256 = cleanupTemplate.rendered_sha256;
        cleanup.cleanup_stdout_sha256 = cleanupDbProof.stdout_sha256;
        cleanup.cleanup_stderr_sha256 = cleanupDbProof.stderr_sha256;

        const remaining = await readFixtureManifest(
          config,
          context.actor.userId,
          context.requestId,
        );
        cleanup.manifest_rows_after_cleanup = remaining.length;
        assertCondition(remaining.length === 0, 'CLEANUP_MANIFEST_RESIDUE', {
          observed: remaining.length,
        });
      } catch (error) {
        cleanupFailure = error instanceof SafeError ? error : new SafeError('FIXTURE_CLEANUP_FAILED');
      }
    }

    if (context.actor) {
      try {
        await revokeActorSession(config, context.actor);
        cleanup.session_revoked = true;
      } catch (error) {
        cleanup.session_revoked = false;
        cleanupFailure ??= error instanceof SafeError
          ? error
          : new SafeError('AUTH_SESSION_REVOKE_FAILED');
      }
      const cleanupClosed = !fixtureAttempted || (
        cleanupFailure === null
        && cleanup.fixture_cleanup_ran
        && cleanup.manifest_rows_after_cleanup === 0
      );
      if (cleanupClosed) {
        try {
          await deleteDisposableActor(config, context.actor.userId);
          cleanup.actor_deleted = true;
        } catch (error) {
          cleanup.actor_retained_for_cleanup = true;
          cleanupFailure ??= error instanceof SafeError
            ? error
            : new SafeError('AUTH_ACTOR_DELETE_FAILED');
        }
      } else {
        // Preserve the exact UUID/email database-side cleanup identity after
        // an ambiguous fixture commit or cleanup failure. Its session is
        // revoked above, so it cannot mutate data while recovery is pending.
        cleanup.actor_retained_for_cleanup = true;
      }
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
      && cleanup.session_revoked
      && cleanup.actor_deleted,
    'CLEANUP_PROOF_INCOMPLETE',
    cleanup,
  );
  evidence.cleanup = cleanup;
  return evidence;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(`${helpText()}\n`);
    return;
  }
  assertCondition(typeof fetch === 'function', 'NODE_FETCH_UNAVAILABLE');
  const config = loadConfig(args.expectedPreviewRef);
  const startedAt = new Date().toISOString();
  const scenarios = [];
  for (const scenario of SCENARIOS) {
    scenarios.push(await executeScenario(config, scenario));
  }
  const evidence = sanitizeEvidence({
    schema_version: EVIDENCE_SCHEMA,
    status: 'passed',
    environment: 'preview',
    project_ref_sha256: sha256(config.projectRef),
    cli_version_sha256: sha256(SUPABASE_CLI_VERSION),
    started_at_utc: startedAt,
    completed_at_utc: new Date().toISOString(),
    scenario_count: scenarios.length,
    exact_expected_counts: EXPECTED,
    scenarios,
  });
  evidence.evidence_sha256 = sha256(evidence);
  process.stdout.write(`${JSON.stringify(evidence)}\n`);
}

main().catch((error) => {
  const safeError = error instanceof SafeError ? error : new SafeError('UNEXPECTED_FAILURE');
  const evidence = sanitizeEvidence({
    schema_version: EVIDENCE_SCHEMA,
    status: 'failed',
    code: safeError.code,
    details: safeError.details,
    failed_at_utc: new Date().toISOString(),
  });
  evidence.evidence_sha256 = sha256(evidence);
  process.stderr.write(`${JSON.stringify(evidence)}\n`);
  process.exitCode = 1;
});
