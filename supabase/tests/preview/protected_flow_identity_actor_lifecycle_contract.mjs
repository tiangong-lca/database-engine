#!/usr/bin/env node

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  chmod, mkdtemp, realpath, rm, symlink, writeFile,
} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { afterEach, test } from 'node:test';

import { __test } from './protected_flow_identity_rest_e2e.mjs';

const ORIGINAL_FETCH = globalThis.fetch;
const CONFIG = Object.freeze({
  supabaseUrl: 'https://aaaaaaaaaaaaaaa.supabase.co',
  anonKey: 'anon-key-at-least-20-characters',
  serviceRoleKey: 'service-key-at-least-20-characters',
  rpcTimeoutMs: 1_000,
});
const REQUEST_ID = '11111111-1111-4111-8111-111111111111';
const NAMESPACE = 'fie2e-hosted-0123456789abcdef01234567';
const OWNER_ID = '22222222-2222-4222-8222-222222222222';
const OTHER_ID = '33333333-3333-4333-8333-333333333333';
const FIXTURE_SHA256 = createHash('sha256')
  .update('database-engine#269')
  .digest('hex');

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value);
}

function canonicalSha256(value) {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

afterEach(() => {
  globalThis.fetch = ORIGINAL_FETCH;
});

function jsonResponse(status, body = null, headers = {}) {
  const normalizedHeaders = Object.fromEntries(
    Object.entries(headers).map(([name, value]) => [name.toLowerCase(), String(value)]),
  );
  return {
    status,
    headers: {
      get(name) { return normalizedHeaders[name.toLowerCase()] ?? null; },
    },
    async text() {
      return body === null ? '' : JSON.stringify(body);
    },
  };
}

function userListResponse(users, { total = users.length, link = null } = {}) {
  return jsonResponse(200, { users }, {
    'x-total-count': String(total),
    ...(link === null ? {} : { link }),
  });
}

function nextUserListLink(page, filter) {
  const query = new URLSearchParams({
    page: String(page),
    per_page: '200',
    filter,
  });
  return `<${CONFIG.supabaseUrl}/admin/users?${query.toString()}>; rel="next"`;
}

function installFetchScript(steps) {
  let index = 0;
  globalThis.fetch = async (input, options = {}) => {
    assert.ok(index < steps.length, `unexpected fetch ${String(input)}`);
    const step = steps[index];
    index += 1;
    return step(String(input), options);
  };
  return () => assert.equal(index, steps.length, 'not every expected fetch ran');
}

class FakeIpc extends EventEmitter {
  constructor(onSend) {
    super();
    this.connected = true;
    this.onSend = onSend;
  }

  send(message, callback) {
    callback?.(null);
    this.onSend(message, this);
  }
}

function context() {
  const checkpointStages = [];
  const value = {
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    actorCandidates: [],
    authCensusBaselineTotal: null,
    checkpointStages,
    recoverySelectors: null,
    recoveryCheckpointClient: {
      async checkpoint(stage) {
        checkpointStages.push(stage);
        return { stage };
      },
      summary() {
        return {
          checkpoint_count: checkpointStages.length,
          last_checkpoint_sha256: '0'.repeat(64),
        };
      },
    },
  };
  value.recoverySelectors = __test.createRecoverySelectors(value);
  return value;
}

function exactUser(email, overrides = {}) {
  return {
    id: OWNER_ID,
    email,
    app_metadata: {
      preview_e2e: true,
      fixture_sha256: FIXTURE_SHA256,
      preview_e2e_request_id: REQUEST_ID,
      preview_e2e_namespace: NAMESPACE,
      preview_e2e_role: 'owner',
    },
    ...overrides,
  };
}

function candidateFixture(overrides = {}) {
  return {
    role: 'owner',
    email: 'fixture-owner@example.invalid',
    password: 'fixture-password',
    fixtureSha256: FIXTURE_SHA256,
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    userId: OWNER_ID,
    accessToken: 'fixture-access-token',
    createRequested: true,
    createAcknowledged: true,
    preCreateAbsenceReadbackAttempted: true,
    preCreateAbsenceConfirmed: true,
    lookupAttempted: true,
    lastUserListTotal: 1,
    actorBoundCheckpointAcknowledged: true,
    signInRequested: true,
    signInComplete: true,
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
    ...overrides,
  };
}

test('normal actor lifecycle proves exact identity, global logout, delete, and GET 404', async () => {
  const lifecycle = context();
  let createdEmail = null;
  const assertScriptComplete = installFetchScript([
    (url) => {
      const parsed = new URL(url);
      assert.equal(
        parsed.searchParams.get('filter'),
        'flow-identity-owner-0123456789abcdef01234567@example.invalid',
      );
      return userListResponse([]);
    },
    (url, options) => {
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users`);
      const body = JSON.parse(options.body);
      createdEmail = body.email;
      assert.deepEqual(body.app_metadata, {
        preview_e2e: true,
        fixture_sha256: body.app_metadata.fixture_sha256,
        preview_e2e_request_id: REQUEST_ID,
        preview_e2e_namespace: NAMESPACE,
        preview_e2e_role: 'owner',
      });
      assert.match(body.app_metadata.fixture_sha256, /^[0-9a-f]{64}$/);
      return jsonResponse(201, { id: OWNER_ID });
    },
    (url) => {
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OWNER_ID}`);
      return jsonResponse(200, exactUser(createdEmail));
    },
    (url) => {
      assert.match(url, /\/auth\/v1\/token\?grant_type=password$/);
      return jsonResponse(200, { user: { id: OWNER_ID }, access_token: 'owner-token' });
    },
    (url) => {
      assert.match(url, /\/auth\/v1\/logout\?scope=global$/);
      return jsonResponse(204);
    },
    (url, options) => {
      assert.equal(options.method, 'DELETE');
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OWNER_ID}`);
      assert.equal(options.headers['content-type'], 'application/json');
      assert.deepEqual(JSON.parse(options.body), { should_soft_delete: false });
      return jsonResponse(200, {});
    },
    (url, options) => {
      assert.equal(options.method, 'GET');
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OWNER_ID}`);
      return jsonResponse(404);
    },
    (url, options) => {
      assert.equal(options.method, 'GET');
      assert.match(
        url,
        /\/auth\/v1\/admin\/users\?page=1&per_page=200&filter=/,
      );
      assert.equal(new URL(url).searchParams.get('filter'), createdEmail);
      return userListResponse([]);
    },
  ]);

  const actor = await __test.createDisposableActor(CONFIG, 'owner', lifecycle);
  await __test.cleanupActorCandidate(CONFIG, actor.candidate);
  assertScriptComplete();
  assert.equal(lifecycle.actorCandidates.length, 1);
  assert.equal(actor.candidate.sessionRevoked, true);
  assert.equal(actor.candidate.deleteAcknowledged, true);
  assert.equal(actor.candidate.absenceConfirmed, true);
  assert.equal(__test.actorRecoverySummary(actor.candidate).retained, false);
});

test('lost create response remains recoverable from exact metadata and is deleted', async () => {
  const lifecycle = context();
  const assertCreateComplete = installFetchScript([
    () => userListResponse([]),
    () => { throw new Error('simulated lost response'); },
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'HTTP_TRANSPORT_FAILED',
  );
  assertCreateComplete();
  const candidate = lifecycle.actorCandidates[0];
  assert.equal(candidate.createRequested, true);
  assert.equal(candidate.createAcknowledged, false);

  const assertCleanupComplete = installFetchScript([
    () => userListResponse([exactUser(candidate.email)]),
    () => jsonResponse(200, {
      user: { id: OWNER_ID }, access_token: 'cleanup-access-token',
    }),
    () => jsonResponse(204),
    (url, options) => {
      assert.equal(options.method, 'DELETE');
      return jsonResponse(204);
    },
    (url, options) => {
      assert.equal(options.method, 'GET');
      return jsonResponse(404);
    },
    () => userListResponse([]),
  ]);
  await __test.cleanupActorCandidate(CONFIG, candidate);
  assertCleanupComplete();
  assert.equal(candidate.userId, OWNER_ID);
  assert.equal(candidate.absenceConfirmed, true);
});

test('malformed create response recovers one paginated exact actor and ignores decoys', async () => {
  const lifecycle = context();
  let createdEmail = null;
  const assertScriptComplete = installFetchScript([
    () => userListResponse([]),
    (_url, options) => {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, {});
    },
    () => userListResponse(
      Array.from({ length: 200 }, (_, index) => exactUser(
        `decoy-${index}@example.invalid`,
        {
          id: `${String(index).padStart(8, '0')}-0000-4000-8000-000000000000`,
          app_metadata: { preview_e2e: true, preview_e2e_request_id: 'wrong' },
        },
      )),
      { total: 201, link: nextUserListLink(2, createdEmail) },
    ),
    () => userListResponse([exactUser(createdEmail)], { total: 201 }),
    () => jsonResponse(200, { user: { id: OWNER_ID }, access_token: 'owner-token' }),
    () => jsonResponse(204),
    () => jsonResponse(200, {}),
    () => jsonResponse(404),
    () => userListResponse([]),
  ]);
  const actor = await __test.createDisposableActor(CONFIG, 'owner', lifecycle);
  await __test.cleanupActorCandidate(CONFIG, actor.candidate);
  assertScriptComplete();
  assert.equal(actor.userId, OWNER_ID);
  assert.equal(actor.candidate.lookupAttempted, true);
  assert.equal(actor.candidate.absenceConfirmed, true);
});

test('lost sign-in response reacquires a cleanup session before global logout and delete', async () => {
  const lifecycle = context();
  let createdEmail = null;
  const assertCreateComplete = installFetchScript([
    () => userListResponse([]),
    (_url, options) => {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, { id: OWNER_ID });
    },
    () => jsonResponse(200, exactUser(createdEmail)),
    () => { throw new Error('simulated lost sign-in response'); },
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'HTTP_TRANSPORT_FAILED',
  );
  assertCreateComplete();
  const candidate = lifecycle.actorCandidates[0];

  const assertCleanupComplete = installFetchScript([
    (url) => {
      assert.match(url, /\/auth\/v1\/token\?grant_type=password$/);
      return jsonResponse(200, { user: { id: OWNER_ID }, access_token: 'cleanup-token' });
    },
    (url) => {
      assert.match(url, /\/auth\/v1\/logout\?scope=global$/);
      return jsonResponse(204);
    },
    () => jsonResponse(200, {}),
    () => jsonResponse(404),
    () => userListResponse([]),
  ]);
  await __test.cleanupActorCandidate(CONFIG, candidate);
  assertCleanupComplete();
  assert.equal(candidate.cleanupSignInComplete, true);
  assert.equal(candidate.sessionRevoked, true);
  assert.equal(candidate.absenceConfirmed, true);
});

test('multiple exact recovery matches fail closed', async () => {
  const candidate = candidateFixture({
    userId: null,
    accessToken: null,
    lookupAttempted: false,
    signInRequested: false,
    signInComplete: false,
  });
  const assertScriptComplete = installFetchScript([
    () => userListResponse([
        exactUser(candidate.email),
        exactUser(candidate.email, { id: OTHER_ID }),
      ]),
  ]);
  await assert.rejects(
    __test.findDisposableActor(CONFIG, candidate),
    (error) => error.code === 'AUTH_ACTOR_RECOVERY_AMBIGUOUS',
  );
  assertScriptComplete();
});

test('recovery selector rejects a user with the wrong fixture hash', async () => {
  const candidate = candidateFixture({
    userId: null,
    accessToken: null,
    lookupAttempted: false,
    signInRequested: false,
    signInComplete: false,
  });
  const assertScriptComplete = installFetchScript([
    () => userListResponse([
      exactUser(candidate.email, {
        app_metadata: {
          ...exactUser(candidate.email).app_metadata,
          fixture_sha256: '0'.repeat(64),
        },
      }),
    ]),
  ]);
  assert.equal(await __test.findDisposableActor(CONFIG, candidate), null);
  assertScriptComplete();
});

test('DELETE acknowledgement without GET 404 fails and retains recovery evidence', async () => {
  const candidate = candidateFixture();
  const assertScriptComplete = installFetchScript([
    () => jsonResponse(204),
    () => jsonResponse(200, {}),
    () => jsonResponse(200, exactUser(candidate.email)),
    () => userListResponse([exactUser(candidate.email)]),
  ]);
  let failure;
  try {
    await __test.cleanupActorCandidate(CONFIG, candidate);
  } catch (error) {
    failure = error;
  }
  assertScriptComplete();
  assert.equal(failure.code, 'AUTH_ACTOR_DELETE_READBACK_FAILED');
  assert.equal(candidate.deleteAcknowledged, true);
  assert.equal(candidate.absenceConfirmed, false);

  const attached = __test.attachCleanupDetails(
    new __test.SafeError('FIXTURE_CLEANUP_FAILED'),
    { fixture_cleanup_ran: false },
    [candidate],
  );
  assert.equal(attached.details.recovery_actors.length, 1);
  assert.equal(attached.details.recovery_actors[0].user_id, OWNER_ID);
  assert.equal(attached.details.recovery_actors[0].retained, true);
});

test('create response identity mismatch fails closed and deletes only the exact metadata actor', async () => {
  const lifecycle = context();
  let createdEmail = null;
  const assertCreateComplete = installFetchScript([
    () => userListResponse([]),
    (_url, options) => {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, { id: OTHER_ID });
    },
    () => jsonResponse(200, exactUser(createdEmail, {
      id: OTHER_ID,
      app_metadata: { preview_e2e: true, preview_e2e_request_id: 'wrong' },
    })),
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'AUTH_CREATE_RESPONSE_ACTOR_MISMATCH',
  );
  assertCreateComplete();

  const candidate = lifecycle.actorCandidates[0];
  const assertCleanupComplete = installFetchScript([
    () => userListResponse([exactUser(candidate.email)]),
    () => jsonResponse(200, {
      user: { id: OWNER_ID }, access_token: 'cleanup-access-token',
    }),
    () => jsonResponse(204),
    (url, options) => {
      assert.equal(options.method, 'DELETE');
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OWNER_ID}`);
      assert.notEqual(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OTHER_ID}`);
      return jsonResponse(204);
    },
    () => jsonResponse(404),
    () => userListResponse([]),
  ]);
  await __test.cleanupActorCandidate(CONFIG, candidate);
  assertCleanupComplete();
  assert.equal(candidate.absenceConfirmed, true);
});

test('sign-in identity mismatch never trusts the mismatched token and cleans the exact actor', async () => {
  const lifecycle = context();
  let createdEmail = null;
  const assertCreateComplete = installFetchScript([
    () => userListResponse([]),
    (_url, options) => {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, { id: OWNER_ID });
    },
    () => jsonResponse(200, exactUser(createdEmail)),
    () => jsonResponse(200, { user: { id: OTHER_ID }, access_token: 'wrong-token' }),
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'AUTH_SIGN_IN_ACTOR_MISMATCH',
  );
  assertCreateComplete();
  const candidate = lifecycle.actorCandidates[0];
  assert.equal(candidate.accessToken, null);

  const assertCleanupComplete = installFetchScript([
    () => jsonResponse(200, { user: { id: OWNER_ID }, access_token: 'cleanup-token' }),
    () => jsonResponse(204),
    (url, options) => {
      assert.equal(options.method, 'DELETE');
      assert.equal(url, `${CONFIG.supabaseUrl}/auth/v1/admin/users/${OWNER_ID}`);
      return jsonResponse(204);
    },
    () => jsonResponse(404),
    () => userListResponse([]),
  ]);
  await __test.cleanupActorCandidate(CONFIG, candidate);
  assertCleanupComplete();
  assert.equal(candidate.sessionRevoked, true);
  assert.equal(candidate.absenceConfirmed, true);
});

test('post-delete GET 404 is insufficient when the exact selector still matches', async () => {
  const candidate = candidateFixture({
    accessToken: null,
    signInRequested: false,
    signInComplete: false,
  });
  const assertScriptComplete = installFetchScript([
    () => jsonResponse(200, {
      user: { id: OWNER_ID }, access_token: 'cleanup-access-token',
    }),
    () => jsonResponse(204),
    () => jsonResponse(204),
    () => jsonResponse(404),
    () => userListResponse([exactUser(candidate.email)]),
  ]);
  await assert.rejects(
    __test.cleanupActorCandidate(CONFIG, candidate),
    (error) => error.code === 'AUTH_ACTOR_SELECTOR_READBACK_FAILED'
      && error.details.cause_code === 'AUTH_ACTOR_SELECTOR_RESIDUE',
  );
  assertScriptComplete();
  assert.equal(candidate.selectorAbsenceConfirmed, false);
  assert.equal(candidate.absenceConfirmed, false);
});

test('user-list pagination rejects skipped pages and truncated terminal coverage', async () => {
  const candidate = candidateFixture({ userId: null, lookupAttempted: false });
  const fullPage = Array.from({ length: 200 }, (_, index) => exactUser(
    `decoy-${index}@example.invalid`,
    { id: `${String(index).padStart(8, '0')}-0000-4000-8000-000000000000` },
  ));
  let assertScriptComplete = installFetchScript([
    () => userListResponse(fullPage, {
      total: 401,
      link: nextUserListLink(3, candidate.email),
    }),
  ]);
  await assert.rejects(
    __test.findDisposableActor(CONFIG, candidate),
    (error) => error.code === 'AUTH_USER_LIST_PAGE_INVALID',
  );
  assertScriptComplete();

  assertScriptComplete = installFetchScript([
    () => userListResponse([], { total: 1 }),
  ]);
  await assert.rejects(
    __test.findDisposableActor(CONFIG, candidate),
    (error) => error.code === 'AUTH_USER_LIST_PAGINATION_INCOMPLETE',
  );
  assertScriptComplete();
});

test('fixture cleanup failure revokes both sessions, retains both actors, and continues after one failure', async () => {
  const owner = candidateFixture();
  const foreign = candidateFixture({
    role: 'foreign',
    email: 'fixture-foreign@example.invalid',
    userId: OTHER_ID,
    accessToken: 'foreign-access-token',
  });
  const assertScriptComplete = installFetchScript([
    () => jsonResponse(500, { code: 'simulated_owner_revoke_failure' }),
    () => jsonResponse(204),
  ]);
  const failures = await __test.cleanupActorCandidates(
    CONFIG,
    [owner, foreign],
    false,
  );
  assertScriptComplete();
  assert.equal(failures.length, 1);
  assert.equal(failures[0].code, 'AUTH_SESSION_REVOKE_FAILED');
  assert.equal(owner.sessionRevoked, false);
  assert.equal(foreign.sessionRevoked, true);
  assert.equal(owner.deleteAttempted, false);
  assert.equal(foreign.deleteAttempted, false);
  assert.equal(owner.absenceConfirmed, false);
  assert.equal(foreign.absenceConfirmed, false);
});

test('pre-create exact selector residue blocks creation without deleting the preexisting actor', async () => {
  const lifecycle = context();
  const deterministicEmail =
    'flow-identity-owner-0123456789abcdef01234567@example.invalid';
  const assertScriptComplete = installFetchScript([
    () => userListResponse([exactUser(deterministicEmail)]),
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'AUTH_ACTOR_PREEXISTING',
  );
  assertScriptComplete();
  const candidate = lifecycle.actorCandidates[0];
  assert.equal(candidate.email, deterministicEmail);
  assert.equal(candidate.createRequested, false);
  assert.equal(candidate.deleteAttempted, false);
  assert.equal(candidate.preCreateAbsenceConfirmed, false);
});

test('lost create response plus one zero census remains unresolved for outer recovery', async () => {
  const lifecycle = context();
  let assertScriptComplete = installFetchScript([
    () => userListResponse([]),
    () => { throw new Error('simulated lost create response'); },
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'HTTP_TRANSPORT_FAILED',
  );
  assertScriptComplete();
  const candidate = lifecycle.actorCandidates[0];

  assertScriptComplete = installFetchScript([
    () => userListResponse([]),
  ]);
  await assert.rejects(
    __test.cleanupActorCandidate(CONFIG, candidate),
    (error) => error.code === 'AUTH_ACTOR_RECOVERY_FAILED'
      && error.details.cause_code === 'AUTH_ACTOR_RECOVERY_UNRESOLVED',
  );
  assertScriptComplete();
  assert.equal(candidate.selectorAbsenceConfirmed, true);
  assert.equal(candidate.absenceConfirmed, false);
});

test('bounded response reader aborts before retaining an oversized body', async () => {
  const controller = new AbortController();
  await assert.rejects(
    __test.readResponseTextBounded(
      { async text() { return 'oversized'; } },
      4,
      controller,
      'TEST_RESPONSE_TOO_LARGE',
    ),
    (error) => error.code === 'TEST_RESPONSE_TOO_LARGE',
  );
  assert.equal(controller.signal.aborted, true);
});

test('recovery checkpoint client requires an exact ACK and rejects disconnects', async () => {
  const sentFrames = [];
  const safePayload = {
    actors: [],
    recovery_selectors: __test.createRecoverySelectors({
      requestId: REQUEST_ID,
      namespace: NAMESPACE,
    }),
  };
  const ipc = new FakeIpc((message, emitter) => {
    sentFrames.push(message.frame);
    queueMicrotask(() => emitter.emit('message', {
      type: 'protected-flow-identity-recovery-checkpoint-ack',
      schema_version: 'protected-flow-identity-recovery-checkpoint-ack.v1',
      sequence: message.frame.sequence,
      frame_sha256: message.frame.frame_sha256,
    }));
  });
  const client = __test.createRecoveryCheckpointClient({
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    ipc,
    timeoutMs: 1_000,
  });
  const frame = await client.checkpoint('runner_ready', safePayload);
  assert.equal(frame.sequence, 1);
  assert.equal(
    frame.previous_sha256,
    createHash('sha256')
      .update('protected-flow-identity-recovery-checkpoint-genesis.v1')
      .digest('hex'),
  );
  const { frame_sha256: firstFrameSha256, ...firstFramePayload } = frame;
  assert.equal(firstFrameSha256, canonicalSha256(firstFramePayload));
  assert.equal(client.summary().checkpoint_count, 1);
  const secondFrame = await client.checkpoint('owner_actor_registered', safePayload);
  assert.equal(secondFrame.sequence, 2);
  assert.equal(secondFrame.previous_sha256, frame.frame_sha256);
  const { frame_sha256: secondFrameSha256, ...secondFramePayload } = secondFrame;
  assert.equal(secondFrameSha256, canonicalSha256(secondFramePayload));
  assert.deepEqual(sentFrames, [frame, secondFrame]);
  await assert.rejects(
    client.checkpoint('owner_actor_create_requested', {
      ...safePayload,
      jwt: 'forbidden',
      anon_key: 'forbidden',
      connection_string: 'forbidden',
    }),
    (error) => error.code === 'RECOVERY_CHECKPOINT_PAYLOAD_SCHEMA_INVALID',
  );
  await assert.rejects(
    client.checkpoint('unexpected_stage', safePayload),
    (error) => error.code === 'RECOVERY_CHECKPOINT_STAGE_FORBIDDEN',
  );

  const wrongAck = new FakeIpc((message, emitter) => {
    queueMicrotask(() => emitter.emit('message', {
      type: 'protected-flow-identity-recovery-checkpoint-ack',
      schema_version: 'protected-flow-identity-recovery-checkpoint-ack.v1',
      sequence: message.frame.sequence,
      frame_sha256: 'f'.repeat(64),
    }));
  });
  const wrongClient = __test.createRecoveryCheckpointClient({
    requestId: REQUEST_ID, namespace: NAMESPACE, ipc: wrongAck, timeoutMs: 1_000,
  });
  await assert.rejects(
    wrongClient.checkpoint('runner_ready', safePayload),
    (error) => error.code === 'RECOVERY_CHECKPOINT_ACK_INVALID',
  );

  const extraAckField = new FakeIpc((message, emitter) => {
    queueMicrotask(() => emitter.emit('message', {
      type: 'protected-flow-identity-recovery-checkpoint-ack',
      schema_version: 'protected-flow-identity-recovery-checkpoint-ack.v1',
      sequence: message.frame.sequence,
      frame_sha256: message.frame.frame_sha256,
      unexpected: true,
    }));
  });
  const extraFieldClient = __test.createRecoveryCheckpointClient({
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    ipc: extraAckField,
    timeoutMs: 1_000,
  });
  await assert.rejects(
    extraFieldClient.checkpoint('runner_ready', safePayload),
    (error) => error.code === 'RECOVERY_CHECKPOINT_ACK_INVALID',
  );

  const disconnected = new FakeIpc((_message, emitter) => {
    queueMicrotask(() => {
      emitter.connected = false;
      emitter.emit('disconnect');
    });
  });
  const disconnectedClient = __test.createRecoveryCheckpointClient({
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    ipc: disconnected,
    timeoutMs: 1_000,
  });
  await assert.rejects(
    disconnectedClient.checkpoint('runner_ready', safePayload),
    (error) => error.code === 'RECOVERY_CHECKPOINT_IPC_DISCONNECTED',
  );
});

test('actor sign-in cannot begin before the actor-bound checkpoint is acknowledged', async () => {
  const lifecycle = context();
  let releaseActorBound;
  const actorBoundGate = new Promise((resolve) => { releaseActorBound = resolve; });
  lifecycle.recoveryCheckpointClient.checkpoint = async (stage, payload) => {
    lifecycle.checkpointStages.push(stage);
    if (stage === 'owner_actor_registered') {
      assert.deepEqual(payload.actors[0], {
        role: 'owner',
        email: 'flow-identity-owner-0123456789abcdef01234567@example.invalid',
        fixture_sha256: createHash('sha256').update('database-engine#269').digest('hex'),
        request_id: REQUEST_ID,
        namespace: NAMESPACE,
        user_id: null,
        create_requested: false,
        actor_bound_checkpoint_acknowledged: false,
        locator_sha256: createHash('sha256')
          .update('flow-identity-owner-0123456789abcdef01234567@example.invalid')
          .digest('hex'),
      });
    }
    if (stage === 'owner_actor_bound') await actorBoundGate;
    return { stage };
  };
  let fetchCount = 0;
  let createdEmail;
  globalThis.fetch = async (_input, options = {}) => {
    fetchCount += 1;
    if (fetchCount === 1) return userListResponse([]);
    if (fetchCount === 2) {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, { id: OWNER_ID });
    }
    if (fetchCount === 3) return jsonResponse(200, exactUser(createdEmail));
    if (fetchCount === 4) {
      return jsonResponse(200, {
        user: { id: OWNER_ID }, access_token: 'actor-access-token',
      });
    }
    throw new Error('unexpected fetch');
  };
  const actorPromise = __test.createDisposableActor(CONFIG, 'owner', lifecycle);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(fetchCount, 3);
  assert.equal(lifecycle.actorCandidates[0].signInRequested, false);
  releaseActorBound();
  const actor = await actorPromise;
  assert.equal(fetchCount, 4);
  assert.equal(actor.candidate.actorBoundCheckpointAcknowledged, true);
});

test('actor-bound checkpoint rejection fails before sign-in and hard delete', async () => {
  const lifecycle = context();
  lifecycle.recoveryCheckpointClient.checkpoint = async (stage) => {
    lifecycle.checkpointStages.push(stage);
    if (stage === 'owner_actor_bound') {
      throw new __test.SafeError('RECOVERY_CHECKPOINT_ACK_INVALID');
    }
    return { stage };
  };
  let createdEmail;
  const assertScriptComplete = installFetchScript([
    () => userListResponse([]),
    (_url, options) => {
      createdEmail = JSON.parse(options.body).email;
      return jsonResponse(201, { id: OWNER_ID });
    },
    () => jsonResponse(200, exactUser(createdEmail)),
  ]);
  await assert.rejects(
    __test.createDisposableActor(CONFIG, 'owner', lifecycle),
    (error) => error.code === 'RECOVERY_CHECKPOINT_ACK_INVALID',
  );
  assertScriptComplete();
  const candidate = lifecycle.actorCandidates[0];
  assert.equal(candidate.signInRequested, false);
  assert.equal(candidate.deleteAttempted, false);
  assert.equal(candidate.actorBoundCheckpointAcknowledged, false);
});

test('cleanup sign-in wrong actor retains the exact actor without logout or delete', async () => {
  const candidate = candidateFixture({
    accessToken: null,
    signInRequested: false,
    signInComplete: false,
  });
  const assertScriptComplete = installFetchScript([
    () => jsonResponse(200, {
      user: { id: OTHER_ID },
      access_token: 'wrong-actor-access-token',
    }),
  ]);
  await assert.rejects(
    __test.cleanupActorCandidate(CONFIG, candidate),
    (error) => error.code === 'AUTH_CLEANUP_SIGN_IN_FAILED',
  );
  assertScriptComplete();
  assert.equal(candidate.sessionRevokeAttempted, false);
  assert.equal(candidate.deleteAttempted, false);
  assert.equal(candidate.absenceConfirmed, false);
});

test('global logout failure retains the actor and forbids hard delete', async () => {
  const candidate = candidateFixture();
  const assertScriptComplete = installFetchScript([
    (_url, options) => {
      assert.equal(options.method, 'POST');
      return jsonResponse(500, { code: 'logout_failed' });
    },
  ]);
  await assert.rejects(
    __test.cleanupActorCandidate(CONFIG, candidate),
    (error) => error.code === 'AUTH_SESSION_REVOKE_FAILED',
  );
  assertScriptComplete();
  assert.equal(candidate.sessionRevoked, false);
  assert.equal(candidate.deleteAttempted, false);
  assert.equal(candidate.absenceConfirmed, false);
});

test('one actor logout failure does not prevent exact cleanup of the other actor', async () => {
  const owner = candidateFixture();
  const foreign = candidateFixture({
    role: 'foreign',
    email: 'fixture-foreign@example.invalid',
    userId: OTHER_ID,
    accessToken: 'foreign-access-token',
  });
  const assertScriptComplete = installFetchScript([
    () => jsonResponse(500, { code: 'owner_logout_failed' }),
    () => jsonResponse(204),
    () => jsonResponse(204),
    () => jsonResponse(404),
    () => userListResponse([]),
  ]);
  const failures = await __test.cleanupActorCandidates(CONFIG, [owner, foreign]);
  assertScriptComplete();
  assert.equal(failures.length, 1);
  assert.equal(owner.deleteAttempted, false);
  assert.equal(foreign.sessionRevoked, true);
  assert.equal(foreign.absenceConfirmed, true);
});

test('private temp directory must be exact, empty, owner-only, and non-symlink', async () => {
  const directory = await mkdtemp(path.join(
    await realpath(os.tmpdir()),
    'fi269-private-',
  ));
  const link = `${directory}-link`;
  try {
    assert.equal(await __test.validatePrivateTempDir(directory), directory);
    await writeFile(path.join(directory, 'residue'), 'x');
    await assert.rejects(
      __test.validatePrivateTempDir(directory),
      (error) => error.code === 'PRIVATE_TEMP_DIR_NOT_EMPTY',
    );
    await rm(path.join(directory, 'residue'));
    await chmod(directory, 0o755);
    await assert.rejects(
      __test.validatePrivateTempDir(directory),
      (error) => error.code === 'PRIVATE_TEMP_DIR_MODE_INVALID',
    );
    await chmod(directory, 0o700);
    await symlink(directory, link);
    await assert.rejects(
      __test.validatePrivateTempDir(link),
      (error) => [
        'PRIVATE_TEMP_DIR_TYPE_INVALID',
        'PRIVATE_TEMP_DIR_NOT_CANONICAL',
      ].includes(error.code),
    );
  } finally {
    await rm(link, { force: true });
    await rm(directory, { recursive: true, force: true });
  }
});
