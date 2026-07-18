#!/usr/bin/env node

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
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

function context() {
  return {
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    actorCandidates: [],
    authCensusBaselineTotal: null,
  };
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
    signInRequested: true,
    signInComplete: true,
    cleanupSignInAttempted: false,
    cleanupSignInComplete: false,
    sessionRevokeAttempted: false,
    sessionRevoked: false,
    deleteAttempted: false,
    deleteAcknowledged: false,
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
