#!/usr/bin/env node

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { __test } from './protected_flow_identity_rest_e2e.mjs';

const REQUEST_ID = '11111111-1111-4111-8111-111111111111';
const OWNER_ID = '22222222-2222-4222-8222-222222222222';
const FOREIGN_ID = '33333333-3333-4333-8333-333333333333';
const RECEIPT_ID = '44444444-4444-4444-8444-444444444444';
const SCOPE_ID = '55555555-5555-4555-8555-555555555555';
const DERIVATIVE_REQUEST_IDS = [
  '66666666-6666-4666-8666-666666666666',
  '77777777-7777-4777-8777-777777777777',
];
const DERIVATIVE_BATCH_IDS = [
  '88888888-8888-4888-8888-888888888888',
  '99999999-9999-4999-8999-999999999999',
];
const NAMESPACE = 'fie2e-hosted-0123456789abcdef01234567';

function fixtureContext() {
  return {
    requestId: REQUEST_ID,
    namespace: NAMESPACE,
    config: { projectRef: 'aaaaaaaaaaaaaaa' },
    owner: { userId: OWNER_ID },
    foreign: { userId: FOREIGN_ID },
  };
}

function fixtureSelectors(context) {
  return {
    ...__test.createRecoverySelectors(context),
    actor_user_ids: [OWNER_ID, FOREIGN_ID],
    receipt_ids: [RECEIPT_ID],
    scope_ids: [SCOPE_ID],
    derivative_request_ids: DERIVATIVE_REQUEST_IDS,
    derivative_batch_ids: DERIVATIVE_BATCH_IDS,
    http_request_ids: ['42', '43'],
    fixture_backend_pids: ['1234', '5678'],
  };
}

test('dedicated residue renderer binds exact selectors into read-only SQL', async () => {
  const context = fixtureContext();
  const rendered = await __test.renderResidueReadback(
    context,
    fixtureSelectors(context),
  );
  assert.match(rendered.templateSha256, /^[0-9a-f]{64}$/);
  assert.match(rendered.renderedSha256, /^[0-9a-f]{64}$/);
  assert.match(rendered.sql, /begin read only;/i);
  assert.match(rendered.sql, /rollback;/i);
  assert.doesNotMatch(rendered.sql, /\{\{[A-Z0-9_]+\}\}/);
  assert.match(rendered.sql, new RegExp(REQUEST_ID, 'g'));
  assert.match(rendered.sql, /\["1234","5678"\]/);
  assert.match(rendered.sql, /# residue_counts=/);

  const withoutLineComments = rendered.sql.replace(/--[^\n]*/g, '');
  assert.doesNotMatch(
    withoutLineComments,
    /\b(?:insert|update|delete|merge|truncate|create|alter|drop|grant|revoke|call)\b/i,
  );
  assert.doesNotMatch(withoutLineComments, /service[_ -]?role[_ -]?key/i);
  assert.doesNotMatch(
    withoutLineComments,
    /project_secret_key[^\n]*decrypted_secret/i,
  );
});

test('residue count parser accepts exactly 39 named zero counts', () => {
  const counts = Object.fromEntries(
    __test.RESIDUE_COUNT_NAMES.map((name) => [name, 0]),
  );
  assert.deepEqual(
    __test.parseResidueCounts(`# residue_counts=${JSON.stringify(counts)}\n`),
    counts,
  );
});

test('residue count parser rejects a nonzero or incomplete count set', () => {
  const counts = Object.fromEntries(
    __test.RESIDUE_COUNT_NAMES.map((name) => [name, 0]),
  );
  counts.auth_users = 1;
  assert.throws(
    () => __test.parseResidueCounts(`# residue_counts=${JSON.stringify(counts)}\n`),
    (error) => error.code === 'RESIDUE_COUNT_NONZERO',
  );
  delete counts.auth_users;
  assert.throws(
    () => __test.parseResidueCounts(`# residue_counts=${JSON.stringify(counts)}\n`),
    (error) => error.code === 'RESIDUE_COUNT_NAME_SET_INVALID',
  );
});

test('residue count parser rejects duplicate, truncated, or ANSI-decorated markers', () => {
  const counts = Object.fromEntries(
    __test.RESIDUE_COUNT_NAMES.map((name) => [name, 0]),
  );
  const marker = `# residue_counts=${JSON.stringify(counts)}`;
  for (const output of [
    `${marker}\n${marker}\n`,
    `${marker.slice(0, -1)}\n`,
    `\u001b[32m${marker}\u001b[0m\n`,
  ]) {
    assert.throws(
      () => __test.parseResidueCounts(output),
      (error) => error.code === 'RESIDUE_COUNT_OUTPUT_INVALID'
        || error.code === 'RESIDUE_COUNT_JSON_INVALID',
    );
  }
});

test('database SQL binding asserts the exact application name inside the connection', () => {
  const bound = __test.bindDbApplicationName(
    'begin read only;\nrollback;',
    'fi269-fie2e-hosted-0123456789abcdef01234567',
  );
  assert.match(bound, /^\\set ON_ERROR_STOP on\nset application_name = 'fi269-fie2e-hosted-/);
  assert.match(bound, /current_setting\('application_name'\)/);
  assert.match(bound, /DB_APPLICATION_NAME_BINDING_FAILED/);
  assert.match(bound, /begin read only;\nrollback;$/);
  assert.throws(
    () => __test.bindDbApplicationName('select 1;', 'foreign-session'),
    (error) => error.code === 'DB_APPLICATION_NAME_INVALID',
  );
});

test('residue renderer rejects over-broad dynamic selector sets', async () => {
  const context = fixtureContext();
  const selectors = fixtureSelectors(context);
  selectors.scope_ids = [
    SCOPE_ID,
    'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
  ];
  await assert.rejects(
    __test.renderResidueReadback(context, selectors),
    (error) => error.code === 'RESIDUE_SCOPE_IDS_JSON_SQL_INVALID',
  );
});
