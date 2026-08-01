import assert from 'node:assert/strict';
import test from 'node:test';

const baseUrl = process.env.SUPABASE_URL?.replace(/\/$/, '');
const publishableKey = process.env.SUPABASE_PUBLISHABLE_KEY;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const expectedProjectRef = process.env.EXPECTED_PROJECT_REF;

assert.ok(baseUrl, 'SUPABASE_URL is required');
assert.ok(publishableKey, 'SUPABASE_PUBLISHABLE_KEY is required');

if (expectedProjectRef) {
  assert.equal(new URL(baseUrl).hostname.split('.')[0], expectedProjectRef);
}

async function request({ key, path, method = 'GET', profile, body }) {
  const headers = {
    apikey: key,
  };
  // Legacy anon/service_role keys are JWTs and may be used as bearer tokens.
  // New hosted publishable/secret keys are gateway API keys, not JWTs; sending
  // them as Authorization Bearer values makes the gateway reject the request.
  if (key.startsWith('eyJ')) headers.Authorization = `Bearer ${key}`;
  if (profile) {
    headers[method === 'GET' || method === 'HEAD' ? 'Accept-Profile' : 'Content-Profile'] = profile;
  }
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    payload = text;
  }
  return { status: response.status, payload };
}

test('api profile is present in the PostgREST schema cache', async () => {
  const result = await request({
    key: publishableKey,
    path: '/rest/v1/processes_v1?select=id&limit=1',
    profile: 'api',
  });
  assert.equal(result.status, 200);
  assert.ok(Array.isArray(result.payload));
});

for (const [schema, relation] of [
  ['private', 'worker_jobs'],
  ['util', 'pending_embedding_jobs'],
  ['archive', 'worker_legacy_job_table_rows'],
]) {
  test(`${schema} is not exposed by the Data API`, async () => {
    const result = await request({
      key: publishableKey,
      path: `/rest/v1/${relation}?select=*&limit=1`,
      profile: schema,
    });
    assert.equal(result.status, 406);
    assert.equal(result.payload?.code, 'PGRST106');
  });
}

test('anonymous callers cannot execute a service-only api RPC', async () => {
  const result = await request({
    key: publishableKey,
    path: '/rest/v1/rpc/worker_read_jobs_by_ids_v1',
    method: 'POST',
    profile: 'api',
    body: { p_job_ids: [], p_include_internal: false },
  });
  assert.equal(result.status, 401);
  assert.equal(result.payload?.code, '42501');
});

test('service role can execute the bounded api adapter', { skip: !serviceRoleKey }, async () => {
  const result = await request({
    key: serviceRoleKey,
    path: '/rest/v1/rpc/worker_read_jobs_by_ids_v1',
    method: 'POST',
    profile: 'api',
    body: { p_job_ids: [], p_include_internal: false },
  });
  assert.equal(result.status, 200);
  assert.deepEqual(result.payload, { ok: true, data: [] });
});
