begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(30);

select has_function(
  'api',
  'svc_ai_tidas_suggestion_enqueue',
  array['uuid', 'text', 'jsonb'],
  'service AI suggestion enqueue facade exists'
);

select has_function(
  'api',
  'svc_ai_tidas_suggestion_read',
  array['uuid', 'uuid'],
  'requester-scoped AI suggestion read facade exists'
);

select has_function(
  'private',
  'worker_claim_jobs',
  array['text', 'text', 'integer', 'integer'],
  'shared worker claim function remains available'
);

select ok(
  pg_catalog.has_function_privilege(
    'service_role',
    'api.svc_ai_tidas_suggestion_enqueue(uuid,text,jsonb)',
    'EXECUTE'
  ),
  'service_role can enqueue AI jobs'
);

select ok(
  not pg_catalog.has_function_privilege(
    'authenticated',
    'api.svc_ai_tidas_suggestion_enqueue(uuid,text,jsonb)',
    'EXECUTE'
  ),
  'authenticated callers cannot enqueue AI jobs directly'
);

select ok(
  pg_catalog.has_function_privilege(
    'service_role',
    'api.svc_ai_tidas_suggestion_read(uuid,uuid)',
    'EXECUTE'
  ),
  'service_role can read a requester-scoped AI job'
);

select ok(
  not pg_catalog.has_function_privilege(
    'authenticated',
    'api.svc_ai_tidas_suggestion_read(uuid,uuid)',
    'EXECUTE'
  ),
  'authenticated callers cannot read AI jobs directly'
);

select is(
  (
    select concat_ws(
      '|',
      worker_runtime,
      worker_queue,
      payload_schema_version,
      result_schema_version,
      default_visibility
    )
    from private.worker_job_kinds
    where job_kind = 'ai.tidas_suggestion'
  ),
  'calculator|ai|ai.tidas_suggestion.request.v1|ai.tidas_suggestion.result.v1|user',
  'the first AI handler is registered on the reusable ai queue with exact schemas'
);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    'a1000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'ai-owner@example.com',
    'test',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"ai-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'a1000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'ai-other@example.com',
    'test',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"ai-other@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

create temporary table ai_worker_test_results (
  label text primary key,
  result jsonb not null
);
grant all on ai_worker_test_results to public;

set local role service_role;
select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);

insert into ai_worker_test_results (label, result)
values (
  'enqueue',
  api.svc_ai_tidas_suggestion_enqueue(
    'a1000000-0000-0000-0000-000000000001',
    'process',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":"test"}}}}}'::jsonb
  )
);

select is(
  (select result->>'ok' from ai_worker_test_results where label = 'enqueue'),
  'true',
  'a valid Process request enqueues successfully'
);

select is(
  (select result #>> '{data,status}' from ai_worker_test_results where label = 'enqueue'),
  'queued',
  'a new AI job starts queued'
);

select is(
  (select result #>> '{data,jobKind}' from ai_worker_test_results where label = 'enqueue'),
  'ai.tidas_suggestion',
  'the facade fixes the handler kind'
);

select is(
  (select result #>> '{data,payloadSchemaVersion}' from ai_worker_test_results where label = 'enqueue'),
  'ai.tidas_suggestion.request.v1',
  'the facade fixes the request schema version'
);

select is(
  (select result #>> '{data,payload,dataType}' from ai_worker_test_results where label = 'enqueue'),
  'process',
  'the internal payload preserves normalized dataType'
);

select ok(
  (select result #> '{data,payload,data,processDataSet}' from ai_worker_test_results where label = 'enqueue') is not null,
  'the internal payload preserves the matching TIDAS root'
);

select is(
  pg_catalog.length(
    (select result #>> '{data,requestHash}' from ai_worker_test_results where label = 'enqueue')
  ),
  64,
  'the database binds the canonical request payload to a SHA-256 hash'
);

insert into ai_worker_test_results (label, result)
values (
  'reused',
  api.svc_ai_tidas_suggestion_enqueue(
    'a1000000-0000-0000-0000-000000000001',
    'process',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":"test"}}}}}'::jsonb
  )
);

select is(
  (select result->>'reused' from ai_worker_test_results where label = 'reused'),
  'true',
  'an identical active request reuses the existing job'
);

select is(
  (select result #>> '{data,id}' from ai_worker_test_results where label = 'reused'),
  (select result #>> '{data,id}' from ai_worker_test_results where label = 'enqueue'),
  'active-request reuse returns the same worker job id'
);

select is(
  api.svc_ai_tidas_suggestion_enqueue(
    'a1000000-0000-0000-0000-000000000001',
    'source',
    '{"sourceDataSet":{}}'::jsonb
  )->>'code',
  'AI_DATA_TYPE_INVALID',
  'unsupported dataset types fail closed'
);

select is(
  api.svc_ai_tidas_suggestion_enqueue(
    'a1000000-0000-0000-0000-000000000001',
    'flow',
    '{"processDataSet":{}}'::jsonb
  )->>'code',
  'AI_DATA_INVALID',
  'a mismatched dataset root fails closed'
);

insert into ai_worker_test_results (label, result)
select
  'read_queued',
  api.svc_ai_tidas_suggestion_read(
    'a1000000-0000-0000-0000-000000000001',
    (result #>> '{data,id}')::uuid
  )
from ai_worker_test_results
where label = 'enqueue';

select is(
  (select result->>'ok' from ai_worker_test_results where label = 'read_queued'),
  'true',
  'the owning requester can read the AI job through the service facade'
);

select is(
  api.svc_ai_tidas_suggestion_read(
    'a1000000-0000-0000-0000-000000000002',
    (
      select (result #>> '{data,id}')::uuid
      from ai_worker_test_results
      where label = 'enqueue'
    )
  )->>'code',
  'AI_JOB_NOT_FOUND',
  'a foreign requester receives the same not-found projection'
);

select ok(
  not (
    select (result->'data') ? 'payload'
    from ai_worker_test_results
    where label = 'read_queued'
  ),
  'the requester projection hides the submitted payload'
);

insert into ai_worker_test_results (label, result)
values (
  'claim',
  private.worker_claim_jobs('ai', 'ai-worker-test', 1, 900)
);

select is(
  (select result->>'ok' from ai_worker_test_results where label = 'claim'),
  'true',
  'the shared claim function accepts the dedicated ai queue'
);

select is(
  (select result #>> '{data,0,id}' from ai_worker_test_results where label = 'claim'),
  (select result #>> '{data,id}' from ai_worker_test_results where label = 'enqueue'),
  'the AI worker claims the queued suggestion job'
);

select ok(
  nullif(
    (select result #>> '{data,0,leaseToken}' from ai_worker_test_results where label = 'claim'),
    ''
  ) is not null,
  'the claimed AI job receives a lease token'
);

insert into ai_worker_test_results (label, result)
select
  'record',
  private.worker_record_job_result(
    (result #>> '{data,0,id}')::uuid,
    (result #>> '{data,0,leaseToken}')::uuid,
    'completed',
    jsonb_build_object(
      'schemaVersion', 'ai.tidas_suggestion.result.v1',
      'status', 'complete',
      'dataType', 'process',
      'data', jsonb_build_object('processDataSet', jsonb_build_object())
    ),
    'ai.tidas_suggestion.result.v1',
    null,
    '{"runner":"ai-worker"}'::jsonb,
    null,
    null,
    null,
    array[]::text[],
    null,
    null
  )
from ai_worker_test_results
where label = 'claim';

select is(
  (select result->>'ok' from ai_worker_test_results where label = 'record'),
  'true',
  'a lease-fenced AI result records successfully'
);

insert into ai_worker_test_results (label, result)
select
  'read_completed',
  api.svc_ai_tidas_suggestion_read(
    'a1000000-0000-0000-0000-000000000001',
    (result #>> '{data,id}')::uuid
  )
from ai_worker_test_results
where label = 'enqueue';

select is(
  (select result #>> '{data,status}' from ai_worker_test_results where label = 'read_completed'),
  'completed',
  'the requester projection exposes the terminal worker status'
);

select is(
  (select result #>> '{data,resultSchemaVersion}' from ai_worker_test_results where label = 'read_completed'),
  'ai.tidas_suggestion.result.v1',
  'the requester projection exposes the exact result schema'
);

select is(
  (select result #>> '{data,result,status}' from ai_worker_test_results where label = 'read_completed'),
  'complete',
  'the requester projection exposes the advisory result status'
);

select throws_ok(
  format(
    'update private.worker_jobs set status = %L where id = %L::uuid',
    'blocked',
    (
      select result #>> '{data,id}'
      from ai_worker_test_results
      where label = 'enqueue'
    )
  ),
  '23514',
  null,
  'AI suggestion jobs cannot enter the blocked workflow state'
);

select * from finish();
rollback;
