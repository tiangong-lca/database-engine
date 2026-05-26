begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(24);

do $$
begin
  perform pgmq.create('dataset_extraction_jobs');
exception when others then
  if to_regclass('pgmq.q_dataset_extraction_jobs') is null then
    raise;
  end if;
end
$$;

delete from pgmq.q_dataset_extraction_jobs;
delete from util.dataset_extraction_job_failures;
delete from vault.secrets where name = 'project_secret_key';

select vault.create_secret(
  'test-dataset-extraction-service-key',
  'project_secret_key',
  'pgTAP dataset extraction service auth'
);

select set_config('request.jwt.claim.role', 'authenticated', true);

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
)
values (
  '00000000-0000-0000-0000-000000000000',
  '96000000-0000-0000-0000-000000000001',
  'authenticated',
  'authenticated',
  'dataset-extraction-owner@example.com',
  'test-password-hash',
  now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"sub":"96000000-0000-0000-0000-000000000001","email":"dataset-extraction-owner@example.com"}'::jsonb,
  now(),
  now(),
  false,
  false
);

insert into public.users (id, raw_user_meta_data, contact)
values (
  '96000000-0000-0000-0000-000000000001',
  '{"email":"dataset-extraction-owner@example.com"}'::jsonb,
  null
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '96000000-0000-0000-0000-000000000001', true);

create temporary table dataset_create_results (
  result jsonb not null
) on commit drop;

insert into dataset_create_results (result)
select public.cmd_dataset_create(
  'flows',
  '97000000-0000-0000-0000-000000000001',
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "common:UUID": "97000000-0000-0000-0000-000000000001",
          "name": {
            "baseName": [
              {
                "@xml:lang": "en",
                "#text": "Dataset extraction test flow"
              }
            ]
          }
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::jsonb,
  null,
  true,
  '{"command":"dataset_create"}'::jsonb
);

select is(
  (select result->>'ok' from dataset_create_results),
  'true',
  'flow cmd_dataset_create succeeds with compact extraction trigger'
);

select is(
  (select result->'data'->>'version' from dataset_create_results),
  '01.00.000',
  'minimal create result preserves version'
);

select is(
  (select result->'data'->>'rule_verification' from dataset_create_results),
  'true',
  'minimal create result preserves rule_verification'
);

select ok(
  (select result->'data' ? 'id' from dataset_create_results)
    and (select result->'data' ? 'state_code' from dataset_create_results)
    and (select result->'data' ? 'user_id' from dataset_create_results)
    and (select result->'data' ? 'team_id' from dataset_create_results)
    and (select result->'data' ? 'model_id' from dataset_create_results),
  'minimal create result exposes the stable create contract keys'
);

select ok(
  not (select result->'data' ? 'json' from dataset_create_results)
    and not (select result->'data' ? 'json_ordered' from dataset_create_results)
    and not (select result->'data' ? 'embedding_ft' from dataset_create_results)
    and not (select result->'data' ? 'extracted_md' from dataset_create_results)
    and not (select result->'data' ? 'extracted_text' from dataset_create_results),
  'minimal create result does not include heavy json or derived embedding fields'
);

reset role;

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs
  ),
  2,
  'flow create enqueues two dataset extraction jobs'
);

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs
    where message ? 'json'
       or message ? 'json_ordered'
       or message ? 'embedding'
       or message ? 'embedding_ft'
  ),
  0,
  'dataset extraction jobs do not carry json/json_ordered or embedding payloads'
);

select is(
  (
    select string_agg(message->>'extraction_kind', ',' order by message->>'extraction_kind')
    from pgmq.q_dataset_extraction_jobs
  ),
  'extracted_md,extracted_text',
  'flow create enqueues extracted_md and extracted_text jobs'
);

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs
    where message @> jsonb_build_object(
      'schema', 'public',
      'table', 'flows',
      'id', '97000000-0000-0000-0000-000000000001',
      'version', '01.00.000',
      'entity_kind', 'flow'
    )
  ),
  2,
  'dataset extraction jobs use compact flow identity payloads'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname in ('flow_extract_md_trigger_insert', 'flow_extract_text_trigger_insert')
      and not tgisinternal
  ),
  0,
  'old flow INSERT webhook triggers are removed'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname in ('flow_extract_md_trigger_update', 'flow_extract_text_trigger_update')
      and not tgisinternal
  ),
  2,
  'flow UPDATE extraction triggers remain unchanged in v1'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid = 'public.processes'::regclass
      and tgname in ('process_extract_md_trigger_insert', 'process_extract_text_trigger_insert')
      and not tgisinternal
  ),
  2,
  'process INSERT webhook triggers remain for the tracked follow-up'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);

create temporary table claimed_dataset_extraction_jobs (
  result jsonb not null
) on commit drop;

insert into claimed_dataset_extraction_jobs (result)
select public.cmd_dataset_extraction_claim(10, 300, 5);

select is(
  (select result->>'ok' from claimed_dataset_extraction_jobs),
  'true',
  'service role can claim dataset extraction jobs'
);

select is(
  (select jsonb_array_length(result->'data') from claimed_dataset_extraction_jobs),
  2,
  'claim returns both queued flow extraction jobs'
);

select ok(
  (
    select bool_and(
      not ((job->'message') ? 'json')
      and not ((job->'message') ? 'json_ordered')
    )
    from claimed_dataset_extraction_jobs,
      lateral jsonb_array_elements(result->'data') as job
  ),
  'claimed jobs stay compact'
);

select is(
  (
    select public.cmd_dataset_extraction_ack(
      array_agg((job->>'msg_id')::bigint order by (job->>'msg_id')::bigint)
    )->>'ok'
    from claimed_dataset_extraction_jobs,
      lateral jsonb_array_elements(result->'data') as job
  ),
  'true',
  'service role can ack claimed dataset extraction jobs'
);

reset role;

select is(
  (select count(*)::integer from pgmq.q_dataset_extraction_jobs),
  0,
  'acked dataset extraction jobs are removed from the live queue'
);

set local role service_role;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.headers', '{"apikey":"wrong-service-key"}', true);

select is(
  public.cmd_dataset_extraction_claim(1, 300, 5)->>'code',
  'SERVICE_ROLE_REQUIRED',
  'dataset extraction RPCs reject non-service request context'
);

select set_config('request.jwt.claim.role', '', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
select set_config('request.headers', '{}', true);

select is(
  public.cmd_dataset_extraction_ack(array[]::bigint[])->>'ok',
  'true',
  'dataset extraction RPCs accept service_role in request.jwt.claims'
);

select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.headers', '{"apikey":"test-dataset-extraction-service-key"}', true);

select is(
  public.cmd_dataset_extraction_claim(1, 300, 5)->>'ok',
  'true',
  'dataset extraction RPCs accept project_secret_key apikey headers'
);

select set_config('request.headers', '{"authorization":"Bearer test-dataset-extraction-service-key"}', true);

select is(
  public.cmd_dataset_extraction_ack(array[]::bigint[])->>'ok',
  'true',
  'dataset extraction RPCs accept project_secret_key authorization headers'
);

select is(
  public.cmd_dataset_extraction_record_failure(
    999001,
    1,
    'service auth smoke',
    '{"schema":"public","table":"flows"}'::jsonb,
    'test failure',
    false
  )->>'ok',
  'true',
  'dataset extraction failure RPC accepts project_secret_key headers'
);

reset role;
delete from util.dataset_extraction_job_failures where msg_id = 999001;

select pgmq.send(
  queue_name => 'dataset_extraction_jobs',
  msg => jsonb_build_object(
    'schema', 'public',
    'table', 'flows',
    'id', '97000000-0000-0000-0000-000000000002',
    'version', '01.00.000',
    'entity_kind', 'flow',
    'extraction_kind', 'extracted_md',
    'created_at', now()
  )
);

update pgmq.q_dataset_extraction_jobs
set
  read_ct = 5,
  vt = clock_timestamp() - interval '1 second';

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);

do $$
begin
  perform public.cmd_dataset_extraction_claim(10, 300, 5);
end
$$;

reset role;

select is(
  (select count(*)::integer from util.dataset_extraction_job_failures),
  1,
  'retry-capped dataset extraction jobs are recorded as failures'
);

select is(
  (select count(*)::integer from pgmq.q_dataset_extraction_jobs),
  0,
  'retry-capped dataset extraction jobs are removed from the live queue'
);

select * from finish();

rollback;
