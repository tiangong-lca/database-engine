begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(26);

select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'processes_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_md_trigger_update');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_update');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'zz_processes_extracted_text_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flows_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_md_trigger_update');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_text_trigger_update');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_dataset_extraction_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'zz_flows_extracted_text_sync_trigger');

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
  '28100000-0000-0000-0000-000000000001',
  'authenticated',
  'authenticated',
  'issue-281-review-admin@example.com',
  'test-password-hash',
  now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"sub":"28100000-0000-0000-0000-000000000001","email":"issue-281-review-admin@example.com","display_name":"Issue 281 Review Admin"}'::jsonb,
  now(),
  now(),
  false,
  false
);

insert into public.users (id, raw_user_meta_data)
values (
  '28100000-0000-0000-0000-000000000001',
  '{"email":"issue-281-review-admin@example.com","display_name":"Issue 281 Review Admin"}'::jsonb
);

insert into public.roles (user_id, team_id, role)
values (
  '28100000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'review-admin'
);

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  reviews
)
values
  (
    '28120000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"flowDataSet":{"name":"pending shared reference"}}'::jsonb,
    '{"flowDataSet":{"name":"pending shared reference"}}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000001"},{"key":1,"id":"28130000-0000-0000-0000-000000000002"}]'::jsonb
  ),
  (
    '28120000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"flowDataSet":{"name":"assigned shared reference"}}'::jsonb,
    '{"flowDataSet":{"name":"assigned shared reference"}}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000001"},{"key":1,"id":"28130000-0000-0000-0000-000000000003"}]'::jsonb
  ),
  (
    '28120000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"flowDataSet":{"name":"completed history reference"}}'::jsonb,
    '{"flowDataSet":{"name":"completed history reference"}}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000001"},{"key":1,"id":"28130000-0000-0000-0000-000000000005"}]'::jsonb
  ),
  (
    '28120000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"flowDataSet":{"name":"malformed review reference"}}'::jsonb,
    '{"flowDataSet":{"name":"malformed review reference"}}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000004"},{"key":1,"id":"not-a-uuid"}]'::jsonb
  ),
  (
    '28120000-0000-0000-0000-000000000005',
    '01.00.000',
    '{"flowDataSet":{"name":"missing review reference"}}'::jsonb,
    '{"flowDataSet":{"name":"missing review reference"}}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000004"},{"key":1,"id":"28130000-0000-0000-0000-000000000099"}]'::jsonb
  );

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  model_id,
  rule_verification,
  reviews
)
values
  (
    '28110000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000001","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000002","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000003","@version":"01.00.000"}]}'::jsonb,
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000001","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000002","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000003","@version":"01.00.000"}]}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    '28140000-0000-0000-0000-000000000001',
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000001"}]'::jsonb
  ),
  (
    '28110000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000001","@version":"01.00.000"}]}'::jsonb,
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000001","@version":"01.00.000"}]}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    '28140000-0000-0000-0000-000000000002',
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000002"}]'::jsonb
  ),
  (
    '28110000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000002","@version":"01.00.000"}]}'::jsonb,
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000002","@version":"01.00.000"}]}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    '28140000-0000-0000-0000-000000000003',
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000003"}]'::jsonb
  ),
  (
    '28110000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000004","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000005","@version":"01.00.000"}]}'::jsonb,
    '{"references":[{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000004","@version":"01.00.000"},{"@type":"flow data set","@refObjectId":"28120000-0000-0000-0000-000000000005","@version":"01.00.000"}]}'::json,
    '28100000-0000-0000-0000-000000000001',
    20,
    '28140000-0000-0000-0000-000000000004',
    true,
    '[{"key":0,"id":"28130000-0000-0000-0000-000000000004"}]'::jsonb
  );

insert into public.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json
)
values
  (
    '28130000-0000-0000-0000-000000000001',
    '28110000-0000-0000-0000-000000000001',
    '01.00.000',
    1,
    '[]'::jsonb,
    '{"comment":{"message":""},"logs":[]}'::jsonb
  ),
  (
    '28130000-0000-0000-0000-000000000002',
    '28110000-0000-0000-0000-000000000002',
    '01.00.000',
    0,
    '[]'::jsonb,
    '{"comment":{"message":""},"logs":[]}'::jsonb
  ),
  (
    '28130000-0000-0000-0000-000000000003',
    '28110000-0000-0000-0000-000000000003',
    '01.00.000',
    1,
    '[]'::jsonb,
    '{"comment":{"message":""},"logs":[]}'::jsonb
  ),
  (
    '28130000-0000-0000-0000-000000000004',
    '28110000-0000-0000-0000-000000000004',
    '01.00.000',
    1,
    '[]'::jsonb,
    '{"comment":{"message":""},"logs":[]}'::jsonb
  ),
  (
    '28130000-0000-0000-0000-000000000005',
    '28110000-0000-0000-0000-000000000001',
    '01.00.000',
    2,
    '[]'::jsonb,
    '{"comment":{"message":""},"logs":[]}'::jsonb
  );

create temporary table reject_results (
  review_id uuid primary key,
  result jsonb not null
) on commit drop;

grant select, insert on reject_results to authenticated;

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', '28100000-0000-0000-0000-000000000001', true);

set local role authenticated;
insert into reject_results (review_id, result)
values (
  '28130000-0000-0000-0000-000000000001',
  public.cmd_review_reject(
    'processes',
    '28130000-0000-0000-0000-000000000001',
    'reject first shared review',
    '{"issue":281,"scenario":"shared-first"}'::jsonb
  )
);
reset role;

select is((select result->>'ok' from reject_results where review_id = '28130000-0000-0000-0000-000000000001'), 'true', 'first shared review rejection succeeds');
select is((select state_code from public.processes where id = '28110000-0000-0000-0000-000000000001' and version = '01.00.000'), 0, 'the rejected review root always returns to draft');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000001' and version = '01.00.000'), 20, 'a non-root reference stays under review while another pending review is active');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000002' and version = '01.00.000'), 20, 'a non-root reference stays under review while another assigned review is active');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000003' and version = '01.00.000'), 0, 'a completed historical review does not keep a non-root reference under review');
select is((select state_code from public.reviews where id = '28130000-0000-0000-0000-000000000001'), -1, 'the rejected review row is marked rejected');

select ok(
  (select result->'data'->'retained_datasets' from reject_results where review_id = '28130000-0000-0000-0000-000000000001') @>
    '[{"table":"flows","id":"28120000-0000-0000-0000-000000000001","reason":"OTHER_ACTIVE_REVIEWS","active_review_ids":["28130000-0000-0000-0000-000000000002"]}]'::jsonb,
  'the response identifies the pending review that retains the shared reference'
);

select ok(
  (select result->'data'->'retained_datasets' from reject_results where review_id = '28130000-0000-0000-0000-000000000001') @>
    '[{"table":"flows","id":"28120000-0000-0000-0000-000000000002","reason":"OTHER_ACTIVE_REVIEWS","active_review_ids":["28130000-0000-0000-0000-000000000003"]}]'::jsonb,
  'the response identifies the assigned review that retains the shared reference'
);

select ok(
  (select result->'data'->'affected_datasets' from reject_results where review_id = '28130000-0000-0000-0000-000000000001') @>
    '[{"table":"flows","id":"28120000-0000-0000-0000-000000000003","state_code":0}]'::jsonb,
  'the response reports references actually restored to draft separately'
);

set local role authenticated;
insert into reject_results (review_id, result)
values (
  '28130000-0000-0000-0000-000000000002',
  public.cmd_review_reject(
    'processes',
    '28130000-0000-0000-0000-000000000002',
    'reject last pending occupant',
    '{"issue":281,"scenario":"shared-last-pending"}'::jsonb
  )
);
reset role;

select is((select result->>'ok' from reject_results where review_id = '28130000-0000-0000-0000-000000000002'), 'true', 'rejecting the last pending occupant succeeds');
select is((select state_code from public.processes where id = '28110000-0000-0000-0000-000000000002' and version = '01.00.000'), 0, 'the pending occupant root returns to draft');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000001' and version = '01.00.000'), 0, 'the shared reference returns to draft after its final active review is rejected');

set local role authenticated;
insert into reject_results (review_id, result)
values (
  '28130000-0000-0000-0000-000000000003',
  public.cmd_review_reject(
    'processes',
    '28130000-0000-0000-0000-000000000003',
    'reject last assigned occupant',
    '{"issue":281,"scenario":"shared-last-assigned"}'::jsonb
  )
);
reset role;

select is((select result->>'ok' from reject_results where review_id = '28130000-0000-0000-0000-000000000003'), 'true', 'rejecting the last assigned occupant succeeds');
select is((select state_code from public.processes where id = '28110000-0000-0000-0000-000000000003' and version = '01.00.000'), 0, 'the assigned occupant root returns to draft');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000002' and version = '01.00.000'), 0, 'the assigned shared reference returns to draft after its final active review is rejected');

select is(
  (select reviews::text from public.flows where id = '28120000-0000-0000-0000-000000000001' and version = '01.00.000'),
  '[{"id": "28130000-0000-0000-0000-000000000001", "key": 0}, {"id": "28130000-0000-0000-0000-000000000002", "key": 1}]',
  'sequential rejections preserve the pending shared reference review history'
);

select is(
  (select reviews::text from public.flows where id = '28120000-0000-0000-0000-000000000002' and version = '01.00.000'),
  '[{"id": "28130000-0000-0000-0000-000000000001", "key": 0}, {"id": "28130000-0000-0000-0000-000000000003", "key": 1}]',
  'sequential rejections preserve the assigned shared reference review history'
);

set local role authenticated;
insert into reject_results (review_id, result)
values (
  '28130000-0000-0000-0000-000000000004',
  public.cmd_review_reject(
    'processes',
    '28130000-0000-0000-0000-000000000004',
    'reject review with unverifiable references',
    '{"issue":281,"scenario":"fail-closed"}'::jsonb
  )
);
reset role;

select is((select result->>'ok' from reject_results where review_id = '28130000-0000-0000-0000-000000000004'), 'true', 'review rejection succeeds when non-root review links are unverifiable');
select is((select state_code from public.processes where id = '28110000-0000-0000-0000-000000000004' and version = '01.00.000'), 0, 'the root still returns to draft when referenced review links are unverifiable');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000004' and version = '01.00.000'), 20, 'a malformed review link fails closed and retains the non-root state');
select is((select state_code from public.flows where id = '28120000-0000-0000-0000-000000000005' and version = '01.00.000'), 20, 'a missing linked review fails closed and retains the non-root state');

select is(
  (select jsonb_array_length(result->'data'->'unverifiable_datasets') from reject_results where review_id = '28130000-0000-0000-0000-000000000004'),
  2,
  'the response lists every unverifiable non-root dataset'
);

select ok(
  (select result->'data'->'retained_datasets' from reject_results where review_id = '28130000-0000-0000-0000-000000000004') @>
    '[{"id":"28120000-0000-0000-0000-000000000004","reason":"UNVERIFIABLE_REVIEW_LINKS","unverifiable_review_refs":[{"reason":"INVALID_REVIEW_REF"}]}]'::jsonb,
  'the response distinguishes a malformed review reference'
);

select ok(
  (select result->'data'->'retained_datasets' from reject_results where review_id = '28130000-0000-0000-0000-000000000004') @>
    '[{"id":"28120000-0000-0000-0000-000000000005","reason":"UNVERIFIABLE_REVIEW_LINKS","unverifiable_review_refs":[{"reason":"REVIEW_NOT_FOUND"}]}]'::jsonb,
  'the response distinguishes a missing linked review'
);

select is(
  (
    select jsonb_array_length(payload->'unverifiable_datasets')
    from public.command_audit_log
    where command = 'cmd_review_reject'
      and target_id = '28130000-0000-0000-0000-000000000004'
    order by created_at desc
    limit 1
  ),
  2,
  'the command audit records every fail-closed dataset'
);

select is(
  (
    select payload->'active_review_state_codes'
    from public.command_audit_log
    where command = 'cmd_review_reject'
      and target_id = '28130000-0000-0000-0000-000000000004'
    order by created_at desc
    limit 1
  )::text,
  '[0, 1]',
  'the command audit records the exact active review state definition'
);

select * from finish();
rollback;
