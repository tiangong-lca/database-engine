begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select no_plan();

select has_schema('api', 'api schema exists');
select ok(has_schema_privilege('anon', 'api', 'USAGE'), 'anon can resolve explicitly granted api objects');
select ok(has_schema_privilege('authenticated', 'api', 'USAGE'), 'authenticated can resolve api objects');
select ok(has_schema_privilege('service_role', 'api', 'USAGE'), 'service role can resolve api objects');
select ok(not has_schema_privilege('public', 'api', 'USAGE'), 'PUBLIC has no api schema usage');

select has_view('api', 'processes_v1', 'versioned Process DTO exists');
select has_view('api', 'review_comments_v1', 'versioned review comment DTO exists');
select ok((
  select coalesce(c.reloptions, '{}') @> array['security_invoker=true']
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'api' and c.relname = 'processes_v1'
), 'Process DTO is security_invoker');
select ok((
  select coalesce(c.reloptions, '{}') @> array['security_invoker=true']
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'api' and c.relname = 'review_comments_v1'
), 'review comment DTO is security_invoker');

select ok(has_table_privilege('anon', 'api.processes_v1', 'SELECT'), 'anon may enter the Process DTO and remains constrained by base RLS');
select ok(has_table_privilege('authenticated', 'api.processes_v1', 'SELECT'), 'authenticated may select the Process DTO');
select ok(has_table_privilege('service_role', 'api.processes_v1', 'SELECT'), 'service role may select the Process DTO');
select ok(not has_table_privilege('anon', 'api.review_comments_v1', 'SELECT'), 'anon cannot select review comments');
select ok(has_table_privilege('authenticated', 'api.review_comments_v1', 'SELECT'), 'authenticated may select review comments through base RLS');

select has_function('api', 'cmd_review_save_comment_draft_v1', array['uuid', 'jsonb', 'jsonb'], 'authenticated write adapter exists');
select has_function('api', 'worker_list_jobs_by_concurrency_key_v1', array['text', 'text', 'text[]', 'integer', 'boolean'], 'service list adapter exists');
select has_function('api', 'worker_read_jobs_by_ids_v1', array['uuid[]', 'boolean'], 'service batch adapter exists');
select ok(has_function_privilege('authenticated', 'api.cmd_review_save_comment_draft_v1(uuid,jsonb,jsonb)', 'EXECUTE'), 'authenticated can execute review write adapter');
select ok(not has_function_privilege('anon', 'api.cmd_review_save_comment_draft_v1(uuid,jsonb,jsonb)', 'EXECUTE'), 'anon cannot execute review write adapter');
select ok(has_function_privilege('service_role', 'api.worker_list_jobs_by_concurrency_key_v1(text,text,text[],integer,boolean)', 'EXECUTE'), 'service role can execute Worker list adapter');
select ok(not has_function_privilege('authenticated', 'api.worker_list_jobs_by_concurrency_key_v1(text,text,text[],integer,boolean)', 'EXECUTE'), 'authenticated cannot execute service Worker list adapter');
select ok(not has_function_privilege('anon', 'api.worker_read_jobs_by_ids_v1(uuid[],boolean)', 'EXECUTE'), 'anon cannot execute service Worker batch adapter');

select ok((
  select not p.prosecdef and p.proconfig @> array['search_path=""']
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'api' and p.proname = 'cmd_review_save_comment_draft_v1'
), 'authenticated adapter is invoker-rights with an empty search_path');
select ok((
  select bool_and(not p.prosecdef and p.proconfig @> array['search_path=""'])
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'api' and p.proname like 'worker_%_v1'
), 'service adapters are invoker-rights with an empty search_path');

select ok((
  select exists (
    select 1
    from pg_rewrite rw
    join pg_depend d on d.objid = rw.oid
    where rw.ev_class = 'api.processes_v1'::regclass
      and d.refobjid = 'public.processes'::regclass
  )
), 'Process DTO has an explicit dependency on the core public table');
select ok((
  select exists (
    select 1
    from pg_rewrite rw
    join pg_depend d on d.objid = rw.oid
    where rw.ev_class = 'api.review_comments_v1'::regclass
      and d.refobjid = 'public.comments'::regclass
  )
), 'review DTO depends on the compatibility relation during Expand');
select ok((
  select exists (
    select 1 from pg_constraint
    where conrelid = 'public.comments'::regclass
      and confrelid = 'public.reviews'::regclass
      and contype = 'f'
  )
), 'the underlying comment-to-review FK remains intact');
select ok((
  select exists (
    select 1 from pg_trigger
    where tgrelid = 'public.comments'::regclass
      and tgname = 'comments_v2_kind_guard'
      and not tgisinternal
  )
), 'the underlying comment guard trigger remains intact');

select ok((
  select not exists (
    select 1 from pg_publication_tables
    where schemaname in ('api', 'private')
  )
), 'DTO and private schemas are not accidental Realtime publication sources');
select ok((
  select pg_get_functiondef('public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)'::regprocedure)
    like '%null::public.worker_jobs%'
), 'Expand preserves the public worker_jobs composite dependency until Contract');

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_save_comment_draft_v1(
    '81000000-0000-0000-0000-000000000099',
    '[]'::jsonb,
    '{}'::jsonb
  ),
  public.cmd_review_save_comment_draft(
    '81000000-0000-0000-0000-000000000099',
    '[]'::jsonb,
    '{}'::jsonb
  ),
  'new and compatibility authenticated commands preserve the invalid-payload envelope'
);

insert into public.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json,
  review_kind,
  target_table,
  submitted_revision_checksum,
  target_owner_id,
  scope_schema_version,
  scope_history
) values (
  '81000000-0000-0000-0000-000000000010',
  '81000000-0000-0000-0000-000000000020',
  '01.00.000',
  1,
  '["81000000-0000-0000-0000-000000000001"]'::jsonb,
  '{"user":{"id":"81000000-0000-0000-0000-000000000001"},"data":{"id":"81000000-0000-0000-0000-000000000020","version":"01.00.000"}}'::jsonb,
  'root',
  'processes',
  repeat('a', 64),
  '81000000-0000-0000-0000-000000000001',
  'review_scope.v1',
  '{"schema_version":"review_scope.v1","snapshots":[]}'::jsonb
);

set local role authenticated;
create temporary table api_private_poc_write_result as
select api.cmd_review_save_comment_draft_v1(
  '81000000-0000-0000-0000-000000000010',
  '{"schemaVersion":"review-comment.v1","comment":"api boundary POC"}'::jsonb,
  '{"source":"api-private-poc"}'::jsonb
) as result;
reset role;

select ok((select (result->>'ok')::boolean from api_private_poc_write_result), 'authenticated api command performs a real write');
select is((
  select json::jsonb->>'comment' from public.comments
  where review_id = '81000000-0000-0000-0000-000000000010'
    and reviewer_id = '81000000-0000-0000-0000-000000000001'
), 'api boundary POC', 'write reaches the compatibility table through the existing trigger/FK contract');
select is((
  select count(*)::integer from public.command_audit_log
  where command = 'cmd_review_save_comment_draft'
    and target_id = '81000000-0000-0000-0000-000000000010'
), 1, 'authenticated api command preserves audit behavior');

select set_config('request.jwt.claim.role', 'service_role', true);
set local role service_role;
select is(
  api.worker_read_jobs_by_ids_v1('{}'::uuid[], false),
  jsonb_build_object('ok', true, 'data', '[]'::jsonb),
  'service-role API batch adapter executes through the private Worker contract'
);
reset role;

select * from finish();
rollback;
