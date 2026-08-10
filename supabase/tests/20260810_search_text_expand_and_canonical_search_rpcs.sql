begin;

create extension if not exists pgtap with schema extensions;

select extensions.plan(17);

select extensions.is(
  (select count(*) from information_schema.columns
   where table_schema = 'public'
     and table_name in ('contacts', 'flowproperties', 'flows', 'lifecyclemodels', 'processes', 'sources', 'unitgroups')
     and column_name = 'search_text'
     and is_nullable = 'YES'
     and column_default is null),
  7::bigint,
  'all seven search_text columns are nullable and have no default'
);

select extensions.ok(
  (
    select pg_get_triggerdef(trigger.oid, true) like '%search_text%'
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.flows'::regclass
      and trigger.tgname = 'dataset_flow_identity_flow_active_fence'
      and not trigger.tgisinternal
  ),
  'Flow identity fence treats search_text as guard-neutral'
);

select extensions.is(
  (select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'api'
     and p.proname in ('search_contacts', 'search_flowproperties', 'search_flows', 'search_lifecyclemodels', 'search_processes', 'search_sources', 'search_unitgroups')),
  7::bigint,
  'seven unique canonical lexical Search RPCs exist'
);

select extensions.is(
  (select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'api'
     and p.proname in ('hybrid_search_contacts', 'hybrid_search_flowproperties', 'hybrid_search_flows', 'hybrid_search_lifecyclemodels', 'hybrid_search_processes', 'hybrid_search_sources', 'hybrid_search_unitgroups')),
  7::bigint,
  'seven unique canonical Hybrid Search RPCs exist'
);

select extensions.ok(
  not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    join unnest(coalesce(p.proargnames, '{}'::text[])) argument_name on true
    where n.nspname = 'api'
      and p.proname in ('search_flows', 'search_lifecyclemodels', 'search_processes')
      and argument_name = 'order_by'
  ),
  'canonical lexical core RPCs do not expose unused order_by'
);

select extensions.ok(
  pg_get_functiondef('private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure)
    like '%extracted_md%',
  'Expand keeps Process lexical execution on extracted_md'
);

select extensions.ok(
  has_function_privilege('service_role', 'api.svc_dataset_search_text_backfill_enqueue(text,uuid,text,integer)'::regprocedure, 'EXECUTE')
  and not has_function_privilege('authenticated', 'api.svc_dataset_search_text_backfill_enqueue(text,uuid,text,integer)'::regprocedure, 'EXECUTE')
  and not has_function_privilege('anon', 'api.svc_dataset_search_text_backfill_enqueue(text,uuid,text,integer)'::regprocedure, 'EXECUTE'),
  'search_text backfill enqueue is service-role-only'
);

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('not-a-kind', null, null, 1) ->> 'code'),
  'SERVICE_ROLE_REQUIRED',
  'enqueue rejects a non-service call before processing input'
);

select set_config('request.jwt.claim.role', 'service_role', true);

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('not-a-kind', null, null, 1) ->> 'code'),
  'INVALID_ENTITY_KIND',
  'enqueue has a fixed entity allowlist'
);

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('flow', null, '01.00.000', 1) ->> 'code'),
  'INVALID_CURSOR',
  'enqueue requires an id with a version cursor'
);

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('flow', null, null, 5000) -> 'data' ->> 'limit')::integer,
  500,
  'enqueue caps batches at 500'
);

delete from pgmq.q_dataset_extraction_jobs;
alter table public.flows disable trigger user;
insert into public.flows (id, version, user_id, state_code, json, json_ordered)
values (
  'fa459000-0000-4000-8000-000000000001'::uuid,
  '01.00.000',
  'fa459000-0000-4000-8000-000000000002'::uuid,
  100,
  '{}'::jsonb,
  '{}'::json
);
alter table public.flows enable trigger user;

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('flow', null, null, 500) -> 'data' ->> 'enqueued')::integer,
  1,
  'missing Flow search_text is enqueued once as a bounded replay job'
);

select extensions.ok(
  exists (
    select 1
    from pgmq.q_dataset_extraction_jobs as queued
    where queued.message @> jsonb_build_object(
      'schema', 'public',
      'table', 'flows',
      'id', 'fa459000-0000-4000-8000-000000000001',
      'version', '01.00.000',
      'entity_kind', 'flow',
      'extraction_kind', 'search_text'
    )
  ),
  'replay payload is compact, allowlisted, and names search_text explicitly'
);

select extensions.is(
  (api.svc_dataset_search_text_backfill_enqueue('flow', null, null, 500) -> 'data' ->> 'already_queued')::integer,
  1,
  'replay enqueue deduplicates an existing matching queue job'
);

select extensions.is(
  (select search_text from public.flows where id = 'fa459000-0000-4000-8000-000000000001'::uuid),
  null,
  'enqueue does not write the projection synchronously'
);

select extensions.ok(
  pg_get_functiondef('api.search_processes(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure)
    like '%private.search_processes_latest_v2_impl%',
  'canonical Process facade calls the existing private implementation'
);

select extensions.ok(
  pg_get_functiondef('api.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure)
    like '%private.search_processes_latest_v2_impl%',
  'legacy Process facade calls the same private implementation'
);

select * from extensions.finish();

rollback;
