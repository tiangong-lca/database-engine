-- Database A / workspace#565 Release 1 expand.
--
-- Keep lexical reads on extracted_md in this additive migration.  search_text
-- is a future Edge-owned projection; its indexes and source cutover are
-- deliberately separate Database B work.

set lock_timeout = '5s';
set statement_timeout = '120s';

alter table public.contacts add column if not exists search_text text;
alter table public.flowproperties add column if not exists search_text text;
alter table public.flows add column if not exists search_text text;
alter table public.lifecyclemodels add column if not exists search_text text;
alter table public.processes add column if not exists search_text text;
alter table public.sources add column if not exists search_text text;
alter table public.unitgroups add column if not exists search_text text;

comment on column public.contacts.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.flowproperties.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.flows.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.lifecyclemodels.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.processes.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.sources.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';
comment on column public.unitgroups.search_text is
  'Edge-owned multilingual lexical projection. Nullable during the Release 1 backfill; not a lexical search source until Database B.';

-- search_text is an asynchronous derivative just like extracted_md.  It must
-- not contend with an active Flow identity rewrite or touch authored metadata.
drop trigger if exists dataset_flow_identity_flow_active_fence on public.flows;
create trigger dataset_flow_identity_flow_active_fence
before update on public.flows
for each row
when (
  (to_jsonb(new) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
  is distinct from
  (to_jsonb(old) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
)
execute function private.dataset_flow_identity_active_fence_v2();

comment on trigger dataset_flow_identity_flow_active_fence on public.flows is
  'Fail-closed Step 3 actor fence. Only extracted_md, embedding_ft, embedding_ft_at, and asynchronous search_text projection updates bypass the owner-wide fence.';

create or replace function api.svc_dataset_search_text_backfill_enqueue(
  p_entity_kind text,
  p_after_id uuid default null,
  p_after_version text default null,
  p_limit integer default 100
) returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_entity_kind text := lower(btrim(coalesce(p_entity_kind, '')));
  v_table name;
  v_queue_entity_kind text;
  v_limit integer := least(greatest(coalesce(p_limit, 100), 1), 500);
  v_cursor_version character(9);
  v_result jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  select mapping.table_name, mapping.queue_entity_kind
  into v_table, v_queue_entity_kind
  from (values
    ('contact', 'contacts'::name, 'contact'),
    ('flowproperty', 'flowproperties'::name, 'flowproperty'),
    ('flow', 'flows'::name, 'flow'),
    ('lifecyclemodel', 'lifecyclemodels'::name, 'lifecyclemodel'),
    ('process', 'processes'::name, 'process'),
    ('source', 'sources'::name, 'source'),
    ('unitgroup', 'unitgroups'::name, 'unitgroup')
  ) as mapping(entity_kind, table_name, queue_entity_kind)
  where mapping.entity_kind = v_entity_kind;

  if v_table is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ENTITY_KIND',
      'status', 400,
      'message', 'entity_kind must be one of contact, flowproperty, flow, lifecyclemodel, process, source, or unitgroup'
    );
  end if;

  if p_after_id is null and p_after_version is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_CURSOR',
      'status', 400,
      'message', 'after_version requires after_id'
    );
  end if;

  v_cursor_version := nullif(btrim(coalesce(p_after_version, '')), '')::character(9);

  execute format($sql$
    with candidates as materialized (
      select row.id, row.version
      from public.%1$I as row
      where row.search_text is null
        and (
          $1::uuid is null
          or (row.id, row.version) > ($1::uuid, coalesce($2::character(9), ''::character(9)))
        )
      order by row.id, row.version
      limit $3
    ), enqueueable as materialized (
      select candidate.id, candidate.version
      from candidates as candidate
      where not exists (
        select 1
        from pgmq.q_dataset_extraction_jobs as queued
        where queued.message ->> 'schema' = 'public'
          and queued.message ->> 'table' = %1$L
          and queued.message ->> 'id' = candidate.id::text
          and queued.message ->> 'version' = candidate.version::text
          and queued.message ->> 'extraction_kind' = 'search_text'
      )
    ), sent as (
      select pgmq.send(
        'dataset_extraction_jobs',
        jsonb_build_object(
          'schema', 'public',
          'table', %1$L,
          'id', enqueueable.id,
          'version', enqueueable.version,
          'entity_kind', %2$L,
          'extraction_kind', 'search_text',
          'created_at', clock_timestamp()
        )
      ) as msg_id
      from enqueueable
    ), summary as (
      select
        (select count(*) from candidates) as scanned,
        (select count(*) from sent) as enqueued,
        (select count(*) from candidates) - (select count(*) from sent) as already_queued,
        (select id from candidates order by id desc, version desc limit 1) as next_after_id,
        (select version from candidates order by id desc, version desc limit 1) as next_after_version
    )
    select jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'entity_kind', %2$L,
        'table', %1$L,
        'limit', $3,
        'scanned', summary.scanned,
        'enqueued', summary.enqueued,
        'already_queued', summary.already_queued,
        'next_after_id', summary.next_after_id,
        'next_after_version', summary.next_after_version
      )
    )
    from summary
  $sql$, v_table, v_queue_entity_kind)
  into v_result
  using p_after_id, v_cursor_version, v_limit;

  return v_result;
end
$function$;

alter function api.svc_dataset_search_text_backfill_enqueue(text, uuid, text, integer) owner to postgres;
revoke all on function api.svc_dataset_search_text_backfill_enqueue(text, uuid, text, integer)
  from public, anon, authenticated;
comment on function api.svc_dataset_search_text_backfill_enqueue(text, uuid, text, integer) is
  'Service-role-only, bounded, cursor-based and queue-deduplicated search_text replay enqueue. It never writes projections or embeds data.';

-- Make the no-suffix Release 1 names exact facades over the already reviewed
-- v2/latest implementations. The only removed lexical argument is order_by,
-- which no implementation consumed. Keeping the function bodies derived from
-- the live compatibility entrypoints preserves their ACL-safe private calls,
-- visibility, latest-revision, filters, ranking, and pagination behavior.
-- The API cutover deliberately removed postgres' membership in the constrained
-- executor role. Restore it only for this owner assignment, then revoke it
-- again below so the executor remains non-login and tightly scoped.
grant api_internal_executor to postgres;
grant create on schema api to api_internal_executor;
set role api_internal_executor;
do $canonical_facades$
declare
  facade record;
  definition text;
  target regprocedure;
  source_name name;
begin
  for facade in
    select *
    from (values
      ('api.search_contacts_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure, 'search_contacts', false, false, 'text,jsonb,integer,integer,text,text,uuid,integer'),
      ('api.search_flowproperties_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure, 'search_flowproperties', false, false, 'text,jsonb,integer,integer,text,text,uuid,integer'),
      ('api.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure, 'search_flows', true, false, 'text,jsonb,integer,integer,text,text,uuid,integer,text[]'),
      ('api.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure, 'search_lifecyclemodels', true, false, 'text,jsonb,integer,integer,text,text,uuid,integer,text[]'),
      ('api.search_processes_latest_v2(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure, 'search_processes', true, false, 'text,jsonb,integer,integer,text,text,uuid,integer,text,text[],boolean'),
      ('api.search_sources_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure, 'search_sources', false, false, 'text,jsonb,integer,integer,text,text,uuid,integer'),
      ('api.search_unitgroups_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure, 'search_unitgroups', false, false, 'text,jsonb,integer,integer,text,text,uuid,integer'),
      ('api.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure, 'hybrid_search_contacts', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid'),
      ('api.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure, 'hybrid_search_flowproperties', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid'),
      ('api.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure, 'hybrid_search_flows', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]'),
      ('api.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure, 'hybrid_search_lifecyclemodels', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]'),
      ('api.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure, 'hybrid_search_processes', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]'),
      ('api.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure, 'hybrid_search_sources', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid'),
      ('api.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure, 'hybrid_search_unitgroups', false, true, 'text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid')
    ) as facade(source, target_name, remove_order_by, json_filter, target_args)
  loop
    definition := pg_get_functiondef(facade.source);
    select proc.proname
    into source_name
    from pg_proc as proc
    where proc.oid = facade.source;
    definition := replace(
      definition,
      format('CREATE OR REPLACE FUNCTION api.%I(', source_name),
      format('CREATE OR REPLACE FUNCTION api.%I(', facade.target_name)
    );
    if facade.remove_order_by then
      definition := replace(definition, 'order_by jsonb DEFAULT ''{}''::jsonb, ', '');
    end if;
    if facade.json_filter then
      definition := replace(
        definition,
        'filter_condition text DEFAULT ''''::text',
        'filter_condition jsonb DEFAULT ''{}''::jsonb'
      );
      definition := regexp_replace(
        definition,
        E'(\\n\\s*)filter_condition,',
        E'\\1filter_condition::text,',
        'g'
      );
    else
      definition := replace(definition, 'page_size bigint DEFAULT 10', 'page_size integer DEFAULT 10');
      definition := replace(definition, 'page_current bigint DEFAULT 1', 'page_current integer DEFAULT 1');
      definition := regexp_replace(
        definition,
        E'(\\n\\s*)page_size,(\\n\\s*)page_current,',
        E'\\1page_size::bigint,\\2page_current::bigint,',
        'g'
      );
    end if;
    execute definition;

    target := to_regprocedure(format('api.%I(%s)', facade.target_name, facade.target_args));
    if target is null then
      raise exception 'canonical search facade was not created: %', facade.target_name;
    end if;
    execute format('revoke all on function %s from public', target);
    execute format(
      'comment on function %s is %L',
      target,
      format('Canonical Release 1 Search RPC. Compatibility entrypoint %s delegates to the same private implementation; lexical reads remain on extracted_md until Database B.', facade.source::text)
    );
  end loop;
end
$canonical_facades$;

-- The oldest Process alias predates owner_draft_only. Make it a thin legacy
-- wrapper over the same v2 private implementation used by the canonical API.
create or replace function api.search_processes_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer,
  type_of_data_set_filter text default 'all'::text,
  query_terms text[] default null::text[]
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  model_id uuid,
  total_count bigint
)
language sql
security definer
set search_path to 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $function$
  select *
  from private.search_processes_latest_v2_impl(
    query_text, filter_condition, page_size, page_current, data_source,
    this_user_id, team_id_filter, state_code_filter, type_of_data_set_filter,
    query_terms, false
  )
$function$;

alter function api.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[])
  security definer;
revoke all on function api.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[])
  from public;
comment on function api.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) is
  'Compatibility Process Search RPC. Delegates to the canonical Process private implementation with owner_draft_only=false.';

reset role;
revoke create on schema api from api_internal_executor;
revoke api_internal_executor from postgres;

-- The closed API contract admits every external execute grant by exact
-- regprocedure identity. Canonical search and its compatibility window retain
-- the established NX-CORE-02 anon/authenticated contract; there is no caller
-- evidence for a service-role expansion. The bounded queue control is the
-- sole service-only Database A capability.
insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
values
  ('api.search_contacts(text, jsonb, integer, integer, text, text, uuid, integer)', 'NX-CORE-02', true, true, false),
  ('api.search_flowproperties(text, jsonb, integer, integer, text, text, uuid, integer)', 'NX-CORE-02', true, true, false),
  ('api.search_flows(text, jsonb, integer, integer, text, text, uuid, integer, text[])', 'NX-CORE-02', true, true, false),
  ('api.search_lifecyclemodels(text, jsonb, integer, integer, text, text, uuid, integer, text[])', 'NX-CORE-02', true, true, false),
  ('api.search_processes(text, jsonb, integer, integer, text, text, uuid, integer, text, text[], boolean)', 'NX-CORE-02', true, true, false),
  ('api.search_sources(text, jsonb, integer, integer, text, text, uuid, integer)', 'NX-CORE-02', true, true, false),
  ('api.search_unitgroups(text, jsonb, integer, integer, text, text, uuid, integer)', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_contacts(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_flowproperties(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_flows(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_lifecyclemodels(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_processes(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_sources(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)', 'NX-CORE-02', true, true, false),
  ('api.hybrid_search_unitgroups(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)', 'NX-CORE-02', true, true, false),
  ('api.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[])', 'NX-CORE-02', true, true, false),
  ('api.svc_dataset_search_text_backfill_enqueue(text, uuid, text, integer)', 'DBA-SEARCH-01', false, false, true)
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

do $apply_database_a_postgres_capabilities$
declare
  grant_row record;
begin
  for grant_row in
    select *
    from private.api_capability_grants
    where routine_identity = 'api.svc_dataset_search_text_backfill_enqueue(text, uuid, text, integer)'
  loop
    if grant_row.allow_service_role then
      execute format('grant execute on function %s to service_role', grant_row.routine_identity);
    end if;
  end loop;
end
$apply_database_a_postgres_capabilities$;

grant api_internal_executor to postgres;
grant usage on schema private to api_internal_executor;
grant select on table private.api_capability_grants to api_internal_executor;
set role api_internal_executor;
do $apply_database_a_executor_capabilities$
declare
  grant_row record;
begin
  for grant_row in
    select manifest.*
    from private.api_capability_grants as manifest
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(manifest.routine_identity)
    where routine.proowner = current_user::regrole
      and manifest.routine_identity = any (array[
        'api.search_contacts(text, jsonb, integer, integer, text, text, uuid, integer)',
        'api.search_flowproperties(text, jsonb, integer, integer, text, text, uuid, integer)',
        'api.search_flows(text, jsonb, integer, integer, text, text, uuid, integer, text[])',
        'api.search_lifecyclemodels(text, jsonb, integer, integer, text, text, uuid, integer, text[])',
        'api.search_processes(text, jsonb, integer, integer, text, text, uuid, integer, text, text[], boolean)',
        'api.search_sources(text, jsonb, integer, integer, text, text, uuid, integer)',
        'api.search_unitgroups(text, jsonb, integer, integer, text, text, uuid, integer)',
        'api.hybrid_search_contacts(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)',
        'api.hybrid_search_flowproperties(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)',
        'api.hybrid_search_flows(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])',
        'api.hybrid_search_lifecyclemodels(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])',
        'api.hybrid_search_processes(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])',
        'api.hybrid_search_sources(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)',
        'api.hybrid_search_unitgroups(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)',
        'api.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[])'
      ])
  loop
    if grant_row.allow_anon then
      execute format('grant execute on function %s to anon', grant_row.routine_identity);
    end if;
    if grant_row.allow_authenticated then
      execute format('grant execute on function %s to authenticated', grant_row.routine_identity);
    end if;
    if grant_row.allow_service_role then
      execute format('grant execute on function %s to service_role', grant_row.routine_identity);
    end if;
  end loop;
end
$apply_database_a_executor_capabilities$;
reset role;
revoke select on table private.api_capability_grants from api_internal_executor;
revoke usage on schema private from api_internal_executor;
revoke api_internal_executor from postgres;
