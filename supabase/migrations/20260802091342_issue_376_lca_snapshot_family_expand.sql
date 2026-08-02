begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

do $preflight$
declare
  public_count integer;
  private_count integer;
  physical_schema text;
  table_record record;
begin
  if not exists (
    select 1
    from supabase_migrations.schema_migrations
    where version = '20260802090000'
  ) then
    raise exception 'issue 376 requires predecessor migration 20260802090000';
  end if;

  select count(*) into public_count
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'r'
    and c.relname = any (array[
      'lca_active_snapshots',
      'lca_network_snapshots',
      'lca_snapshot_artifacts'
    ]);

  select count(*) into private_count
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'private'
    and c.relkind = 'r'
    and c.relname = any (array[
      'lca_active_snapshots',
      'lca_network_snapshots',
      'lca_snapshot_artifacts'
    ]);

  if (public_count, private_count) not in ((3, 0), (0, 3)) then
    raise exception 'issue 376 mixed physical state: public=%, private=%', public_count, private_count;
  end if;

  physical_schema := case when public_count = 3 then 'public' else 'private' end;

  for table_record in
    select c.oid, c.relname, c.relacl, c.relrowsecurity, c.relforcerowsecurity,
           pg_get_userbyid(c.relowner) as owner_name
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = physical_schema
      and c.relname = any (array[
        'lca_active_snapshots',
        'lca_network_snapshots',
        'lca_snapshot_artifacts'
      ])
  loop
    if table_record.owner_name <> 'postgres'
       or not table_record.relrowsecurity
       or table_record.relforcerowsecurity then
      raise exception 'issue 376 source posture drift for %.%',
        physical_schema, table_record.relname;
    end if;

    if coalesce(table_record.relacl::text, '') <> '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}' then
      raise exception 'issue 376 source ACL drift for %.%: %',
        physical_schema, table_record.relname, table_record.relacl;
    end if;
  end loop;

  if (
    select count(*)
    from pg_policies
    where schemaname = physical_schema
      and tablename = any (array[
        'lca_active_snapshots',
        'lca_network_snapshots',
        'lca_snapshot_artifacts'
      ])
      and cmd = 'ALL'
      and roles = array['service_role']::name[]
      and qual = 'true'
      and with_check = 'true'
  ) <> 3 then
    raise exception 'issue 376 source policy drift in %', physical_schema;
  end if;

  if (
    select count(*)
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and format('%I(%s)', p.proname, pg_get_function_identity_arguments(p.oid)) = any (array[
        'qry_review_get_admin_root_queue_items_v2(p_status text, p_page integer, p_page_size integer, p_sort_by text, p_sort_order text)',
        'qry_review_get_member_root_queue_items_v2(p_status text, p_page integer, p_page_size integer, p_sort_by text, p_sort_order text)',
        'qry_root_review_reference_progress_v2(p_root_review_id uuid)'
      ])
      and p.prosecdef
      and pg_get_userbyid(p.proowner) = 'postgres'
      and coalesce(p.proacl::text, '') = '{postgres=X/postgres,authenticated=X/postgres,service_role=X/postgres}'
      and p.proconfig in (
        array['search_path=""'],
        array['search_path=pg_catalog, pg_temp']
      )
  ) <> 3 then
    raise exception 'issue 376 inherited issue 323 SECURITY DEFINER posture drift';
  end if;
end
$preflight$;

alter function public.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) set search_path = pg_catalog, pg_temp;
alter function public.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) set search_path = pg_catalog, pg_temp;
alter function public.qry_root_review_reference_progress_v2(uuid)
  set search_path = pg_catalog, pg_temp;

do $move$
begin
  if to_regclass('public.lca_active_snapshots') is not null
     and (select relkind from pg_class where oid = 'public.lca_active_snapshots'::regclass) = 'r' then
    alter table public.lca_active_snapshots set schema private;
    alter table public.lca_network_snapshots set schema private;
    alter table public.lca_snapshot_artifacts set schema private;
  end if;
end
$move$;

create or replace view public.lca_active_snapshots
with (security_invoker = true) as
select scope, snapshot_id, source_hash, activated_at, activated_by, note
from private.lca_active_snapshots;

create or replace view public.lca_network_snapshots
with (security_invoker = true) as
select id, scope, process_filter, lcia_method_id, lcia_method_version,
       provider_matching_rule, source_hash, status, created_by, created_at, updated_at
from private.lca_network_snapshots;

create or replace view public.lca_snapshot_artifacts
with (security_invoker = true) as
select id, snapshot_id, artifact_url, artifact_sha256, artifact_byte_size,
       artifact_format, process_count, flow_count, impact_count, a_nnz, b_nnz,
       c_nnz, coverage, status, created_at, updated_at, snapshot_index_sha256,
       snapshot_build_contract_hash, effective_scope_hash, data_snapshot_token,
       closure_bundle_hash
from private.lca_snapshot_artifacts;

alter view public.lca_active_snapshots owner to postgres;
alter view public.lca_network_snapshots owner to postgres;
alter view public.lca_snapshot_artifacts owner to postgres;

revoke all on public.lca_active_snapshots, public.lca_network_snapshots,
  public.lca_snapshot_artifacts from public, anon, authenticated, service_role,
  api_internal_executor;
grant all on public.lca_active_snapshots, public.lca_network_snapshots,
  public.lca_snapshot_artifacts to service_role;
grant select on public.lca_active_snapshots, public.lca_network_snapshots,
  public.lca_snapshot_artifacts to api_internal_executor;

comment on view public.lca_active_snapshots is
  'Issue #376 Expand compatibility; canonical=private.lca_active_snapshots; fallback=none; remove only after static/runtime/owner consumer-zero, burn-in, and Contract approval.';
comment on view public.lca_network_snapshots is
  'Issue #376 Expand compatibility; canonical=private.lca_network_snapshots; fallback=none; remove only after static/runtime/owner consumer-zero, burn-in, and Contract approval.';
comment on view public.lca_snapshot_artifacts is
  'Issue #376 Expand compatibility; canonical=private.lca_snapshot_artifacts; fallback=none; remove only after static/runtime/owner consumer-zero, burn-in, and Contract approval.';

create or replace function api.lca_snapshot_active_read_v1(p_scope text)
returns table(snapshot_id uuid, source_hash text, activated_at timestamptz)
language sql
stable
security invoker
set search_path = ''
as $function$
  select a.snapshot_id, a.source_hash, a.activated_at
  from private.lca_active_snapshots a
  where a.scope = p_scope
  limit 1
$function$;

create or replace function api.lca_snapshot_scope_read_v1(p_snapshot_id uuid)
returns table(id uuid, scope text, process_filter jsonb, status text)
language sql
stable
security invoker
set search_path = ''
as $function$
  select s.id, s.scope, s.process_filter, s.status
  from private.lca_network_snapshots s
  where s.id = p_snapshot_id
  limit 1
$function$;

create or replace function api.lca_snapshot_resolve_v1(
  p_scope text,
  p_process_filter jsonb
)
returns table(id uuid, created_at timestamptz, process_filter jsonb)
language plpgsql
stable
security invoker
set search_path = ''
as $function$
begin
  if p_scope is null
     or btrim(p_scope) = ''
     or p_process_filter is null
     or jsonb_typeof(p_process_filter) <> 'object' then
    raise exception using errcode = '22023', message = 'invalid_snapshot_resolve_request';
  end if;

  return query
  select s.id, s.created_at, s.process_filter
  from private.lca_network_snapshots s
  where s.status = 'ready'
    and (s.scope = 'full_library' or s.scope = p_scope)
    and s.process_filter @> p_process_filter
  order by s.created_at desc, s.id
  limit 100;
end
$function$;

create or replace function api.lca_snapshot_artifact_read_v1(p_snapshot_id uuid)
returns table(
  snapshot_id uuid,
  artifact_url text,
  artifact_format text,
  process_count integer,
  status text,
  created_at timestamptz
)
language sql
stable
security invoker
set search_path = ''
as $function$
  select a.snapshot_id, a.artifact_url, a.artifact_format, a.process_count,
         a.status, a.created_at
  from private.lca_snapshot_artifacts a
  where a.snapshot_id = p_snapshot_id
    and a.status = 'ready'
  order by a.created_at desc, a.id
  limit 1
$function$;

create or replace function api.lca_snapshot_artifact_latest_v1()
returns table(
  snapshot_id uuid,
  artifact_url text,
  artifact_format text,
  process_count integer,
  status text,
  created_at timestamptz
)
language sql
stable
security invoker
set search_path = ''
as $function$
  select a.snapshot_id, a.artifact_url, a.artifact_format, a.process_count,
         a.status, a.created_at
  from private.lca_snapshot_artifacts a
  where a.status = 'ready'
  order by a.created_at desc, a.id
  limit 1
$function$;

create or replace function api.cmd_lca_snapshot_create_v1(
  p_snapshot_id uuid,
  p_scope text,
  p_process_filter jsonb,
  p_created_by uuid
)
returns jsonb
language plpgsql
volatile
security invoker
set search_path = ''
as $function$
declare
  inserted_count integer;
begin
  if p_snapshot_id is null
     or p_scope <> 'full_library'
     or p_process_filter is null
     or jsonb_typeof(p_process_filter) <> 'object'
     or p_created_by is null then
    raise exception using errcode = '22023', message = 'invalid_snapshot_create_request';
  end if;

  insert into private.lca_network_snapshots (
    id, scope, process_filter, status, created_by
  ) values (
    p_snapshot_id, p_scope, p_process_filter, 'draft', p_created_by
  )
  on conflict (id) do nothing;
  get diagnostics inserted_count = row_count;

  return jsonb_build_object(
    'snapshotId', p_snapshot_id,
    'created', inserted_count = 1
  );
end
$function$;

alter function api.lca_snapshot_active_read_v1(text) owner to postgres;
alter function api.lca_snapshot_scope_read_v1(uuid) owner to postgres;
alter function api.lca_snapshot_resolve_v1(text,jsonb) owner to postgres;
alter function api.lca_snapshot_artifact_read_v1(uuid) owner to postgres;
alter function api.lca_snapshot_artifact_latest_v1() owner to postgres;
alter function api.cmd_lca_snapshot_create_v1(uuid,text,jsonb,uuid) owner to postgres;

revoke all on function api.lca_snapshot_active_read_v1(text),
  api.lca_snapshot_scope_read_v1(uuid),
  api.lca_snapshot_resolve_v1(text,jsonb),
  api.lca_snapshot_artifact_read_v1(uuid),
  api.lca_snapshot_artifact_latest_v1(),
  api.cmd_lca_snapshot_create_v1(uuid,text,jsonb,uuid)
from public, anon, authenticated, service_role, api_internal_executor;

grant execute on function api.lca_snapshot_active_read_v1(text),
  api.lca_snapshot_scope_read_v1(uuid),
  api.lca_snapshot_resolve_v1(text,jsonb),
  api.lca_snapshot_artifact_read_v1(uuid),
  api.lca_snapshot_artifact_latest_v1(),
  api.cmd_lca_snapshot_create_v1(uuid,text,jsonb,uuid)
to service_role;

comment on function api.lca_snapshot_active_read_v1(text) is
  'supabase-consumer.v1; capability=lca snapshot active read; transport=data-api-rpc; caller=service-role; fallback=none.';
comment on function api.lca_snapshot_scope_read_v1(uuid) is
  'supabase-consumer.v1; capability=lca snapshot exact scope read; transport=data-api-rpc; caller=service-role; fallback=none.';
comment on function api.lca_snapshot_resolve_v1(text,jsonb) is
  'supabase-consumer.v1; capability=lca snapshot scoped resolve/freshness; transport=data-api-rpc; caller=service-role; fallback=none.';
comment on function api.lca_snapshot_artifact_read_v1(uuid) is
  'supabase-consumer.v1; capability=lca snapshot artifact readback; transport=data-api-rpc; caller=service-role; fallback=none.';
comment on function api.lca_snapshot_artifact_latest_v1() is
  'supabase-consumer.v1; capability=lca snapshot latest artifact resolve; transport=data-api-rpc; caller=service-role; fallback=none.';
comment on function api.cmd_lca_snapshot_create_v1(uuid,text,jsonb,uuid) is
  'supabase-consumer.v1; capability=lca snapshot create; transport=data-api-rpc; caller=service-role; fallback=none.';

do $postflight$
declare
  moved_count integer;
  compat_count integer;
  api_count integer;
begin
  select count(*) into moved_count
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'private'
    and c.relkind = 'r'
    and c.relname = any (array[
      'lca_active_snapshots', 'lca_network_snapshots', 'lca_snapshot_artifacts'
    ])
    and c.relrowsecurity
    and not c.relforcerowsecurity;

  select count(*) into compat_count
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'v'
    and c.relname = any (array[
      'lca_active_snapshots', 'lca_network_snapshots', 'lca_snapshot_artifacts'
    ])
    and c.reloptions @> array['security_invoker=true'];

  select count(*) into api_count
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'api'
    and p.proname = any (array[
      'lca_snapshot_active_read_v1',
      'lca_snapshot_scope_read_v1',
      'lca_snapshot_resolve_v1',
      'lca_snapshot_artifact_read_v1',
      'lca_snapshot_artifact_latest_v1',
      'cmd_lca_snapshot_create_v1'
    ]);

  if moved_count <> 3 or compat_count <> 3 or api_count <> 6 then
    raise exception 'issue 376 incomplete postflight: moved=%, compat=%, api=%',
      moved_count, compat_count, api_count;
  end if;

  if has_schema_privilege('anon', 'private', 'usage')
     or has_schema_privilege('authenticated', 'private', 'usage') then
    raise exception 'issue 376 private schema exposed to browser roles';
  end if;
end
$postflight$;

notify pgrst, 'reload schema';

commit;
