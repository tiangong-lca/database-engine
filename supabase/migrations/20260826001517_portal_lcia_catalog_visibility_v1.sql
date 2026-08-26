begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- The publication projection is already bound into Worker and Release
-- evidence.  Snapshot every frozen legacy reader plus the authoritative LCIA
-- visibility predicate before adding a catalog-only projection.
create temporary table portal_lcia_catalog_frozen_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit drop;

insert into portal_lcia_catalog_frozen_before (
  routine_identity, definition, owner_name, security_definer, proconfig,
  acl_text
)
with expected(routine_identity) as (
  values
    ('api.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb)'),
    ('api.cmd_lcia_result_build_request_v2(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)'),
    ('private.cmd_lcia_result_package_mark_ready(uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb)'),
    ('api.cmd_lcia_result_package_publish(uuid, text, text, jsonb)'),
    ('api.cmd_lcia_result_publication_unpublish(uuid, text, jsonb)'),
    ('api.get_lcia_result_package_preview(uuid)'),
    ('api.get_published_lcia_result_package(uuid, text, text)'),
    ('api.cmd_lca_release_prepare(uuid, text, text, text, jsonb, text, text, jsonb, text, text, jsonb)'),
    ('private.cmd_lca_release_artifacts_finalize_service(uuid, text, jsonb, text, jsonb, jsonb)'),
    ('api.cmd_lca_release_approve(uuid, text, timestamp with time zone, text, jsonb)'),
    ('api.cmd_lca_release_publish(uuid, uuid, text, text, text, text, text, jsonb)'),
    ('api.cmd_lca_release_readback_verify(uuid, text, jsonb, jsonb)'),
    ('api.cmd_lca_release_unpublish(uuid, text, jsonb)'),
    ('api.get_current_lca_release()'),
    ('private.portal_lcia_projection_is_public_v1(uuid)'),
    ('api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)')
)
select
  expected.routine_identity,
  pg_catalog.pg_get_functiondef(routine.oid),
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

do $portal_lcia_catalog_frozen_snapshot_guard$
begin
  if (select count(*) from portal_lcia_catalog_frozen_before) <> 16 then
    raise exception 'Portal LCIA catalog frozen routine snapshot is incomplete';
  end if;
end
$portal_lcia_catalog_frozen_snapshot_guard$;

create temporary table portal_lcia_catalog_target_metadata_before (
  routine_identity text primary key,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit drop;

insert into portal_lcia_catalog_target_metadata_before (
  routine_identity, owner_name, security_definer, proconfig, acl_text
)
with expected(routine_identity) as (
  values
    ('api.portal_search_processes_v1(text, jsonb, text, text, integer)'),
    ('api.portal_get_dataset_v1(text, uuid, text)'),
    ('api.portal_list_versions_v1(text, uuid, text, integer)')
)
select
  expected.routine_identity,
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

do $portal_lcia_catalog_target_snapshot_guard$
begin
  if (select count(*) from portal_lcia_catalog_target_metadata_before) <> 3 then
    raise exception 'Portal LCIA catalog target metadata snapshot is incomplete';
  end if;
end
$portal_lcia_catalog_target_snapshot_guard$;

-- A narrow executor-owned definer keeps the established capability helper and
-- all of its private dependencies ACL-stable.  It returns one boolean only;
-- postgres receives EXECUTE on this bridge, never on the dependency graph.
create function private.portal_process_open_capability_bridge_v1(
  p_state_code integer,
  p_json jsonb
)
returns boolean
language sql
stable
security definer
set search_path = ''
as $function$
  select coalesce((
    private.portal_capabilities_v1('process', p_state_code, p_json)
      ->> 'exchangesVisible'
  )::boolean, false)
$function$;

revoke all on function private.portal_process_open_capability_bridge_v1(
  integer, jsonb
) from public, anon, authenticated, service_role;

-- Resolve one exact current publication by reusing the already reviewed
-- projection-level predicate.  The outer joins bind that predicate to the
-- exact finalized, non-revoked current publication row and to an open state-100
-- Process; a publication for state 200, Flow, or a different exact version can
-- never be projected into a catalog DTO.
create function private.portal_current_lcia_publication_for_process_v1(
  p_process_id uuid,
  p_process_version text
)
returns jsonb
language sql
stable
security definer
set search_path = ''
as $function$
  with visible as materialized (
    select
      binding.projection_id,
      binding.lcia_result_publication_id,
      binding.package_id,
      binding.package_version,
      binding.source_published_at
    from private.portal_lcia_projection_publications as binding
    join private.portal_lcia_projection_headers as projection
      on projection.id = binding.projection_id
    join private.portal_lcia_projection_process_axis as process_axis
      on process_axis.projection_id = binding.projection_id
     and process_axis.process_id = p_process_id
     and process_axis.process_version = p_process_version
    join private.lcia_result_publications as publication
      on publication.id = binding.lcia_result_publication_id
     and publication.package_id = binding.package_id
    join private.lcia_result_packages as package
      on package.id = binding.package_id
    join public.processes as process
      on process.id = process_axis.process_id
     and process.version::text = process_axis.process_version
    where p_process_id is not null
      and p_process_version ~ '^\d{2}\.\d{2}\.\d{3}$'
      and binding.status = 'finalized'
      and binding.revoked_at is null
      and projection.status = 'prepared'
      and projection.content_hash = binding.projection_content_hash
      and publication.is_current
      and publication.status = 'current'
      and publication.publication_series_key = 'global'
      and publication.publication_channel = 'public'
      and publication.visibility_scope = 'public'
      and publication.published_at = binding.source_published_at
      and package.status = 'preview_ready'
      and package.package_version = binding.package_version
      and package.package_result_hash = binding.package_result_hash
      and process.state_code = 100
      and jsonb_typeof(process.json) = 'object'
      and private.portal_process_open_capability_bridge_v1(
        process.state_code, process.json
      )
      and private.portal_lcia_projection_is_public_v1(binding.projection_id)
    order by binding.source_published_at desc, binding.id
    limit 1
  ), methods as materialized (
    select
      visible.projection_id,
      jsonb_agg(
        jsonb_build_object(
          'id', method.method_id::text,
          'version', method.method_version
        )
        order by method.method_id, method.method_version
      ) as lcia_methods
    from visible
    cross join lateral (
      select distinct impact.method_id, impact.method_version
      from private.portal_lcia_projection_impact_axis as impact
      where impact.projection_id = visible.projection_id
    ) as method
    group by visible.projection_id
  )
  select jsonb_build_object(
    'publicationId', visible.lcia_result_publication_id::text,
    'packageId', visible.package_id::text,
    'packageVersion', visible.package_version,
    'publishedAt', private.portal_timestamp_v1(visible.source_published_at),
    'lciaMethods', methods.lcia_methods
  )
  from visible
  join methods using (projection_id)
  where jsonb_array_length(methods.lcia_methods) > 0
$function$;

revoke all on function private.portal_current_lcia_publication_for_process_v1(
  uuid, text
) from public, anon, authenticated, service_role;
grant execute on function private.portal_current_lcia_publication_for_process_v1(
  uuid, text
) to portal_public_executor;
comment on function private.portal_current_lcia_publication_for_process_v1(
  uuid, text
) is
  'Returns locator-free current finalized non-revoked publication context for one exact open Process by reusing portal_lcia_projection_is_public_v1.';

-- Both Search and Versions expose an items array with the same key,
-- accessLevel, and capabilities fields.  Rebuild only those exact item objects
-- in ordinality order; every other top-level field, cursor, and strict DTO key
-- remains byte-equivalent JSONB content.
create function private.portal_lcia_decorate_item_page_v1(p_page jsonb)
returns jsonb
language sql
stable
set search_path = ''
as $function$
  select case
    when jsonb_typeof(p_page) <> 'object'
      or jsonb_typeof(p_page -> 'items') <> 'array'
      then null
    else jsonb_set(
      p_page,
      '{items}',
      coalesce((
        select jsonb_agg(
          item.value || jsonb_build_object(
            'capabilities',
            jsonb_set(
              item.value -> 'capabilities',
              '{lciaVisible}',
              to_jsonb(evidence.publication is not null),
              false
            )
          )
          order by item.ordinality
        )
        from jsonb_array_elements(p_page -> 'items')
          with ordinality as item(value, ordinality)
        cross join lateral (
          select case
            when item.value #>> '{key,kind}' = 'process'
              and item.value ->> 'accessLevel' = 'open'
              then private.portal_current_lcia_publication_for_process_v1(
                (item.value #>> '{key,id}')::uuid,
                item.value #>> '{key,version}'
              )
            else null
          end as publication
        ) as evidence
      ), '[]'::jsonb),
      false
    )
  end
$function$;

create function private.portal_lcia_decorate_dataset_v1(p_envelope jsonb)
returns jsonb
language sql
stable
set search_path = ''
as $function$
  select case
    when p_envelope is null then null
    when jsonb_typeof(p_envelope) <> 'object'
      or jsonb_typeof(p_envelope -> 'key') <> 'object'
      or jsonb_typeof(p_envelope -> 'capabilities') <> 'object'
      then null
    else jsonb_set(
      jsonb_set(
        p_envelope,
        '{capabilities,lciaVisible}',
        to_jsonb(evidence.publication is not null),
        false
      ),
      '{publication}',
      coalesce(evidence.publication, 'null'::jsonb),
      false
    )
  end
  from lateral (
    select case
      when p_envelope #>> '{key,kind}' = 'process'
        and p_envelope ->> 'accessLevel' = 'open'
        then private.portal_current_lcia_publication_for_process_v1(
          (p_envelope #>> '{key,id}')::uuid,
          p_envelope #>> '{key,version}'
        )
      else null
    end as publication
  ) as evidence
$function$;

revoke all on function private.portal_lcia_decorate_item_page_v1(jsonb)
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_decorate_dataset_v1(jsonb)
  from public, anon, authenticated, service_role;

-- The constrained executor owns the decorators exactly like the original
-- catalog helpers.  Temporarily restore SET/CREATE authority only for this
-- handoff, then return to the reviewed membership and schema privilege state.
grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
alter function private.portal_process_open_capability_bridge_v1(integer, jsonb)
  owner to portal_public_executor;
alter function private.portal_lcia_decorate_item_page_v1(jsonb)
  owner to portal_public_executor;
alter function private.portal_lcia_decorate_dataset_v1(jsonb)
  owner to portal_public_executor;
revoke create on schema private from portal_public_executor;
set local role portal_public_executor;
grant execute on function private.portal_process_open_capability_bridge_v1(
  integer, jsonb
) to postgres;

create or replace function api.portal_search_processes_v1(
  p_query text,
  p_filters jsonb default '{}'::jsonb,
  p_sort text default 'relevance',
  p_cursor text default null,
  p_limit integer default 20
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
begin
  return private.portal_lcia_decorate_item_page_v1(
    private.portal_search_v1(
      'process', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create or replace function api.portal_get_dataset_v1(
  p_kind text,
  p_id uuid,
  p_version text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_version is null
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return private.portal_lcia_decorate_dataset_v1(
    private.portal_dataset_projection_v1(p_kind, p_id, p_version)
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

-- Versions has no private core in the frozen V1 surface, so retain its
-- established body verbatim and decorate only the final strict item page.
create or replace function api.portal_list_versions_v1(
  p_kind text,
  p_id uuid,
  p_cursor text default null,
  p_limit integer default 20
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_cursor jsonb;
  v_cursor_version text;
  v_items jsonb;
  v_next_cursor text;
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_limit is null
     or p_limit not between 1 and 50 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 4
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'kind' <> p_kind
       or v_cursor ->> 'id' <> p_id::text
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_version := v_cursor ->> 'version';
  end if;

  with all_versions as materialized (
    select source.*,
      row_number() over (order by source.version desc) = 1 as is_latest,
      private.portal_capabilities_v1(
        p_kind, source.state_code, source.json_data
      ) as capabilities
    from private.portal_dataset_rows_v1(p_kind, p_id) as source
  ), ordered as materialized (
    select all_versions.*,
      row_number() over (order by all_versions.version desc) as page_rank
    from all_versions
    where v_cursor_version is null or all_versions.version < v_cursor_version
    order by all_versions.version desc
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'key', jsonb_build_object(
        'kind', p_kind,
        'id', ordered.id::text,
        'version', ordered.version
      ),
      'accessLevel', case
        when (ordered.capabilities ->> 'exchangesVisible')::boolean
          then 'open'
        else 'metadata_only'
      end,
      'capabilities', ordered.capabilities,
      'modifiedAt', private.portal_timestamp_v1(ordered.modified_at),
      'isLatest', ordered.is_latest
    ) order by ordered.page_rank)
      filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit
      then private.portal_cursor_encode_v1(
        (jsonb_agg(jsonb_build_object(
          'v', 1,
          'kind', p_kind,
          'id', p_id::text,
          'version', ordered.version
        ) order by ordered.page_rank)
          filter (where ordered.page_rank = p_limit)) -> 0
      )
      else null
    end
  into v_items, v_next_cursor
  from ordered;

  return private.portal_lcia_decorate_item_page_v1(
    jsonb_build_object(
      'schemaVersion', 'portal.public-version-page.v1',
      'dataset', jsonb_build_object('kind', p_kind, 'id', p_id::text),
      'items', v_items,
      'nextCursor', v_next_cursor
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

comment on function private.portal_lcia_decorate_item_page_v1(jsonb) is
  'Preserves strict item-page order and cursor while projecting authoritative lciaVisible onto Process Search or Versions items.';
comment on function private.portal_lcia_decorate_dataset_v1(jsonb) is
  'Preserves the strict dataset envelope while projecting authoritative LCIA capability and publication context.';
comment on function private.portal_process_open_capability_bridge_v1(
  integer, jsonb
) is
  'Executor-owned boolean bridge that reuses the fail-closed Process numeric capability policy without widening its helper ACL graph.';
comment on function api.portal_search_processes_v1(
  text, jsonb, text, text, integer
) is
  'Locator-free public Process lexical/identifier search with authoritative publication-bound LCIA capability.';
comment on function api.portal_get_dataset_v1(text, uuid, text) is
  'Exact locator-free public Process/Flow metadata envelope with authoritative publication-bound LCIA context.';
comment on function api.portal_list_versions_v1(
  text, uuid, text, integer
) is
  'Keyset page of exact visible versions with authoritative publication-bound Process LCIA capability.';

reset role;
revoke create on schema api from portal_public_executor;
revoke portal_public_executor from postgres;

-- CREATE OR REPLACE must not alter the externally governed owner, security,
-- query budget, defaults, or ACL of the three catalog signatures.
do $verify_portal_lcia_catalog_target_metadata$
begin
  if exists (
    with current_metadata as (
      select
        before.routine_identity,
        owner_role.rolname as owner_name,
        routine.prosecdef as security_definer,
        coalesce(routine.proconfig, '{}'::text[]) as proconfig,
        coalesce(routine.proacl::text, '') as acl_text
      from portal_lcia_catalog_target_metadata_before as before
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
      join pg_catalog.pg_roles as owner_role
        on owner_role.oid = routine.proowner
    )
    select 1
    from (
      (select * from current_metadata
       except select * from portal_lcia_catalog_target_metadata_before)
      union all
      (select * from portal_lcia_catalog_target_metadata_before
       except select * from current_metadata)
    ) as difference
  ) then
    raise exception 'Portal LCIA catalog target metadata drifted';
  end if;
end
$verify_portal_lcia_catalog_target_metadata$;

do $verify_portal_lcia_catalog_helper_acl$
declare
  v_routine regprocedure;
begin
  v_routine := 'private.portal_current_lcia_publication_for_process_v1(uuid,text)'::regprocedure;
  if (
    select not routine.prosecdef
      or routine.proowner <> 'postgres'::regrole
      or not (routine.proconfig @> array['search_path=""']::text[])
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  )
  or not pg_catalog.has_function_privilege(
    'portal_public_executor', v_routine, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('service_role', v_routine, 'EXECUTE') then
    raise exception 'Portal LCIA current-publication helper ACL mismatch';
  end if;

  v_routine := 'private.portal_process_open_capability_bridge_v1(integer,jsonb)'::regprocedure;
  if (
    select not routine.prosecdef
      or routine.proowner <> 'portal_public_executor'::regrole
      or not (routine.proconfig @> array['search_path=""']::text[])
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  )
  or not pg_catalog.has_function_privilege('postgres', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('service_role', v_routine, 'EXECUTE') then
    raise exception 'Portal Process open-capability bridge ACL mismatch';
  end if;

  foreach v_routine in array array[
    'private.portal_lcia_decorate_item_page_v1(jsonb)'::regprocedure,
    'private.portal_lcia_decorate_dataset_v1(jsonb)'::regprocedure
  ] loop
    if (
      select routine.prosecdef
        or routine.proowner <> 'portal_public_executor'::regrole
        or not (routine.proconfig @> array['search_path=""']::text[])
      from pg_catalog.pg_proc as routine
      where routine.oid = v_routine
    )
    or pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
    or pg_catalog.has_function_privilege('authenticated', v_routine, 'EXECUTE')
    or pg_catalog.has_function_privilege('service_role', v_routine, 'EXECUTE') then
      raise exception 'Portal LCIA decorator ACL mismatch';
    end if;
  end loop;
end
$verify_portal_lcia_catalog_helper_acl$;

-- No established Data Product/Release/LCIA reader or visibility predicate may
-- drift as a side effect of catalog enrichment.
do $verify_portal_lcia_catalog_frozen_after$
begin
  if exists (
    with current_state as (
      select
        before.routine_identity,
        pg_catalog.pg_get_functiondef(routine.oid) as definition,
        owner_role.rolname as owner_name,
        routine.prosecdef as security_definer,
        coalesce(routine.proconfig, '{}'::text[]) as proconfig,
        coalesce(routine.proacl::text, '') as acl_text
      from portal_lcia_catalog_frozen_before as before
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
      join pg_catalog.pg_roles as owner_role
        on owner_role.oid = routine.proowner
    )
    select 1
    from (
      (select * from current_state
       except select * from portal_lcia_catalog_frozen_before)
      union all
      (select * from portal_lcia_catalog_frozen_before
       except select * from current_state)
    ) as difference
  ) then
    raise exception 'Portal LCIA catalog migration changed a frozen routine';
  end if;
end
$verify_portal_lcia_catalog_frozen_after$;

do $portal_lcia_catalog_executor_membership_guard$
begin
  if (
    select count(*)
    from pg_catalog.pg_auth_members as membership
    where membership.member = 'portal_public_executor'::regrole
       or membership.roleid = 'portal_public_executor'::regrole
  ) <> 1
  or not exists (
    select 1
    from pg_catalog.pg_auth_members as membership
    where membership.roleid = 'portal_public_executor'::regrole
      and membership.member = 'postgres'::regrole
      and membership.admin_option
      and not membership.inherit_option
      and not membership.set_option
  ) then
    raise exception 'portal_public_executor has unexpected role membership'
      using errcode = '42501';
  end if;
end
$portal_lcia_catalog_executor_membership_guard$;

commit;
