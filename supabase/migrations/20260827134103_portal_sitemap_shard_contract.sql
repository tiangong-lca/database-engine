-- Issue #539: expose a constant-cost sitemap manifest plus 64 deterministic,
-- globally disjoint, bounded shard pages over the synchronized narrow Portal
-- facet projection. Existing Portal sitemap/search consumers remain unchanged.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_sitemap_shard_prerequisite_guard$
begin
  if pg_catalog.to_regprocedure(
       'api.portal_sitemap_manifest_v1()'
     ) is not null
     or pg_catalog.to_regprocedure(
       'api.portal_sitemap_shard_v1(text)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_sitemap_shard_index_v1()'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_projection_contract_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_facet_contract_v1()'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_sitemap_shard_v1_idx'
     ) is null
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'portal_public_executor'
         and not role.rolcanlogin
         and not role.rolinherit
         and not role.rolbypassrls
         and not role.rolsuper
     )
     or (
       select pg_catalog.md5(routine.prosrc)
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'api.portal_sitemap_entries_v1(text,text,integer)'::regprocedure
     ) <> '03dd37bd0871c220fcd94cb2dec203ed' then
    raise exception 'Portal sitemap shard prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_sitemap_shard_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

create function private.assert_portal_sitemap_shard_index_v1()
returns void
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_index regclass :=
    pg_catalog.to_regclass('private.portal_sitemap_shard_v1_idx');
  v_expected_expression constant text :=
    $$(get_byte(decode(md5(((dataset_kind || ':'::text) || (id)::text)), 'hex'::text), 0) / 4)$$;
begin
  if v_index is null
     or not exists (
       select 1
       from pg_catalog.pg_index as index_catalog
       where index_catalog.indexrelid = v_index
         and index_catalog.indrelid =
           'private.portal_catalog_facet_rows_v1'::regclass
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
         and index_catalog.indnkeyatts = 7
         and index_catalog.indnatts = 7
         and index_catalog.indpred is null
         and pg_catalog.pg_get_expr(
           index_catalog.indexprs,
           index_catalog.indrelid
         ) = v_expected_expression
     ) then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;
end
$function$;

revoke all on function private.assert_portal_sitemap_shard_index_v1()
  from public, anon, authenticated, service_role, api_internal_executor;

create function api.portal_sitemap_manifest_v1()
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '2s'
set max_parallel_workers_per_gather = '0'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_shards jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_shard_index_v1();

  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'shardCursor',
      private.portal_cursor_encode_v1(pg_catalog.jsonb_build_object(
        'v', 1,
        'scope', 'sitemap-shard',
        'bucket', shard.bucket,
        'shardCount', 64
      )),
      'maxItems', 4096
    )
    order by shard.bucket
  )
  into v_shards
  from pg_catalog.generate_series(0, 63) as shard(bucket);

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-manifest.v1',
    'shards', v_shards
  );
exception
  when query_canceled then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  when others then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
end
$function$;

create function api.portal_sitemap_shard_v1(p_shard_cursor text)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '4s'
set work_mem = '8MB'
set plan_cache_mode = 'force_custom_plan'
set max_parallel_workers_per_gather = '0'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_cursor jsonb;
  v_bucket integer;
  v_items jsonb;
  v_result jsonb;
begin
  if p_shard_cursor is null
     or pg_catalog.octet_length(p_shard_cursor) not between 1 and 4096
     or p_shard_cursor ~ '[[:space:]]' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  v_cursor := private.portal_cursor_decode_v1(p_shard_cursor);
  if v_cursor is null
     or pg_catalog.jsonb_typeof(v_cursor) <> 'object'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 4
     or v_cursor ->> 'v' <> '1'
     or v_cursor ->> 'scope' <> 'sitemap-shard'
     or v_cursor ->> 'shardCount' <> '64'
     or coalesce(v_cursor ->> 'bucket', '') !~ '^([0-9]|[1-5][0-9]|6[0-3])$'
     or private.portal_cursor_encode_v1(v_cursor) <> p_shard_cursor then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_bucket := (v_cursor ->> 'bucket')::integer;

  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_shard_index_v1();

  with latest as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.modified_at
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.state_code in (100, 200)
      and facet.facet_contract_version = 1
      and (
        pg_catalog.get_byte(
          pg_catalog.decode(
            pg_catalog.md5(
              facet.dataset_kind || ':'::text || facet.id::text
            ),
            'hex'::text
          ),
          0
        ) / 4
      ) = v_bucket
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc,
      facet.facet_contract_version
    limit 4097
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'key', pg_catalog.jsonb_build_object(
      'kind', latest.dataset_kind,
      'id', latest.id::text,
      'version', latest.version
    ),
    'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
  ) order by latest.dataset_kind, latest.id), '[]'::jsonb)
  into v_items
  from latest;

  if pg_catalog.jsonb_array_length(v_items) > 4096 then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-shard.v1',
    'shardCursor', p_shard_cursor,
    'items', v_items
  );
  if pg_catalog.octet_length(v_result::text) > 2 * 1024 * 1024 then
    raise exception using
      errcode = '54000',
      message = 'portal sitemap response exceeded its budget';
  end if;
  return v_result;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  when others then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
end
$function$;

comment on function api.portal_sitemap_manifest_v1() is
  'Constant-cost ordered manifest of 64 opaque, globally disjoint Portal sitemap shard cursors.';
comment on function api.portal_sitemap_shard_v1(text) is
  'Bounded latest-visible Process/Flow sitemap page for one opaque stable-hash shard cursor.';

revoke all on function api.portal_sitemap_manifest_v1()
  from public, anon, authenticated, service_role;
revoke all on function api.portal_sitemap_shard_v1(text)
  from public, anon, authenticated, service_role;
grant execute on function api.portal_sitemap_manifest_v1()
  to anon, authenticated;
grant execute on function api.portal_sitemap_shard_v1(text)
  to anon, authenticated;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values
  (
    'api.portal_sitemap_manifest_v1()',
    'PORTAL-CATALOG-01',
    true,
    true,
    false
  ),
  (
    'api.portal_sitemap_shard_v1(text)',
    'PORTAL-CATALOG-01',
    true,
    true,
    false
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

do $verify_portal_sitemap_shard_contract_v1$
declare
  v_manifest regprocedure :=
    'api.portal_sitemap_manifest_v1()'::regprocedure;
  v_shard regprocedure :=
    'api.portal_sitemap_shard_v1(text)'::regprocedure;
  v_assert regprocedure :=
    'private.assert_portal_sitemap_shard_index_v1()'::regprocedure;
begin
  if (
    select not (
      routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.prorettype = 'pg_catalog.jsonb'::regtype
      and coalesce(routine.proconfig, '{}'::text[]) @> array[
        'search_path=""',
        'statement_timeout=2s',
        'max_parallel_workers_per_gather=0',
        'jit=off',
        'row_security=on'
      ]::text[]
    )
    from pg_catalog.pg_proc as routine
    where routine.oid = v_manifest
  ) is not false
  or (
    select not (
      routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.prorettype = 'pg_catalog.jsonb'::regtype
      and coalesce(routine.proconfig, '{}'::text[]) @> array[
        'search_path=""',
        'statement_timeout=4s',
        'work_mem=8MB',
        'plan_cache_mode=force_custom_plan',
        'max_parallel_workers_per_gather=0',
        'jit=off',
        'row_security=on'
      ]::text[]
    )
    from pg_catalog.pg_proc as routine
    where routine.oid = v_shard
  ) is not false
  or (
    select not (
      routine.proowner = 'portal_public_executor'::regrole
      and not routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.prorettype = 'pg_catalog.void'::regtype
      and coalesce(routine.proconfig, '{}'::text[]) @>
        array['search_path=""']::text[]
    )
    from pg_catalog.pg_proc as routine
    where routine.oid = v_assert
  ) is not false
  or (
    select pg_catalog.md5(routine.prosrc)
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_sitemap_entries_v1(text,text,integer)'::regprocedure
  ) <> '03dd37bd0871c220fcd94cb2dec203ed'
  or (
    select pg_catalog.count(*)
    from private.api_capability_grants as manifest
    where manifest.routine_identity in (
      'api.portal_sitemap_manifest_v1()',
      'api.portal_sitemap_shard_v1(text)'
    )
      and manifest.capability_id = 'PORTAL-CATALOG-01'
      and manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ) <> 2
  or not pg_catalog.has_function_privilege('anon', v_manifest, 'EXECUTE')
  or not pg_catalog.has_function_privilege(
    'authenticated', v_manifest, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('service_role', v_manifest, 'EXECUTE')
  or not pg_catalog.has_function_privilege('anon', v_shard, 'EXECUTE')
  or not pg_catalog.has_function_privilege(
    'authenticated', v_shard, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('service_role', v_shard, 'EXECUTE')
  or pg_catalog.has_function_privilege('anon', v_assert, 'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated', v_assert, 'EXECUTE')
  or pg_catalog.has_function_privilege('service_role', v_assert, 'EXECUTE')
  or pg_catalog.has_function_privilege(
    'api_internal_executor', v_assert, 'EXECUTE'
  )
  or exists (
    select 1
    from pg_catalog.aclexplode(
      coalesce(
        (select routine.proacl
         from pg_catalog.pg_proc as routine
         where routine.oid in (v_manifest::oid, v_shard::oid)
         limit 1),
        '{}'::aclitem[]
      )
    ) as acl
    where acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  )
  or (
    select routine.prosrc !~ 'generate_series\(0, 63\)'
      or routine.prosrc ~
        'portal_catalog_(facet|search)_rows_v1|public\.(processes|flows)'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_manifest
  )
  or (
    select routine.prosrc !~ 'portal_catalog_facet_rows_v1'
      or routine.prosrc !~ 'md5'
      or routine.prosrc ~
        'portal_catalog_search_rows_v1|public\.(processes|flows)|card|document|json_data|search_text|extracted_md|embedding_ft|team_id|user_id|review_id|privateLocator|objectLocator'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_shard
  ) then
    raise exception 'Portal sitemap shard contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_sitemap_shard_contract_v1$;

commit;
