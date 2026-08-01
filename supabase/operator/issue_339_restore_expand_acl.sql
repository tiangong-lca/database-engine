\set ON_ERROR_STOP on

-- Emergency compatibility rollback.  This restores only the environment's
-- captured pre-Expand grants; it does not remove additive schemas or data.
begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $rollback$
declare
  record_row record;
  signature text;
  function_oid regprocedure;
  grant_target text;
  object_keyword text;
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using errcode = '42501', message = 'rollback requires the postgres owner session';
  end if;
  if to_regclass('archive.security_acl_expand_20260801_snapshot') is null then
    raise exception 'security ACL rollback snapshot is missing';
  end if;

  grant api_internal_executor to postgres;

  for signature in select column1 from (values
    ('public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'),
    ('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
    ('public.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
    ('public.search_processes_latest_v2(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'),
    ('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'),
    ('public.semantic_search_contacts_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_flowproperties_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_sources_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_unitgroups_v1(text,text,double precision,integer,text,integer,uuid)')
  ) signatures
  loop
    function_oid := to_regprocedure(signature);
    execute format('alter function %s owner to postgres', function_oid);
    execute format('alter function %s security invoker', function_oid);
  end loop;

  revoke all on all tables in schema public from api_internal_executor;
  revoke execute on all functions in schema public, private from api_internal_executor;
  revoke usage on schema public, private from api_internal_executor;
  revoke api_internal_executor from postgres;
  revoke authenticated from api_internal_executor;
  drop role api_internal_executor;

  revoke all on schema private, util, archive from public, anon, authenticated;
  revoke execute on all functions in schema private, util, archive from public, anon, authenticated;
  revoke all on all tables in schema private, util, archive from public, anon, authenticated;
  revoke all on all sequences in schema private, util, archive from public, anon, authenticated;
  revoke all on table
    public.lca_active_snapshots,
    public.lca_factorization_registry,
    public.lca_latest_all_unit_results,
    public.lca_network_snapshots,
    public.lca_package_artifacts,
    public.lca_package_export_items,
    public.lca_package_request_cache,
    public.lca_result_cache,
    public.lca_results,
    public.lca_snapshot_artifacts
  from anon, authenticated;
  revoke execute on function public.save_lifecycle_model_bundle(jsonb) from public, anon, authenticated, service_role;
  revoke execute on function public.delete_lifecycle_model_bundle(uuid,text) from public, anon, authenticated, service_role;

  for record_row in
    select * from archive.security_acl_expand_20260801_snapshot
    where object_class in ('schema', 'relation', 'function')
    order by object_class, object_schema, object_identity, grantee, privilege_type
  loop
    grant_target := case when record_row.grantee = 'PUBLIC' then 'PUBLIC'
      else format('%I', record_row.grantee) end;
    object_keyword := case record_row.object_class
      when 'schema' then 'SCHEMA'
      when 'relation' then 'TABLE'
      when 'function' then 'FUNCTION'
    end;
    execute format('GRANT %s ON %s %s TO %s',
      record_row.privilege_type,
      object_keyword,
      case when record_row.object_class = 'function' then record_row.object_identity
           when record_row.object_class = 'schema' then format('%I', record_row.object_identity)
           else format('%I.%I', record_row.object_schema, record_row.object_identity) end,
      grant_target);
  end loop;

  alter default privileges for role postgres in schema public, api, private, util, archive
    revoke all on tables from public, anon, authenticated, service_role;
  alter default privileges for role postgres in schema public, api, private, util, archive
    revoke all on sequences from public, anon, authenticated, service_role;
  alter default privileges for role postgres in schema public, api, private, util, archive
    revoke execute on functions from public, anon, authenticated, service_role;

  for record_row in
    select * from archive.security_acl_expand_20260801_snapshot
    where object_class like 'default_%'
    order by object_class, object_schema, grantee, privilege_type
  loop
    grant_target := case when record_row.grantee = 'PUBLIC' then 'PUBLIC'
      else format('%I', record_row.grantee) end;
    object_keyword := case record_row.object_class
      when 'default_r' then 'TABLES'
      when 'default_S' then 'SEQUENCES'
      when 'default_f' then 'FUNCTIONS'
    end;
    execute format('ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA %I GRANT %s ON %s TO %s',
      record_row.object_schema, record_row.privilege_type, object_keyword, grant_target);
  end loop;
end
$rollback$;

notify pgrst, 'reload schema';
commit;
