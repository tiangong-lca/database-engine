-- Issue #414: move the snapshot retention/GC audit tables to the private
-- physical boundary while retaining one writable, security-invoker public
-- compatibility path.  The move preserves both relation OIDs and never copies
-- or dual-writes data.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create or replace function pg_temp.issue_414_relation_fingerprint(
  p_relation pg_catalog.regclass
) returns text
language sql
stable
set search_path = pg_catalog
as $function$
  with catalog as (
    select 'relation' as category,
      pg_catalog.concat_ws('|',
        relation.relowner::pg_catalog.regrole,
        relation.relkind,
        relation.relpersistence,
        relation.relrowsecurity,
        relation.relforcerowsecurity,
        relation.relreplident,
        coalesce(access_method.amname, ''),
        coalesce(tablespace.spcname, ''),
        coalesce(relation.reloptions::text, ''),
        relation.relispartition,
        coalesce(pg_catalog.pg_get_expr(relation.relpartbound, relation.oid), ''),
        coalesce((
          select pg_catalog.string_agg(
            pg_catalog.concat_ws(':',
              case when acl.grantee = 0 then 'PUBLIC'
                else pg_catalog.pg_get_userbyid(acl.grantee) end,
              pg_catalog.pg_get_userbyid(acl.grantor),
              acl.privilege_type,
              acl.is_grantable
            ), ',' order by
              case when acl.grantee = 0 then 'PUBLIC'
                else pg_catalog.pg_get_userbyid(acl.grantee) end,
              pg_catalog.pg_get_userbyid(acl.grantor),
              acl.privilege_type,
              acl.is_grantable
          )
          from pg_catalog.aclexplode(relation.relacl) acl
        ), ''),
        coalesce(pg_catalog.obj_description(relation.oid, 'pg_class'), '')
      ) as value
    from pg_catalog.pg_class relation
    left join pg_catalog.pg_am access_method
      on access_method.oid = relation.relam
    left join pg_catalog.pg_tablespace tablespace
      on tablespace.oid = relation.reltablespace
    where relation.oid = p_relation

    union all
    select 'column',
      pg_catalog.concat_ws('|',
        attribute.attnum,
        attribute.attname,
        pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
        attribute.attnotnull,
        attribute.attidentity,
        attribute.attgenerated,
        attribute.attstorage,
        attribute.attcompression,
        attribute.attstattarget,
        coalesce(attribute.attoptions::text, ''),
        attribute.atthasmissing,
        coalesce(attribute.attmissingval::text, ''),
        coalesce(collation_row.collname, ''),
        coalesce(pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid), ''),
        coalesce((
          select pg_catalog.string_agg(
            pg_catalog.concat_ws(':',
              case when acl.grantee = 0 then 'PUBLIC'
                else pg_catalog.pg_get_userbyid(acl.grantee) end,
              pg_catalog.pg_get_userbyid(acl.grantor),
              acl.privilege_type,
              acl.is_grantable
            ), ',' order by
              case when acl.grantee = 0 then 'PUBLIC'
                else pg_catalog.pg_get_userbyid(acl.grantee) end,
              pg_catalog.pg_get_userbyid(acl.grantor),
              acl.privilege_type,
              acl.is_grantable
          )
          from pg_catalog.aclexplode(attribute.attacl) acl
        ), ''),
        coalesce(pg_catalog.col_description(attribute.attrelid, attribute.attnum), '')
      )
    from pg_catalog.pg_attribute attribute
    left join pg_catalog.pg_attrdef default_value
      on default_value.adrelid = attribute.attrelid
     and default_value.adnum = attribute.attnum
    left join pg_catalog.pg_collation collation_row
      on collation_row.oid = attribute.attcollation
    where attribute.attrelid = p_relation
      and attribute.attnum > 0
      and not attribute.attisdropped

    union all
    select 'constraint',
      pg_catalog.concat_ws('|',
        constraint_row.conname,
        constraint_row.contype,
        constraint_row.condeferrable,
        constraint_row.condeferred,
        constraint_row.convalidated,
        coalesce(constraint_row.conkey::text, ''),
        coalesce(constraint_row.confrelid::pg_catalog.regclass::text, ''),
        coalesce(constraint_row.confkey::text, ''),
        pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
      )
    from pg_catalog.pg_constraint constraint_row
    where constraint_row.conrelid = p_relation

    union all
    select 'index',
      pg_catalog.concat_ws('|',
        index_relation.relname,
        index_row.indisunique,
        index_row.indisprimary,
        index_row.indisvalid,
        index_row.indisready,
        index_row.indisreplident,
        index_row.indisclustered,
        coalesce(index_row.indkey::text, ''),
        coalesce(pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid), ''),
        coalesce(pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid), ''),
        pg_catalog.pg_get_indexdef(index_row.indexrelid)
      )
    from pg_catalog.pg_index index_row
    join pg_catalog.pg_class index_relation
      on index_relation.oid = index_row.indexrelid
    where index_row.indrelid = p_relation

    union all
    select 'trigger',
      pg_catalog.concat_ws('|',
        case when trigger_row.tgisinternal
          then coalesce(constraint_row.conname, '<internal>')
          else trigger_row.tgname end,
        trigger_row.tgenabled,
        trigger_row.tgisinternal,
        trigger_row.tgtype,
        trigger_row.tgfoid::pg_catalog.regprocedure,
        case when trigger_row.tgisinternal then ''
          else pg_catalog.pg_get_triggerdef(trigger_row.oid, true) end
      )
    from pg_catalog.pg_trigger trigger_row
    left join pg_catalog.pg_constraint constraint_row
      on constraint_row.oid = trigger_row.tgconstraint
    where trigger_row.tgrelid = p_relation

    union all
    select 'policy',
      pg_catalog.concat_ws('|',
        policy.polname,
        policy.polcmd,
        policy.polpermissive,
        coalesce((
          select pg_catalog.string_agg(
            pg_catalog.pg_get_userbyid(role_oid), ','
            order by pg_catalog.pg_get_userbyid(role_oid)
          )
          from pg_catalog.unnest(policy.polroles) role_oid
        ), ''),
        coalesce(pg_catalog.pg_get_expr(policy.polqual, policy.polrelid), ''),
        coalesce(pg_catalog.pg_get_expr(policy.polwithcheck, policy.polrelid), '')
      )
    from pg_catalog.pg_policy policy
    where policy.polrelid = p_relation

    union all
    select 'publication',
      pg_catalog.concat_ws('|',
        publication.pubname,
        publication.puballtables,
        publication.pubinsert,
        publication.pubupdate,
        publication.pubdelete,
        publication.pubtruncate,
        publication.pubviaroot,
        coalesce(
          pg_catalog.pg_get_expr(
            publication_relation.prqual,
            publication_relation.prrelid
          ), ''
        ),
        coalesce(publication_relation.prattrs::text, '')
      )
    from pg_catalog.pg_publication publication
    left join pg_catalog.pg_publication_rel publication_relation
      on publication_relation.prpubid = publication.oid
     and publication_relation.prrelid = p_relation
    where publication.puballtables
       or publication_relation.prrelid is not null
  )
  select pg_catalog.md5(
    pg_catalog.string_agg(
      category || pg_catalog.chr(31) || value,
      E'\n' order by category, value
    )
  )
  from catalog
$function$;

do $state$
declare
  v_public_tables integer;
  v_public_views integer;
  v_private_tables integer;
begin
  if not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260804100000'
  ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 requires predecessor migration 20260804100000';
  end if;

  select
    count(*) filter (where namespace.nspname = 'public' and relation.relkind = 'r'),
    count(*) filter (where namespace.nspname = 'public' and relation.relkind = 'v'),
    count(*) filter (where namespace.nspname = 'private' and relation.relkind = 'r')
  into v_public_tables, v_public_views, v_private_tables
  from pg_catalog.pg_class relation
  join pg_catalog.pg_namespace namespace
    on namespace.oid = relation.relnamespace
  where namespace.nspname in ('public', 'private')
    and relation.relname in (
      'lca_snapshot_gc_runs',
      'lca_snapshot_gc_run_items'
    );

  if (v_public_tables, v_public_views, v_private_tables)
     not in ((2, 0, 0), (0, 2, 2)) then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'Issue 414 mixed relation state: publicTables=%s publicViews=%s privateTables=%s',
        v_public_tables, v_public_views, v_private_tables
      );
  end if;
end
$state$;

-- Serialize all writers before any row count or content hash is sampled.
do $lock$
begin
  if pg_catalog.to_regclass('private.lca_snapshot_gc_runs') is null then
    execute 'lock table public.lca_snapshot_gc_runs, public.lca_snapshot_gc_run_items in access exclusive mode';
  else
    execute 'lock table private.lca_snapshot_gc_runs, private.lca_snapshot_gc_run_items in access exclusive mode';
  end if;
end
$lock$;

do $preflight$
declare
  v_expanded boolean :=
    pg_catalog.to_regclass('private.lca_snapshot_gc_runs') is not null;
  v_runs_fingerprint text;
  v_items_fingerprint text;
  v_expected_runs_fingerprint text;
  v_expected_items_fingerprint text;
  v_routine_count integer;
  v_exact_routine_count integer;
begin
  v_expected_runs_fingerprint := case when v_expanded
    then 'eddc56d22585cb6af9562b551afb06e8'
    else 'ad841baccb43a081d8d4bfd0c5599d4f' end;
  v_expected_items_fingerprint := case when v_expanded
    then '8b472ff342e5d2ad59a59dfda0df884e'
    else '43e5174ff4856098340d2bc5e638b40d' end;

  if not exists (
    select 1 from pg_catalog.pg_roles
    where rolname = 'lca_worker_runtime'
      and not rolcanlogin
      and not rolsuper
      and not rolbypassrls
  ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 requires the exact lca_worker_runtime group role';
  end if;

  if pg_catalog.has_schema_privilege('anon', 'private', 'USAGE')
     or pg_catalog.has_schema_privilege('authenticated', 'private', 'USAGE') then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 browser roles unexpectedly have private schema usage';
  end if;

  select
    pg_temp.issue_414_relation_fingerprint(
      pg_catalog.to_regclass(case when v_expanded
        then 'private.lca_snapshot_gc_runs'
        else 'public.lca_snapshot_gc_runs' end)
    ),
    pg_temp.issue_414_relation_fingerprint(
      pg_catalog.to_regclass(case when v_expanded
        then 'private.lca_snapshot_gc_run_items'
        else 'public.lca_snapshot_gc_run_items' end)
    )
  into v_runs_fingerprint, v_items_fingerprint;

  if v_runs_fingerprint is distinct from v_expected_runs_fingerprint
     or v_items_fingerprint is distinct from v_expected_items_fingerprint then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'Issue 414 relation fingerprint drifted: runs=%s items=%s',
        v_runs_fingerprint,
        v_items_fingerprint
      );
  end if;

  select count(*) into v_routine_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace
    on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'util'
    and procedure.proname in (
      'preview_lca_snapshot_retention',
      'list_lca_snapshot_gc_candidates_without_closure_protection'
    );

  select count(*) into v_exact_routine_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace
    on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'util'
    and (
      procedure.proname = 'preview_lca_snapshot_retention'
      and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
        'p_snapshot_retention_window interval, p_orphan_retention_window interval, p_as_of timestamp with time zone'
      and procedure.proowner = 'postgres'::pg_catalog.regrole
      and not procedure.prosecdef
      and procedure.proconfig = array['search_path=""']::text[]
      and procedure.proacl::text =
        '{postgres=X/postgres,service_role=X/postgres}'
      and pg_catalog.obj_description(procedure.oid, 'pg_proc') =
        'Operator dry-run helper for lca-results/snapshots retention. Job references are counted from worker_jobs after public.lca_jobs retirement.'
      and pg_catalog.md5(pg_catalog.pg_get_functiondef(procedure.oid)) =
        case when v_expanded then '464fb33486848ccb0ee9f13bf82a1eef'
        else '5dba44e77a3b5c1d731e702d8b819c58' end
    ) or (
      procedure.proname =
        'list_lca_snapshot_gc_candidates_without_closure_protection'
      and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
        'p_snapshot_retention_window interval, p_orphan_retention_window interval, p_as_of timestamp with time zone, p_max_snapshots integer, p_max_orphan_dirs integer, p_max_bytes bigint'
      and procedure.proowner = 'postgres'::pg_catalog.regrole
      and not procedure.prosecdef
      and procedure.proconfig = array['search_path=""']::text[]
      and procedure.proacl::text = '{postgres=X/postgres}'
      and pg_catalog.obj_description(procedure.oid, 'pg_proc') =
        'Operator snapshot GC candidate helper. Job references are counted from worker_jobs after public.lca_jobs retirement.'
      and pg_catalog.md5(pg_catalog.pg_get_functiondef(procedure.oid)) =
        case when v_expanded then 'a6b39a91a33df29ef1db632563a0484d'
        else '9a40c7ae36a70b8eae41f355acaaee4e' end
    );

  if v_routine_count <> 2 or v_exact_routine_count <> 2 then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 util routine definition or ACL drifted';
  end if;

  if v_expanded and (
    not exists (
      select 1
      from pg_catalog.pg_class relation
      join pg_catalog.pg_namespace namespace
        on namespace.oid = relation.relnamespace
      where namespace.nspname = 'public'
        and relation.relname = 'lca_snapshot_gc_runs'
        and relation.relkind = 'v'
        and relation.relowner = 'postgres'::pg_catalog.regrole
        and relation.reloptions = array['security_invoker=true']::text[]
        and relation.relacl::text =
          '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
        and pg_catalog.md5(pg_catalog.pg_get_viewdef(relation.oid, true)) =
          '55ead76979a9ca06d88a5dceeedbff87'
        and pg_catalog.obj_description(relation.oid, 'pg_class') =
          'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_runs; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.'
    ) or not exists (
      select 1
      from pg_catalog.pg_class relation
      join pg_catalog.pg_namespace namespace
        on namespace.oid = relation.relnamespace
      where namespace.nspname = 'public'
        and relation.relname = 'lca_snapshot_gc_run_items'
        and relation.relkind = 'v'
        and relation.relowner = 'postgres'::pg_catalog.regrole
        and relation.reloptions = array['security_invoker=true']::text[]
        and relation.relacl::text =
          '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
        and pg_catalog.md5(pg_catalog.pg_get_viewdef(relation.oid, true)) =
          'bce1e04e51db0ffb73ca32e7f259bc03'
        and pg_catalog.obj_description(relation.oid, 'pg_class') =
          'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_run_items; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.'
    )
  ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 expanded compatibility view drifted';
  end if;
end
$preflight$;

create temporary table issue_414_before (
  relation_name text primary key,
  relation_oid oid not null,
  row_count bigint not null,
  content_hash text not null
) on commit drop;

do $snapshot$
declare
  v_schema text := case
    when pg_catalog.to_regclass('private.lca_snapshot_gc_runs') is null
      then 'public'
    else 'private'
  end;
  v_relation text;
begin
  foreach v_relation in array array[
    'lca_snapshot_gc_runs',
    'lca_snapshot_gc_run_items'
  ] loop
    execute pg_catalog.format(
      'insert into issue_414_before
       select %L, %L::pg_catalog.regclass::oid, count(*),
         pg_catalog.md5(coalesce(pg_catalog.string_agg(
           pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text), ''''
           order by source_row.id
         ), ''''))
       from %I.%I source_row',
      v_relation,
      v_schema || '.' || v_relation,
      v_schema,
      v_relation
    );
  end loop;
end
$snapshot$;

do $move$
begin
  if pg_catalog.to_regclass('private.lca_snapshot_gc_runs') is null then
    execute 'alter table public.lca_snapshot_gc_run_items set schema private';
    execute 'alter table public.lca_snapshot_gc_runs set schema private';
  end if;

  if not exists (
    select 1 from pg_catalog.pg_policy
    where polrelid = 'private.lca_snapshot_gc_runs'::pg_catalog.regclass
      and polname = 'lca_snapshot_gc_runs_worker_runtime_all'
  ) then
    execute 'create policy lca_snapshot_gc_runs_worker_runtime_all
      on private.lca_snapshot_gc_runs to lca_worker_runtime
      using (true) with check (true)';
  end if;

  if not exists (
    select 1 from pg_catalog.pg_policy
    where polrelid = 'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
      and polname = 'lca_snapshot_gc_run_items_worker_runtime_all'
  ) then
    execute 'create policy lca_snapshot_gc_run_items_worker_runtime_all
      on private.lca_snapshot_gc_run_items to lca_worker_runtime
      using (true) with check (true)';
  end if;
end
$move$;

revoke all
on private.lca_snapshot_gc_runs, private.lca_snapshot_gc_run_items
from public, anon, authenticated, lca_worker_runtime;
grant select, insert, update
on private.lca_snapshot_gc_runs, private.lca_snapshot_gc_run_items
to lca_worker_runtime;

do $rewrite$
declare
  v_routine record;
  v_definition text;
begin
  for v_routine in
    select procedure.oid
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace
      on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'util'
      and procedure.proname in (
        'preview_lca_snapshot_retention',
        'list_lca_snapshot_gc_candidates_without_closure_protection'
      )
      and pg_catalog.md5(pg_catalog.pg_get_functiondef(procedure.oid)) in (
        '5dba44e77a3b5c1d731e702d8b819c58',
        '9a40c7ae36a70b8eae41f355acaaee4e'
      )
  loop
    v_definition := pg_catalog.pg_get_functiondef(v_routine.oid);
    v_definition := pg_catalog.replace(
      v_definition,
      'public.lca_active_snapshots',
      'private.lca_active_snapshots'
    );
    v_definition := pg_catalog.replace(
      v_definition,
      'public.lca_network_snapshots',
      'private.lca_network_snapshots'
    );
    v_definition := pg_catalog.replace(
      v_definition,
      'public.lca_snapshot_artifacts',
      'private.lca_snapshot_artifacts'
    );
    execute v_definition;
  end loop;
end
$rewrite$;

create or replace view public.lca_snapshot_gc_runs
with (security_invoker = true) as
select id, mode, status, started_at, finished_at, as_of,
       snapshot_retention_window, orphan_retention_window, max_snapshots,
       max_orphan_dirs, max_bytes, candidate_snapshot_count,
       candidate_orphan_dir_count, candidate_object_count,
       candidate_storage_bytes, storage_deleted_count, storage_failed_count,
       db_snapshot_deleted_count, diagnostics, created_at
from private.lca_snapshot_gc_runs;

create or replace view public.lca_snapshot_gc_run_items
with (security_invoker = true) as
select id, run_id, candidate_type, snapshot_id, bucket_id, object_name,
       storage_bytes, reason, delete_db_snapshot, action_status, error_message,
       created_at, updated_at
from private.lca_snapshot_gc_run_items;

alter view public.lca_snapshot_gc_runs owner to postgres;
alter view public.lca_snapshot_gc_run_items owner to postgres;

revoke all
on public.lca_snapshot_gc_runs, public.lca_snapshot_gc_run_items
from public, anon, authenticated, service_role, api_internal_executor,
     lca_worker_runtime;
grant all
on public.lca_snapshot_gc_runs, public.lca_snapshot_gc_run_items
to service_role;
grant select
on public.lca_snapshot_gc_runs, public.lca_snapshot_gc_run_items
to api_internal_executor;

comment on table private.lca_snapshot_gc_runs is
  'Issue #414 canonical snapshot retention/GC run audit table; direct DML is restricted to approved service/Worker identities.';
comment on table private.lca_snapshot_gc_run_items is
  'Issue #414 canonical per-object snapshot retention/GC audit table; direct DML is restricted to approved service/Worker identities.';
comment on view public.lca_snapshot_gc_runs is
  'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_runs; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.';
comment on view public.lca_snapshot_gc_run_items is
  'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_run_items; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.';

do $postflight$
declare
  v_before record;
  v_after_count bigint;
  v_after_hash text;
  v_runs_fingerprint text;
  v_items_fingerprint text;
begin
  v_runs_fingerprint := pg_temp.issue_414_relation_fingerprint(
    'private.lca_snapshot_gc_runs'::pg_catalog.regclass
  );
  v_items_fingerprint := pg_temp.issue_414_relation_fingerprint(
    'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
  );

  if v_runs_fingerprint <> 'eddc56d22585cb6af9562b551afb06e8'
     or v_items_fingerprint <> '8b472ff342e5d2ad59a59dfda0df884e' then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'Issue 414 postflight relation fingerprint drifted: runs=%s items=%s',
        v_runs_fingerprint,
        v_items_fingerprint
      );
  end if;

  for v_before in select * from issue_414_before loop
    execute pg_catalog.format(
      'select count(*), pg_catalog.md5(coalesce(pg_catalog.string_agg(
         pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text), ''''
         order by source_row.id
       ), '''')) from private.%I source_row',
      v_before.relation_name
    ) into v_after_count, v_after_hash;

    if v_before.relation_oid <>
         pg_catalog.to_regclass(
           'private.' || v_before.relation_name
         )::oid
       or v_before.row_count <> v_after_count
       or v_before.content_hash <> v_after_hash then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'Issue 414 identity/data parity failed for %s',
          v_before.relation_name
        );
    end if;
  end loop;

  if not pg_catalog.has_table_privilege(
       'lca_worker_runtime',
       'private.lca_snapshot_gc_runs',
       'SELECT,INSERT,UPDATE'
     )
     or not pg_catalog.has_table_privilege(
       'lca_worker_runtime',
       'private.lca_snapshot_gc_run_items',
       'SELECT,INSERT,UPDATE'
     )
     or pg_catalog.has_table_privilege(
       'lca_worker_runtime',
       'private.lca_snapshot_gc_runs',
       'DELETE,TRUNCATE,REFERENCES,TRIGGER'
     )
     or pg_catalog.has_table_privilege(
       'lca_worker_runtime',
       'private.lca_snapshot_gc_run_items',
       'DELETE,TRUNCATE,REFERENCES,TRIGGER'
     ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 lca_worker_runtime relation ACL drifted';
  end if;

  if pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
     )) <> '464fb33486848ccb0ee9f13bf82a1eef'
     or pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
     )) <> 'a6b39a91a33df29ef1db632563a0484d' then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 util routine rewrite drifted';
  end if;
end
$postflight$;

notify pgrst, 'reload schema';

commit;
