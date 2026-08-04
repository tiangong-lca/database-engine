-- Emergency rollback for Issue #414 physical Expand.  The Supabase migration
-- ledger is intentionally left unchanged; use the paired explicit roll-forward
-- operator to restore the private physical layout.

\set ON_ERROR_STOP on

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
    select 'column', pg_catalog.concat_ws('|',
      attribute.attnum, attribute.attname,
      pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
      attribute.attnotnull, attribute.attidentity, attribute.attgenerated,
      attribute.attstorage, attribute.attcompression, attribute.attstattarget,
      coalesce(attribute.attoptions::text, ''), attribute.atthasmissing,
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
      and attribute.attnum > 0 and not attribute.attisdropped
    union all
    select 'constraint', pg_catalog.concat_ws('|',
      constraint_row.conname, constraint_row.contype,
      constraint_row.condeferrable, constraint_row.condeferred,
      constraint_row.convalidated, coalesce(constraint_row.conkey::text, ''),
      coalesce(constraint_row.confrelid::pg_catalog.regclass::text, ''),
      coalesce(constraint_row.confkey::text, ''),
      pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
    )
    from pg_catalog.pg_constraint constraint_row
    where constraint_row.conrelid = p_relation
    union all
    select 'index', pg_catalog.concat_ws('|',
      index_relation.relname, index_row.indisunique, index_row.indisprimary,
      index_row.indisvalid, index_row.indisready, index_row.indisreplident,
      index_row.indisclustered, coalesce(index_row.indkey::text, ''),
      coalesce(pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid), ''),
      coalesce(pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid), ''),
      pg_catalog.pg_get_indexdef(index_row.indexrelid)
    )
    from pg_catalog.pg_index index_row
    join pg_catalog.pg_class index_relation
      on index_relation.oid = index_row.indexrelid
    where index_row.indrelid = p_relation
    union all
    select 'trigger', pg_catalog.concat_ws('|',
      case when trigger_row.tgisinternal
        then coalesce(constraint_row.conname, '<internal>')
        else trigger_row.tgname end,
      trigger_row.tgenabled, trigger_row.tgisinternal, trigger_row.tgtype,
      trigger_row.tgfoid::pg_catalog.regprocedure,
      case when trigger_row.tgisinternal then ''
        else pg_catalog.pg_get_triggerdef(trigger_row.oid, true) end
    )
    from pg_catalog.pg_trigger trigger_row
    left join pg_catalog.pg_constraint constraint_row
      on constraint_row.oid = trigger_row.tgconstraint
    where trigger_row.tgrelid = p_relation
    union all
    select 'policy', pg_catalog.concat_ws('|',
      policy.polname, policy.polcmd, policy.polpermissive,
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
    select 'publication', pg_catalog.concat_ws('|',
      publication.pubname, publication.puballtables, publication.pubinsert,
      publication.pubupdate, publication.pubdelete, publication.pubtruncate,
      publication.pubviaroot,
      coalesce(pg_catalog.pg_get_expr(
        publication_relation.prqual, publication_relation.prrelid
      ), ''),
      coalesce(publication_relation.prattrs::text, '')
    )
    from pg_catalog.pg_publication publication
    left join pg_catalog.pg_publication_rel publication_relation
      on publication_relation.prpubid = publication.oid
     and publication_relation.prrelid = p_relation
    where publication.puballtables
       or publication_relation.prrelid is not null
  )
  select pg_catalog.md5(pg_catalog.string_agg(
    category || pg_catalog.chr(31) || value,
    E'\n' order by category, value
  )) from catalog
$function$;

do $state$
begin
  if pg_catalog.to_regclass('private.lca_snapshot_gc_runs') is null
     or pg_catalog.to_regclass('private.lca_snapshot_gc_run_items') is null
     or not exists (
       select 1 from pg_catalog.pg_class relation
       join pg_catalog.pg_namespace namespace
         on namespace.oid = relation.relnamespace
       where namespace.nspname = 'public'
         and relation.relname = 'lca_snapshot_gc_runs'
         and relation.relkind = 'v'
     )
     or not exists (
       select 1 from pg_catalog.pg_class relation
       join pg_catalog.pg_namespace namespace
         on namespace.oid = relation.relnamespace
       where namespace.nspname = 'public'
         and relation.relname = 'lca_snapshot_gc_run_items'
         and relation.relkind = 'v'
     ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 rollback requires the complete expanded topology';
  end if;
end
$state$;

lock table private.lca_snapshot_gc_runs,
  private.lca_snapshot_gc_run_items in access exclusive mode;

do $preflight$
begin
  if pg_temp.issue_414_relation_fingerprint(
       'private.lca_snapshot_gc_runs'::pg_catalog.regclass
     ) <> 'eddc56d22585cb6af9562b551afb06e8'
     or pg_temp.issue_414_relation_fingerprint(
       'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
     ) <> '8b472ff342e5d2ad59a59dfda0df884e'
     or pg_catalog.md5(pg_catalog.pg_get_viewdef(
       'public.lca_snapshot_gc_runs'::pg_catalog.regclass, true
     )) <> '55ead76979a9ca06d88a5dceeedbff87'
     or pg_catalog.md5(pg_catalog.pg_get_viewdef(
       'public.lca_snapshot_gc_run_items'::pg_catalog.regclass, true
     )) <> 'bce1e04e51db0ffb73ca32e7f259bc03'
     or pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
     )) <> '464fb33486848ccb0ee9f13bf82a1eef'
     or pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
     )) <> 'a6b39a91a33df29ef1db632563a0484d' then
    raise exception using
      errcode = '55000',
      message = 'Issue 414 rollback preflight catalog drifted';
  end if;
end
$preflight$;

create temporary table issue_414_rollback_before (
  relation_name text primary key,
  relation_oid oid not null,
  row_count bigint not null,
  content_hash text not null
) on commit drop;

do $snapshot$
declare
  v_relation text;
begin
  foreach v_relation in array array[
    'lca_snapshot_gc_runs',
    'lca_snapshot_gc_run_items'
  ] loop
    execute pg_catalog.format(
      'insert into issue_414_rollback_before
       select %L, %L::pg_catalog.regclass::oid, count(*),
         pg_catalog.md5(coalesce(pg_catalog.string_agg(
           pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text), ''''
           order by source_row.id
         ), ''''))
       from private.%I source_row',
      v_relation,
      'private.' || v_relation,
      v_relation
    );
  end loop;
end
$snapshot$;

drop view public.lca_snapshot_gc_run_items;
drop view public.lca_snapshot_gc_runs;

drop policy lca_snapshot_gc_run_items_worker_runtime_all
  on private.lca_snapshot_gc_run_items;
drop policy lca_snapshot_gc_runs_worker_runtime_all
  on private.lca_snapshot_gc_runs;

revoke all
on private.lca_snapshot_gc_runs, private.lca_snapshot_gc_run_items
from lca_worker_runtime;

alter table private.lca_snapshot_gc_run_items set schema public;
alter table private.lca_snapshot_gc_runs set schema public;

grant select, insert, update, delete, truncate, references, trigger
on public.lca_snapshot_gc_runs, public.lca_snapshot_gc_run_items
to anon, authenticated;

comment on table public.lca_snapshot_gc_runs is
  'Audit header for worker-driven lca-results/snapshots object-aware garbage collection runs.';
comment on table public.lca_snapshot_gc_run_items is
  'Per-object audit items for worker-driven lca-results/snapshots object-aware garbage collection runs.';

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
  loop
    v_definition := pg_catalog.pg_get_functiondef(v_routine.oid);
    v_definition := pg_catalog.replace(
      v_definition,
      'private.lca_active_snapshots',
      'public.lca_active_snapshots'
    );
    v_definition := pg_catalog.replace(
      v_definition,
      'private.lca_network_snapshots',
      'public.lca_network_snapshots'
    );
    v_definition := pg_catalog.replace(
      v_definition,
      'private.lca_snapshot_artifacts',
      'public.lca_snapshot_artifacts'
    );
    execute v_definition;
  end loop;
end
$rewrite$;

do $postflight$
declare
  v_before record;
  v_after_count bigint;
  v_after_hash text;
  v_runs_fingerprint text;
  v_items_fingerprint text;
begin
  v_runs_fingerprint := pg_temp.issue_414_relation_fingerprint(
    'public.lca_snapshot_gc_runs'::pg_catalog.regclass
  );
  v_items_fingerprint := pg_temp.issue_414_relation_fingerprint(
    'public.lca_snapshot_gc_run_items'::pg_catalog.regclass
  );

  if v_runs_fingerprint <> 'ad841baccb43a081d8d4bfd0c5599d4f'
     or v_items_fingerprint <> '43e5174ff4856098340d2bc5e638b40d'
     or pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
     )) <> '5dba44e77a3b5c1d731e702d8b819c58'
     or pg_catalog.md5(pg_catalog.pg_get_functiondef(
       'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
     )) <> '9a40c7ae36a70b8eae41f355acaaee4e' then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'Issue 414 rollback postflight catalog drifted: runs=%s items=%s',
        v_runs_fingerprint,
        v_items_fingerprint
      );
  end if;

  for v_before in select * from issue_414_rollback_before loop
    execute pg_catalog.format(
      'select count(*), pg_catalog.md5(coalesce(pg_catalog.string_agg(
         pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text), ''''
         order by source_row.id
       ), '''')) from public.%I source_row',
      v_before.relation_name
    ) into v_after_count, v_after_hash;

    if v_before.relation_oid <>
         pg_catalog.to_regclass('public.' || v_before.relation_name)::oid
       or v_before.row_count <> v_after_count
       or v_before.content_hash <> v_after_hash then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'Issue 414 rollback identity/data parity failed for %s',
          v_before.relation_name
        );
    end if;
  end loop;
end
$postflight$;

notify pgrst, 'reload schema';
commit;
