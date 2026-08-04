-- Issue #407 Phase B: move the document-validation evidence cache to the
-- private physical boundary while retaining one explicit public compatibility
-- view.  ALTER TABLE ... SET SCHEMA preserves the table OID, data, indexes,
-- constraints, RLS state, and foreign-key identity; there is no copy and no
-- dual write.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  v_public_table_count integer;
  v_public_view_count integer;
  v_private_table_count integer;
  v_column_names text[];
  v_catalog_fingerprint text;
  v_named_routine_count integer;
  v_exact_routine_count integer;
  v_named_public_routine_count integer;
  v_exact_public_routine_count integer;
begin
  if not exists (
    select 1
    from supabase_migrations.schema_migrations
    where version = '20260803163000'
  ) then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B requires Phase A migration 20260803163000';
  end if;

  select
    count(*) filter (where namespace.nspname = 'public' and relation.relkind = 'r'),
    count(*) filter (where namespace.nspname = 'public' and relation.relkind = 'v'),
    count(*) filter (where namespace.nspname = 'private' and relation.relkind = 'r')
  into v_public_table_count, v_public_view_count, v_private_table_count
  from pg_catalog.pg_class relation
  join pg_catalog.pg_namespace namespace
    on namespace.oid = relation.relnamespace
  where namespace.nspname in ('public', 'private')
    and relation.relname = 'lcia_document_validation_evidence';

  if (v_public_table_count, v_public_view_count, v_private_table_count)
     not in ((1, 0, 0), (0, 1, 1)) then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'issue 407 Phase B mixed relation state: publicTable=%s publicView=%s privateTable=%s',
        v_public_table_count, v_public_view_count, v_private_table_count
      );
  end if;

  if v_public_view_count = 1 and exists (
    select 1
    from pg_catalog.pg_class relation
    where relation.oid =
      'public.lcia_document_validation_evidence'::pg_catalog.regclass
      and (
        relation.relowner <> 'postgres'::pg_catalog.regrole
        or relation.reloptions is distinct from
          array['security_invoker=true']::text[]
        or relation.relacl::text is distinct from
          '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
        or pg_catalog.obj_description(relation.oid, 'pg_class') is distinct from
          'Issue #407 Expand compatibility view; canonical=private.lcia_document_validation_evidence; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.'
        or pg_catalog.md5(pg_catalog.pg_get_viewdef(relation.oid, true)) <>
          'bee04ae1ec41afe2d87957fe98bd300e'
      )
  ) then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B retry compatibility view drifted';
  end if;

  if v_public_table_count = 1 then
    if exists (
      select 1
      from pg_catalog.pg_class relation
      where relation.oid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
        and (
          relation.relowner <> 'postgres'::pg_catalog.regrole
          or not relation.relrowsecurity
          or relation.relforcerowsecurity
          or relation.relacl::text is distinct from
            '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
        )
    ) then
      raise exception using
        errcode = '55000',
        message = 'issue 407 Phase B predecessor owner/RLS/ACL drifted';
    end if;

    select pg_catalog.array_agg(attribute.attname order by attribute.attnum)
    into v_column_names
    from pg_catalog.pg_attribute attribute
    where attribute.attrelid =
      'public.lcia_document_validation_evidence'::pg_catalog.regclass
      and attribute.attnum > 0
      and not attribute.attisdropped;

    if v_column_names is distinct from array[
      'id',
      'dataset_type',
      'dataset_id',
      'dataset_version',
      'canonical_content_hash',
      'document_validator_version',
      'document_validation_profile',
      'validation_report_schema_version',
      'validator_engine_fingerprint',
      'tidas_schema_lock_sha256',
      'status',
      'summary',
      'issue_artifact_ref',
      'issue_artifact_hash',
      'source_worker_job_id',
      'created_at'
    ]::text[] then
      raise exception using
        errcode = '55000',
        message = 'issue 407 Phase B predecessor column identity drifted';
    end if;
  end if;

  with target as (
    select pg_catalog.to_regclass(case
      when v_public_table_count = 1
        then 'public.lcia_document_validation_evidence'
      else 'private.lcia_document_validation_evidence'
    end) as oid
  ), catalog as (
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
        coalesce(
          pg_catalog.pg_get_expr(relation.relpartbound, relation.oid), ''
        ),
        coalesce(relation.relacl::text, ''),
        coalesce(
          pg_catalog.obj_description(relation.oid, 'pg_class'), ''
        )
      ) as value
    from pg_catalog.pg_class relation
    join target on target.oid = relation.oid
    left join pg_catalog.pg_am access_method
      on access_method.oid = relation.relam
    left join pg_catalog.pg_tablespace tablespace
      on tablespace.oid = relation.reltablespace
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
        coalesce(
          pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid), ''
        ),
        coalesce(attribute.attacl::text, ''),
        coalesce(
          pg_catalog.col_description(attribute.attrelid, attribute.attnum), ''
        )
      )
    from pg_catalog.pg_attribute attribute
    join target on target.oid = attribute.attrelid
    left join pg_catalog.pg_attrdef default_value
      on default_value.adrelid = attribute.attrelid
     and default_value.adnum = attribute.attnum
    left join pg_catalog.pg_collation collation_row
      on collation_row.oid = attribute.attcollation
    where attribute.attnum > 0 and not attribute.attisdropped
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
    join target on target.oid = constraint_row.conrelid
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
        coalesce(
          pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid), ''
        ),
        coalesce(
          pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid), ''
        ),
        pg_catalog.pg_get_indexdef(index_row.indexrelid)
      )
    from pg_catalog.pg_index index_row
    join target on target.oid = index_row.indrelid
    join pg_catalog.pg_class index_relation
      on index_relation.oid = index_row.indexrelid
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
    join target on target.oid = trigger_row.tgrelid
    left join pg_catalog.pg_constraint constraint_row
      on constraint_row.oid = trigger_row.tgconstraint
    union all
    select 'policy',
      pg_catalog.concat_ws('|',
        policy.polname,
        policy.polcmd,
        policy.polpermissive,
        policy.polroles::text,
        coalesce(
          pg_catalog.pg_get_expr(policy.polqual, policy.polrelid), ''
        ),
        coalesce(
          pg_catalog.pg_get_expr(policy.polwithcheck, policy.polrelid), ''
        )
      )
    from pg_catalog.pg_policy policy
    join target on target.oid = policy.polrelid
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
    cross join target
    left join pg_catalog.pg_publication_rel publication_relation
      on publication_relation.prpubid = publication.oid
     and publication_relation.prrelid = target.oid
    where publication.puballtables
       or publication_relation.prrelid is not null
  )
  select pg_catalog.md5(
    pg_catalog.string_agg(
      catalog.category || pg_catalog.chr(31) || catalog.value,
      E'\n' order by catalog.category, catalog.value
    )
  ) into v_catalog_fingerprint
  from catalog;

  if v_catalog_fingerprint is distinct from (case
    when v_public_table_count = 1 then '13987c5504a3eae73c07533b3a3e39db'
    else '4633234c541c50b1dd6bcc7dbb2e57d5'
  end) then
    raise exception using
      errcode = '55000',
      message = pg_catalog.format(
        'issue 407 Phase B relation catalog fingerprint drifted: %s',
        v_catalog_fingerprint
      );
  end if;

  select count(*) into v_named_routine_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace
    on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'private'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  select count(*) into v_exact_routine_count
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace
      on namespace.oid = procedure.pronamespace
    join pg_catalog.pg_language language
      on language.oid = procedure.prolang
    where namespace.nspname = 'private'
      and (
        (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
         and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_cache_keys jsonb')
        or
        (procedure.proname = 'svc_lcia_document_validation_evidence_record'
         and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid')
      )
      and procedure.proowner = 'postgres'::pg_catalog.regrole
      and language.lanname = 'plpgsql'
      and procedure.prokind = 'f'
      and procedure.prorettype = 'pg_catalog.jsonb'::pg_catalog.regtype
      and not procedure.proretset
      and procedure.provolatile = 'v'
      and procedure.proparallel = 'u'
      and not procedure.proisstrict
      and not procedure.proleakproof
      and procedure.prosecdef
      and procedure.proconfig =
        array['search_path=pg_catalog, pg_temp']::text[]
      and procedure.proacl::text =
        '{postgres=X/postgres,lca_worker_runtime=X/postgres}'
      and procedure.pronargdefaults = case
        when procedure.proname =
          'svc_lcia_document_validation_evidence_lookup' then 0
        else 1 end
      and pg_catalog.pg_get_function_arguments(procedure.oid) = case
        when procedure.proname =
          'svc_lcia_document_validation_evidence_lookup'
          then 'p_cache_keys jsonb'
        else 'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid'
      end
      and pg_catalog.md5(procedure.prosrc) = case
        when v_public_table_count = 1 and procedure.proname =
          'svc_lcia_document_validation_evidence_lookup'
          then 'bd277cd343a10462fc536a64390459c5'
        when v_public_table_count = 1
          then '2759f5215c8dd4b253db2ed2264cc8ab'
        when procedure.proname =
          'svc_lcia_document_validation_evidence_lookup'
          then '910ebb77d0b5335d5138dfe09a38f881'
        else '72eb5d600ed803ee77a474c5ef8baf06'
      end
      and pg_catalog.obj_description(procedure.oid, 'pg_proc') = case
        when v_public_table_count = 1 and procedure.proname =
          'svc_lcia_document_validation_evidence_lookup'
          then 'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.'
        when v_public_table_count = 1
          then 'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
        when procedure.proname =
          'svc_lcia_document_validation_evidence_lookup'
          then 'Issue #407 Phase B canonical Worker lookup over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime.'
        else 'Issue #407 Phase B canonical Worker idempotent record command over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
      end;

  if v_named_routine_count <> 2 or v_exact_routine_count <> 2 then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B private canonical routine definition or ACL drifted';
  end if;

  select count(*) into v_named_public_routine_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace
    on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'public'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  select count(*) into v_exact_public_routine_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace
    on namespace.oid = procedure.pronamespace
  join pg_catalog.pg_language language
    on language.oid = procedure.prolang
  where namespace.nspname = 'public'
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
         'p_cache_keys jsonb')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
         'p_records jsonb, p_source_worker_job_id uuid')
    )
    and procedure.proowner = 'postgres'::pg_catalog.regrole
    and language.lanname = 'plpgsql'
    and procedure.prokind = 'f'
    and procedure.prorettype = 'pg_catalog.jsonb'::pg_catalog.regtype
    and not procedure.proretset
    and procedure.provolatile = 'v'
    and procedure.proparallel = 'u'
    and not procedure.proisstrict
    and not procedure.proleakproof
    and procedure.prosecdef
    and procedure.proconfig =
      array['search_path=pg_catalog, pg_temp']::text[]
    and procedure.proacl::text =
      '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
    and pg_catalog.md5(procedure.prosrc) = case
      when procedure.proname =
        'svc_lcia_document_validation_evidence_lookup'
        then '6f6fb65152a4125c25babc79397d1626'
      else 'efd249089fa40ea58fe8efe3e1e894b0'
    end
    and pg_catalog.obj_description(procedure.oid, 'pg_proc') =
      'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.';

  if v_named_public_routine_count <> 2
     or v_exact_public_routine_count <> 2 then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B public compatibility routine definition or ACL drifted';
  end if;
end
$preflight$;

-- Acquire the metadata-changing lock before sampling OID/catalog/data state.
-- This prevents a concurrent committed write between the snapshot and the
-- namespace move from producing a false row-count mismatch.
do $lock_source$
begin
  if exists (
    select 1
    from pg_catalog.pg_class relation
    where relation.oid =
      'public.lcia_document_validation_evidence'::pg_catalog.regclass
      and relation.relkind = 'r'
  ) then
    lock table public.lcia_document_validation_evidence
      in access exclusive mode;
  end if;
end
$lock_source$;

create temporary table issue_407_phase_b_relation_before on commit drop as
select
  relation.oid,
  relation.reltype,
  relation.relowner,
  relation.relrowsecurity,
  relation.relforcerowsecurity,
  relation.relreplident,
  relation.relacl::text as acl,
  pg_catalog.obj_description(relation.oid, 'pg_class') as relation_comment,
  (select count(*) from public.lcia_document_validation_evidence) as row_count
from pg_catalog.pg_class relation
where relation.oid =
  'public.lcia_document_validation_evidence'::pg_catalog.regclass
  and relation.relkind = 'r';

do $move$
begin
  if pg_catalog.to_regclass('public.lcia_document_validation_evidence') is not null
     and exists (
       select 1
       from pg_catalog.pg_class relation
       where relation.oid =
         'public.lcia_document_validation_evidence'::pg_catalog.regclass
         and relation.relkind = 'r'
     ) then
    alter table public.lcia_document_validation_evidence set schema private;
  end if;
end
$move$;

create or replace view public.lcia_document_validation_evidence
with (security_invoker = true) as
select
  id,
  dataset_type,
  dataset_id,
  dataset_version,
  canonical_content_hash,
  document_validator_version,
  document_validation_profile,
  validation_report_schema_version,
  validator_engine_fingerprint,
  tidas_schema_lock_sha256,
  status,
  summary,
  issue_artifact_ref,
  issue_artifact_hash,
  source_worker_job_id,
  created_at
from private.lcia_document_validation_evidence;

alter view public.lcia_document_validation_evidence owner to postgres;

revoke all on public.lcia_document_validation_evidence
from public, anon, authenticated, service_role, api_internal_executor,
  lca_worker_runtime;
grant all on public.lcia_document_validation_evidence to service_role;
grant select on public.lcia_document_validation_evidence
to api_internal_executor;

comment on table private.lcia_document_validation_evidence is
  'Issue #407 Phase B canonical document-validation evidence cache. Worker access is mediated by the two private routines; the predecessor service ACL is preserved and no Worker relation grant is added.';
comment on view public.lcia_document_validation_evidence is
  'Issue #407 Expand compatibility view; canonical=private.lcia_document_validation_evidence; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.';

-- Rewrite only the two reviewed private canonical routine bodies.  The
-- functions retain their OIDs, owners, ACLs, arguments, fixed search paths,
-- comments, and public wrappers; only the physical relation qualification
-- changes from public to private.
do $rewrite$
declare
  v_routine record;
  v_definition text;
begin
  for v_routine in
    select procedure.oid,
           procedure.oid::pg_catalog.regprocedure::text as identity,
           pg_catalog.pg_get_functiondef(procedure.oid) as definition
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace
      on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'private'
      and (
        (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
         and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_cache_keys jsonb')
        or
        (procedure.proname = 'svc_lcia_document_validation_evidence_record'
         and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid')
      )
    order by procedure.oid::pg_catalog.regprocedure::text
  loop
    if pg_catalog.strpos(
      v_routine.definition,
      'public.lcia_document_validation_evidence'
    ) = 0 then
      if pg_catalog.strpos(
        v_routine.definition,
        'private.lcia_document_validation_evidence'
      ) = 0 then
        raise exception using
          errcode = '55000',
          message = pg_catalog.format(
            'issue 407 Phase B routine %s has no reviewed relation reference',
            v_routine.identity
          );
      end if;
      continue;
    end if;

    v_definition := pg_catalog.replace(
      v_routine.definition,
      'public.lcia_document_validation_evidence',
      'private.lcia_document_validation_evidence'
    );
    execute v_definition;
  end loop;
end
$rewrite$;

comment on function private.svc_lcia_document_validation_evidence_lookup(jsonb) is
  'Issue #407 Phase B canonical Worker lookup over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime.';
comment on function private.svc_lcia_document_validation_evidence_record(jsonb, uuid) is
  'Issue #407 Phase B canonical Worker idempotent record command over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.';

do $postflight$
declare
  v_before issue_407_phase_b_relation_before%rowtype;
  v_private pg_catalog.pg_class%rowtype;
  v_public pg_catalog.pg_class%rowtype;
  v_private_row_count bigint;
begin
  select * into v_before
  from issue_407_phase_b_relation_before;

  select relation.* into strict v_private
  from pg_catalog.pg_class relation
  join pg_catalog.pg_namespace namespace
    on namespace.oid = relation.relnamespace
  where namespace.nspname = 'private'
    and relation.relname = 'lcia_document_validation_evidence'
    and relation.relkind = 'r';

  select relation.* into strict v_public
  from pg_catalog.pg_class relation
  join pg_catalog.pg_namespace namespace
    on namespace.oid = relation.relnamespace
  where namespace.nspname = 'public'
    and relation.relname = 'lcia_document_validation_evidence'
    and relation.relkind = 'v';

  if v_before.oid is not null and (
    v_private.oid <> v_before.oid
    or v_private.reltype <> v_before.reltype
    or v_private.relowner <> v_before.relowner
    or v_private.relrowsecurity <> v_before.relrowsecurity
    or v_private.relforcerowsecurity <> v_before.relforcerowsecurity
    or v_private.relreplident <> v_before.relreplident
    or v_private.relacl::text is distinct from v_before.acl
  ) then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B OID/RLS/ACL identity changed during move';
  end if;

  select count(*) into v_private_row_count
  from private.lcia_document_validation_evidence;
  if v_before.oid is not null
     and v_private_row_count <> v_before.row_count then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B row count changed during metadata-only move';
  end if;

  if not (v_public.reloptions @> array['security_invoker=true']) then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B public compatibility is not security_invoker';
  end if;

  if v_public.relacl::text is distinct from
    '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
  then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B compatibility ACL drifted';
  end if;

  if (
    select count(*)
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace
      on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'private'
      and procedure.proname in (
        'svc_lcia_document_validation_evidence_lookup',
        'svc_lcia_document_validation_evidence_record'
      )
      and pg_catalog.pg_get_functiondef(procedure.oid) like
        '%private.lcia_document_validation_evidence%'
      and pg_catalog.pg_get_functiondef(procedure.oid) not like
        '%public.lcia_document_validation_evidence%'
      and procedure.proacl::text =
        '{postgres=X/postgres,lca_worker_runtime=X/postgres}'
      and procedure.proconfig =
        array['search_path=pg_catalog, pg_temp']::text[]
  ) <> 2 then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B private routines did not bind the private relation';
  end if;

  if pg_catalog.has_schema_privilege('anon', 'private', 'USAGE')
     or pg_catalog.has_schema_privilege('authenticated', 'private', 'USAGE')
     or pg_catalog.has_table_privilege(
       'lca_worker_runtime',
       'private.lcia_document_validation_evidence',
       'SELECT,INSERT,UPDATE,DELETE'
     ) then
    raise exception using
      errcode = '55000',
      message = 'issue 407 Phase B private boundary leaked to browser/Worker relation access';
  end if;
end
$postflight$;

notify pgrst, 'reload schema';

commit;
