-- Emergency rollback for Issue #407 Phase B physical Expand.
-- This restores the Phase A physical layout without changing the migration
-- ledger. Roll forward with the paired physical Expand roll-forward operator;
-- ordinary Supabase migration replay will skip an already-ledgered version.

\set ON_ERROR_STOP on

begin;
set local lock_timeout = '5s';
set local statement_timeout = '2min';

-- Freeze writers before sampling identity/catalog/data state.
lock table private.lcia_document_validation_evidence
  in access exclusive mode;

create temporary table issue_407_phase_b_rollback_before on commit drop as
select relation.oid,
       relation.reltype,
       relation.relowner,
       relation.relrowsecurity,
       relation.relforcerowsecurity,
       relation.relreplident,
       relation.relacl,
       (select count(*)
        from private.lcia_document_validation_evidence) as row_count
from pg_class relation
where relation.oid = 'private.lcia_document_validation_evidence'::regclass;

do $preflight$
declare
  v_named_routines integer;
  v_exact_routines integer;
  v_named_public_routines integer;
  v_exact_public_routines integer;
  v_column_hash text;
  v_constraint_hash text;
  v_index_hash text;
  v_trigger_hash text;
  v_policy_hash text;
  v_publication_hash text;
begin
  if not exists (
    select 1
    from pg_class relation
    join pg_namespace namespace on namespace.oid = relation.relnamespace
    where namespace.nspname = 'private'
      and relation.relname = 'lcia_document_validation_evidence'
      and relation.relkind = 'r'
      and relation.relowner = 'postgres'::regrole
      and relation.relrowsecurity
      and not relation.relforcerowsecurity
      and relation.relpersistence = 'p'
      and relation.relreplident = 'd'
      and relation.reloptions is null
      and not relation.relispartition
      and relation.relacl::text =
        '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
      and obj_description(relation.oid, 'pg_class') =
        'Issue #407 Phase B canonical document-validation evidence cache. Worker access is mediated by the two private routines; the predecessor service ACL is preserved and no Worker relation grant is added.'
  ) or not exists (
    select 1
    from pg_class relation
    join pg_namespace namespace on namespace.oid = relation.relnamespace
    where namespace.nspname = 'public'
      and relation.relname = 'lcia_document_validation_evidence'
      and relation.relkind = 'v'
      and relation.relowner = 'postgres'::regrole
      and relation.reloptions = array['security_invoker=true']::text[]
      and relation.relacl::text =
        '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
      and obj_description(relation.oid, 'pg_class') =
        'Issue #407 Expand compatibility view; canonical=private.lcia_document_validation_evidence; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.'
      and md5(pg_get_viewdef(relation.oid, true)) =
        'bee04ae1ec41afe2d87957fe98bd300e'
  ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback requires the exact table/view topology';
  end if;

  select md5(string_agg(concat_ws('|',
    attribute.attnum,
    attribute.attname,
    format_type(attribute.atttypid, attribute.atttypmod),
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
    coalesce(pg_get_expr(default_value.adbin, default_value.adrelid), ''),
    coalesce(attribute.attacl::text, ''),
    coalesce(col_description(attribute.attrelid, attribute.attnum), '')
  ), E'\n' order by attribute.attnum)) into v_column_hash
  from pg_attribute attribute
  left join pg_attrdef default_value
    on default_value.adrelid = attribute.attrelid
   and default_value.adnum = attribute.attnum
  left join pg_collation collation_row
    on collation_row.oid = attribute.attcollation
  where attribute.attrelid =
      'private.lcia_document_validation_evidence'::regclass
    and attribute.attnum > 0
    and not attribute.attisdropped;

  select md5(string_agg(concat_ws('|',
    constraint_row.conname,
    constraint_row.contype,
    constraint_row.condeferrable,
    constraint_row.condeferred,
    constraint_row.convalidated,
    coalesce(constraint_row.conkey::text, ''),
    coalesce(constraint_row.confrelid::regclass::text, ''),
    coalesce(constraint_row.confkey::text, ''),
    pg_get_constraintdef(constraint_row.oid, true)
  ), E'\n' order by constraint_row.conname)) into v_constraint_hash
  from pg_constraint constraint_row
  where constraint_row.conrelid =
    'private.lcia_document_validation_evidence'::regclass;

  select md5(string_agg(concat_ws('|',
    index_relation.relname,
    index_row.indisunique,
    index_row.indisprimary,
    index_row.indisvalid,
    index_row.indisready,
    index_row.indisreplident,
    index_row.indisclustered,
    coalesce(index_row.indkey::text, ''),
    coalesce(pg_get_expr(index_row.indexprs, index_row.indrelid), ''),
    coalesce(pg_get_expr(index_row.indpred, index_row.indrelid), ''),
    pg_get_indexdef(index_row.indexrelid)
  ), E'\n' order by index_relation.relname)) into v_index_hash
  from pg_index index_row
  join pg_class index_relation on index_relation.oid = index_row.indexrelid
  where index_row.indrelid =
    'private.lcia_document_validation_evidence'::regclass;

  select coalesce(md5(string_agg(concat_ws('|',
    case when trigger_row.tgisinternal
      then coalesce(constraint_row.conname, '<internal>')
      else trigger_row.tgname end,
    trigger_row.tgenabled,
    trigger_row.tgisinternal,
    trigger_row.tgtype,
    trigger_row.tgfoid::regprocedure,
    case when trigger_row.tgisinternal then ''
      else pg_get_triggerdef(trigger_row.oid, true) end
  ), E'\n' order by
    case when trigger_row.tgisinternal
      then coalesce(constraint_row.conname, '<internal>')
      else trigger_row.tgname end)), md5('')) into v_trigger_hash
  from pg_trigger trigger_row
  left join pg_constraint constraint_row
    on constraint_row.oid = trigger_row.tgconstraint
  where trigger_row.tgrelid =
    'private.lcia_document_validation_evidence'::regclass;

  select coalesce(md5(string_agg(concat_ws('|',
    policy.polname,
    policy.polcmd,
    policy.polpermissive,
    policy.polroles::text,
    coalesce(pg_get_expr(policy.polqual, policy.polrelid), ''),
    coalesce(pg_get_expr(policy.polwithcheck, policy.polrelid), '')
  ), E'\n' order by policy.polname)), md5('')) into v_policy_hash
  from pg_policy policy
  where policy.polrelid =
    'private.lcia_document_validation_evidence'::regclass;

  select coalesce(md5(string_agg(concat_ws('|',
    publication.pubname,
    publication.puballtables,
    publication.pubinsert,
    publication.pubupdate,
    publication.pubdelete,
    publication.pubtruncate,
    publication.pubviaroot,
    coalesce(pg_get_expr(
      publication_relation.prqual,
      publication_relation.prrelid
    ), ''),
    coalesce(publication_relation.prattrs::text, '')
  ), E'\n' order by publication.pubname)), md5('')) into v_publication_hash
  from pg_publication publication
  left join pg_publication_rel publication_relation
    on publication_relation.prpubid = publication.oid
   and publication_relation.prrelid =
     'private.lcia_document_validation_evidence'::regclass
  where publication.puballtables
     or publication_relation.prrelid is not null;

  if (v_column_hash, v_constraint_hash, v_index_hash, v_trigger_hash,
      v_policy_hash, v_publication_hash) is distinct from (
    '7e13c103e977ea621879f569c80a7ada',
    '47acc98789b4d24dd98d4630ff400648',
    'd6ecb91ca57fa9461bec2593a38880ba',
    '4c205eb982499003ba45a780e05313a8',
    'd41d8cd98f00b204e9800998ecf8427e',
    'd41d8cd98f00b204e9800998ecf8427e'
  ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback refuses relation catalog drift';
  end if;

  select count(*) into v_named_routines
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'private'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  select count(*) into v_exact_routines
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  join pg_language language on language.oid = procedure.prolang
  where namespace.nspname = 'private'
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_get_function_identity_arguments(procedure.oid) =
         'p_cache_keys jsonb')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_get_function_identity_arguments(procedure.oid) =
         'p_records jsonb, p_source_worker_job_id uuid')
    )
    and procedure.proowner = 'postgres'::regrole
    and language.lanname = 'plpgsql'
    and procedure.prokind = 'f'
    and procedure.prorettype = 'jsonb'::regtype
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
    and md5(procedure.prosrc) = case
      when procedure.proname =
        'svc_lcia_document_validation_evidence_lookup'
        then '910ebb77d0b5335d5138dfe09a38f881'
      else '72eb5d600ed803ee77a474c5ef8baf06'
    end
    and obj_description(procedure.oid, 'pg_proc') = case
      when procedure.proname =
        'svc_lcia_document_validation_evidence_lookup'
        then 'Issue #407 Phase B canonical Worker lookup over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime.'
      else 'Issue #407 Phase B canonical Worker idempotent record command over private.lcia_document_validation_evidence. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
    end
    and pg_get_functiondef(procedure.oid) like
      '%private.lcia_document_validation_evidence%'
    and pg_get_functiondef(procedure.oid) not like
      '%public.lcia_document_validation_evidence%';

  if v_named_routines <> 2 or v_exact_routines <> 2 then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback refuses canonical routine drift';
  end if;

  select count(*) into v_named_public_routines
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'public'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  select count(*) into v_exact_public_routines
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  join pg_language language on language.oid = procedure.prolang
  where namespace.nspname = 'public'
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_get_function_identity_arguments(procedure.oid) =
         'p_cache_keys jsonb')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_get_function_identity_arguments(procedure.oid) =
         'p_records jsonb, p_source_worker_job_id uuid')
    )
    and procedure.proowner = 'postgres'::regrole
    and language.lanname = 'plpgsql'
    and procedure.prokind = 'f'
    and procedure.prorettype = 'jsonb'::regtype
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
    and md5(procedure.prosrc) = case
      when procedure.proname =
        'svc_lcia_document_validation_evidence_lookup'
        then '6f6fb65152a4125c25babc79397d1626'
      else 'efd249089fa40ea58fe8efe3e1e894b0'
    end
    and obj_description(procedure.oid, 'pg_proc') =
      'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.';

  if v_named_public_routines <> 2 or v_exact_public_routines <> 2 then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback refuses public compatibility routine drift';
  end if;
end
$preflight$;

drop view public.lcia_document_validation_evidence;
alter table private.lcia_document_validation_evidence set schema public;

do $rewrite$
declare
  v_routine record;
  v_definition text;
begin
  for v_routine in
    select procedure.oid,
           procedure.oid::regprocedure::text as identity,
           pg_get_functiondef(procedure.oid) as definition
    from pg_proc procedure
    join pg_namespace namespace on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'private'
      and (
        (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
         and pg_get_function_identity_arguments(procedure.oid) =
           'p_cache_keys jsonb')
        or
        (procedure.proname = 'svc_lcia_document_validation_evidence_record'
         and pg_get_function_identity_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid')
      )
    order by procedure.oid::regprocedure::text
  loop
    if strpos(
      v_routine.definition,
      'private.lcia_document_validation_evidence'
    ) = 0 then
      raise exception using
        errcode = '55000',
        message = format(
          'Issue 407 Phase B rollback refuses unbound routine %s',
          v_routine.identity
        );
    end if;

    v_definition := replace(
      v_routine.definition,
      'private.lcia_document_validation_evidence',
      'public.lcia_document_validation_evidence'
    );
    execute v_definition;
  end loop;
end
$rewrite$;

comment on table public.lcia_document_validation_evidence is null;
comment on function private.svc_lcia_document_validation_evidence_lookup(jsonb) is
  'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.';
comment on function private.svc_lcia_document_validation_evidence_record(jsonb, uuid) is
  'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.';

do $postflight$
declare
  v_before issue_407_phase_b_rollback_before%rowtype;
  v_after pg_class%rowtype;
  v_row_count bigint;
begin
  select * into strict v_before
  from issue_407_phase_b_rollback_before;

  select relation.* into strict v_after
  from pg_class relation
  join pg_namespace namespace on namespace.oid = relation.relnamespace
  where namespace.nspname = 'public'
    and relation.relname = 'lcia_document_validation_evidence'
    and relation.relkind = 'r';

  select count(*) into v_row_count
  from public.lcia_document_validation_evidence;

  if v_after.oid <> v_before.oid
     or v_after.reltype <> v_before.reltype
     or v_after.relowner <> v_before.relowner
     or v_after.relrowsecurity <> v_before.relrowsecurity
     or v_after.relforcerowsecurity <> v_before.relforcerowsecurity
     or v_after.relreplident <> v_before.relreplident
     or v_after.relacl is distinct from v_before.relacl
     or v_row_count <> v_before.row_count then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback changed table identity, ACL, RLS, replica identity, or data';
  end if;

  if to_regclass('private.lcia_document_validation_evidence') is not null
     or exists (
       select 1
       from pg_proc procedure
       join pg_namespace namespace on namespace.oid = procedure.pronamespace
       where namespace.nspname = 'private'
         and procedure.proname in (
           'svc_lcia_document_validation_evidence_lookup',
           'svc_lcia_document_validation_evidence_record'
         )
         and (
           pg_get_functiondef(procedure.oid) not like
             '%public.lcia_document_validation_evidence%'
           or pg_get_functiondef(procedure.oid) like
             '%private.lcia_document_validation_evidence%'
         )
     ) then
    raise exception using
      errcode = '55000',
      message = 'Issue 407 Phase B rollback predecessor topology mismatch';
  end if;
end
$postflight$;

notify pgrst, 'reload schema';
commit;
