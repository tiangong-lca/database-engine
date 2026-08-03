begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

-- Phase A is deliberately additive.  The public relation remains physical until
-- a separately authorized Contract phase; this migration only introduces the
-- private Worker contract and turns the two public RPCs into compatibility
-- wrappers.
do $preflight$
declare
  v_private_count integer;
  v_private_exact_count integer;
  v_public_count integer;
  v_public_exact_count integer;
  v_column_contract text[];
  v_source_fingerprint text;
begin
  if not exists (
    select 1
    from supabase_migrations.schema_migrations
    where version = '20260803090000'
  ) then
    raise exception 'issue 407 requires exact predecessor migration 20260803090000';
  end if;

  if to_regnamespace('private') is null
     or to_regclass('public.lcia_document_validation_evidence') is null then
    raise exception 'issue 407 source schema or relation is missing';
  end if;

  if not exists (
    select 1
    from pg_class relation
    where relation.oid = 'public.lcia_document_validation_evidence'::regclass
      and relation.relkind = 'r'
      and relation.relowner = 'postgres'::regrole
      and relation.relrowsecurity
  ) then
    raise exception 'issue 407 source must remain the postgres-owned RLS physical table';
  end if;

  select array_agg(
    attribute.attname || ':' || pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
      || ':' || attribute.attnotnull::text
    order by attribute.attnum
  ) into v_column_contract
  from pg_attribute attribute
  where attribute.attrelid = 'public.lcia_document_validation_evidence'::regclass
    and attribute.attnum > 0
    and not attribute.attisdropped;

  if v_column_contract is distinct from array[
    'id:uuid:true',
    'dataset_type:text:true',
    'dataset_id:uuid:true',
    'dataset_version:text:true',
    'canonical_content_hash:text:true',
    'document_validator_version:text:true',
    'document_validation_profile:text:true',
    'validation_report_schema_version:text:true',
    'validator_engine_fingerprint:text:true',
    'tidas_schema_lock_sha256:text:true',
    'status:text:true',
    'summary:jsonb:true',
    'issue_artifact_ref:jsonb:true',
    'issue_artifact_hash:text:false',
    'source_worker_job_id:uuid:false',
    'created_at:timestamp with time zone:true'
  ]::text[] then
    raise exception 'issue 407 source column contract drifted: %', v_column_contract;
  end if;

  -- Full deterministic source catalog: relation/columns/defaults, all
  -- constraints and indexes, triggers, policies, publications, ACL/comments,
  -- and the complete dependency rows. OID-bearing internal toast names are
  -- normalized so this value is portable across independent PG17 clusters.
  with
  relation_state as (
    select pg_catalog.jsonb_build_array(
      relation.relowner::pg_catalog.regrole::text, relation.relkind,
      relation.relpersistence, relation.relrowsecurity,
      relation.relforcerowsecurity, relation.relreplident,
      coalesce(access_method.amname, ''),
      coalesce(tablespace.spcname, ''),
      coalesce((select pg_catalog.array_agg(option_value order by option_value)
        from pg_catalog.unnest(relation.reloptions) option_value), '{}'),
      relation.relispartition,
      coalesce(pg_catalog.pg_get_expr(relation.relpartbound, relation.oid), ''),
      coalesce(relation.relacl::text, ''),
      coalesce(pg_catalog.obj_description(relation.oid, 'pg_class'), '')
    ) as value
    from pg_catalog.pg_class relation
    left join pg_catalog.pg_am access_method on access_method.oid = relation.relam
    left join pg_catalog.pg_tablespace tablespace on tablespace.oid = relation.reltablespace
    where relation.oid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ),
  column_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      attribute.attnum, attribute.attname,
      pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
      attribute.attnotnull, attribute.attidentity, attribute.attgenerated,
      attribute.attstorage, attribute.attcompression,
      attribute.attstattarget,
      coalesce((select pg_catalog.array_agg(option_value order by option_value)
        from pg_catalog.unnest(attribute.attoptions) option_value), '{}'),
      attribute.atthasmissing, coalesce(attribute.attmissingval::text, ''),
      coalesce(collation_row.collname, ''),
      coalesce(pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid), ''),
      coalesce(attribute.attacl::text, ''),
      coalesce(pg_catalog.col_description(attribute.attrelid, attribute.attnum), '')
    ) order by attribute.attnum), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_attribute attribute
    left join pg_catalog.pg_attrdef default_value
      on default_value.adrelid = attribute.attrelid
     and default_value.adnum = attribute.attnum
    left join pg_catalog.pg_collation collation_row
      on collation_row.oid = attribute.attcollation
    where attribute.attrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
      and attribute.attnum > 0 and not attribute.attisdropped
  ),
  constraint_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      constraint_row.conname, constraint_row.contype,
      constraint_row.condeferrable, constraint_row.condeferred,
      constraint_row.convalidated, coalesce(constraint_row.conkey::text, ''),
      coalesce(constraint_row.confrelid::pg_catalog.regclass::text, ''),
      coalesce(constraint_row.confkey::text, ''),
      pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
    ) order by constraint_row.conname), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_constraint constraint_row
    where constraint_row.conrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ),
  index_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      index_row.indexrelid::pg_catalog.regclass::text,
      index_row.indisunique, index_row.indisprimary, index_row.indisexclusion,
      index_row.indimmediate, index_row.indisclustered, index_row.indisvalid,
      index_row.indisready, index_row.indislive,
      coalesce(index_row.indkey::text, ''),
      coalesce(pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid), ''),
      coalesce(pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid), ''),
      pg_catalog.pg_get_indexdef(index_row.indexrelid)
    ) order by index_row.indexrelid::pg_catalog.regclass::text), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_index index_row
    where index_row.indrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ),
  trigger_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      case when trigger_row.tgisinternal
        then coalesce(trigger_constraint.conname, '<internal>')
        else trigger_row.tgname end,
      trigger_row.tgenabled, trigger_row.tgisinternal, trigger_row.tgtype,
      trigger_row.tgdeferrable, trigger_row.tginitdeferred,
      trigger_row.tgfoid::pg_catalog.regprocedure::text,
      case when trigger_row.tgisinternal then ''
        else pg_catalog.pg_get_triggerdef(trigger_row.oid, true) end
    ) order by
      case when trigger_row.tgisinternal
        then coalesce(trigger_constraint.conname, '<internal>')
        else trigger_row.tgname end,
      trigger_row.tgfoid::pg_catalog.regprocedure::text
    ), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_trigger trigger_row
    left join pg_catalog.pg_constraint trigger_constraint
      on trigger_constraint.oid = trigger_row.tgconstraint
    where trigger_row.tgrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ),
  policy_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      policy.polname, policy.polcmd, policy.polpermissive,
      coalesce((select pg_catalog.array_agg(
        coalesce(role_row.rolname, 'PUBLIC') order by coalesce(role_row.rolname, 'PUBLIC')
      ) from pg_catalog.unnest(policy.polroles) role_oid(oid)
        left join pg_catalog.pg_roles role_row on role_row.oid = role_oid.oid), '{}'),
      coalesce(pg_catalog.pg_get_expr(policy.polqual, policy.polrelid), ''),
      coalesce(pg_catalog.pg_get_expr(policy.polwithcheck, policy.polrelid), '')
    ) order by policy.polname), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_policy policy
    where policy.polrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ),
  publication_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      publication.pubname, publication.puballtables, publication.pubinsert,
      publication.pubupdate, publication.pubdelete, publication.pubtruncate,
      publication.pubviaroot,
      coalesce(pg_catalog.pg_get_expr(member.prqual, member.prrelid), ''),
      coalesce((select pg_catalog.array_agg(attribute.attname order by attribute.attnum)
        from pg_catalog.unnest(member.prattrs::pg_catalog.int2[]) attribute_number(attnum)
        join pg_catalog.pg_attribute attribute
          on attribute.attrelid = member.prrelid
         and attribute.attnum = attribute_number.attnum), '{}')
    ) order by publication.pubname), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_publication publication
    left join pg_catalog.pg_publication_rel member
      on member.prpubid = publication.oid
     and member.prrelid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
    where publication.puballtables or member.prrelid is not null
  ),
  dependency_state as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
      dependency.deptype,
      pg_catalog.regexp_replace(pg_catalog.pg_describe_object(
        dependency.classid, dependency.objid, dependency.objsubid
      ), 'pg_toast_[0-9]+', 'pg_toast_<oid>', 'g'),
      pg_catalog.regexp_replace(pg_catalog.pg_describe_object(
        dependency.refclassid, dependency.refobjid, dependency.refobjsubid
      ), 'pg_toast_[0-9]+', 'pg_toast_<oid>', 'g')
    ) order by dependency.deptype,
      pg_catalog.regexp_replace(pg_catalog.pg_describe_object(
        dependency.classid, dependency.objid, dependency.objsubid
      ), 'pg_toast_[0-9]+', 'pg_toast_<oid>', 'g'),
      pg_catalog.regexp_replace(pg_catalog.pg_describe_object(
        dependency.refclassid, dependency.refobjid, dependency.refobjsubid
      ), 'pg_toast_[0-9]+', 'pg_toast_<oid>', 'g')
    ), '[]'::pg_catalog.jsonb) as value
    from pg_catalog.pg_depend dependency
    where (dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
      and dependency.objid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass)
       or (dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
      and dependency.refobjid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass)
  )
  select pg_catalog.md5(pg_catalog.jsonb_build_object(
    'relation', relation_state.value, 'columns', column_state.value,
    'constraints', constraint_state.value, 'indexes', index_state.value,
    'triggers', trigger_state.value, 'policies', policy_state.value,
    'publications', publication_state.value,
    'dependencies', dependency_state.value
  )::text) into v_source_fingerprint
  from relation_state, column_state, constraint_state, index_state,
       trigger_state, policy_state, publication_state, dependency_state;

  if v_source_fingerprint is distinct from 'c5ca4c53ff5746dba23a65d5170bc816' then
    raise exception 'issue 407 source relation catalog drifted: %', v_source_fingerprint;
  end if;

  if (
    select pg_catalog.jsonb_build_array(
      namespace.nspowner::pg_catalog.regrole::text,
      coalesce(namespace.nspacl::text, ''),
      coalesce(pg_catalog.obj_description(namespace.oid, 'pg_namespace'), '')
    )
    from pg_catalog.pg_namespace namespace
    where namespace.nspname = 'private'
  ) is distinct from pg_catalog.jsonb_build_array(
    'postgres',
    '{postgres=UC/postgres,service_role=U/postgres,api_internal_executor=U/postgres,lca_result_gc_executor=U/postgres,lca_worker_runtime=U/postgres}',
    ''
  ) then
    raise exception 'issue 407 private schema catalog drifted';
  end if;

  if exists (
    select 1
    from (values
      ('postgres', true, true),
      ('anon', false, false),
      ('authenticated', false, false),
      ('service_role', true, false),
      ('api_internal_executor', true, false),
      ('lca_worker_runtime', true, false)
    ) expected(role_name, expected_usage, expected_create)
    where pg_catalog.has_schema_privilege(expected.role_name, 'private', 'USAGE')
            is distinct from expected.expected_usage
       or pg_catalog.has_schema_privilege(expected.role_name, 'private', 'CREATE')
            is distinct from expected.expected_create
  ) then
    raise exception 'issue 407 private schema role matrix drifted';
  end if;

  if not exists (
    select 1 from pg_roles role_row
    where role_row.rolname = 'lca_worker_runtime'
      and role_row.rolinherit
      and not role_row.rolsuper
      and not role_row.rolcreatedb
      and not role_row.rolcreaterole
      and not role_row.rolcanlogin
      and not role_row.rolbypassrls
      and not role_row.rolreplication
      and role_row.rolconfig is null
  ) then
    raise exception 'issue 407 requires the safe lca_worker_runtime group role';
  end if;

  if exists (
    select 1
    from pg_auth_members membership
    join pg_roles member_role on member_role.oid = membership.member
    join pg_roles granted_role on granted_role.oid = membership.roleid
    where (
      member_role.rolname = 'lca_worker_runtime'
      or granted_role.rolname = 'lca_worker_runtime'
    )
      and not (
        -- Supabase bootstrap owns this one creator baseline edge.  Runtime
        -- LOGIN edges below must instead be granted directly by postgres.
        member_role.rolname = 'postgres'
        and granted_role.rolname = 'lca_worker_runtime'
        and membership.grantor = 'supabase_admin'::regrole
        and membership.admin_option
        and not membership.inherit_option
        and not membership.set_option
      )
      and not (
        granted_role.rolname = 'lca_worker_runtime'
        and member_role.rolname not in (
          'anon', 'authenticated', 'service_role', 'api_internal_executor'
        )
        and member_role.rolcanlogin
        and member_role.rolinherit
        and not member_role.rolsuper
        and not member_role.rolcreatedb
        and not member_role.rolcreaterole
        and not member_role.rolbypassrls
        and not member_role.rolreplication
        and member_role.rolconfig is null
        and membership.grantor = 'postgres'::pg_catalog.regrole
        and not membership.admin_option
        and membership.inherit_option
        and not membership.set_option
        and not pg_catalog.pg_has_role(
          member_role.oid, 'api_internal_executor'::regrole, 'member'
        )
        and not exists (
          select 1
          from pg_auth_members other_membership
          where other_membership.member = member_role.oid
            and other_membership.roleid not in (
              'lca_worker_runtime'::regrole, 'service_role'::regrole
            )
        )
        and (
          not pg_catalog.pg_has_role(
            member_role.oid, 'service_role'::regrole, 'member'
          )
          or (
            exists (
              select 1
              from pg_auth_members service_membership
              where service_membership.member = member_role.oid
                and service_membership.roleid = 'service_role'::regrole
                and service_membership.grantor = 'postgres'::pg_catalog.regrole
                and not service_membership.admin_option
                and service_membership.inherit_option
                and not service_membership.set_option
            )
          )
        )
      )
  ) then
    raise exception 'issue 407 lca_worker_runtime membership graph drifted';
  end if;

  if has_table_privilege(
    'lca_worker_runtime', 'public.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ) then
    raise exception 'issue 407 lca_worker_runtime must not have source table privileges';
  end if;

  select count(*) into v_public_count
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'public'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );
  select count(*) into v_public_exact_count
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'public'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    )
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) = 'p_cache_keys jsonb')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid')
    );
  if v_public_count <> 2 or v_public_exact_count <> 2 then
    raise exception 'issue 407 requires the two exact public predecessor signatures';
  end if;

  select count(*) into v_private_count
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'private'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  select count(*) into v_private_exact_count
  from pg_proc procedure
  join pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname = 'private'
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    )
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) = 'p_cache_keys jsonb')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid')
    );

  if v_private_count not in (0, 2)
     or v_private_exact_count <> v_private_count then
    raise exception 'issue 407 refuses a partial private routine state';
  end if;

  if v_private_count = 0 then
    if exists (
      select 1
      from pg_proc procedure
      join pg_namespace namespace on namespace.oid = procedure.pronamespace
      join pg_catalog.pg_language language on language.oid = procedure.prolang
      where namespace.nspname = 'public'
        and procedure.proname in (
          'svc_lcia_document_validation_evidence_lookup',
          'svc_lcia_document_validation_evidence_record'
        )
        and (
          procedure.proowner <> 'postgres'::regrole
          or language.lanname <> 'plpgsql'
          or procedure.prokind <> 'f'
          or procedure.prorettype <> 'pg_catalog.jsonb'::pg_catalog.regtype
          or procedure.proretset
          or procedure.provolatile <> 'v'
          or procedure.proparallel <> 'u'
          or procedure.proisstrict
          or procedure.proleakproof
          or not procedure.prosecdef
          or procedure.proconfig is distinct from
             array['search_path=public, pg_temp']::text[]
          or procedure.proacl::text is distinct from
             '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
          or pg_catalog.obj_description(procedure.oid, 'pg_proc') is not null
          or procedure.pronargdefaults is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup' then 0
            else 1 end
          or procedure.proargnames is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then array['p_cache_keys']::text[]
            else array['p_records', 'p_source_worker_job_id']::text[] end
          or pg_catalog.pg_get_function_arguments(procedure.oid) is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then 'p_cache_keys jsonb'
            else 'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid' end
          or (
            procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
            and pg_catalog.md5(procedure.prosrc) <> 'da8b84eaaf6cfb408e2d2f4627827b36'
          )
          or (
            procedure.proname = 'svc_lcia_document_validation_evidence_record'
            and pg_catalog.md5(procedure.prosrc) <> '149cc3f6a715ae72bbceb458d3f33433'
          )
        )
    ) then
      raise exception 'issue 407 public predecessor definition drifted';
    end if;
  else
    if exists (
      select 1
      from pg_proc procedure
      join pg_namespace namespace on namespace.oid = procedure.pronamespace
      join pg_catalog.pg_language language on language.oid = procedure.prolang
      where namespace.nspname in ('public', 'private')
        and procedure.proname in (
          'svc_lcia_document_validation_evidence_lookup',
          'svc_lcia_document_validation_evidence_record'
        )
        and (
          procedure.proowner <> 'postgres'::regrole
          or language.lanname <> 'plpgsql'
          or procedure.prokind <> 'f'
          or procedure.prorettype <> 'pg_catalog.jsonb'::pg_catalog.regtype
          or procedure.proretset
          or procedure.provolatile <> 'v'
          or procedure.proparallel <> 'u'
          or procedure.proisstrict
          or procedure.proleakproof
          or not procedure.prosecdef
          or procedure.proconfig is distinct from
             array['search_path=pg_catalog, pg_temp']::text[]
          or procedure.pronargdefaults is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup' then 0
            else 1 end
          or procedure.proargnames is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then array['p_cache_keys']::text[]
            else array['p_records', 'p_source_worker_job_id']::text[] end
          or pg_catalog.pg_get_function_arguments(procedure.oid) is distinct from case
            when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then 'p_cache_keys jsonb'
            else 'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid' end
        )
    ) then
      raise exception 'issue 407 retry state has unsafe routine metadata';
    end if;
    if exists (
      select 1
      from pg_proc procedure
      join pg_namespace namespace on namespace.oid = procedure.pronamespace
      where namespace.nspname in ('public', 'private')
        and procedure.proname in (
          'svc_lcia_document_validation_evidence_lookup',
          'svc_lcia_document_validation_evidence_record'
        )
        and (
          pg_catalog.md5(procedure.prosrc) is distinct from case
            when namespace.nspname = 'private'
             and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then 'bd277cd343a10462fc536a64390459c5'
            when namespace.nspname = 'private'
             and procedure.proname = 'svc_lcia_document_validation_evidence_record'
              then '2759f5215c8dd4b253db2ed2264cc8ab'
            when namespace.nspname = 'public'
             and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then '6f6fb65152a4125c25babc79397d1626'
            when namespace.nspname = 'public'
             and procedure.proname = 'svc_lcia_document_validation_evidence_record'
              then 'efd249089fa40ea58fe8efe3e1e894b0'
          end
          or procedure.proacl::text is distinct from case
            when namespace.nspname = 'private'
              then '{postgres=X/postgres,lca_worker_runtime=X/postgres}'
            else '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
          end
          or pg_catalog.obj_description(procedure.oid, 'pg_proc') is distinct from case
            when namespace.nspname = 'private'
             and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
              then 'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.'
            when namespace.nspname = 'private'
              then 'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
            else 'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.'
          end
        )
    ) then
      raise exception 'issue 407 retry state definition or ACL drifted';
    end if;
  end if;
end
$preflight$;

create or replace function private.svc_lcia_document_validation_evidence_lookup(
  p_cache_keys pg_catalog.jsonb
) returns pg_catalog.jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
begin
  if pg_catalog.jsonb_typeof(
    coalesce(p_cache_keys, 'null'::pg_catalog.jsonb)
  ) <> 'array' then
    return public.lcia_scope_closure_error(
      'invalid_document_evidence_keys', 400, 'Cache keys must be an array'
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok', true,
    'data', coalesce((
      with requested as (
        select
          value->>'datasetType' as dataset_type,
          nullif(value->>'datasetId', '')::pg_catalog.uuid as dataset_id,
          value->>'datasetVersion' as dataset_version,
          value->>'canonicalContentHash' as canonical_content_hash,
          value->>'documentValidatorVersion' as document_validator_version,
          value->>'documentValidationProfile' as document_validation_profile,
          value->>'validationReportSchemaVersion' as validation_report_schema_version,
          value->>'validatorEngineFingerprint' as validator_engine_fingerprint,
          value->>'tidasSchemaLockSha256' as tidas_schema_lock_sha256
        from pg_catalog.jsonb_array_elements(p_cache_keys)
      )
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'datasetType', evidence.dataset_type,
          'datasetId', evidence.dataset_id,
          'datasetVersion', evidence.dataset_version,
          'canonicalContentHash', evidence.canonical_content_hash,
          'documentValidatorVersion', evidence.document_validator_version,
          'documentValidationProfile', evidence.document_validation_profile,
          'validationReportSchemaVersion', evidence.validation_report_schema_version,
          'validatorEngineFingerprint', evidence.validator_engine_fingerprint,
          'tidasSchemaLockSha256', evidence.tidas_schema_lock_sha256,
          'status', evidence.status,
          'summary', evidence.summary,
          'issueArtifactRef', evidence.issue_artifact_ref,
          'issueArtifactHash', evidence.issue_artifact_hash
        ) order by evidence.dataset_type, evidence.dataset_id, evidence.dataset_version
      )
      from requested
      join public.lcia_document_validation_evidence evidence
        on (
          evidence.dataset_type,
          evidence.dataset_id,
          evidence.dataset_version,
          evidence.canonical_content_hash,
          evidence.document_validator_version,
          evidence.document_validation_profile,
          evidence.validation_report_schema_version,
          evidence.validator_engine_fingerprint,
          evidence.tidas_schema_lock_sha256
        ) = (
          requested.dataset_type,
          requested.dataset_id,
          requested.dataset_version,
          requested.canonical_content_hash,
          requested.document_validator_version,
          requested.document_validation_profile,
          requested.validation_report_schema_version,
          requested.validator_engine_fingerprint,
          requested.tidas_schema_lock_sha256
        )
    ), '[]'::pg_catalog.jsonb)
  );
exception
  when invalid_text_representation then
    return public.lcia_scope_closure_error(
      'invalid_document_evidence_keys', 400,
      'Cache key contains invalid identity values'
    );
end
$function$;

create or replace function private.svc_lcia_document_validation_evidence_record(
  p_records pg_catalog.jsonb,
  p_source_worker_job_id pg_catalog.uuid default null
) returns pg_catalog.jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_record pg_catalog.jsonb;
  v_inserted integer := 0;
begin
  if pg_catalog.jsonb_typeof(
    coalesce(p_records, 'null'::pg_catalog.jsonb)
  ) <> 'array' then
    return public.lcia_scope_closure_error(
      'invalid_document_evidence_records', 400,
      'Evidence records must be an array'
    );
  end if;

  for v_record in
    select item.value
    from pg_catalog.jsonb_array_elements(p_records)
      with ordinality as item(value, ordinal)
    order by
      item.value->>'datasetType',
      item.value->>'datasetId',
      item.value->>'datasetVersion',
      item.value->>'canonicalContentHash',
      item.value->>'documentValidatorVersion',
      item.value->>'documentValidationProfile',
      item.value->>'validationReportSchemaVersion',
      item.value->>'validatorEngineFingerprint',
      item.value->>'tidasSchemaLockSha256',
      item.ordinal
  loop
    insert into public.lcia_document_validation_evidence (
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
      source_worker_job_id
    ) values (
      nullif(v_record->>'datasetType', ''),
      nullif(v_record->>'datasetId', '')::pg_catalog.uuid,
      nullif(v_record->>'datasetVersion', ''),
      nullif(v_record->>'canonicalContentHash', ''),
      nullif(v_record->>'documentValidatorVersion', ''),
      nullif(v_record->>'documentValidationProfile', ''),
      nullif(v_record->>'validationReportSchemaVersion', ''),
      nullif(v_record->>'validatorEngineFingerprint', ''),
      nullif(v_record->>'tidasSchemaLockSha256', ''),
      nullif(v_record->>'status', ''),
      coalesce(v_record->'summary', '{}'::pg_catalog.jsonb),
      coalesce(v_record->'issueArtifactRef', '{}'::pg_catalog.jsonb),
      nullif(v_record->>'issueArtifactHash', ''),
      p_source_worker_job_id
    )
    on conflict (
      dataset_type,
      dataset_id,
      dataset_version,
      canonical_content_hash,
      document_validator_version,
      document_validation_profile,
      validation_report_schema_version,
      validator_engine_fingerprint,
      tidas_schema_lock_sha256
    ) do nothing;
    if found then
      v_inserted := v_inserted + 1;
    end if;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok', true,
    'data', pg_catalog.jsonb_build_object('insertedCount', v_inserted)
  );
exception
  when not_null_violation or invalid_text_representation or check_violation then
    return public.lcia_scope_closure_error(
      'invalid_document_evidence_records', 400,
      'Evidence record violates the cache contract'
    );
end
$function$;

revoke all on function
  private.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb),
  private.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
from public, anon, authenticated, service_role, api_internal_executor,
  lca_worker_runtime;

grant execute on function
  private.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb),
  private.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
to lca_worker_runtime;

create or replace function public.svc_lcia_document_validation_evidence_lookup(
  p_cache_keys pg_catalog.jsonb
) returns pg_catalog.jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_caller_category text;
begin
  v_caller_category := case
    when pg_catalog.current_setting('role', true) = 'api_internal_executor'
      then 'api_internal_executor'
    when pg_catalog.current_setting('request.jwt.claim.role', true) = 'service_role'
      then 'service_role'
    when session_user = 'postgres' then 'postgres'
    else 'other'
  end;
  raise log 'issue_407_public_compat function=lookup caller_category=%',
    v_caller_category;

  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;

  return private.svc_lcia_document_validation_evidence_lookup(p_cache_keys);
end
$function$;

create or replace function public.svc_lcia_document_validation_evidence_record(
  p_records pg_catalog.jsonb,
  p_source_worker_job_id pg_catalog.uuid default null
) returns pg_catalog.jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_caller_category text;
begin
  v_caller_category := case
    when pg_catalog.current_setting('role', true) = 'api_internal_executor'
      then 'api_internal_executor'
    when pg_catalog.current_setting('request.jwt.claim.role', true) = 'service_role'
      then 'service_role'
    when session_user = 'postgres' then 'postgres'
    else 'other'
  end;
  raise log 'issue_407_public_compat function=record caller_category=%',
    v_caller_category;

  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;

  return private.svc_lcia_document_validation_evidence_record(
    p_records, p_source_worker_job_id
  );
end
$function$;

revoke all on function
  public.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb),
  public.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
from public, anon, authenticated, service_role, api_internal_executor,
  lca_worker_runtime;

grant execute on function
  public.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb),
  public.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
to service_role, api_internal_executor;

comment on function private.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb)
is 'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.';
comment on function private.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
is 'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.';
comment on function public.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb)
is 'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.';
comment on function public.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid)
is 'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.';

do $postflight$
declare
  v_exact_count integer;
begin
  select count(*) into v_exact_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace on namespace.oid = procedure.pronamespace
  join pg_catalog.pg_language language on language.oid = procedure.prolang
  where namespace.nspname in ('public', 'private')
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_catalog.pg_get_function_identity_arguments(procedure.oid) = 'p_cache_keys jsonb')
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
    and procedure.proconfig = array['search_path=pg_catalog, pg_temp']::text[]
    and procedure.pronargdefaults = case
      when procedure.proname = 'svc_lcia_document_validation_evidence_lookup' then 0
      else 1 end
    and procedure.proargnames = case
      when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
        then array['p_cache_keys']::text[]
      else array['p_records', 'p_source_worker_job_id']::text[] end
    and pg_catalog.pg_get_function_arguments(procedure.oid) = case
      when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
        then 'p_cache_keys jsonb'
      else 'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid' end
    and procedure.proacl::text = case when namespace.nspname = 'private'
      then '{postgres=X/postgres,lca_worker_runtime=X/postgres}'
      else '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}' end
    and pg_catalog.md5(procedure.prosrc) = case
      when namespace.nspname = 'private'
       and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
        then 'bd277cd343a10462fc536a64390459c5'
      when namespace.nspname = 'private'
        then '2759f5215c8dd4b253db2ed2264cc8ab'
      when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
        then '6f6fb65152a4125c25babc79397d1626'
      else 'efd249089fa40ea58fe8efe3e1e894b0' end
    and pg_catalog.obj_description(procedure.oid, 'pg_proc') = case
      when namespace.nspname = 'private'
       and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
        then 'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.'
      when namespace.nspname = 'private'
        then 'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
      else 'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.' end;

  if v_exact_count <> 4 then
    raise exception 'issue 407 postflight four-function catalog mismatch';
  end if;

  if (
    select relation.relacl::text
    from pg_catalog.pg_class relation
    where relation.oid = 'public.lcia_document_validation_evidence'::pg_catalog.regclass
  ) is distinct from
    '{postgres=arwdDxtm/postgres,service_role=arwdDxtm/postgres,api_internal_executor=r/postgres}'
  then
    raise exception 'issue 407 postflight source relation ACL drifted';
  end if;

  if exists (
    select 1
    from (values
      ('postgres', true, true), ('anon', false, false),
      ('authenticated', false, false), ('service_role', true, false),
      ('api_internal_executor', true, false), ('lca_worker_runtime', true, false)
    ) expected(role_name, expected_usage, expected_create)
    where pg_catalog.has_schema_privilege(expected.role_name, 'private', 'USAGE')
            is distinct from expected.expected_usage
       or pg_catalog.has_schema_privilege(expected.role_name, 'private', 'CREATE')
            is distinct from expected.expected_create
  ) then
    raise exception 'issue 407 postflight private schema role matrix drifted';
  end if;
end
$postflight$;

commit;
