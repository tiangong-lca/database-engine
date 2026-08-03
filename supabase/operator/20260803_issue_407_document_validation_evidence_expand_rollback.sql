begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  v_exact_count integer;
  v_named_count integer;
begin
  if to_regclass('public.lcia_document_validation_evidence') is null
     or to_regprocedure(
       'private.svc_lcia_document_validation_evidence_lookup(jsonb)'
     ) is null
     or to_regprocedure(
       'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)'
     ) is null then
    raise exception 'issue 407 rollback requires the complete Phase A contract';
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
        procedure.proowner <> 'postgres'::regrole
        or not procedure.prosecdef
        or procedure.proconfig is distinct from
           array['search_path=pg_catalog, pg_temp']::text[]
      )
  ) then
    raise exception 'issue 407 rollback refuses drifted Phase A routines';
  end if;

  select count(*) into v_named_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace on namespace.oid = procedure.pronamespace
  where namespace.nspname in ('public', 'private')
    and procedure.proname in (
      'svc_lcia_document_validation_evidence_lookup',
      'svc_lcia_document_validation_evidence_record'
    );

  if v_named_count <> 4 then
    raise exception 'issue 407 rollback refuses missing or extra Phase A overloads';
  end if;

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
    raise exception 'issue 407 rollback requires the exact four-function Phase A catalog';
  end if;
end
$preflight$;

create or replace function public.svc_lcia_document_validation_evidence_lookup(
  p_cache_keys jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_cache_keys,'null'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache keys must be an array'); end if;
  return jsonb_build_object('ok',true,'data',coalesce((
    with requested as (
      select (value->>'datasetType') dataset_type, nullif(value->>'datasetId','')::uuid dataset_id,
        value->>'datasetVersion' dataset_version, value->>'canonicalContentHash' canonical_content_hash,
        value->>'documentValidatorVersion' document_validator_version,
        value->>'documentValidationProfile' document_validation_profile,
        value->>'validationReportSchemaVersion' validation_report_schema_version,
        value->>'validatorEngineFingerprint' validator_engine_fingerprint,
        value->>'tidasSchemaLockSha256' tidas_schema_lock_sha256
      from jsonb_array_elements(p_cache_keys)
    )
    select jsonb_agg(jsonb_build_object('datasetType',e.dataset_type,'datasetId',e.dataset_id,'datasetVersion',e.dataset_version,'canonicalContentHash',e.canonical_content_hash,'documentValidatorVersion',e.document_validator_version,'documentValidationProfile',e.document_validation_profile,'validationReportSchemaVersion',e.validation_report_schema_version,'validatorEngineFingerprint',e.validator_engine_fingerprint,'tidasSchemaLockSha256',e.tidas_schema_lock_sha256,'status',e.status,'summary',e.summary,'issueArtifactRef',e.issue_artifact_ref,'issueArtifactHash',e.issue_artifact_hash) order by e.dataset_type,e.dataset_id,e.dataset_version)
    from requested r join public.lcia_document_validation_evidence e on (e.dataset_type,e.dataset_id,e.dataset_version,e.canonical_content_hash,e.document_validator_version,e.document_validation_profile,e.validation_report_schema_version,e.validator_engine_fingerprint,e.tidas_schema_lock_sha256)=(r.dataset_type,r.dataset_id,r.dataset_version,r.canonical_content_hash,r.document_validator_version,r.document_validation_profile,r.validation_report_schema_version,r.validator_engine_fingerprint,r.tidas_schema_lock_sha256)
  ),'[]'::jsonb));
exception when invalid_text_representation then return public.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache key contains invalid identity values');
end;
$$;

create or replace function public.svc_lcia_document_validation_evidence_record(
  p_records jsonb, p_source_worker_job_id uuid default null
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_record jsonb; v_inserted integer:=0;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_records,'null'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence records must be an array'); end if;
  for v_record in select value from jsonb_array_elements(p_records) loop
    insert into public.lcia_document_validation_evidence(dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256,status,summary,issue_artifact_ref,issue_artifact_hash,source_worker_job_id)
    values(nullif(v_record->>'datasetType',''),nullif(v_record->>'datasetId','')::uuid,nullif(v_record->>'datasetVersion',''),nullif(v_record->>'canonicalContentHash',''),nullif(v_record->>'documentValidatorVersion',''),nullif(v_record->>'documentValidationProfile',''),nullif(v_record->>'validationReportSchemaVersion',''),nullif(v_record->>'validatorEngineFingerprint',''),nullif(v_record->>'tidasSchemaLockSha256',''),nullif(v_record->>'status',''),coalesce(v_record->'summary','{}'::jsonb),coalesce(v_record->'issueArtifactRef','{}'::jsonb),nullif(v_record->>'issueArtifactHash',''),p_source_worker_job_id)
    on conflict (dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256) do nothing;
    if found then v_inserted:=v_inserted+1; end if;
  end loop;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('insertedCount',v_inserted));
exception when not_null_violation or invalid_text_representation or check_violation then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence record violates the cache contract');
end;
$$;

-- The public compatibility identities predate Phase A. Both CREATE OR REPLACE
-- statements above preserve their OIDs and dependent-object identity; only the
-- new private canonical routines are removed.
drop function private.svc_lcia_document_validation_evidence_lookup(jsonb);
drop function private.svc_lcia_document_validation_evidence_record(jsonb,uuid);

revoke all on function
  public.svc_lcia_document_validation_evidence_lookup(jsonb),
  public.svc_lcia_document_validation_evidence_record(jsonb,uuid)
from public, anon, authenticated, service_role, api_internal_executor,
  lca_worker_runtime;
grant execute on function
  public.svc_lcia_document_validation_evidence_lookup(jsonb),
  public.svc_lcia_document_validation_evidence_record(jsonb,uuid)
to service_role, api_internal_executor;

comment on function public.svc_lcia_document_validation_evidence_lookup(jsonb)
is null;
comment on function public.svc_lcia_document_validation_evidence_record(jsonb,uuid)
is null;

do $postflight$
declare
  v_exact_count integer;
begin
  if exists (
    select 1 from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'private'
      and procedure.proname in (
        'svc_lcia_document_validation_evidence_lookup',
        'svc_lcia_document_validation_evidence_record'
      )
  ) then
    raise exception 'issue 407 rollback retained a private routine';
  end if;

  select count(*) into v_exact_count
  from pg_catalog.pg_proc procedure
  join pg_catalog.pg_namespace namespace on namespace.oid = procedure.pronamespace
  join pg_catalog.pg_language language on language.oid = procedure.prolang
  where namespace.nspname = 'public'
    and (
      (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
       and pg_catalog.pg_get_function_arguments(procedure.oid) = 'p_cache_keys jsonb'
       and pg_catalog.md5(procedure.prosrc) = 'da8b84eaaf6cfb408e2d2f4627827b36')
      or
      (procedure.proname = 'svc_lcia_document_validation_evidence_record'
       and pg_catalog.pg_get_function_arguments(procedure.oid) =
           'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid'
       and pg_catalog.md5(procedure.prosrc) = '149cc3f6a715ae72bbceb458d3f33433')
    )
    and procedure.proowner = 'postgres'::pg_catalog.regrole
    and language.lanname = 'plpgsql'
    and procedure.prokind = 'f'
    and procedure.prorettype = 'pg_catalog.jsonb'::pg_catalog.regtype
    and procedure.prosecdef
    and procedure.proconfig = array['search_path=public, pg_temp']::text[]
    and procedure.proacl::text =
      '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
    and pg_catalog.obj_description(procedure.oid, 'pg_proc') is null;

  if v_exact_count <> 2 then
    raise exception 'issue 407 rollback predecessor postflight mismatch';
  end if;
end
$postflight$;

commit;
