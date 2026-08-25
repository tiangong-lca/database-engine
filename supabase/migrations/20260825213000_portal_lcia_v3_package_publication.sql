begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- Certified V2/V3 packages hash a closure-certificate manifest, while the
-- frozen legacy publisher compares its older all_eligible manifest hash.  Do
-- not weaken or rewrite that legacy boundary.  This additive V3-only path
-- compares the exact current Process identity set, binds a prepared Portal
-- projection, and requires an exact pre-write plan hash.
create temporary table portal_lcia_legacy_publisher_before
on commit drop as
select
  pg_catalog.pg_get_functiondef(routine.oid) as definition,
  routine.proowner,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]) as proconfig,
  coalesce(routine.proacl::text, '') as acl_text
from pg_catalog.pg_proc as routine
where routine.oid =
  'api.cmd_lcia_result_package_publish(uuid,text,text,jsonb)'::regprocedure;

do $portal_lcia_legacy_publisher_snapshot_guard$
begin
  if (select count(*) from portal_lcia_legacy_publisher_before) <> 1 then
    raise exception 'Legacy LCIA package publisher snapshot is incomplete';
  end if;
end
$portal_lcia_legacy_publisher_snapshot_guard$;

create function private.portal_lcia_v3_package_publish_prepare_v1(
  p_package_id uuid,
  p_display_default_impact_category text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_package private.lcia_result_packages%rowtype;
  v_job private.worker_jobs%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_current_manifest jsonb;
  v_package_processes jsonb;
  v_current_processes jsonb;
  v_default_impact text;
  v_current_publication private.lcia_result_publications%rowtype;
  v_current_package private.lcia_result_packages%rowtype;
  v_current_process_set_hash text;
  v_publish_plan_hash text;
  v_projection_id uuid;
  v_fields text[];
begin
  if p_package_id is null then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Portal LCIA package identity is required'
    );
  end if;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  if v_package.id is null or v_package.status <> 'preview_ready' then
    return api.lcia_result_error(
      'package_not_ready', 400,
      'Package must be preview_ready before publication'
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_package.build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.payload_json ->> 'portalProjectionHashContractVersion'
          <> 'portal.lcia-projection.int32be-frame-sha256.v1'
     or v_package.coverage_mode <> 'global_eligible'
     or v_package.included_input_count <> v_package.eligible_input_count
     or v_package.included_input_count < 1
     or jsonb_typeof(v_package.input_manifest -> 'processes') <> 'array'
     or jsonb_typeof(v_package.available_impact_categories) <> 'array'
     or v_package.artifact_manifest ->> 'portalProjectionId'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_package.package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Only an exact ready global Portal LCIA V3 package can publish'
    );
  end if;
  begin
    v_projection_id := (
      v_package.artifact_manifest ->> 'portalProjectionId'
    )::uuid;
  exception when invalid_text_representation then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Portal projection identity is invalid'
    );
  end;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_projection_id;
  if v_projection.id is null
     or v_projection.status <> 'prepared'
     or v_projection.build_worker_job_id <> v_package.build_worker_job_id
     or v_projection.content_hash
          <> v_package.artifact_manifest ->> 'portalProjectionContentHash'
     or v_projection.input_manifest_hash <> v_package.input_manifest_hash
     or v_projection.closure_certificate_hash
          <> v_package.closure_certificate_hash
     or v_projection.snapshot_hash <> v_package.closure_snapshot_hash
     or v_projection.result_artifact_sha256
          <> v_package.result_artifact_ref ->> 'artifactSha256'
     or v_projection.query_artifact_sha256
          <> v_package.query_artifact_ref ->> 'artifactSha256'
     or v_projection.bundle_content_hash
          <> v_package.artifact_manifest ->> 'bundleContentHash'
     or v_projection.bundle_manifest_sha256
          <> v_package.artifact_manifest ->> 'bundleManifestSha256'
     or v_projection.lcia_chunk_set_sha256
          <> v_package.artifact_manifest ->> 'lciaChunkSetSha256'
     or v_projection.process_count <> v_package.included_input_count
     or v_projection.impact_count < 1
     or v_projection.impact_count <>
          jsonb_array_length(v_package.available_impact_categories) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Package and prepared Portal projection evidence do not match'
    );
  end if;
  if exists (
    select 1
    from private.portal_lcia_projection_process_axis as process_row
    where process_row.projection_id = v_projection.id
      and (
        v_package.input_manifest -> 'processes' -> process_row.process_index
          ->> 'id' is distinct from process_row.process_id::text
        or v_package.input_manifest -> 'processes' -> process_row.process_index
          ->> 'version' is distinct from process_row.process_version
      )
  ) or v_package.available_impact_categories is distinct from (
    select coalesce(
      jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
      '[]'::jsonb
    )
    from private.portal_lcia_projection_impact_axis as impact
    where impact.projection_id = v_projection.id
  ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection axes do not match the package identities'
    );
  end if;

  v_current_manifest := api.lcia_result_current_eligible_manifest();
  v_package_processes := v_package.input_manifest -> 'processes';
  v_current_processes := v_current_manifest #> '{inputManifest,processes}';
  if jsonb_typeof(v_current_processes) <> 'array'
     or v_package.eligible_input_count <>
          coalesce((v_current_manifest ->> 'eligibleInputCount')::integer, -1)
     or jsonb_array_length(v_package_processes) <>
          v_package.included_input_count
     or jsonb_array_length(v_current_processes) <>
          v_package.included_input_count
     or exists (
       select 1
       from jsonb_array_elements(v_package_processes) as process(value)
       where private.portal_lcia_json_object_has_keys_v1(
         process.value, array['id', 'version']
       ) is not true
         or process.value ->> 'id'
              !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or process.value ->> 'version' !~ '^\d{2}\.\d{2}\.\d{3}$'
     )
     or (
       select count(distinct (process.value ->> 'id', process.value ->> 'version'))
       from jsonb_array_elements(v_package_processes) as process(value)
     ) <> jsonb_array_length(v_package_processes)
     or exists (
       (
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_package_processes) as process(value)
         except
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_current_processes) as process(value)
       )
       union all
       (
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_current_processes) as process(value)
         except
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_package_processes) as process(value)
       )
     ) then
    return api.lcia_result_error(
      'package_stale_eligibility', 409,
      'Current eligible Process identities differ from the certified package'
    );
  end if;

  select array[
    'portal.lcia-v3-current-process-set.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    jsonb_array_length(v_current_processes)::text
  ] || coalesce(
    array_agg(field.value order by process.value ->> 'id',
      process.value ->> 'version', field.position),
    '{}'::text[]
  ) into v_fields
  from jsonb_array_elements(v_current_processes) as process(value)
  cross join lateral (
    values (1, process.value ->> 'id'), (2, process.value ->> 'version')
  ) as field(position, value);
  v_current_process_set_hash :=
    private.portal_lcia_projection_sha256_fields_v1(variadic v_fields);

  v_default_impact := coalesce(
    nullif(btrim(coalesce(p_display_default_impact_category, '')), ''),
    v_package.default_impact_category
  );
  if v_default_impact is null
     or private.portal_lcia_public_text_valid_v1(v_default_impact, 512)
          is not true
     or not exists (
       select 1
       from jsonb_array_elements_text(
         v_package.available_impact_categories
       ) as impact(value)
       where impact.value = v_default_impact
     )
     or v_package.result_artifact_ref = '{}'::jsonb then
    return api.lcia_result_error(
      'default_impact_missing', 400,
      'Default impact category or result evidence is unavailable'
    );
  end if;

  select publication.* into v_current_publication
  from private.lcia_result_publications as publication
  where publication.publication_series_key = 'global'
    and publication.publication_channel = 'public'
    and publication.visibility_scope = 'public'
    and publication.is_current
    and publication.status = 'current';
  if v_current_publication.id is not null then
    select package.* into v_current_package
    from private.lcia_result_packages as package
    where package.id = v_current_publication.package_id;
    if v_current_package.id is null
       or v_current_publication.published_at is null then
      return api.lcia_result_error(
        'publication_precondition_invalid', 409,
        'Current publication evidence is incomplete'
      );
    end if;
  end if;

  v_publish_plan_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-v3-package-publish-plan.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_package.id::text,
    v_package.package_version,
    v_package.package_result_hash,
    v_package.input_manifest_hash,
    v_package.closure_certificate_hash,
    v_package.closure_snapshot_hash,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    v_projection.id::text,
    v_projection.content_hash,
    v_projection.process_axis_hash,
    v_projection.impact_axis_hash,
    v_projection.value_grid_hash,
    v_projection.relation_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_default_impact,
    v_current_process_set_hash,
    v_current_publication.id::text,
    v_current_publication.package_id::text,
    v_current_package.package_version,
    case when v_current_publication.published_at is null then null
      else private.portal_timestamp_v1(v_current_publication.published_at) end
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publishPlanHash', v_publish_plan_hash,
      'package', jsonb_build_object(
        'id', v_package.id,
        'version', v_package.package_version,
        'resultHash', v_package.package_result_hash,
        'inputManifestHash', v_package.input_manifest_hash,
        'closureCertificateHash', v_package.closure_certificate_hash,
        'snapshotHash', v_package.closure_snapshot_hash,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'valueCount', v_projection.expected_value_count
      ),
      'projection', jsonb_build_object(
        'id', v_projection.id,
        'contentHash', v_projection.content_hash,
        'processAxisHash', v_projection.process_axis_hash,
        'impactAxisHash', v_projection.impact_axis_hash,
        'valueGridHash', v_projection.value_grid_hash,
        'relationHash', v_projection.relation_hash
      ),
      'artifacts', jsonb_build_object(
        'bundleContentHash', v_projection.bundle_content_hash,
        'bundleManifestSha256', v_projection.bundle_manifest_sha256,
        'lciaChunkSetSha256', v_projection.lcia_chunk_set_sha256,
        'resultArtifactSha256', v_projection.result_artifact_sha256,
        'queryArtifactSha256', v_projection.query_artifact_sha256
      ),
      'displayDefaultImpactCategory', v_default_impact,
      'currentProcessSetHash', v_current_process_set_hash,
      'currentPublication', case
        when v_current_publication.id is null then 'null'::jsonb
        else jsonb_build_object(
          'publicationId', v_current_publication.id,
          'packageId', v_current_publication.package_id,
          'packageVersion', v_current_package.package_version,
          'publishedAt', private.portal_timestamp_v1(
            v_current_publication.published_at
          )
        )
      end
    )
  );
end
$function$;

revoke all on function private.portal_lcia_v3_package_publish_prepare_v1(
  uuid, text
) from public, anon, authenticated, service_role;

create function api.qry_portal_lcia_result_package_publish_prepare_v1(
  p_package_id uuid,
  p_display_default_impact_category text default null::text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
begin
  if auth.uid() is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  return private.portal_lcia_v3_package_publish_prepare_v1(
    p_package_id, p_display_default_impact_category
  );
end
$function$;

create function api.cmd_portal_lcia_result_package_publish_v1(
  p_package_id uuid,
  p_display_default_impact_category text,
  p_expected_publish_plan_hash text,
  p_reason text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_prepared jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_existing private.lcia_result_publications%rowtype;
  v_retry_audit jsonb;
  v_previous_id uuid;
  v_publication private.lcia_result_publications%rowtype;
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_default_impact text;
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_package_id is null
     or coalesce(p_expected_publish_plan_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_safe_audit_v1(p_audit) is not true
     or (
       v_reason is not null
       and private.portal_lcia_public_text_valid_v1(v_reason, 2000) is not true
     ) then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Invalid Portal LCIA package publication request'
    );
  end if;

  lock table private.lcia_result_publications in exclusive mode;
  select publication.* into v_existing
  from private.lcia_result_publications as publication
  where publication.package_id = p_package_id
  order by publication.published_at desc nulls last, publication.id
  limit 1;
  if v_existing.id is not null then
    select audit.payload into v_retry_audit
    from private.command_audit_log as audit
    where audit.command = 'cmd_portal_lcia_result_package_publish_v1'
      and audit.target_table = 'lcia_result_publications'
      and audit.target_id = v_existing.id
      and audit.payload ->> 'publishPlanHash'
            = p_expected_publish_plan_hash
    order by audit.created_at desc
    limit 1;
    if v_existing.is_current
       and v_existing.status = 'current'
       and v_retry_audit is not null then
      select package.* into v_package
      from private.lcia_result_packages as package
      where package.id = p_package_id;
      select projection.* into v_projection
      from private.portal_lcia_projection_headers as projection
      where projection.id::text =
        v_package.artifact_manifest ->> 'portalProjectionId';
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'publicationId', v_existing.id,
          'packageId', v_package.id,
          'previousPublicationId', v_retry_audit -> 'previousPublicationId',
          'isCurrent', true,
          'packageVersion', v_package.package_version,
          'projectionId', v_projection.id,
          'projectionContentHash', v_projection.content_hash,
          'publishPlanHash', p_expected_publish_plan_hash,
          'publishedAt', private.portal_timestamp_v1(v_existing.published_at)
        )
      );
    end if;
    return api.lcia_result_error(
      'package_publication_conflict', 409,
      'Package already has a different or non-current publication history'
    );
  end if;

  v_prepared := private.portal_lcia_v3_package_publish_prepare_v1(
    p_package_id, p_display_default_impact_category
  );
  if coalesce((v_prepared ->> 'ok')::boolean, false) is not true then
    return v_prepared;
  end if;
  if v_prepared #>> '{data,publishPlanHash}'
       <> p_expected_publish_plan_hash then
    return api.lcia_result_error(
      'publish_plan_drift', 409,
      'Portal LCIA package publication evidence changed after approval'
    );
  end if;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id
  for share;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id::text = v_prepared #>> '{data,projection,id}';
  v_default_impact := v_prepared #>> '{data,displayDefaultImpactCategory}';

  update private.lcia_result_publications
  set is_current = false,
      status = 'superseded',
      updated_at = v_now
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current
  returning id into v_previous_id;

  insert into private.lcia_result_publications (
    package_id, publication_series_key, publication_channel,
    visibility_scope, is_current, status,
    display_default_impact_category, published_by, published_at, reason
  ) values (
    v_package.id, 'global', 'public', 'public', true, 'current',
    v_default_impact, v_actor, v_now, v_reason
  ) returning * into v_publication;

  update private.lca_results
  set is_pinned = true
  where id = v_package.result_id;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_result_package_publish_v1',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    v_package.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'packageId', v_package.id,
      'projectionId', v_projection.id,
      'projectionContentHash', v_projection.content_hash,
      'publishPlanHash', p_expected_publish_plan_hash,
      'previousPublicationId', v_previous_id,
      'displayDefaultImpactCategory', v_default_impact
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'isCurrent', true,
      'packageVersion', v_package.package_version,
      'projectionId', v_projection.id,
      'projectionContentHash', v_projection.content_hash,
      'publishPlanHash', p_expected_publish_plan_hash,
      'publishedAt', private.portal_timestamp_v1(v_publication.published_at)
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'latest_conflict', 409,
      'Another current publication already exists'
    );
end
$function$;

revoke all on function api.qry_portal_lcia_result_package_publish_prepare_v1(
  uuid, text
) from public, anon, authenticated, service_role;
revoke all on function api.cmd_portal_lcia_result_package_publish_v1(
  uuid, text, text, text, jsonb
) from public, anon, authenticated, service_role;
grant execute on function api.qry_portal_lcia_result_package_publish_prepare_v1(
  uuid, text
) to authenticated;
grant execute on function api.cmd_portal_lcia_result_package_publish_v1(
  uuid, text, text, text, jsonb
) to authenticated;

insert into private.api_capability_grants (
  routine_identity, capability_id,
  allow_anon, allow_authenticated, allow_service_role
)
values
  (
    'api.qry_portal_lcia_result_package_publish_prepare_v1(uuid, text)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  ),
  (
    'api.cmd_portal_lcia_result_package_publish_v1(uuid, text, text, text, jsonb)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

comment on function api.qry_portal_lcia_result_package_publish_prepare_v1(
  uuid, text
) is
  'Returns the locator-free, exact-hash V3 package/projection/current-publication precondition required before approval.';
comment on function api.cmd_portal_lcia_result_package_publish_v1(
  uuid, text, text, text, jsonb
) is
  'Publishes only the exact approved Portal LCIA V3 package plan and reconciles response-loss retries from durable audit evidence.';

do $verify_portal_lcia_legacy_publisher_unchanged$
begin
  if exists (
    with after_state as (
      select
        pg_catalog.pg_get_functiondef(routine.oid) as definition,
        routine.proowner,
        routine.prosecdef,
        coalesce(routine.proconfig, '{}'::text[]) as proconfig,
        coalesce(routine.proacl::text, '') as acl_text
      from pg_catalog.pg_proc as routine
      where routine.oid =
        'api.cmd_lcia_result_package_publish(uuid,text,text,jsonb)'::regprocedure
    )
    (select * from portal_lcia_legacy_publisher_before
     except select * from after_state)
    union all
    (select * from after_state
     except select * from portal_lcia_legacy_publisher_before)
  ) then
    raise exception 'Portal V3 publisher changed the legacy package publisher';
  end if;
end
$verify_portal_lcia_legacy_publisher_unchanged$;

notify pgrst, 'reload schema';

commit;
