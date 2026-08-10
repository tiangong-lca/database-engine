CREATE OR REPLACE FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb" DEFAULT NULL::"jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_table text := lower(coalesce(p_target_table, ''));
  v_root_row jsonb;
  v_checksum text;
  v_gate_assertion jsonb;
  v_root_review_id uuid := gen_random_uuid();
  v_root_review private.reviews%rowtype;
  v_reference private.reviews%rowtype;
  v_target record;
  v_target_checksum text;
  v_items jsonb := '[]'::jsonb;
  v_item jsonb;
  v_scope_history jsonb;
  v_affected jsonb := '[]'::jsonb;
  v_reference_ids uuid[] := array[]::uuid[];
  v_old_reference_ids uuid[] := array[]::uuid[];
  v_impacted_root private.reviews%rowtype;
  v_current_snapshot jsonb;
  v_repair_items jsonb;
  v_conflict_version text;
  v_event_key text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if v_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review submission'
    );
  end if;

  v_root_row := api.cmd_review_get_dataset_row(
    v_table,
    p_target_id,
    p_target_version,
    true
  );

  if v_root_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if nullif(v_root_row->>'user_id', '')::uuid is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROOT_DATASET_NOT_OWNED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if coalesce((v_root_row->>'state_code')::integer, 0) <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', case
        when coalesce((v_root_row->>'state_code')::integer, 0) = 100
          then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATA_UNDER_REVIEW'
      end,
      'status', 409,
      'message', 'Only a Draft dataset can be submitted'
    );
  end if;

  v_checksum := private.review_revision_fingerprint_v1(v_table, v_root_row);

  if v_table = 'processes' then
    if nullif(p_gate_context->>'reviewSubmitJobId', '') is not null then
      if not exists (
        select 1
        from private.dataset_review_submit_requests as submit_job
        left join private.worker_jobs as gate_job
          on gate_job.id = submit_job.gate_worker_job_id
        where submit_job.id =
            (p_gate_context->>'reviewSubmitJobId')::uuid
          and submit_job.requested_by = v_actor
          and submit_job.dataset_table = v_table
          and submit_job.dataset_id = p_target_id
          and submit_job.dataset_version = p_target_version
          and submit_job.revision_checksum = v_checksum
          and submit_job.status in ('submitting', 'submitted')
          and (
            (
              submit_job.gate_worker_job_id is not null
              and gate_job.status = 'completed'
              and gate_job.result_json->>'status' = 'passed'
              and gate_job.result_json
                #>> '{datasetRevision,revisionChecksum}' = v_checksum
            )
            or submit_job.gate_run_id is not null
          )
      ) then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_SUBMIT_JOB_GATE_INVALID',
          'status', 409,
          'message', 'Review-submit job does not prove a passed Process Gate'
        );
      end if;
    else
      if p_gate_context is null
        or nullif(p_gate_context->>'reviewSubmitGateRunId', '') is null
        or nullif(p_gate_context->>'revisionChecksum', '') is null then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_GATE_REQUIRED',
          'status', 409,
          'message', 'Process review submission requires a passed Gate'
        );
      end if;

      if p_gate_context->>'revisionChecksum' <> v_checksum then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_GATE_CHECKSUM_MISMATCH',
          'status', 409,
          'message', 'Gate checksum does not match the current Process revision'
        );
      end if;

      v_gate_assertion := api.cmd_dataset_assert_review_submit_gate_passed(
        v_table,
        p_target_id,
        p_target_version,
        (p_gate_context->>'reviewSubmitGateRunId')::uuid,
        p_gate_context->>'revisionChecksum',
        coalesce(
          nullif(p_gate_context->>'policyProfile', ''),
          'review_submit_fast.v1'
        ),
        coalesce(
          nullif(p_gate_context->>'reportSchemaVersion', ''),
          'review_submit_gate_report.v1'
        )
      );

      if coalesce((v_gate_assertion->>'ok')::boolean, false) is false then
        return v_gate_assertion;
      end if;
    end if;
  elsif p_gate_context is not null
    and p_gate_context <> '{}'::jsonb
    and p_gate_context <> 'null'::jsonb then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_GATE_NOT_APPLICABLE',
      'status', 400,
      'message', 'Gate context is only accepted for Process datasets'
    );
  end if;


  select coalesce(array_agg(rejected.id order by rejected.id), array[]::uuid[])
  into v_old_reference_ids
  from private.reviews as rejected
  where rejected.review_kind = 'reference'
    and rejected.target_table = v_table
    and rejected.data_id = p_target_id
    and btrim(rejected.data_version::text) = p_target_version
    and rejected.state_code = -1
    and exists (
      select 1
      from private.reviews as root_review
      where root_review.review_kind = 'root'
        and root_review.current_reference_review_ids
          @> array[rejected.id]::uuid[]
    );

  if cardinality(v_old_reference_ids) > 0 then
    v_reference := private.review_get_or_create_reference_v1(
      v_table,
      v_root_row,
      v_checksum,
      v_actor
    );

    perform set_config('app.review_controlled_write', 'on', true);
    execute format(
      'update public.%I
          set state_code = 20,
              modified_at = now()
        where id = $1
          and version = $2
          and state_code = 0',
      v_table
    ) using p_target_id, p_target_version;
    perform set_config('app.review_controlled_write', 'off', true);

    for v_impacted_root in
      select root_review.*
      from private.reviews as root_review
      where root_review.review_kind = 'root'
        and root_review.state_code in (0, 1)
        and root_review.current_reference_review_ids && v_old_reference_ids
      order by root_review.id
      for update
    loop
      v_current_snapshot := private.review_scope_current_snapshot_v1(
        v_impacted_root.scope_history
      );
      v_repair_items := private.review_replace_reference_item_v1(
        v_current_snapshot->'items',
        v_table,
        p_target_id,
        p_target_version,
        v_checksum,
        v_reference.id,
        nullif(v_root_row->>'user_id', '')::uuid,
        nullif(v_root_row->>'team_id', '')::uuid
      );

      perform private.review_append_scope_snapshot_v1(
        v_impacted_root.id,
        'reference_repair',
        v_current_snapshot->>'root_revision_checksum',
        v_repair_items,
        v_actor
      );

      perform private.review_notify_event_v1(
        'reference_repaired',
        v_reference.id,
        v_impacted_root.target_owner_id,
        v_actor,
        v_table,
        p_target_id,
        p_target_version,
        v_impacted_root.id,
        (v_impacted_root.scope_history->>'current_version')::integer + 1,
        null
      );
    end loop;

    insert into private.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_review_submit_v2',
      v_actor,
      v_table,
      p_target_id,
      p_target_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_id', v_reference.id,
        'review_kind', 'reference',
        'submission_mode', 'reference_repair',
        'replaced_reference_review_ids', to_jsonb(v_old_reference_ids)
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'reviewId', v_reference.id,
        'reviewKind', 'reference',
        'submissionMode', 'reference_repair'
      )
    );
  end if;

  if exists (
    select 1
    from private.reviews as active_root
    where active_root.review_kind = 'root'
      and active_root.target_table = v_table
      and active_root.data_id = p_target_id
      and btrim(active_root.data_version::text) = p_target_version
      and active_root.state_code in (0, 1)
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'An active Root Review already exists'
    );
  end if;

  create temporary table if not exists review_submit_v2_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;
  truncate table review_submit_v2_targets;

  insert into review_submit_v2_targets
  select *
  from api.cmd_review_collect_dataset_targets(
    jsonb_build_array(jsonb_build_object(
      'table', v_table,
      'id', p_target_id,
      'version', p_target_version,
      'is_root', true
    )),
    true
  );

  if not exists (
    select 1
    from review_submit_v2_targets
    where is_root
      and table_name = v_table
      and dataset_id = p_target_id
      and dataset_version = p_target_version
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_table in ('processes', 'lifecyclemodels') then
    v_gate_assertion := private.cmd_review_assert_lifecycle_closure(
      jsonb_build_array(jsonb_build_object(
        'table', v_table,
        'id', p_target_id,
        'version', p_target_version
      )),
      'submit',
      v_actor
    );
    if v_gate_assertion is not null then
      return v_gate_assertion;
    end if;
  end if;

  for v_target in
    select *
    from review_submit_v2_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    if nullif(v_target.dataset_row->>'user_id', '') is null then
      return jsonb_build_object(
        'ok', false,
        'code', case when v_target.is_root
          then 'ROOT_OWNER_UNRESOLVED'
          else 'REFERENCE_OWNER_UNRESOLVED'
        end,
        'status', 409,
        'message', 'Dataset owner could not be resolved'
      );
    end if;

    if not v_target.is_root
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor
      and not private.review_dataset_can_read_v1(
        v_actor,
        v_target.table_name,
        v_target.dataset_row
      ) then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_ACCESS_DENIED',
        'status', 403,
        'message', 'A referenced dataset is not accessible'
      );
    end if;

    select btrim(active_reference.data_version::text)
    into v_conflict_version
    from private.reviews as active_reference
    where active_reference.review_kind = 'reference'
      and active_reference.target_table = v_target.table_name
      and active_reference.data_id = v_target.dataset_id
      and btrim(active_reference.data_version::text)
        <> v_target.dataset_version
      and active_reference.state_code in (0, 1)
    order by active_reference.created_at desc
    limit 1;

    if v_conflict_version is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_REVISION_CONFLICT',
        'status', 409,
        'message', 'Another version of a referenced dataset is under review'
      );
    end if;

    v_target_checksum := private.review_revision_fingerprint_v1(
      v_target.table_name,
      v_target.dataset_row
    );

    if v_target.is_root then
      v_item := jsonb_build_object(
        'item_kind', 'root',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', null,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'root',
        'relation_path', '$',
        'introduced_by', 'submitted_data',
        'introduced_field_path', null
      );
    else
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_actor
      );

      perform private.review_rebind_active_roots_to_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_reference.id,
        v_actor
      );

      v_reference_ids := array_append(v_reference_ids, v_reference.id);
      v_item := jsonb_build_object(
        'item_kind', 'reference',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', v_reference.id,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'dependency',
        'relation_path', '$',
        'introduced_by', 'submitted_data',
        'introduced_field_path', null
      );
    end if;

    v_items := v_items || jsonb_build_array(v_item);
  end loop;

  v_scope_history := jsonb_build_object(
    'schema_version', 'review_scope.v1',
    'current_version', 1,
    'snapshots', jsonb_build_array(jsonb_build_object(
      'version_no', 1,
      'scope_basis', 'submitted',
      'root_revision_checksum', v_checksum,
      'scope_checksum', private.review_scope_checksum_v1(v_items),
      'created_by', v_actor,
      'created_at', to_jsonb(now()),
      'items', v_items
    ))
  );

  insert into private.reviews (
    id,
    data_id,
    data_version,
    state_code,
    reviewer_id,
    json,
    review_kind,
    target_table,
    submitted_revision_checksum,
    target_owner_id,
    target_team_id,
    scope_schema_version,
    scope_history
  )
  values (
    v_root_review_id,
    p_target_id,
    p_target_version,
    0,
    '[]'::jsonb,
    private.review_build_json_v1(
      v_table,
      v_root_row,
      v_actor,
      'submit_review',
      v_actor
    ),
    'root',
    v_table,
    v_checksum,
    v_actor,
    nullif(v_root_row->>'team_id', '')::uuid,
    'review_scope.v1',
    v_scope_history
  )
  returning * into v_root_review;

  perform private.review_validate_scope_history_v1(
    v_root_review_id,
    v_scope_history
  );

  perform set_config('app.review_controlled_write', 'on', true);
  for v_target in
    select *
    from review_submit_v2_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = case when state_code < 20 then 20 else state_code end,
              reviews = case
                when state_code < 100
                  then api.cmd_review_append_review_ref(reviews, $1)
                else reviews
              end,
              modified_at = now()
        where id = $2
          and version = $3',
      v_target.table_name
    ) using v_root_review_id, v_target.dataset_id, v_target.dataset_version;

    if not v_target.is_root
      and v_target.state_code < 20
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor then
      v_event_key := private.review_notify_event_v1(
        'reference_entered_review',
        (
          select item.reference_review_id
          from private.review_scope_current_items_v1(v_scope_history) as item
          where item.item_kind = 'reference'
            and item.target_table = v_target.table_name
            and item.data_id = v_target.dataset_id
            and item.data_version = v_target.dataset_version
          limit 1
        ),
        nullif(v_target.dataset_row->>'user_id', '')::uuid,
        v_actor,
        v_target.table_name,
        v_target.dataset_id,
        v_target.dataset_version,
        v_root_review_id,
        1,
        null
      );
    end if;

    v_affected := v_affected || jsonb_build_array(jsonb_build_object(
      'table', v_target.table_name,
      'id', v_target.dataset_id,
      'version', v_target.dataset_version,
      'previous_state_code', v_target.state_code,
      'state_code', case
        when v_target.state_code < 20 then 20
        else v_target.state_code
      end
    ));
  end loop;
  perform set_config('app.review_controlled_write', 'off', true);

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_review_submit_v2',
    v_actor,
    v_table,
    p_target_id,
    p_target_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_id', v_root_review_id,
      'review_kind', 'root',
      'submission_mode', 'root',
      'scope_version', 1,
      'reference_review_ids', to_jsonb(v_reference_ids),
      'affected_datasets', v_affected
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'reviewId', v_root_review_id,
      'reviewKind', 'root',
      'submissionMode', 'root',
      'review', to_jsonb(v_root_review),
      'affectedDatasets', v_affected
    )
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    if sqlerrm = 'REFERENCE_OWNER_UNRESOLVED' then
      return jsonb_build_object(
        'ok', false,
        'code', sqlerrm,
        'status', 409,
        'message', 'Review submission could not be completed'
      );
    end if;
    raise;
end;
$_$;

ALTER FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") TO "authenticated";
