-- Permit an unchanged rejected Reference Review revision to enter a new cycle.
-- Rejected rows remain immutable history; active and approved rows still reuse
-- the partial unique identity enforced by reviews_reference_revision_active_uidx.

do $migration$
declare
  v_definition text;
  v_rejected_reference_guard text := $guard$
  if exists (
    select 1
    from private.reviews as rejected
    where rejected.review_kind = 'reference'
      and rejected.target_table = p_target_table
      and rejected.data_id = (p_target_row->>'id')::uuid
      and btrim(rejected.data_version::text) = p_target_row->>'version'
      and rejected.submitted_revision_checksum = p_checksum
      and rejected.state_code = -1
  ) then
    raise exception using
      errcode = '23505',
      message = 'REFERENCE_REVISION_REJECTED_UNCHANGED';
  end if;
$guard$;
  v_owner_resubmit_guard text := $guard$
  if exists (
    select 1
    from private.reviews as rejected
    where rejected.review_kind = 'reference'
      and rejected.target_table = v_table
      and rejected.data_id = p_target_id
      and btrim(rejected.data_version::text) = p_target_version
      and rejected.submitted_revision_checksum = v_checksum
      and rejected.state_code = -1
      and exists (
        select 1
        from private.reviews as root_review
        where root_review.review_kind = 'root'
          and root_review.current_reference_review_ids
            @> array[rejected.id]::uuid[]
      )
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REFERENCE_REVISION_REJECTED_UNCHANGED',
      'status', 409,
      'message', 'The rejected reference must be changed before resubmission'
    );
  end if;
$guard$;
  v_dependency_wrapper text := $guard$
      begin
        v_reference := private.review_get_or_create_reference_v1(
          v_target.table_name,
          v_target.dataset_row,
          v_target_checksum,
          v_actor
        );
      exception
        when unique_violation then
          if sqlerrm = 'REFERENCE_REVISION_REJECTED_UNCHANGED' then
            return jsonb_build_object(
              'ok', false,
              'code', 'REFERENCE_REVISION_REJECTED_UNCHANGED',
              'status', 409,
              'message', 'A rejected reference revision must be changed'
            );
          end if;
          raise;
      end;
$guard$;
  v_dependency_call text := $replacement$
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_actor
      );
$replacement$;
  v_outer_error_mapping text := $guard$
    if sqlerrm in (
      'REFERENCE_REVISION_REJECTED_UNCHANGED',
      'REFERENCE_OWNER_UNRESOLVED'
    ) then
$guard$;
  v_owner_error_mapping text := $replacement$
    if sqlerrm = 'REFERENCE_OWNER_UNRESOLVED' then
$replacement$;
begin
  v_definition := pg_catalog.pg_get_functiondef(
    'private.review_get_or_create_reference_v1(text,jsonb,text,uuid)'
      ::pg_catalog.regprocedure
  );

  if pg_catalog.strpos(v_definition, v_rejected_reference_guard) = 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_REFERENCE_HELPER_GUARD_NOT_FOUND';
  end if;

  v_definition := pg_catalog.replace(
    v_definition,
    v_rejected_reference_guard,
    pg_catalog.chr(10)
  );
  execute v_definition;

  v_definition := pg_catalog.pg_get_functiondef(
    'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'
      ::pg_catalog.regprocedure
  );

  if pg_catalog.strpos(v_definition, v_owner_resubmit_guard) = 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_OWNER_RESUBMIT_GUARD_NOT_FOUND';
  end if;

  if pg_catalog.strpos(v_definition, v_dependency_wrapper) = 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_DEPENDENCY_WRAPPER_NOT_FOUND';
  end if;

  if pg_catalog.strpos(v_definition, v_outer_error_mapping) = 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_OUTER_ERROR_MAPPING_NOT_FOUND';
  end if;

  v_definition := pg_catalog.replace(
    v_definition,
    v_owner_resubmit_guard,
    pg_catalog.chr(10)
  );
  v_definition := pg_catalog.replace(
    v_definition,
    v_dependency_wrapper,
    v_dependency_call
  );
  v_definition := pg_catalog.replace(
    v_definition,
    v_outer_error_mapping,
    v_owner_error_mapping
  );
  execute v_definition;

  if pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'private.review_get_or_create_reference_v1(text,jsonb,text,uuid)'
        ::pg_catalog.regprocedure
    ),
    'REFERENCE_REVISION_REJECTED_UNCHANGED'
  ) > 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_REFERENCE_HELPER_GUARD_REMAINS';
  end if;

  if pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'REFERENCE_REVISION_REJECTED_UNCHANGED'
  ) > 0 then
    raise exception using
      errcode = '55000',
      message = 'ISSUE_439_REVIEW_SUBMIT_GUARD_REMAINS';
  end if;
end;
$migration$;
