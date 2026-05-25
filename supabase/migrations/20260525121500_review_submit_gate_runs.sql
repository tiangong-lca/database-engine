create table if not exists public.dataset_review_submit_gate_runs (
  id uuid primary key default gen_random_uuid(),
  dataset_table text not null,
  dataset_id uuid not null,
  dataset_version text not null,
  revision_checksum text not null,
  policy_profile text not null default 'review_submit_fast.v1',
  report_schema_version text not null default 'review_submit_gate_report.v1',
  status text not null default 'queued',
  requested_by uuid not null,
  supersedes_gate_run_id uuid references public.dataset_review_submit_gate_runs(id),
  calculator_report jsonb,
  blocking_reasons jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  modified_at timestamptz not null default now(),
  completed_at timestamptz,
  constraint dataset_review_submit_gate_runs_table_check
    check (dataset_table in ('processes', 'lifecyclemodels')),
  constraint dataset_review_submit_gate_runs_checksum_check
    check (revision_checksum ~ '^[a-f0-9]{64}$'),
  constraint dataset_review_submit_gate_runs_status_check
    check (status in ('queued', 'running', 'passed', 'blocked', 'error', 'stale')),
  constraint dataset_review_submit_gate_runs_blocking_reasons_check
    check (jsonb_typeof(blocking_reasons) = 'array'),
  constraint dataset_review_submit_gate_runs_calculator_report_check
    check (calculator_report is null or jsonb_typeof(calculator_report) = 'object')
);

alter table public.dataset_review_submit_gate_runs enable row level security;

create index if not exists dataset_review_submit_gate_runs_revision_idx
  on public.dataset_review_submit_gate_runs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    created_at desc
  );

create index if not exists dataset_review_submit_gate_runs_requested_by_idx
  on public.dataset_review_submit_gate_runs (requested_by, created_at desc);

create index if not exists dataset_review_submit_gate_runs_status_idx
  on public.dataset_review_submit_gate_runs (status, modified_at desc);

revoke all on public.dataset_review_submit_gate_runs from anon, authenticated;
grant all on public.dataset_review_submit_gate_runs to service_role;

create or replace function public.cmd_dataset_review_submit_gate_payload(
  p_run public.dataset_review_submit_gate_runs,
  p_status_override text default null
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'status', coalesce(p_status_override, (p_run).status),
    'gateRunId', (p_run).id,
    'datasetRevision', jsonb_build_object(
      'table', (p_run).dataset_table,
      'id', (p_run).dataset_id,
      'version', (p_run).dataset_version,
      'revisionChecksum', (p_run).revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', (p_run).policy_profile
    ),
    'calculatorReport',
      case
        when (p_run).calculator_report is null then null
        else jsonb_build_object(
          'schemaVersion', (p_run).report_schema_version
        ) || (p_run).calculator_report
      end,
    'blockingReasons', coalesce((p_run).blocking_reasons, '[]'::jsonb),
    'createdAt', to_jsonb((p_run).created_at),
    'modifiedAt', to_jsonb((p_run).modified_at),
    'completedAt', to_jsonb((p_run).completed_at)
  )
$$;

create or replace function public.cmd_dataset_review_submit_gate(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1',
  p_action text default 'ensure',
  p_gate_run_id uuid default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_existing public.dataset_review_submit_gate_runs%rowtype;
  v_supersedes uuid;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review-submit gate'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  if v_action not in ('ensure', 'read', 'rerun') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_ACTION',
      'status', 400,
      'message', 'action must be ensure, read, or rerun'
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can run or read the review-submit gate'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_existing
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id
    for update;

    if v_existing.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_existing.dataset_table <> p_table
      or v_existing.dataset_id <> p_id
      or v_existing.dataset_version <> p_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run belongs to a different dataset revision'
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(
          v_existing,
          case
            when v_existing.revision_checksum <> p_revision_checksum then 'stale'
            else null
          end
        )
      );
    end if;

    if v_action = 'ensure' and v_existing.revision_checksum = p_revision_checksum then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_existing)
      );
    end if;

    v_supersedes := v_existing.id;
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_gate',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version
      ),
      0
    )
  );

  if v_action in ('ensure', 'read') and p_gate_run_id is null then
    select *
      into v_run
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and revision_checksum = p_revision_checksum
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
    order by created_at desc
    limit 1
    for update;

    if v_run.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;
  end if;

  if v_action = 'rerun' and p_gate_run_id is null then
    select id
      into v_supersedes
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
    order by created_at desc
    limit 1;
  end if;

  insert into public.dataset_review_submit_gate_runs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    supersedes_gate_run_id
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    'queued',
    v_actor,
    v_supersedes
  )
  returning *
    into v_run;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', v_action,
      'gate_run_id', v_run.id,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version,
      'supersedes_gate_run_id', v_supersedes
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_gate_record_result(
  p_gate_run_id uuid,
  p_status text,
  p_calculator_report jsonb default null,
  p_blocking_reasons jsonb default '[]'::jsonb,
  p_report_schema_version text default 'review_submit_gate_report.v1',
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_status text := lower(trim(coalesce(p_status, '')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
begin
  if v_status not in ('passed', 'blocked', 'error') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT_STATUS',
      'status', 400,
      'message', 'result status must be passed, blocked, or error'
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  if p_calculator_report is not null and jsonb_typeof(p_calculator_report) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_REPORT',
      'status', 400,
      'message', 'calculator report must be a JSON object'
    );
  end if;

  if jsonb_typeof(coalesce(p_blocking_reasons, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_BLOCKING_REASONS',
      'status', 400,
      'message', 'blockingReasons must be a JSON array'
    );
  end if;

  if v_status = 'passed' and jsonb_array_length(coalesce(p_blocking_reasons, '[]'::jsonb)) > 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT',
      'status', 400,
      'message', 'passed gate results cannot include blockingReasons'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  update public.dataset_review_submit_gate_runs
    set status = v_status,
        calculator_report = p_calculator_report,
        blocking_reasons = coalesce(p_blocking_reasons, '[]'::jsonb),
        report_schema_version = p_report_schema_version,
        modified_at = now(),
        completed_at = now()
  where id = p_gate_run_id
  returning *
    into v_run;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate_record_result',
    coalesce(v_actor, v_run.requested_by),
    v_run.dataset_table,
    v_run.dataset_id,
    v_run.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'gate_run_id', v_run.id,
      'status', v_status,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

create or replace function public.cmd_dataset_assert_review_submit_gate_passed(
  p_table text,
  p_id uuid,
  p_version text,
  p_gate_run_id uuid default null,
  p_revision_checksum text default null,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1'
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_run public.dataset_review_submit_gate_runs%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object('ok', true);
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if p_gate_run_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_REQUIRED',
      'status', 400,
      'message', 'A passed review-submit gate run is required before process review submission'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  if v_run.dataset_table <> p_table
    or v_run.dataset_id <> p_id
    or v_run.dataset_version <> p_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run belongs to a different dataset revision'
    );
  end if;

  if v_run.revision_checksum <> p_revision_checksum then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_STALE',
      'status', 409,
      'message', 'Review-submit gate run is stale for the submitted dataset revision',
      'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
    );
  end if;

  if v_run.policy_profile <> coalesce(p_policy_profile, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different policy profile',
      'details', jsonb_build_object(
        'expected', p_policy_profile,
        'actual', v_run.policy_profile
      )
    );
  end if;

  if v_run.report_schema_version <> coalesce(p_report_schema_version, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different report schema version',
      'details', jsonb_build_object(
        'expected', p_report_schema_version,
        'actual', v_run.report_schema_version
      )
    );
  end if;

  case v_run.status
    when 'passed' then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    when 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    when 'queued', 'running' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    when 'error' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate failed before review submission',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    else
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate run is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
  end case;
end;
$$;

drop function if exists public.cmd_review_submit_without_gate(text, uuid, text, jsonb);

alter function public.cmd_review_submit(text, uuid, text, jsonb)
  rename to cmd_review_submit_without_gate;

revoke all on function public.cmd_review_submit_without_gate(text, uuid, text, jsonb) from public;
revoke all on function public.cmd_review_submit_without_gate(text, uuid, text, jsonb) from authenticated;
revoke all on function public.cmd_review_submit_without_gate(text, uuid, text, jsonb) from service_role;

create or replace function public.cmd_review_submit(
  p_table text,
  p_id uuid,
  p_version text,
  p_audit jsonb default '{}'::jsonb,
  p_review_submit_gate_run_id uuid default null,
  p_review_submit_revision_checksum text default null,
  p_review_submit_policy_profile text default 'review_submit_fast.v1',
  p_review_submit_report_schema_version text default 'review_submit_gate_report.v1'
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_gate_assertion jsonb;
begin
  v_gate_assertion := public.cmd_dataset_assert_review_submit_gate_passed(
    p_table,
    p_id,
    p_version,
    p_review_submit_gate_run_id,
    p_review_submit_revision_checksum,
    p_review_submit_policy_profile,
    p_review_submit_report_schema_version
  );

  if coalesce((v_gate_assertion->>'ok')::boolean, false) is false then
    return v_gate_assertion;
  end if;

  return public.cmd_review_submit_without_gate(
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_gate_run_id', p_review_submit_gate_run_id,
      'review_submit_revision_checksum', p_review_submit_revision_checksum,
      'review_submit_policy_profile', p_review_submit_policy_profile,
      'review_submit_report_schema_version', p_review_submit_report_schema_version
    )
  );
end;
$$;

revoke all on function public.cmd_dataset_review_submit_gate_payload(
  public.dataset_review_submit_gate_runs,
  text
) from public;
revoke all on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) from public;
revoke all on function public.cmd_dataset_review_submit_gate_record_result(
  uuid,
  text,
  jsonb,
  jsonb,
  text,
  jsonb
) from public;
revoke all on function public.cmd_dataset_assert_review_submit_gate_passed(
  text,
  uuid,
  text,
  uuid,
  text,
  text,
  text
) from public;
revoke all on function public.cmd_review_submit(
  text,
  uuid,
  text,
  jsonb,
  uuid,
  text,
  text,
  text
) from public;

grant execute on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) to authenticated;
grant execute on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) to service_role;
grant execute on function public.cmd_dataset_review_submit_gate_record_result(
  uuid,
  text,
  jsonb,
  jsonb,
  text,
  jsonb
) to service_role;
grant execute on function public.cmd_review_submit(
  text,
  uuid,
  text,
  jsonb,
  uuid,
  text,
  text,
  text
) to authenticated;
grant execute on function public.cmd_review_submit(
  text,
  uuid,
  text,
  jsonb,
  uuid,
  text,
  text,
  text
) to service_role;
