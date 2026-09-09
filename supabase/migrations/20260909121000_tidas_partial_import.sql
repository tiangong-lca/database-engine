-- Database #634 / workspace #1077. Additive, service-only partial package import.
-- Validation is Worker-owned and package-local. These functions run only after
-- the immutable validation plan is complete; they never validate against live data.
begin;

alter table private.lca_package_artifacts drop constraint lca_package_artifacts_format_chk;
alter table private.lca_package_artifacts add constraint lca_package_artifacts_format_chk
  check (artifact_format in ('tidas-package-zip:v1','tidas-package-export-report:v1',
    'tidas-package-import-report:v1','tidas-package-import-report:v2','tidas-package-import-details:v2'));
alter table private.lca_package_artifacts drop constraint lca_package_artifacts_kind_chk;
alter table private.lca_package_artifacts add constraint lca_package_artifacts_kind_chk
  check (artifact_kind in ('import_source','export_zip','export_report','import_report','import_details'));

create table private.tidas_import_plans_v2 (
  worker_job_id uuid primary key references private.worker_jobs(id),
  source_artifact_id uuid not null references private.lca_package_artifacts(id),
  source_sha256 text not null check (source_sha256 ~ '^[0-9a-f]{64}$'),
  plan_sha256 text not null check (plan_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz not null default now()
);

create table private.tidas_import_groups_v2 (
  worker_job_id uuid not null references private.tidas_import_plans_v2(worker_job_id),
  root_table text not null check (root_table in ('processes', 'lifecyclemodels')),
  root_id uuid not null,
  root_version text not null check (root_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'),
  entries_sha256 text not null,
  receipt jsonb not null check (jsonb_typeof(receipt) = 'object'),
  committed_at timestamptz not null default now(),
  primary key (worker_job_id, root_table, root_id, root_version)
);

alter table private.tidas_import_plans_v2 enable row level security;
alter table private.tidas_import_groups_v2 enable row level security;
revoke all on private.tidas_import_plans_v2, private.tidas_import_groups_v2 from public, anon, authenticated, service_role;

-- No job-row lock during the insert loop: heartbeats must remain possible.
-- The final fence takes the lock immediately before the transaction returns.
create function private.tidas_import_guard_v2(p_worker_job_id uuid, p_lease_token uuid, p_source_artifact_id uuid)
returns uuid language plpgsql security definer set search_path = '' as $$
declare v_user uuid;
begin
  select j.requested_by into v_user
  from private.worker_jobs j
  join private.lca_package_artifacts a on a.worker_job_id = j.id
  where j.id = p_worker_job_id and j.lease_token = p_lease_token
    and j.status = 'running' and j.lease_expires_at > clock_timestamp()
    and j.job_kind = 'tidas.import_package'
    and j.payload_schema_version = 'tidas.import_package.request.v2'
    and j.payload_json ->> 'import_policy' = 'root_closure_v2'
    and j.payload_json ->> 'source_artifact_id' = p_source_artifact_id::text
    and a.id = p_source_artifact_id and a.artifact_kind = 'import_source'
    and a.status = 'ready' and a.artifact_sha256 ~ '^[0-9a-f]{64}$'
    and a.metadata ->> 'requested_by' = j.requested_by::text;
  if v_user is null then
    raise exception using errcode = '55000', message = 'TIDAS_IMPORT_LEASE_OR_SOURCE_INVALID';
  end if;
  return v_user;
end $$;

create function private.tidas_import_group_apply_v2(
  p_worker_job_id uuid, p_lease_token uuid, p_source_artifact_id uuid,
  p_plan_sha256 text, p_root jsonb, p_entries jsonb
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  v_user uuid;
  v_plan private.tidas_import_plans_v2%rowtype;
  v_previous private.tidas_import_groups_v2%rowtype;
  v_source_sha text;
  v_entries_sha text;
  v_root_table text := p_root ->> 'table';
  v_root_id uuid := (p_root ->> 'id')::uuid;
  v_root_version text := p_root ->> 'version';
  v_entry jsonb;
  v_table text;
  v_id uuid;
  v_version text;
  v_inserted bigint;
  v_count bigint := 0;
  v_items jsonb := '[]'::jsonb;
  v_receipt jsonb;
begin
  v_user := private.tidas_import_guard_v2(p_worker_job_id, p_lease_token, p_source_artifact_id);
  if p_plan_sha256 is null or p_plan_sha256 !~ '^[0-9a-f]{64}$'
     or v_root_table is null or v_root_table not in ('processes', 'lifecyclemodels')
     or v_root_id is null or v_root_version is null
     or v_root_version !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
     or jsonb_typeof(p_entries) is distinct from 'array' then
    raise exception using errcode = '22023', message = 'TIDAS_IMPORT_GROUP_INVALID';
  end if;
  -- Explicit capacity admission; never silently truncate a root closure.
  if jsonb_array_length(p_entries) = 0 or jsonb_array_length(p_entries) > 50000
     or octet_length(p_entries::text) > 67108864 then
    raise exception using errcode = '54000', message = 'TIDAS_IMPORT_GROUP_CAPACITY_EXCEEDED';
  end if;
  if not exists (select 1 from jsonb_array_elements(p_entries) e
    where e ->> 'table' = v_root_table and e ->> 'id' = v_root_id::text
      and e ->> 'version' = v_root_version)
    or exists (select 1 from jsonb_array_elements(p_entries) e
      group by e ->> 'table', e ->> 'id', e ->> 'version' having count(*) > 1) then
    raise exception using errcode = '22023', message = 'TIDAS_IMPORT_GROUP_IDENTITY_INVALID';
  end if;

  select artifact_sha256 into v_source_sha from private.lca_package_artifacts where id = p_source_artifact_id;
  insert into private.tidas_import_plans_v2(worker_job_id, source_artifact_id, source_sha256, plan_sha256)
  values (p_worker_job_id, p_source_artifact_id, v_source_sha, p_plan_sha256)
  on conflict (worker_job_id) do nothing;
  select * into v_plan from private.tidas_import_plans_v2 where worker_job_id = p_worker_job_id for update;
  if v_plan.source_artifact_id <> p_source_artifact_id or v_plan.source_sha256 <> v_source_sha
     or v_plan.plan_sha256 <> p_plan_sha256 then
    raise exception using errcode = '55000', message = 'TIDAS_IMPORT_PLAN_MISMATCH';
  end if;
  v_entries_sha := encode(extensions.digest(convert_to(p_entries::text, 'UTF8'), 'sha256'), 'hex');
  select * into v_previous from private.tidas_import_groups_v2
  where worker_job_id = p_worker_job_id and root_table = v_root_table
    and root_id = v_root_id and root_version = v_root_version;
  if found then
    if v_previous.entries_sha256 <> v_entries_sha then
      raise exception using errcode = '55000', message = 'TIDAS_IMPORT_GROUP_REPLAY_MISMATCH';
    end if;
    return v_previous.receipt;
  end if;

  for v_entry in select e from jsonb_array_elements(p_entries) e order by
    case e ->> 'table' when 'contacts' then 1 when 'sources' then 2
      when 'unitgroups' then 3 when 'flowproperties' then 4 when 'flows' then 5
      when 'lifecyclemodels' then 6 when 'processes' then 7 else 8 end,
    e ->> 'id', e ->> 'version'
  loop
    v_table := v_entry ->> 'table';
    v_id := (v_entry ->> 'id')::uuid;
    v_version := v_entry ->> 'version';
    if v_table is null or v_table not in ('contacts','sources','unitgroups','flowproperties','flows','lifecyclemodels','processes')
       or v_id is null or v_version is null or v_version !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
       or jsonb_typeof(v_entry -> 'json_ordered') is distinct from 'object' then
      raise exception using errcode = '22023', message = 'TIDAS_IMPORT_ENTRY_INVALID';
    end if;
    if v_table = 'lifecyclemodels' then
      insert into public.lifecyclemodels(id, version, json_ordered, rule_verification, json_tg, user_id)
      values (v_id, v_version, v_entry -> 'json_ordered', coalesce((v_entry ->> 'rule_verification')::boolean, true),
        coalesce(nullif(v_entry -> 'json_tg', 'null'::jsonb), '{}'::jsonb), v_user)
      on conflict (id, version) do nothing;
    elsif v_table = 'processes' then
      insert into public.processes(id, version, json_ordered, rule_verification, model_id, user_id)
      values (v_id, v_version, v_entry -> 'json_ordered', coalesce((v_entry ->> 'rule_verification')::boolean, true),
        (v_entry ->> 'model_id')::uuid, v_user)
      on conflict (id, version) do nothing;
    else
      execute format('insert into public.%I(id, version, json_ordered, rule_verification, user_id)
        values ($1,$2,$3,$4,$5) on conflict (id, version) do nothing', v_table)
      using v_id, v_version, v_entry -> 'json_ordered', coalesce((v_entry ->> 'rule_verification')::boolean, true), v_user;
    end if;
    get diagnostics v_inserted = row_count;
    v_count := v_count + v_inserted;
    v_items := v_items || jsonb_build_array(jsonb_build_object(
      'table', v_table, 'id', v_id, 'version', v_version,
      'disposition', case when v_inserted = 1 then 'inserted' else 'existing' end));
  end loop;
  v_receipt := jsonb_build_object('root', p_root,
    'status', case when v_count > 0 then 'imported' else 'reused' end,
    'inserted_count', v_count, 'existing_count', jsonb_array_length(p_entries) - v_count, 'items', v_items);
  insert into private.tidas_import_groups_v2(worker_job_id, root_table, root_id, root_version, entries_sha256, receipt)
  values (p_worker_job_id, v_root_table, v_root_id, v_root_version, v_entries_sha, v_receipt);
  perform 1 from private.worker_jobs where id = p_worker_job_id for update;
  perform private.tidas_import_guard_v2(p_worker_job_id, p_lease_token, p_source_artifact_id);
  return v_receipt;
end $$;

create function api.svc_tidas_package_import_enqueue_v2(
  p_requested_by uuid, p_job_id uuid, p_source_artifact_id uuid,
  p_artifact_sha256 text, p_artifact_byte_size bigint, p_filename text,
  p_content_type text default null
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare v_existing uuid; v_schema text; v_result jsonb; v_worker_id uuid;
begin
  select worker_job_id into v_existing from private.lca_package_artifacts
  where id = p_source_artifact_id and job_id = p_job_id
    and metadata ->> 'requested_by' = p_requested_by::text for update;
  if v_existing is not null then
    select payload_schema_version into v_schema from private.worker_jobs where id = v_existing;
    if v_schema is distinct from 'tidas.import_package.request.v2' then
      return jsonb_build_object('ok', false, 'code', 'IMPORT_POLICY_MISMATCH', 'status', 409);
    end if;
  end if;
  v_result := api.svc_tidas_package_import_enqueue(p_requested_by, p_job_id, p_source_artifact_id,
    p_artifact_sha256, p_artifact_byte_size, p_filename, p_content_type);
  if coalesce((v_result ->> 'ok')::boolean, false) then
    v_worker_id := (v_result ->> 'worker_job_id')::uuid;
    update private.worker_jobs set payload_schema_version = 'tidas.import_package.request.v2',
      payload_json = payload_json || jsonb_build_object('import_policy', 'root_closure_v2')
    where id = v_worker_id and status = 'queued' and attempt_count = 0;
  end if;
  return v_result;
end $$;

revoke all on function private.tidas_import_guard_v2(uuid,uuid,uuid) from public, anon, authenticated, service_role;
revoke all on function private.tidas_import_group_apply_v2(uuid,uuid,uuid,text,jsonb,jsonb) from public, anon, authenticated;
grant execute on function private.tidas_import_group_apply_v2(uuid,uuid,uuid,text,jsonb,jsonb) to service_role;
revoke all on function api.svc_tidas_package_import_enqueue_v2(uuid,uuid,uuid,text,bigint,text,text) from public, anon, authenticated;
grant execute on function api.svc_tidas_package_import_enqueue_v2(uuid,uuid,uuid,text,bigint,text,text) to service_role;
insert into private.api_capability_grants(routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role)
values ('api.svc_tidas_package_import_enqueue_v2(uuid, uuid, uuid, text, bigint, text, text)', 'EDGE-PKG-01', false, false, true);

-- A missing report must never erase evidence of earlier committed groups.
-- Reuse the existing owner check before reading the private receipt ledger.
create function api.svc_tidas_package_read_v2(p_requested_by uuid, p_lookup_id uuid)
returns jsonb language plpgsql stable security definer set search_path = '' as $$
declare v_result jsonb; v_worker uuid; v_progress jsonb;
begin
  v_result := api.svc_tidas_package_read(p_requested_by, p_lookup_id);
  v_worker := (v_result #>> '{data,workerJobId}')::uuid;
  if v_worker is null or v_result #>> '{data,payload,import_policy}' is distinct from 'root_closure_v2' then
    return v_result;
  end if;
  with identities as (
    select item ->> 'table' as tab, item ->> 'id' as id, item ->> 'version' as ver,
      bool_or(item ->> 'disposition' = 'inserted') as inserted
    from private.tidas_import_groups_v2 g
    cross join lateral jsonb_array_elements(g.receipt -> 'items') item
    where g.worker_job_id = v_worker
    group by 1,2,3
  )
  select jsonb_build_object('imported_count', count(*) filter (where inserted),
    'existing_count', count(*) filter (where not inserted),
    'successful_root_count', (select count(*) from private.tidas_import_groups_v2 where worker_job_id = v_worker),
    'source', 'committed_receipts') into v_progress from identities;
  return jsonb_set(v_result, '{data,importProgress}', v_progress);
end $$;
revoke all on function api.svc_tidas_package_read_v2(uuid,uuid) from public, anon, authenticated;
grant execute on function api.svc_tidas_package_read_v2(uuid,uuid) to service_role;
insert into private.api_capability_grants(routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role)
values ('api.svc_tidas_package_read_v2(uuid, uuid)', 'EDGE-PKG-01', false, false, true);

commit;
