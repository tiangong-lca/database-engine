CREATE OR REPLACE FUNCTION "private"."tidas_import_group_apply_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid", "p_plan_sha256" "text", "p_root" "jsonb", "p_entries" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
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
end $_$;

ALTER FUNCTION "private"."tidas_import_group_apply_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid", "p_plan_sha256" "text", "p_root" "jsonb", "p_entries" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."tidas_import_group_apply_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid", "p_plan_sha256" "text", "p_root" "jsonb", "p_entries" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."tidas_import_group_apply_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid", "p_plan_sha256" "text", "p_root" "jsonb", "p_entries" "jsonb") TO "service_role";
