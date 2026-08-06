CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_report private.worker_job_artifacts%rowtype;
  v_machine_result private.worker_job_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
begin
  if new.certificate_status <> 'valid' then
    return new;
  end if;
  select * into v_report
  from private.worker_job_artifacts where id = new.report_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts where id = new.closure_bundle_artifact_id;
  select * into v_machine_result
  from private.worker_job_artifacts
  where id = coalesce(
    new.complete_machine_result_artifact_id,
    case
      when coalesce(
        v_bundle.metadata->>'completeMachineResultArtifactId', ''
      ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      then (v_bundle.metadata->>'completeMachineResultArtifactId')::uuid
    end
  );
  if v_report.id is null
     or v_report.job_id <> new.worker_job_id
     or v_report.artifact_role <> 'closure_report'
     or v_report.lifecycle_state <> 'ready'
     or v_machine_result.id is null
     or (
       v_machine_result.job_id <> new.worker_job_id
       and not exists (
         select 1
         from private.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_machine_result.job_id
       )
     )
     or v_machine_result.artifact_role <> 'complete_machine_result'
     or v_machine_result.lifecycle_state <> 'ready'
     or v_bundle.id is null
     or (
       v_bundle.job_id <> new.worker_job_id
       and not exists (
         select 1
         from private.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_bundle.job_id
       )
     )
     or v_bundle.artifact_role <> 'closure_bundle'
     or v_bundle.lifecycle_state <> 'ready'
     or v_report.expires_at is null
     or v_machine_result.expires_at is null
     or v_bundle.expires_at is null then
    raise exception 'closure_certificate_evidence_lifecycle_invalid'
      using errcode = '23514';
  end if;
  new.complete_machine_result_artifact_id := v_machine_result.id;
  new.valid_until := least(
    v_report.expires_at,
    v_machine_result.expires_at,
    v_bundle.expires_at
  );
  if new.valid_until <= coalesce(new.finished_at, now())
     or new.valid_until <= now() then
    raise exception 'closure_certificate_evidence_already_expired'
      using errcode = '23514';
  end if;
  return new;
end;
$_$;

ALTER FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() TO "api_internal_executor";
