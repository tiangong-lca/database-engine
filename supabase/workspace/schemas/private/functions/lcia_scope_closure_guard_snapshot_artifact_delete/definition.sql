CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_guard_snapshot_artifact_delete"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  if exists (
    select 1
    from private.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_artifact_id = old.id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  ) then
    raise exception 'lca_snapshot_artifact_has_valid_closure_certificate'
      using errcode = '23503';
  end if;
  if exists (
    select 1 from private.lcia_result_packages package
    where package.snapshot_id = old.snapshot_id
  ) or exists (
    select 1 from private.lca_results result
    where result.snapshot_id = old.snapshot_id
  ) then
    raise exception 'lca_snapshot_artifact_has_result_reference'
      using errcode = '23503';
  end if;
  return old;
end;
$$;

ALTER FUNCTION "private"."lcia_scope_closure_guard_snapshot_artifact_delete"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_guard_snapshot_artifact_delete"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_guard_snapshot_artifact_delete"() TO "api_internal_executor";
