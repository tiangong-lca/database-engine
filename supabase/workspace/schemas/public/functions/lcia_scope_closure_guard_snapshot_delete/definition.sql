CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_guard_snapshot_delete"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  if exists (
    select 1
    from public.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_id = old.id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  ) then
    raise exception 'lca_snapshot_has_valid_closure_certificate'
      using errcode = '23503';
  end if;
  if exists (
    select 1 from public.lcia_result_packages package
    where package.snapshot_id = old.id
  ) or exists (
    select 1 from public.lca_results result
    where result.snapshot_id = old.id
  ) then
    raise exception 'lca_snapshot_has_result_reference'
      using errcode = '23503';
  end if;
  return old;
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_guard_snapshot_delete"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_guard_snapshot_delete"() FROM PUBLIC;
