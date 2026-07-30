CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_build_admission_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
  v_check_id uuid;
begin
  if new.job_kind <> 'lcia_result.package_build'
     or new.payload_schema_version <> 'lcia_result.package_build.request.v2' then
    return new;
  end if;

  begin
    v_check_id := nullif(new.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    raise exception 'closure_certificate_expired_or_unavailable'
      using errcode = '23514';
  end;

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_check_id
    and requested_by = new.requested_by;
  if v_check.id is null
     or not public.lcia_scope_closure_evidence_usable(v_check) then
    raise exception 'closure_certificate_expired_or_unavailable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_build_admission_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_build_admission_guard"() FROM PUBLIC;
