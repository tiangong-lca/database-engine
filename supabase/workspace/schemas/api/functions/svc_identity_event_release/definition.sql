CREATE OR REPLACE FUNCTION "api"."svc_identity_event_release"("p_event_id" "text") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_deleted integer;
begin
  if nullif(pg_catalog.btrim(p_event_id), '') is null then
    raise exception using errcode = '22023', message = 'IDENTITY_EVENT_ID_REQUIRED';
  end if;
  delete from private.identity_center_processed_events where event_id = p_event_id;
  get diagnostics v_deleted = row_count;
  return v_deleted = 1;
end
$$;

ALTER FUNCTION "api"."svc_identity_event_release"("p_event_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_event_release"("p_event_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_event_release"("p_event_id" "text") TO "service_role";
