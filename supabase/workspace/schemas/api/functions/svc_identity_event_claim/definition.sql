CREATE OR REPLACE FUNCTION "api"."svc_identity_event_claim"("p_event_id" "text", "p_event_type" "text") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_inserted integer;
begin
  if nullif(pg_catalog.btrim(p_event_id), '') is null then
    raise exception using errcode = '22023', message = 'IDENTITY_EVENT_ID_REQUIRED';
  end if;
  insert into private.identity_center_processed_events (event_id, event_type, processed_at)
  values (
    p_event_id,
    coalesce(nullif(pg_catalog.btrim(p_event_type), ''), 'unknown'),
    pg_catalog.now()
  )
  on conflict (event_id) do nothing;
  get diagnostics v_inserted = row_count;
  return v_inserted = 1;
end
$$;

ALTER FUNCTION "api"."svc_identity_event_claim"("p_event_id" "text", "p_event_type" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_event_claim"("p_event_id" "text", "p_event_type" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_event_claim"("p_event_id" "text", "p_event_type" "text") TO "service_role";
