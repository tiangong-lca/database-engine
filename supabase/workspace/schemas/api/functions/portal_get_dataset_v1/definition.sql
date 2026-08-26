CREATE OR REPLACE FUNCTION "api"."portal_get_dataset_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_version is null
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return private.portal_lcia_decorate_dataset_v1(
    private.portal_dataset_projection_v1(p_kind, p_id, p_version)
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_get_dataset_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_get_dataset_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_get_dataset_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_get_dataset_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") TO "authenticated";
