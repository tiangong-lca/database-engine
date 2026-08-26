CREATE OR REPLACE FUNCTION "private"."portal_named_reference_v1"("p_reference" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_reference jsonb;
  v_id text;
  v_version text;
  v_name jsonb;
begin
  v_reference := case
    when jsonb_typeof(p_reference) = 'object' then p_reference
    when jsonb_typeof(p_reference) = 'array'
      and jsonb_array_length(p_reference) = 1 then p_reference -> 0
    else null
  end;
  v_id := nullif(lower(private.portal_scalar_text_v1(v_reference -> '@refObjectId')), '');
  v_version := nullif(private.portal_scalar_text_v1(v_reference -> '@version'), '');
  v_name := private.portal_localized_text_v1(v_reference -> 'common:shortDescription');
  if coalesce(
       v_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
       false
     ) is not true
     or coalesce(v_version ~ '^\d{2}\.\d{2}\.\d{3}$', false) is not true then
    v_id := null;
    v_version := null;
  end if;
  return jsonb_build_object('id', v_id, 'version', v_version, 'name', v_name);
end
$_$;

ALTER FUNCTION "private"."portal_named_reference_v1"("p_reference" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_named_reference_v1"("p_reference" "jsonb") FROM PUBLIC;
