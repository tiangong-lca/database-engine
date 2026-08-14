CREATE OR REPLACE FUNCTION "private"."review_revision_fingerprint_v1"("p_target_table" "text", "p_target_row" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_document jsonb;
begin
  if lower(p_target_table) not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    raise exception using
      errcode = '22023',
      message = 'REVIEW_TARGET_TABLE_INVALID';
  end if;

  v_document := coalesce(
    p_target_row->'json_ordered',
    p_target_row->'json',
    '{}'::jsonb
  );

  return pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        private.review_canonical_json_text_v1(v_document),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );
end;
$$;

ALTER FUNCTION "private"."review_revision_fingerprint_v1"("p_target_table" "text", "p_target_row" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_revision_fingerprint_v1"("p_target_table" "text", "p_target_row" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_revision_fingerprint_v1"("p_target_table" "text", "p_target_row" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."review_revision_fingerprint_v1"("p_target_table" "text", "p_target_row" "jsonb") TO "api_internal_executor";
