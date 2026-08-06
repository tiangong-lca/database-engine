CREATE OR REPLACE FUNCTION "api"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_root jsonb;
  v_assertion jsonb;
begin
  if v_actor is null
    or p_table not in ('processes', 'lifecyclemodels') then
    return private.cmd_dataset_publish_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('review-publication-lifecycle-closure', 0)
  );

  v_root := api.cmd_review_get_dataset_row(
    p_table,
    p_id,
    p_version,
    true
  );

  if v_root is null
    or nullif(v_root->>'user_id', '')::uuid is distinct from v_actor
    or coalesce((v_root->>'state_code')::integer, 0) >= 100 then
    return private.cmd_dataset_publish_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  if p_table = 'processes' then
    lock table public.lifecyclemodels in share row exclusive mode;
  end if;

  v_assertion := private.cmd_review_assert_lifecycle_closure(
    jsonb_build_array(jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version
    )),
    'publish',
    v_actor
  );

  if v_assertion is not null then
    return v_assertion;
  end if;

  return private.cmd_dataset_publish_issue304_legacy(
    p_table,
    p_id,
    p_version,
    p_audit
  );
end;
$$;

ALTER FUNCTION "api"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "authenticated";
