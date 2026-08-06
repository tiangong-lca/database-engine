CREATE OR REPLACE FUNCTION "api"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if lower(coalesce(p_table, '')) = 'processes'
    and p_audit->>'source' = 'cmd_review_submit_from_job'
    and nullif(p_audit->>'review_submit_job_id', '') is not null then
    return api.cmd_review_submit_v2(
      p_table,
      p_id,
      p_version,
      jsonb_build_object(
        'reviewSubmitJobId',
        p_audit->>'review_submit_job_id'
      ),
      p_audit
    );
  end if;

  return private.cmd_review_submit_without_gate_pre_v2(
    p_table,
    p_id,
    p_version,
    p_audit
  );
end;
$$;

ALTER FUNCTION "api"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "api_internal_executor";
