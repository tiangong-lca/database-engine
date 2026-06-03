CREATE OR REPLACE FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb", "p_review_submit_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_review_submit_revision_checksum" "text" DEFAULT NULL::"text", "p_review_submit_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_review_submit_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_gate_assertion jsonb;
begin
  v_gate_assertion := public.cmd_dataset_assert_review_submit_gate_passed(
    p_table,
    p_id,
    p_version,
    p_review_submit_gate_run_id,
    p_review_submit_revision_checksum,
    p_review_submit_policy_profile,
    p_review_submit_report_schema_version
  );

  if coalesce((v_gate_assertion->>'ok')::boolean, false) is false then
    return v_gate_assertion;
  end if;

  return public.cmd_review_submit_without_gate(
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_gate_run_id', p_review_submit_gate_run_id,
      'review_submit_revision_checksum', p_review_submit_revision_checksum,
      'review_submit_policy_profile', p_review_submit_policy_profile,
      'review_submit_report_schema_version', p_review_submit_report_schema_version
    )
  );
end;
$$;

ALTER FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "service_role";
