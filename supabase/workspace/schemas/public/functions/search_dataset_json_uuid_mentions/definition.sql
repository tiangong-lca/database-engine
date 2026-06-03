CREATE OR REPLACE FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[] DEFAULT NULL::"text"[], "p_data_source" "text" DEFAULT 'tg'::"text", "p_this_user_id" "text" DEFAULT ''::"text", "p_team_id_filter" "uuid" DEFAULT NULL::"uuid", "p_state_code_filter" integer DEFAULT NULL::integer, "p_limit" integer DEFAULT 20) RETURNS TABLE("rank" bigint, "source_entity_kind" "text", "source_id" "uuid", "source_version" character, "source_name" "text", "source_modified_at" timestamp with time zone, "source_team_id" "uuid", "source_json" "jsonb", "matched_by" "text", "matched_entity_table" "text")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '20s'
    AS $$
begin
  return query
    select *
    from private.search_dataset_json_uuid_mentions_impl(
      p_uuid,
      p_source_entity_kinds,
      p_data_source,
      p_this_user_id,
      p_team_id_filter,
      p_state_code_filter,
      p_limit
    );
end;
$$;

ALTER FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "service_role";
