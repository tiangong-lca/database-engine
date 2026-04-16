CREATE OR REPLACE FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with normalized as (
    select
      min(item.ordinality) as first_ordinality,
      nullif(btrim(item.value), '') as normalized_value
    from unnest(coalesce(p_values, array[]::text[])) with ordinality as item(value, ordinality)
    group by nullif(btrim(item.value), '')
  )
  select coalesce(
    array(
      select normalized_value
      from normalized
      where normalized_value is not null
      order by first_ordinality
    ),
    array[]::text[]
  );
$$;

ALTER FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "service_role";
