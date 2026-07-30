CREATE OR REPLACE FUNCTION "public"."lca_release_guard_dataset_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  raise exception 'lca_release_dataset_index_immutable'
    using errcode = '23514';
end;
$$;

ALTER FUNCTION "public"."lca_release_guard_dataset_update"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_release_guard_dataset_update"() FROM PUBLIC;
