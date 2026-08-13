CREATE OR REPLACE FUNCTION "private"."lca_release_guard_dataset_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  raise exception 'lca_release_dataset_index_immutable'
    using errcode = '23514';
end;
$$;

ALTER FUNCTION "private"."lca_release_guard_dataset_update"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_release_guard_dataset_update"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_release_guard_dataset_update"() TO "api_internal_executor";
