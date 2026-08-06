CREATE OR REPLACE FUNCTION "api"."get_current_lca_release"() RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_publication private.lca_release_publications%rowtype;
begin
  select * into v_publication
  from private.lca_release_publications
  where is_current = true and status = 'current'
  order by published_at desc
  limit 1;

  if v_publication.id is null then
    return private.lca_release_error('publication_not_found', 404, 'No current public LCA release exists');
  end if;

  return api.get_lca_release_run(v_publication.release_run_id);
end;
$$;

ALTER FUNCTION "api"."get_current_lca_release"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_current_lca_release"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_current_lca_release"() TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_current_lca_release"() TO "anon";

GRANT ALL ON FUNCTION "api"."get_current_lca_release"() TO "authenticated";
