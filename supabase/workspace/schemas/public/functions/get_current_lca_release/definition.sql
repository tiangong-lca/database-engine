CREATE OR REPLACE FUNCTION "public"."get_current_lca_release"() RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_publication public.lca_release_publications%rowtype;
begin
  select * into v_publication
  from public.lca_release_publications
  where is_current = true and status = 'current'
  order by published_at desc
  limit 1;

  if v_publication.id is null then
    return public.lca_release_error('publication_not_found', 404, 'No current public LCA release exists');
  end if;

  return public.get_lca_release_run(v_publication.release_run_id);
end;
$$;

ALTER FUNCTION "public"."get_current_lca_release"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."get_current_lca_release"() FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."get_current_lca_release"() TO "anon";

GRANT ALL ON FUNCTION "public"."get_current_lca_release"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_current_lca_release"() TO "service_role";
