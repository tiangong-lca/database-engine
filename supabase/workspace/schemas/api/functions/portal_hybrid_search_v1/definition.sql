CREATE OR REPLACE FUNCTION "api"."portal_hybrid_search_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $$
declare
  v_input jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,
    p_query_terms,
    p_query_embedding,
    p_filters,
    p_limit
  );
  v_page := private.portal_lcia_decorate_item_page_v1(
    private.portal_projection_hybrid_search_v1_impl(
      v_input ->> 'kind',
      array(
        select term.value
        from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
          with ordinality as term(value, ordinality)
        order by term.ordinality
      ),
      (v_input ->> 'queryEmbedding')::extensions.vector(1024),
      v_input -> 'filters',
      (v_input ->> 'limit')::integer,
      v_input ->> 'queryFingerprint'
    )
  );
  if v_page is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(v_page::text, 'UTF8')
     ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end
$$;

ALTER FUNCTION "api"."portal_hybrid_search_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_hybrid_search_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_hybrid_search_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_hybrid_search_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) TO "authenticated";
