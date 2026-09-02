CREATE OR REPLACE FUNCTION "api"."portal_hybrid_search_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer, "p_cursor" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    AS $_$
declare
  v_input jsonb;
  v_fingerprint text;
  v_cursor jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,p_query_terms,p_query_embedding,p_filters,p_limit);
  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-hybrid-rank-v2:' || (v_input ->> 'queryFingerprint'),'UTF8'),
    'sha256'),'hex');
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
      or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
      or not (v_cursor ?& array['v','fp','kind','rankKey','id','version'])
      or v_cursor ->> 'v' is distinct from '1'
      or v_cursor ->> 'fp' is distinct from v_fingerprint
      or v_cursor ->> 'kind' is distinct from p_kind
      or pg_catalog.jsonb_typeof(v_cursor -> 'rankKey') is distinct from 'string'
      or coalesce(v_cursor ->> 'rankKey','') !~ '^(0(\.\d{1,12})?|1(\.0{1,12})?)$'
      or coalesce(v_cursor ->> 'id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(v_cursor ->> 'version','') !~ '^\d{2}\.\d{2}\.\d{3}$'
      or private.portal_cursor_encode_v1(v_cursor) is distinct from p_cursor then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;
  v_page := private.portal_decorate_card_context_v1(private.portal_lcia_decorate_item_page_v1(
    private.portal_projection_hybrid_search_v2_impl(
      p_kind,
      array(select term.value from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
        with ordinality as term(value,ordinality) order by term.ordinality),
      (v_input ->> 'queryEmbedding')::extensions.vector(1024),
      v_input -> 'filters',p_limit,v_fingerprint,v_cursor
    )
  ));
  v_page := pg_catalog.jsonb_set(v_page,'{schemaVersion}','"portal.public-hybrid-candidate-page.v2"'::jsonb);
  v_page := (v_page - 'nextCursorPayload') || pg_catalog.jsonb_build_object(
    'nextCursor',case when nullif(v_page -> 'nextCursorPayload','null'::jsonb) is null then null
      else private.portal_cursor_encode_v1(v_page -> 'nextCursorPayload') end
  );
  if v_page is null or pg_catalog.octet_length(pg_catalog.convert_to(v_page::text,'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end;
$_$;

ALTER FUNCTION "api"."portal_hybrid_search_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer, "p_cursor" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_hybrid_search_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer, "p_cursor" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_hybrid_search_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer, "p_cursor" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_hybrid_search_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer, "p_cursor" "text") TO "authenticated";
