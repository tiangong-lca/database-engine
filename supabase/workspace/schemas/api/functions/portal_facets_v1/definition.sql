CREATE OR REPLACE FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := pg_catalog.lower(pg_catalog.btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(
    v_kind,
    v_query,
    v_filters,
    'relevance'
  );

  if v_query = '' and v_filters = '{}'::jsonb then
    perform private.assert_portal_catalog_facet_contract_v1();
    return private.catalog_portal_facets_empty_v1_impl(
      v_kind,
      v_fingerprint
    );
  end if;

  if v_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := v_query::uuid;
  end if;
  if v_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          v_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;

  return private.catalog_portal_facets_v1_impl(
    v_kind,
    v_query,
    v_exact_id,
    v_like_pattern,
    v_filters,
    v_fingerprint
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "authenticated";
