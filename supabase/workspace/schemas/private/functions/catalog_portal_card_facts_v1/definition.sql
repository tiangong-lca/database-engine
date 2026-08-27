CREATE OR REPLACE FUNCTION "private"."catalog_portal_card_facts_v1"("p_card" "jsonb", "p_filters" "jsonb", "p_query" "text") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE SECURITY DEFINER PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select pg_catalog.jsonb_build_object(
    'accessLevel', p_card -> 'accessLevel',
    'nameKey', p_card #> '{names,0,value}',
    'nameExact', pg_catalog.to_jsonb(case when p_query = '' then false else
      exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          coalesce(p_card -> 'names', '[]'::jsonb)
        ) as name(item)
        where pg_catalog.lower(pg_catalog.btrim(name.item ->> 'value')) = p_query
      )
    end),
    'nameContains', pg_catalog.to_jsonb(case when p_query = '' then false else
      exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          coalesce(p_card -> 'names', '[]'::jsonb)
        ) as name(item)
        where pg_catalog.strpos(
          pg_catalog.lower(name.item ->> 'value'),
          p_query
        ) > 0
      )
    end),
    'classificationExact', pg_catalog.to_jsonb(
      case when p_query = '' then false else exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          coalesce(p_card -> 'classifications', '[]'::jsonb)
        ) as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(
          classification.item ->> 'code'
        )) = p_query
      ) end
    ),
    'classificationContains', pg_catalog.to_jsonb(
      case when p_query = '' then false else exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          coalesce(p_card -> 'classifications', '[]'::jsonb)
        ) as classification(item)
        where pg_catalog.strpos(
          pg_catalog.lower(classification.item ->> 'code'),
          p_query
        ) > 0
      ) end
    ),
    'classificationFilterMatch', pg_catalog.to_jsonb(
      case when not (p_filters ? 'classification') then false else exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          coalesce(p_card -> 'classifications', '[]'::jsonb)
        ) as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(
          classification.item ->> 'code'
        )) = p_filters ->> 'classification'
      ) end
    ),
    'geographyCode', p_card #> '{geography,code}',
    'referenceYear', p_card -> 'referenceYear',
    'processSubtype', p_card -> 'processSubtype',
    'source', p_card -> 'source',
    'casNumber', p_card -> 'casNumber'
  )
$$;

ALTER FUNCTION "private"."catalog_portal_card_facts_v1"("p_card" "jsonb", "p_filters" "jsonb", "p_query" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_card_facts_v1"("p_card" "jsonb", "p_filters" "jsonb", "p_query" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."catalog_portal_card_facts_v1"("p_card" "jsonb", "p_filters" "jsonb", "p_query" "text") TO "api_internal_executor";
