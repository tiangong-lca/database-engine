CREATE OR REPLACE FUNCTION "private"."portal_catalog_summary_label_v1"("p_card" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'language', label_item.language,
    'value', label_item.label_value
  ) order by label_item.preference,
      pg_catalog.lower(label_item.language) collate pg_catalog."C",
      label_item.label_value collate pg_catalog."C",
      label_item.ordinality), '[]'::jsonb)
  from (
    select
      pg_catalog.btrim(item.value ->> 'language') as language,
      pg_catalog.btrim(item.value ->> 'value') as label_value,
      item.ordinality,
      case pg_catalog.lower(pg_catalog.btrim(item.value ->> 'language'))
        when 'zh-cn' then 0
        when 'en' then 1
        else 2
      end as preference
    from pg_catalog.jsonb_array_elements(
      case pg_catalog.jsonb_typeof(p_card -> 'names')
        when 'array' then p_card -> 'names'
        else '[]'::jsonb
      end
    ) with ordinality as item(value, ordinality)
    where pg_catalog.jsonb_typeof(item.value) = 'object'
      and pg_catalog.jsonb_typeof(item.value -> 'language') = 'string'
      and pg_catalog.jsonb_typeof(item.value -> 'value') = 'string'
      and pg_catalog.btrim(item.value ->> 'language') ~
        '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
      and pg_catalog.length(
        pg_catalog.btrim(item.value ->> 'language')
      ) <= 35
      and nullif(pg_catalog.btrim(item.value ->> 'value'), '') is not null
      and pg_catalog.length(
        pg_catalog.btrim(item.value ->> 'value')
      ) <= 160
      and pg_catalog.octet_length(
        pg_catalog.btrim(item.value ->> 'value')
      ) <= 640
    order by preference,
      pg_catalog.lower(
        pg_catalog.btrim(item.value ->> 'language')
      ) collate pg_catalog."C",
      pg_catalog.btrim(item.value ->> 'value') collate pg_catalog."C",
      item.ordinality
    limit 2
  ) as label_item
$_$;

ALTER FUNCTION "private"."portal_catalog_summary_label_v1"("p_card" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_summary_label_v1"("p_card" "jsonb") FROM PUBLIC;
