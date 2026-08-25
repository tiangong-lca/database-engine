CREATE OR REPLACE FUNCTION "private"."portal_lcia_localized_text_valid_v1"("p_value" "jsonb") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select jsonb_typeof(p_value) = 'array'
    and jsonb_array_length(p_value) between 1 and 64
    and (
      select count(*) = count(distinct item.value ->> 'language')
        and array_agg(item.value ->> 'language' order by item.ordinality)
          = array_agg(item.value ->> 'language'
              order by item.value ->> 'language')
      from jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality)
    )
    and not exists (
      select 1
      from jsonb_array_elements(p_value) as item(value)
      where jsonb_typeof(item.value) <> 'object'
        or (select count(*) from jsonb_object_keys(item.value)) <> 2
        or not (item.value ? 'language' and item.value ? 'value')
        or jsonb_typeof(item.value -> 'language') <> 'string'
        or jsonb_typeof(item.value -> 'value') <> 'string'
        or item.value ->> 'language'
             !~ '^[a-z]{2,3}(-[a-z0-9]{2,8})*$'
        or length(item.value ->> 'language') > 35
        or item.value ->> 'value' <> btrim(item.value ->> 'value')
        or length(item.value ->> 'value') not between 1 and 4096
        or lower(item.value ->> 'value') ~ '(https?://|s3://|gs://|file://)'
    )
$_$;

ALTER FUNCTION "private"."portal_lcia_localized_text_valid_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_localized_text_valid_v1"("p_value" "jsonb") FROM PUBLIC;
