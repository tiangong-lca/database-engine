CREATE OR REPLACE FUNCTION "private"."portal_catalog_summary_valid_cas_v1"("p_value" "text") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select case
    when p_value ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      pg_catalog.right(p_value, 1)::integer = (
        select pg_catalog.mod(
          pg_catalog.sum(
            digit.value::integer * digit.ordinality::integer
          ),
          10
        )
        from pg_catalog.regexp_split_to_table(
          pg_catalog.reverse(pg_catalog.replace(
            pg_catalog.left(p_value, pg_catalog.length(p_value) - 2),
            '-',
            ''
          )),
          ''
        ) with ordinality as digit(value, ordinality)
      )
    else false
  end
$_$;

ALTER FUNCTION "private"."portal_catalog_summary_valid_cas_v1"("p_value" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_summary_valid_cas_v1"("p_value" "text") FROM PUBLIC;
