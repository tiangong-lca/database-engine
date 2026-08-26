CREATE OR REPLACE FUNCTION "private"."portal_lcia_public_text_valid_v1"("p_value" "text", "p_max_length" integer) RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select p_value is not null
    and p_value = btrim(p_value)
    and length(p_value) between 1 and p_max_length
    and p_value !~ '[[:cntrl:]]'
    and lower(p_value) !~ '(https?://|s3://|gs://|file://)'
    and p_value !~ '(^|[/\\])\.\.([/\\]|$)'
$_$;

ALTER FUNCTION "private"."portal_lcia_public_text_valid_v1"("p_value" "text", "p_max_length" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_public_text_valid_v1"("p_value" "text", "p_max_length" integer) FROM PUBLIC;
