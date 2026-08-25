CREATE OR REPLACE FUNCTION "private"."portal_timestamp_v1"("p_value" timestamp with time zone) RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select to_char(p_value at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
$$;

ALTER FUNCTION "private"."portal_timestamp_v1"("p_value" timestamp with time zone) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_timestamp_v1"("p_value" timestamp with time zone) FROM PUBLIC;
