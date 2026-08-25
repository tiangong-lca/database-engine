CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_sha256_fields_v1"(VARIADIC "p_fields" "text"[]) RETURNS "text"
    LANGUAGE "sql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select pg_catalog.encode(
    extensions.digest(
      private.portal_lcia_projection_frame_v1(variadic p_fields),
      'sha256'
    ),
    'hex'
  )
$$;

ALTER FUNCTION "private"."portal_lcia_projection_sha256_fields_v1"(VARIADIC "p_fields" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_sha256_fields_v1"(VARIADIC "p_fields" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_lcia_projection_sha256_fields_v1"(VARIADIC "p_fields" "text"[]) TO "portal_public_executor";
