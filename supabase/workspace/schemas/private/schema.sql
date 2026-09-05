CREATE SCHEMA IF NOT EXISTS "private";

ALTER SCHEMA "private" OWNER TO "postgres";

COMMENT ON SCHEMA "private" IS 'Internal application state and implementation routines; not exposed by PostgREST.';

GRANT USAGE ON SCHEMA "private" TO "service_role";

GRANT USAGE ON SCHEMA "private" TO "authenticated";

GRANT USAGE ON SCHEMA "private" TO "portal_public_executor";

GRANT USAGE ON SCHEMA "private" TO "next_public_search_executor";
