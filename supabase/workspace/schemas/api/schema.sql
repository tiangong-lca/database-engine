CREATE SCHEMA IF NOT EXISTS "api";

ALTER SCHEMA "api" OWNER TO "postgres";

COMMENT ON SCHEMA "api" IS 'Explicit Supabase Data API surface. RPCs and API-facing projections live here.';

GRANT USAGE ON SCHEMA "api" TO "anon";

GRANT USAGE ON SCHEMA "api" TO "authenticated";

GRANT USAGE ON SCHEMA "api" TO "service_role";

GRANT USAGE ON SCHEMA "api" TO "api_internal_executor";
