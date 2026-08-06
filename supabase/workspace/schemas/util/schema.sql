CREATE SCHEMA IF NOT EXISTS "util";

ALTER SCHEMA "util" OWNER TO "postgres";

COMMENT ON SCHEMA "util" IS 'Operational controls, queues, diagnostics, and maintenance helpers; not exposed by PostgREST.';

GRANT USAGE ON SCHEMA "util" TO "service_role";
