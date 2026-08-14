CREATE SCHEMA IF NOT EXISTS "archive";

ALTER SCHEMA "archive" OWNER TO "postgres";

COMMENT ON SCHEMA "archive" IS 'Historical and retired data retained outside runtime API namespaces.';

GRANT USAGE ON SCHEMA "archive" TO "service_role";
