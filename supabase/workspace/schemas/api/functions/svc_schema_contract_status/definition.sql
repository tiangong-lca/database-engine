CREATE OR REPLACE FUNCTION "api"."svc_schema_contract_status"() RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'migrationHead', (
      select max(version) from supabase_migrations.schema_migrations
    ),
    'publicCoreTables', (
      select count(*)
      from pg_catalog.pg_class as class
      join pg_catalog.pg_namespace as namespace on namespace.oid = class.relnamespace
      where namespace.nspname = 'public' and class.relkind in ('r', 'p')
    ),
    'publicRoutines', (
      select count(*)
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
      where namespace.nspname = 'public'
    ),
    'apiRoutines', (
      select count(*)
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
      where namespace.nspname = 'api' and routine.prokind = 'f'
    )
  )
$$;

ALTER FUNCTION "api"."svc_schema_contract_status"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_schema_contract_status"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_schema_contract_status"() TO "service_role";
