CREATE OR REPLACE FUNCTION "api"."qry_system_status"() RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select coalesce(
    (
      select config.config_value || jsonb_build_object('updatedAt', config.updated_at)
      from util.app_runtime_config as config
      where config.config_key = 'tiangong-lca-next.production.system-status'
    ),
    jsonb_build_object(
      'schemaVersion', 1,
      'phase', 'normal',
      'reason', null,
      'targetVersion', null,
      'estimatedEndAt', null,
      'releaseId', null,
      'updatedAt', null
    )
  );
$$;

ALTER FUNCTION "api"."qry_system_status"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_system_status"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_system_status"() TO "anon";

GRANT ALL ON FUNCTION "api"."qry_system_status"() TO "authenticated";
