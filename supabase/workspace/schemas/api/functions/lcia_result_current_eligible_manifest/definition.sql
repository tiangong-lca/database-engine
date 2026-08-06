CREATE OR REPLACE FUNCTION "api"."lcia_result_current_eligible_manifest"() RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  with eligible as (
    select distinct on (id)
      id,
      version,
      state_code
    from public.processes
    where state_code between 100 and 199
      and json ? 'processDataSet'
    order by id, version desc, modified_at desc
  ),
  aggregated as (
    select
      count(*)::integer as eligible_count,
      md5(
        coalesce(
          string_agg(id::text || ':' || version, ',' order by id, version),
          ''
        ) || '|published:100-199:latest-per-id:v1'
      ) as input_manifest_hash,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'id', id,
            'version', version,
            'stateCode', state_code
          )
          order by id, version
        ),
        '[]'::jsonb
      ) as processes
    from eligible
  )
  select jsonb_build_object(
    'predicateVersion', 'published-state-code-100-199:latest-per-id:v1',
    'inputStatusFilter', jsonb_build_object(
      'state_code',
      jsonb_build_object('between', jsonb_build_array(100, 199))
    ),
    'eligibleInputCount', eligible_count,
    'includedInputCount', eligible_count,
    'inputManifestHash', input_manifest_hash,
    'inputManifest', jsonb_build_object(
      'predicateVersion', 'published-state-code-100-199:latest-per-id:v1',
      'selectionMode', 'all_eligible',
      'processes', processes
    )
  )
  from aggregated
$$;

ALTER FUNCTION "api"."lcia_result_current_eligible_manifest"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."lcia_result_current_eligible_manifest"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."lcia_result_current_eligible_manifest"() TO "api_internal_executor";
