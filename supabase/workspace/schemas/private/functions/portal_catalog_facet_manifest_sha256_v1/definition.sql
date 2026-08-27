CREATE OR REPLACE FUNCTION "private"."portal_catalog_facet_manifest_sha256_v1"() RETURNS "text"
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
  with expected(identity) as (
    values
      ('private.portal_catalog_facet_facts_v1(text,jsonb)'::text),
      ('private.sync_portal_catalog_facet_row_v1()')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  from manifest_entries
$$;

ALTER FUNCTION "private"."portal_catalog_facet_manifest_sha256_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_facet_manifest_sha256_v1"() FROM PUBLIC;
