CREATE OR REPLACE FUNCTION "private"."search_text_cutover_gate_v1"() RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_total bigint := 0;
  v_missing bigint := 0;
  v_tables jsonb := '[]'::jsonb;
begin
  with coverage(table_name, total_rows, missing_rows) as (
    select 'contacts', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.contacts
    union all
    select 'flowproperties', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.flowproperties
    union all
    select 'flows', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.flows
    union all
    select 'lifecyclemodels', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.lifecyclemodels
    union all
    select 'processes', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.processes
    union all
    select 'sources', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.sources
    union all
    select 'unitgroups', count(*)::bigint, count(*) filter (where search_text is null)::bigint
    from public.unitgroups
  )
  select
    coalesce(sum(coverage.total_rows), 0)::bigint,
    coalesce(sum(coverage.missing_rows), 0)::bigint,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'table', coverage.table_name,
          'total', coverage.total_rows,
          'missing_search_text', coverage.missing_rows
        )
        order by coverage.table_name
      ),
      '[]'::jsonb
    )
  into v_total, v_missing, v_tables
  from coverage;

  return jsonb_build_object(
    'fresh_database', v_total = 0,
    'ready', v_total = 0 or v_missing = 0,
    'total_rows', v_total,
    'missing_rows', v_missing,
    'tables', v_tables
  );
end
$$;

ALTER FUNCTION "private"."search_text_cutover_gate_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_text_cutover_gate_v1"() FROM PUBLIC;
