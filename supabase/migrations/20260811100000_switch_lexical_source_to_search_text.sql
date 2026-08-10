-- Database B / workspace#565 Release 2 source switch.
--
-- The seven concurrent search_text indexes are deliberately separate
-- migrations above.  This migration changes only the lexical source inside
-- the existing private implementations; public and compatibility wrappers
-- continue to delegate to those same implementations.

set lock_timeout = '5s';
set statement_timeout = '120s';

-- A fresh local/CI database has no dataset rows when migrations run, so it is
-- safe to install the source switch there.  An existing environment with any
-- rows must have complete non-NULL search_text coverage first.  This is a
-- pure catalog/data check: it adds no state table and never performs a
-- backfill.  The function is private and grant-free so it is not a new RPC.
create or replace function private.search_text_cutover_gate_v1()
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
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
$function$;

alter function private.search_text_cutover_gate_v1() owner to postgres;
revoke all on function private.search_text_cutover_gate_v1()
  from public, anon, authenticated, service_role;
comment on function private.search_text_cutover_gate_v1() is
  'Database B gate: an empty new/test database is allowed; an existing environment must have non-NULL search_text for every row in all seven dataset tables before lexical source switch.';

do $gate$
declare
  v_status jsonb;
begin
  v_status := private.search_text_cutover_gate_v1();
  if not coalesce((v_status->>'ready')::boolean, false) then
    raise exception using
      errcode = '55006',
      message = format(
        'Database B search_text source cutover blocked until coverage is complete: %s',
        v_status::text
      );
  end if;
end
$gate$;

create or replace function pg_temp.required_replace_once(
  source_text text,
  old_text text,
  new_text text,
  replacement_label text
) returns text
language plpgsql
as $function$
declare
  replacement_count integer;
begin
  replacement_count := (
    length(source_text) - length(replace(source_text, old_text, ''))
  ) / length(old_text);
  if replacement_count <> 1 then
    raise exception
      'required source replacement expected once but found %: %',
      replacement_count,
      replacement_label;
  end if;
  return replace(source_text, old_text, new_text);
end
$function$;

-- Preserve each existing function definition and replace only its indexed
-- lexical expression.  UUID exact fast paths and all visibility/filter/order/
-- pagination/ranking branches remain untouched.
do $source_switch$
declare
  fn text;
begin
  fn := pg_get_functiondef(
    'api._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'd.extracted_md &@~ $1',
    'd.search_text &@~ $1',
    'foundation lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'where f.extracted_md &@~| $14',
    'where f.search_text &@~| $14',
    'flow lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'where p.extracted_md &@~| $11',
    'where p.search_text &@~| $11',
    'process lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'where l.extracted_md &@~| $10',
    'where l.search_text &@~| $10',
    'lifecycle model lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'd.extracted_md &@~| $1',
    'd.search_text &@~| $1',
    'foundation hybrid lexical source'
  );
  execute fn;
end
$source_switch$;

-- The four core Hybrid v2 implementations call the switched lexical RPC
-- implementations above.  Keep their comments explicit without changing
-- their signatures or implementation shape.
comment on function private.hybrid_search_flows_v2_impl(
  text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]
) is 'Private Flow Hybrid v2 implementation using one search_text lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_processes_v2_impl(
  text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]
) is 'Private Process Hybrid v2 implementation using one search_text lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_lifecyclemodels_v2_impl(
  text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]
) is 'Private LifecycleModel Hybrid v2 implementation using one search_text lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_simple_dataset_v2(
  regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid
) is 'Private allowlisted foundation-dataset Hybrid v2 implementation using search_text plus embedding_ft.';

-- Fail closed if a later schema change accidentally leaves one of the formal
-- private candidates on the old lexical source.
do $verify_source$
declare
  fn text;
begin
  foreach fn in array array[
    'api._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)',
    'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])',
    'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)',
    'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])',
    'private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)',
    'private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
    'private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
    'private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'
  ] loop
    if pg_get_functiondef(fn::regprocedure) like '%extracted_md%' then
      raise exception 'Database B source switch left extracted_md in %', fn;
    end if;
  end loop;
end
$verify_source$;
