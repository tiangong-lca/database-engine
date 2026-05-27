create or replace function util.dataset_json_search_text(
  p_table text,
  p_json jsonb
) returns text
language sql
immutable
parallel safe
as $$
  with recursive allowed(prefix, prefix_path) as (
    select prefix, string_to_array(prefix, '.') as prefix_path
    from unnest(util.dataset_json_search_text_allowed_prefixes(p_table)) as allowed(prefix)
  ),
  path_nodes(prefix, remaining_path, value) as (
    select allowed.prefix, allowed.prefix_path, p_json
    from allowed
    where p_json is not null

    union all

    select n.prefix,
           case
             when jsonb_typeof(n.value) = 'array' then n.remaining_path
             when cardinality(n.remaining_path) > 1 then n.remaining_path[2:cardinality(n.remaining_path)]
             else array[]::text[]
           end as remaining_path,
           child.value
    from path_nodes n
    cross join lateral (
      select elem.value
      from jsonb_array_elements(
        case when jsonb_typeof(n.value) = 'array' then n.value else '[]'::jsonb end
      ) as elem(value)

      union all

      select n.value -> n.remaining_path[1]
      where jsonb_typeof(n.value) = 'object'
        and cardinality(n.remaining_path) > 0
        and n.value ? n.remaining_path[1]
    ) child
    where cardinality(n.remaining_path) > 0
  ),
  subtrees as (
    select distinct value
    from path_nodes
    where cardinality(remaining_path) = 0
      and value is not null
  ),
  walk(path, value) as (
    select array[]::text[], value
    from subtrees

    union all

    select w.path || child.key, child.value
    from walk w
    cross join lateral (
      select elem.ordinality::text as key, elem.value
      from jsonb_array_elements(
        case when jsonb_typeof(w.value) = 'array' then w.value else '[]'::jsonb end
      ) with ordinality as elem(value, ordinality)

      union all

      select obj.key, obj.value
      from jsonb_each(
        case when jsonb_typeof(w.value) = 'object' then w.value else '{}'::jsonb end
      ) as obj(key, value)
    ) child
  ),
  scalar_text as (
    select distinct
           nullif(btrim(value #>> '{}'), '') as text_value,
           path[cardinality(path)] as leaf_key
    from walk
    where jsonb_typeof(value) in ('string', 'number', 'boolean')
  )
  select coalesce(
    string_agg(text_value, ' ' order by lower(text_value), text_value),
    ''
  )
  from scalar_text
  where text_value is not null
    and not util.dataset_json_search_text_is_noise(text_value, leaf_key);
$$;

alter function util.dataset_json_search_text(text, jsonb) owner to postgres;
revoke all on function util.dataset_json_search_text(text, jsonb) from public;
grant execute on function util.dataset_json_search_text(text, jsonb) to service_role;

comment on function util.dataset_json_search_text(text, jsonb) is
  'Builds deterministic dataset full-text search input by walking only entity-specific core JSON subtrees, preserving all authored languages while excluding reference/schema metadata noise.';
