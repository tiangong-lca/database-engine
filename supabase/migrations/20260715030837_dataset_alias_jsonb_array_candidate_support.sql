create or replace function private.dataset_alias_jsonb_array_v1(
  p_value jsonb
) returns jsonb
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select case pg_catalog.jsonb_typeof(p_value)
    when 'array' then p_value
    when 'object' then pg_catalog.jsonb_build_array(p_value)
    else '[]'::jsonb
  end;
$$;

alter function private.dataset_alias_jsonb_array_v1(jsonb)
  owner to postgres;

revoke all on function private.dataset_alias_jsonb_array_v1(jsonb)
  from public, anon, authenticated, service_role;

-- Direct core-table writes are restricted to service_role. PostgreSQL
-- evaluates expression-index functions as the DML caller, so that trusted
-- writer must be able to maintain the two candidate indexes.
grant execute on function private.dataset_alias_jsonb_array_v1(jsonb)
  to service_role;

comment on function private.dataset_alias_jsonb_array_v1(jsonb) is
  'Normalizes singleton-object or array dataset reference collections into an immutable JSONB array for guarded alias candidate indexes and exact rechecks. EXECUTE is limited to postgres-owned definer code and the trusted service_role table writer.';
