begin;

-- Repair two executable stale functions surfaced by the canonical fail-closed
-- lint gate.  Temporary-table diagnostics remain exact analyzer false positives
-- and are separately pinned by the checked-in lint allowlist.
create or replace function public.semantic_search(
  query_embedding text,
  match_threshold double precision default 0.5,
  match_count integer default 20
) returns table(rank bigint,id uuid,"json" jsonb)
language plpgsql
set search_path = public, extensions, pg_temp
as $$
declare
  query_embedding_vector vector(1024);
begin
  query_embedding_vector := query_embedding::vector(1024);
  return query
  select rank() over(order by f.embedding_ft <=> query_embedding_vector),f.id,f.json
  from public.flows f
  where f.embedding_ft is not null
    and f.embedding_ft <=> query_embedding_vector < 1-match_threshold
  order by f.embedding_ft <=> query_embedding_vector
  limit match_count;
end;
$$;

alter function public.cmd_lcia_scope_closure_check_request(text,text,text,jsonb)
  set search_path = public, extensions, pg_temp;

commit;
