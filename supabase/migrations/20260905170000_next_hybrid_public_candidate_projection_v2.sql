-- Database #624: narrow public candidate projection for authenticated Next
-- Process/Flow Hybrid V2. The projection contains only state 100/200 versions
-- with embeddings. It is not an authorization source: every result is joined
-- back to the RLS-protected source table and fully rechecked before hydration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

create or replace function private.next_hybrid_json_codes_v2(
  p_value jsonb,
  p_attribute text
) returns text[]
language sql
immutable
strict
parallel safe
set search_path = ''
as $function$
  select coalesce(
    pg_catalog.array_agg(code.value order by code.value),
    '{}'::text[]
  )
  from (
    select distinct nullif(pg_catalog.btrim(item.value ->> p_attribute), '') as value
    from pg_catalog.jsonb_array_elements(
      private.dataset_alias_jsonb_array_v1(p_value)
    ) as item(value)
  ) as code
  where code.value is not null;
$function$;

alter function private.next_hybrid_json_codes_v2(jsonb, text)
  owner to postgres;
revoke all on function private.next_hybrid_json_codes_v2(jsonb, text)
  from public, anon, authenticated, service_role;
grant execute on function private.next_hybrid_json_codes_v2(jsonb, text)
  to api_internal_executor;
grant execute on function private.dataset_alias_jsonb_array_v1(jsonb)
  to api_internal_executor;

do $next_public_search_executor_role$
begin
  if not exists (
    select 1 from pg_catalog.pg_roles
    where rolname = 'next_public_search_executor'
  ) then
    create role next_public_search_executor
      nologin
      noinherit
      nobypassrls
      nocreatedb
      nocreaterole;
  end if;
end
$next_public_search_executor_role$;

alter role next_public_search_executor
  nologin
  noinherit
  nobypassrls
  nocreatedb
  nocreaterole;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;

create table private.next_hybrid_public_candidates_v2 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null,
  state_code integer not null
    check (state_code in (100, 200)),
  team_id uuid,
  dataset_type text,
  is_emission boolean not null,
  classification_codes text[] not null default '{}',
  elementary_codes text[] not null default '{}',
  source_modified_at timestamptz not null,
  primary key(dataset_kind, id, version)
);

comment on table private.next_hybrid_public_candidates_v2 is
  'Internal public-state search-key projection for bounded Next Hybrid V2 candidate discovery; source RLS and full canonical filters are rechecked during final source hydration.';

alter table private.next_hybrid_public_candidates_v2
  owner to api_internal_executor;

-- This internal table intentionally has no RLS: it contains public-state
-- search keys only, grants no external access, and is read solely through
-- fixed SECURITY DEFINER candidate helpers. Source RLS remains authoritative.
revoke all on table private.next_hybrid_public_candidates_v2
  from public, anon, authenticated, service_role, next_public_search_executor;
grant select (
  dataset_kind, id, version, state_code, team_id, dataset_type,
  is_emission, classification_codes, elementary_codes, source_modified_at
) on table private.next_hybrid_public_candidates_v2
  to next_public_search_executor;

create index next_hybrid_public_candidate_type_v2_idx
on private.next_hybrid_public_candidates_v2(
  dataset_type, dataset_kind, state_code, team_id, id, version
);

create index next_hybrid_public_candidate_team_v2_idx
on private.next_hybrid_public_candidates_v2(
  dataset_kind, state_code, team_id, id, version
);

create index next_hybrid_public_candidate_emission_v2_idx
on private.next_hybrid_public_candidates_v2(
  is_emission, dataset_kind, state_code, team_id, id, version
);

create index next_hybrid_public_candidate_classification_v2_idx
on private.next_hybrid_public_candidates_v2
using gin(classification_codes);

create index next_hybrid_public_candidate_elementary_v2_idx
on private.next_hybrid_public_candidates_v2
using gin(elementary_codes);

set role api_internal_executor;

create function private.sync_next_hybrid_public_process_candidate_v2()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  if tg_op = 'DELETE' then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'process'
      and candidate.id = old.id
      and candidate.version = old.version::text;
    return null;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'process'
      and candidate.id = old.id
      and candidate.version = old.version::text;
  end if;

  if new.state_code in (100, 200) and new.embedding_ft is not null then
    insert into private.next_hybrid_public_candidates_v2(
      dataset_kind, id, version, state_code, team_id, dataset_type,
      is_emission, classification_codes, elementary_codes, source_modified_at
    ) values (
      'process', new.id, new.version::text, new.state_code, new.team_id,
      new.json #>>
        '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}',
      false, '{}', '{}', new.modified_at
    )
    on conflict(dataset_kind, id, version) do update set
      state_code = excluded.state_code,
      team_id = excluded.team_id,
      dataset_type = excluded.dataset_type,
      is_emission = excluded.is_emission,
      classification_codes = excluded.classification_codes,
      elementary_codes = excluded.elementary_codes,
      source_modified_at = excluded.source_modified_at;
  else
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'process'
      and candidate.id = new.id
      and candidate.version = new.version::text;
  end if;
  return null;
end;
$function$;

create function private.sync_next_hybrid_public_flow_candidate_v2()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  if tg_op = 'DELETE' then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = old.id
      and candidate.version = old.version::text;
    return null;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = old.id
      and candidate.version = old.version::text;
  end if;

  if new.state_code in (100, 200) and new.embedding_ft is not null then
    insert into private.next_hybrid_public_candidates_v2(
      dataset_kind, id, version, state_code, team_id, dataset_type,
      is_emission, classification_codes, elementary_codes, source_modified_at
    ) values (
      'flow', new.id, new.version::text, new.state_code, new.team_id,
      new.json #>>
        '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}',
      new.json @>
        '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}',
      coalesce(private.next_hybrid_json_codes_v2(
        new.json #>
          '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
        '@classId'
      ), '{}'),
      coalesce(private.next_hybrid_json_codes_v2(
        new.json #>
          '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
        '@catId'
      ), '{}'),
      new.modified_at
    )
    on conflict(dataset_kind, id, version) do update set
      state_code = excluded.state_code,
      team_id = excluded.team_id,
      dataset_type = excluded.dataset_type,
      is_emission = excluded.is_emission,
      classification_codes = excluded.classification_codes,
      elementary_codes = excluded.elementary_codes,
      source_modified_at = excluded.source_modified_at;
  else
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = new.id
      and candidate.version = new.version::text;
  end if;
  return null;
end;
$function$;

reset role;

revoke all on function private.sync_next_hybrid_public_process_candidate_v2()
  from public, anon, authenticated, service_role, next_public_search_executor;
revoke all on function private.sync_next_hybrid_public_flow_candidate_v2()
  from public, anon, authenticated, service_role, next_public_search_executor;

create trigger zz_next_hybrid_public_process_candidate_v2
after insert or delete or update of
  id, version, state_code, team_id, json, embedding_ft, modified_at
on public.processes
for each row execute function
  private.sync_next_hybrid_public_process_candidate_v2();

create trigger zz_next_hybrid_public_flow_candidate_v2
after insert or delete or update of
  id, version, state_code, team_id, json, embedding_ft, modified_at
on public.flows
for each row execute function
  private.sync_next_hybrid_public_flow_candidate_v2();

revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

commit;
