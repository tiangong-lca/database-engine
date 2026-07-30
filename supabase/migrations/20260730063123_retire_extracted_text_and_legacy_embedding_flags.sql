-- Contract phase for the deterministic extracted_md + embedding_ft pipeline.
--
-- The Expand migration moved all lexical execution to extracted_md and added
-- v2 Hybrid/Search entrypoints. This migration is deliberately fail-closed:
-- it refuses to invalidate an active guarded derivative rebuild or discard a
-- legacy embedding queue item, rewires v2 RPCs away from compatibility
-- signatures, updates the retained guarded-rebuild fingerprints, and only then
-- removes the obsolete schema surface with RESTRICT.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $$
begin
  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.status not in ('completed', 'stale', 'failed')
  ) then
    raise exception using
      errcode = '55006',
      message = 'Cannot retire extracted_text while derivative rebuild requests are active';
  end if;

  if exists (
    select 1
    from util.pending_embedding_jobs as pending
    where pending.status = 'pending'
      and pending.embedding_column <> 'embedding_ft'
  ) then
    raise exception using
      errcode = '55006',
      message = 'Cannot retire legacy embedding fields while non-FT deferred jobs are pending';
  end if;

  if to_regclass('pgmq.q_embedding_jobs') is not null
    and exists (
      select 1
      from pgmq.q_embedding_jobs as queued
      where coalesce(queued.message->>'embeddingColumn', '') <> 'embedding_ft'
    ) then
    raise exception using
      errcode = '55006',
      message = 'Cannot retire legacy embedding fields while non-FT queue jobs are active';
  end if;
end
$$;

create or replace function pg_temp.required_replace_once(
  source_text text,
  old_text text,
  new_text text,
  replacement_label text
) returns text
language plpgsql
as $$
declare
  replacement_count integer;
begin
  if old_text = '' then
    raise exception 'empty required replacement: %', replacement_label;
  end if;

  replacement_count := (
    length(source_text) - length(replace(source_text, old_text, ''))
  ) / length(old_text);

  if replacement_count <> 1 then
    raise exception
      'required replacement expected once but found %: %',
      replacement_count,
      replacement_label;
  end if;

  return replace(source_text, old_text, new_text);
end;
$$;

-- Clone the measured Hybrid implementations behind lexical-only private
-- signatures, then make every public v2 RPC call those signatures directly.
-- This preserves the reviewed candidate, visibility, pagination, and RRF plan
-- while allowing the two-weight compatibility entrypoints to be removed.
do $$
declare
  fn text;
begin
  fn := pg_get_functiondef(
    'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'CREATE OR REPLACE FUNCTION public.hybrid_search_flows(',
    'CREATE OR REPLACE FUNCTION private.hybrid_search_flows_v2_impl(',
    'flow Hybrid v2 implementation name'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, ',
    'lexical_weight double precision DEFAULT 0.5, ',
    'flow Hybrid lexical signature'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);',
    'text_weight := coalesce(lexical_weight, 0);',
    'flow Hybrid lexical weight'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'CREATE OR REPLACE FUNCTION public.hybrid_search_processes(',
    'CREATE OR REPLACE FUNCTION private.hybrid_search_processes_v2_impl(',
    'process Hybrid v2 implementation name'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, ',
    'lexical_weight double precision DEFAULT 0.5, ',
    'process Hybrid lexical signature'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);',
    'text_weight := coalesce(lexical_weight, 0);',
    'process Hybrid lexical weight'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'CREATE OR REPLACE FUNCTION public.hybrid_search_lifecyclemodels(',
    'CREATE OR REPLACE FUNCTION private.hybrid_search_lifecyclemodels_v2_impl(',
    'lifecycle model Hybrid v2 implementation name'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, ',
    'lexical_weight double precision DEFAULT 0.5, ',
    'lifecycle model Hybrid lexical signature'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);',
    'text_weight := coalesce(lexical_weight, 0);',
    'lifecycle model Hybrid lexical weight'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'CREATE OR REPLACE FUNCTION private.hybrid_search_simple_dataset(',
    'CREATE OR REPLACE FUNCTION private.hybrid_search_simple_dataset_v2(',
    'foundation Hybrid v2 implementation name'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, ',
    'lexical_weight double precision DEFAULT 0.5, ',
    'foundation Hybrid lexical signature'
  );
  fn := pg_temp.required_replace_once(
    fn,
    'text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);',
    'text_weight := coalesce(lexical_weight, 0);',
    'foundation Hybrid lexical weight'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_flows(',
    'from private.hybrid_search_flows_v2_impl(',
    'flow Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'flow Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_processes(',
    'from private.hybrid_search_processes_v2_impl(',
    'process Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'process Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_lifecyclemodels(',
    'from private.hybrid_search_lifecyclemodels_v2_impl(',
    'lifecycle model Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'lifecycle model Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_contacts(',
    'from private.hybrid_search_simple_dataset_v2(''public.contacts''::regclass,',
    'contact Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'contact Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_flowproperties(',
    'from private.hybrid_search_simple_dataset_v2(''public.flowproperties''::regclass,',
    'flow property Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'flow property Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_sources(',
    'from private.hybrid_search_simple_dataset_v2(''public.sources''::regclass,',
    'source Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'source Hybrid v2 argument contraction'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    'from public.hybrid_search_unitgroups(',
    'from private.hybrid_search_simple_dataset_v2(''public.unitgroups''::regclass,',
    'unit group Hybrid v2 implementation call'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    lexical_weight,\n    0.0::double precision,\n    semantic_weight,',
    E'    lexical_weight,\n    semantic_weight,',
    'unit group Hybrid v2 argument contraction'
  );
  execute fn;
end
$$;

alter function private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) owner to postgres;
alter function private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) owner to postgres;
alter function private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) owner to postgres;
alter function private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) owner to postgres;

revoke all on function private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) from public;
revoke all on function private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) from public;
revoke all on function private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) from public;
revoke all on function private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) from public;

grant execute on function private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) to anon, authenticated, service_role;
grant execute on function private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) to anon, authenticated, service_role;
grant execute on function private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) to anon, authenticated, service_role;
grant execute on function private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) to anon, authenticated, service_role;

comment on function private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) is
  'Private Flow Hybrid v2 implementation using one extracted_md lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) is
  'Private Process Hybrid v2 implementation using one extracted_md lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) is
  'Private LifecycleModel Hybrid v2 implementation using one extracted_md lexical weight plus embedding_ft semantic weight.';
comment on function private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) is
  'Private allowlisted foundation-dataset Hybrid v2 implementation using extracted_md plus embedding_ft.';

-- Remove the retired fingerprint from every retained guarded-rebuild routine.
-- No request can be active at this point, so changing snapshot composition
-- cannot invalidate an in-flight approval or fence.
do $$
declare
  fn text;
begin
  fn := pg_get_functiondef(
    'util.dataset_derivative_rebuild_snapshot(public.processes)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'  v_extracted_text_sha256 text;\n', '', 'process snapshot declaration');
  fn := pg_temp.required_replace_once(fn, E'    or p_process.extracted_text is null\n', '', 'process snapshot null guard');
  fn := pg_temp.required_replace_once(
    fn,
    E'  v_extracted_text_sha256 := util.dataset_derivative_rebuild_sha256(\n    p_process.extracted_text\n  );\n',
    '',
    'process snapshot retired hash'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    \'extracted_text_sha256\', v_extracted_text_sha256,\n',
    '',
    'process snapshot retired field'
  );
  execute fn;

  fn := pg_get_functiondef(
    'util.dataset_derivative_rebuild_snapshot(public.flows)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'  v_extracted_text_sha256 text;\n', '', 'flow snapshot declaration');
  fn := pg_temp.required_replace_once(fn, E'    or p_flow.extracted_text is null\n', '', 'flow snapshot null guard');
  fn := pg_temp.required_replace_once(
    fn,
    E'  v_extracted_text_sha256 := util.dataset_derivative_rebuild_sha256(\n    p_flow.extracted_text\n  );\n',
    '',
    'flow snapshot retired hash'
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'    \'extracted_text_sha256\', v_extracted_text_sha256,\n',
    '',
    'flow snapshot retired field'
  );
  execute fn;

  fn := pg_get_functiondef(
    'util.dataset_derivative_rebuild_primary_matches(util.dataset_derivative_rebuild_requests,public.processes)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'    and p_process.extracted_text is not null\n', '', 'process primary retired null guard');
  fn := pg_temp.required_replace_once(
    fn,
    E'    and util.dataset_derivative_rebuild_sha256(\n      p_process.json_ordered::jsonb::text\n    ) = p_request.expected_json_ordered_sha256\n    and util.dataset_derivative_rebuild_sha256(\n      p_process.extracted_text\n    ) = p_request.expected_extracted_text_sha256,\n',
    E'    and util.dataset_derivative_rebuild_sha256(\n      p_process.json_ordered::jsonb::text\n    ) = p_request.expected_json_ordered_sha256,\n',
    'process primary retired fingerprint'
  );
  execute fn;

  fn := pg_get_functiondef(
    'util.dataset_derivative_rebuild_primary_matches(util.dataset_derivative_rebuild_requests,public.flows)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'    and p_flow.extracted_text is not null\n', '', 'flow primary retired null guard');
  fn := pg_temp.required_replace_once(
    fn,
    E'    and util.dataset_derivative_rebuild_sha256(\n      p_flow.json_ordered::jsonb::text\n    ) = p_request.expected_json_ordered_sha256\n    and util.dataset_derivative_rebuild_sha256(\n      p_flow.extracted_text\n    ) = p_request.expected_extracted_text_sha256,\n',
    E'    and util.dataset_derivative_rebuild_sha256(\n      p_flow.json_ordered::jsonb::text\n    ) = p_request.expected_json_ordered_sha256,\n',
    'flow primary retired fingerprint'
  );
  execute fn;

  fn := pg_get_functiondef(
    'public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'    expected_extracted_text_sha256,\n', '', 'single rebuild request retired column');
  fn := pg_temp.required_replace_once(fn, E'    v_snapshot->>\'extracted_text_sha256\',\n', '', 'single rebuild request retired value');
  execute fn;

  fn := pg_get_functiondef(
    'util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)'::regprocedure
  );
  fn := pg_temp.required_replace_once(fn, E'      expected_extracted_text_sha256,\n', '', 'batch rebuild request retired column');
  fn := pg_temp.required_replace_once(fn, E'      v_snapshot->>\'extracted_text_sha256\',\n', '', 'batch rebuild request retired value');
  execute fn;

  fn := pg_get_functiondef(
    'public.cmd_dataset_derivative_rebuild_read(uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'      \'extracted_text_sha256\', v_request.expected_extracted_text_sha256,\n',
    '',
    'rebuild read retired fingerprint'
  );
  execute fn;

  fn := pg_get_functiondef(
    'util.read_dataset_flow_identity_derivative_set(uuid,uuid)'::regprocedure
  );
  fn := pg_temp.required_replace_once(
    fn,
    E'        and candidate.current_snapshot->>\'extracted_text_sha256\'\n          = (candidate.request_row).expected_extracted_text_sha256\n',
    '',
    'flow identity derivative retired fingerprint'
  );
  execute fn;
end
$$;

alter table util.dataset_derivative_rebuild_requests
  drop constraint dataset_derivative_rebuild_request_hashes_check;

alter table util.dataset_derivative_rebuild_requests
  add constraint dataset_derivative_rebuild_request_hashes_check check (
    plan_sha256 ~ '^[a-f0-9]{64}$'
    and expected_snapshot_sha256 ~ '^[a-f0-9]{64}$'
    and expected_json_sha256 ~ '^[a-f0-9]{64}$'
    and expected_json_ordered_sha256 ~ '^[a-f0-9]{64}$'
    and plan_request_sha256 ~ '^[a-f0-9]{64}$'
    and action_request_sha256 ~ '^[a-f0-9]{64}$'
    and (
      before_extracted_md_sha256 is null
      or before_extracted_md_sha256 ~ '^[a-f0-9]{64}$'
    )
    and (
      before_embedding_ft_sha256 is null
      or before_embedding_ft_sha256 ~ '^[a-f0-9]{64}$'
    )
  );

-- Rebuild every UPDATE OF trigger that named a retired column. Derived
-- extracted_md / embedding_ft writes remain outside authored modified_at and
-- primary-write fencing, exactly as before.
drop trigger flow_derivative_rebuild_primary_update_fence on public.flows;
create trigger flow_derivative_rebuild_primary_update_fence
before update of
  id,
  json,
  created_at,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.flows
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

drop trigger process_derivative_rebuild_primary_update_fence on public.processes;
create trigger process_derivative_rebuild_primary_update_fence
before update of
  id,
  json,
  created_at,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at,
  team_id,
  review_id,
  rule_verification,
  reviews,
  model_id
on public.processes
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

drop trigger flows_set_modified_at_trigger on public.flows;
create trigger flows_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.flows
for each row
execute function public.update_modified_at();

drop trigger lifecyclemodels_set_modified_at_trigger on public.lifecyclemodels;
create trigger lifecyclemodels_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  json_tg,
  team_id,
  rule_verification,
  reviews
on public.lifecyclemodels
for each row
execute function public.update_modified_at();

drop trigger processes_set_modified_at_trigger on public.processes;
create trigger processes_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews,
  model_id
on public.processes
for each row
execute function public.update_modified_at();

drop trigger zz_contacts_extracted_text_sync_trigger on public.contacts;
drop trigger zz_flowproperties_extracted_text_sync_trigger on public.flowproperties;
drop trigger zz_flows_extracted_text_sync_trigger on public.flows;
drop trigger zz_lifecyclemodels_extracted_text_sync_trigger on public.lifecyclemodels;
drop trigger zz_processes_extracted_text_sync_trigger on public.processes;
drop trigger zz_sources_extracted_text_sync_trigger on public.sources;
drop trigger zz_unitgroups_extracted_text_sync_trigger on public.unitgroups;

drop index public.contacts_text_pgroonga restrict;
drop index public.flowproperties_text_pgroonga restrict;
drop index public.flows_text_pgroonga restrict;
drop index public.lifecyclemodels_text_pgroonga restrict;
drop index public.processes_text_pgroonga restrict;
drop index public.sources_text_pgroonga restrict;
drop index public.unitgroups_text_pgroonga restrict;

-- Compatibility RPCs and retired generation/search helpers. Every drop is
-- RESTRICT so an unaccounted dependency aborts the migration.
drop function public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[]) restrict;
drop function public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[]) restrict;
drop function public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[]) restrict;
drop function public.hybrid_search_contacts(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) restrict;
drop function public.hybrid_search_flowproperties(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) restrict;
drop function public.hybrid_search_sources(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) restrict;
drop function public.hybrid_search_unitgroups(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) restrict;
drop function private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid) restrict;

drop function public.cmd_dataset_extracted_text_backfill(text,integer,uuid,text,text) restrict;
drop function public.pgroonga_search(text) restrict;
drop function public.pgroonga_search_flows_text_v1(text,integer,integer,text) restrict;
drop function public.pgroonga_search_processes_text_v1(text,integer,integer,text) restrict;
drop function public.pgroonga_search_lifecyclemodels_text_v1(text,integer,integer,text) restrict;
drop function util.set_dataset_extracted_text_from_json() restrict;
drop function util.dataset_json_search_text(jsonb) restrict;
drop function util.dataset_json_search_text(text,jsonb) restrict;
drop function util.dataset_json_search_text_allowed_prefixes(text) restrict;
drop function util.dataset_json_search_text_is_noise(text,text) restrict;

drop function public.flows_embedding_input(public.flows) restrict;
drop function public.processes_embedding_input(public.processes) restrict;
drop function public.lifecyclemodels_embedding_input(public.lifecyclemodels) restrict;
drop function public.generate_flow_embedding() restrict;

drop function public._navicat_temp_stored_proc(text,text,text,double precision,integer,numeric,numeric,numeric,integer,text,text,integer,integer) restrict;
drop function public._navicat_temp_stored_proc(text,extensions.vector,text,double precision,integer,numeric,numeric,numeric,integer,text,text,integer,integer) restrict;

alter table public.contacts
  drop column extracted_text restrict;
alter table public.flowproperties
  drop column extracted_text restrict;
alter table public.flows
  drop column extracted_text restrict,
  drop column embedding_flag restrict,
  drop column embedding_at restrict;
alter table public.lifecyclemodels
  drop column extracted_text restrict,
  drop column embedding_flag restrict,
  drop column embedding_at restrict;
alter table public.processes
  drop column extracted_text restrict,
  drop column embedding_flag restrict,
  drop column embedding_at restrict;
alter table public.sources
  drop column extracted_text restrict;
alter table public.unitgroups
  drop column extracted_text restrict;
alter table util.dataset_derivative_rebuild_requests
  drop column expected_extracted_text_sha256 restrict;

do $$
declare
  retained_derivative_column_count integer;
  retained_lexical_index_count integer;
  retained_vector_index_count integer;
begin
  if exists (
    select 1
    from information_schema.columns as column_info
    where column_info.table_schema in ('public', 'util')
      and (
        column_info.column_name = 'extracted_text'
        or column_info.column_name = 'expected_extracted_text_sha256'
        or column_info.column_name = 'embedding_flag'
        or column_info.column_name = 'embedding_at'
      )
  ) then
    raise exception 'retired extracted-text or embedding-flag columns remain';
  end if;

  if exists (
    select 1
    from pg_proc as routine
    join pg_namespace as routine_schema on routine_schema.oid = routine.pronamespace
    where routine_schema.nspname in ('public', 'private', 'util')
      and (
        routine.prosrc ~ '\mextracted_text\M'
        or routine.prosrc ~ '\membedding_flag\M'
        or routine.prosrc ~ '\membedding_at\M'
        or coalesce(routine.proargnames, '{}'::text[])
          && array['full_text_weight', 'extracted_text_weight']::text[]
      )
  ) then
    raise exception 'retired extracted-text or embedding-flag routine dependency remains';
  end if;

  if exists (
    select 1
    from pg_trigger as trigger_row
    where not trigger_row.tgisinternal
      and (
        pg_get_triggerdef(trigger_row.oid, true) ~ '\mextracted_text\M'
        or pg_get_triggerdef(trigger_row.oid, true) ~ '\membedding_flag\M'
        or pg_get_triggerdef(trigger_row.oid, true) ~ '\membedding_at\M'
      )
  ) then
    raise exception 'retired extracted-text or embedding-flag trigger dependency remains';
  end if;

  if exists (
    select 1
    from pg_indexes as index_row
    where index_row.schemaname = 'public'
      and index_row.indexdef ~ '\mextracted_text\M'
  ) then
    raise exception 'retired extracted-text index remains';
  end if;

  select count(*)
  into retained_derivative_column_count
  from information_schema.columns as column_info
  where column_info.table_schema = 'public'
    and column_info.table_name in (
      'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
      'processes', 'sources', 'unitgroups'
    )
    and column_info.column_name in (
      'extracted_md', 'embedding_ft', 'embedding_ft_at'
    );

  if retained_derivative_column_count <> 21 then
    raise exception
      'expected 21 retained derivative columns, found %',
      retained_derivative_column_count;
  end if;

  select count(*)
  into retained_lexical_index_count
  from pg_indexes as index_row
  where index_row.schemaname = 'public'
    and index_row.tablename in (
      'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
      'processes', 'sources', 'unitgroups'
    )
    and index_row.indexdef ~ '\mextracted_md\M'
    and index_row.indexdef ilike '%using pgroonga%';

  if retained_lexical_index_count <> 7 then
    raise exception
      'expected seven extracted_md PGroonga indexes, found %',
      retained_lexical_index_count;
  end if;

  select count(*)
  into retained_vector_index_count
  from pg_indexes as index_row
  where index_row.schemaname = 'public'
    and index_row.tablename in (
      'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
      'processes', 'sources', 'unitgroups'
    )
    and index_row.indexdef ~ '\membedding_ft\M'
    and index_row.indexdef ilike '%using hnsw%';

  if retained_vector_index_count < 7 then
    raise exception
      'expected at least seven embedding_ft HNSW indexes, found %',
      retained_vector_index_count;
  end if;
end
$$;

notify pgrst, 'reload schema';

reset statement_timeout;
reset lock_timeout;
