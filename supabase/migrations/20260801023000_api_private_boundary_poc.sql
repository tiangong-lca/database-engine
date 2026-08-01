-- Representative api/private boundary contract for the full schema refactor.
--
-- This is an additive Expand migration. Physical tables remain in public while
-- consumers move to explicit api DTO/RPC contracts and direct database workers
-- use private schema-qualified relations. Contract-stage physical moves remain
-- separately gated by exact consumer-zero and populated-upgrade evidence.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create schema if not exists api;
revoke all on schema api from public;
grant usage on schema api to anon, authenticated, service_role;

-- New API objects are closed by default. Every DTO/RPC below grants only the
-- roles required by its contract.
alter default privileges for role postgres in schema api
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema api
  revoke all on sequences from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema api
  revoke execute on functions from public, anon, authenticated, service_role;

create or replace view api.processes_v1
with (security_invoker = true)
as
select
  id,
  json,
  created_at,
  user_id,
  state_code,
  version,
  modified_at,
  team_id,
  review_id,
  rule_verification,
  reviews,
  model_id
from public.processes;

revoke all on api.processes_v1 from public, anon, authenticated, service_role;
grant select on api.processes_v1 to anon, authenticated, service_role;

comment on view api.processes_v1 is
  'Versioned, security-invoker core Process read DTO. RLS remains owned by public.processes during Expand.';

create or replace view api.review_comments_v1
with (security_invoker = true)
as
select
  review_id,
  reviewer_id,
  json,
  created_at,
  modified_at,
  state_code
from public.comments;

revoke all on api.review_comments_v1 from public, anon, authenticated, service_role;
grant select on api.review_comments_v1 to authenticated, service_role;

comment on view api.review_comments_v1 is
  'Versioned, security-invoker review comment DTO. The underlying public.comments RLS remains authoritative during Expand.';

create or replace function api.cmd_review_save_comment_draft_v1(
  p_review_id uuid,
  p_json jsonb,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language sql
security invoker
set search_path = ''
as $$
  select public.cmd_review_save_comment_draft(p_review_id, p_json, p_audit)
$$;

alter function api.cmd_review_save_comment_draft_v1(uuid, jsonb, jsonb)
  owner to postgres;
revoke all on function api.cmd_review_save_comment_draft_v1(uuid, jsonb, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function api.cmd_review_save_comment_draft_v1(uuid, jsonb, jsonb)
  to authenticated, service_role;

comment on function api.cmd_review_save_comment_draft_v1(uuid, jsonb, jsonb) is
  'Authenticated versioned command adapter preserving the existing review draft result/error envelope during Expand.';

create or replace function api.worker_list_jobs_by_concurrency_key_v1(
  p_job_kind text,
  p_concurrency_key text,
  p_statuses text[],
  p_limit integer default 20,
  p_include_internal boolean default true
) returns jsonb
language sql
security invoker
set search_path = ''
as $$
  select public.worker_list_jobs_by_concurrency_key(
    p_job_kind,
    p_concurrency_key,
    p_statuses,
    p_limit,
    p_include_internal
  )
$$;

alter function api.worker_list_jobs_by_concurrency_key_v1(text, text, text[], integer, boolean)
  owner to postgres;
revoke all on function api.worker_list_jobs_by_concurrency_key_v1(text, text, text[], integer, boolean)
  from public, anon, authenticated, service_role;
grant execute on function api.worker_list_jobs_by_concurrency_key_v1(text, text, text[], integer, boolean)
  to service_role;

create or replace function api.worker_read_jobs_by_ids_v1(
  p_job_ids uuid[],
  p_include_internal boolean default false
) returns jsonb
language sql
security invoker
set search_path = ''
as $$
  select public.worker_read_jobs_by_ids(p_job_ids, p_include_internal)
$$;

alter function api.worker_read_jobs_by_ids_v1(uuid[], boolean)
  owner to postgres;
revoke all on function api.worker_read_jobs_by_ids_v1(uuid[], boolean)
  from public, anon, authenticated, service_role;
grant execute on function api.worker_read_jobs_by_ids_v1(uuid[], boolean)
  to service_role;

comment on function api.worker_list_jobs_by_concurrency_key_v1(text, text, text[], integer, boolean) is
  'Service-only versioned Data API adapter over the private Worker control-plane Expand contract.';
comment on function api.worker_read_jobs_by_ids_v1(uuid[], boolean) is
  'Service-only versioned bounded batch-read adapter over the private Worker control-plane Expand contract.';

-- Views are deliberately not added to supabase_realtime. Core public tables
-- remain the Realtime publication boundary; api is a PostgREST DTO/RPC layer.
notify pgrst, 'reload schema';

commit;
