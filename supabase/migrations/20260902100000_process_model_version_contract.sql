begin;

alter table public.processes
  add column model_version character(9);

comment on column public.processes.model_version is
  'Exact LifecycleModel version that owns or generated this Process. NULL preserves the legacy same-version fallback to processes.version.';

alter table public.processes
  add constraint processes_model_version_requires_model_id_check
  check (model_version is null or model_id is not null)
  not valid;

alter table public.processes
  validate constraint processes_model_version_requires_model_id_check;

alter table public.processes
  add constraint processes_model_version_format_check
  check (
    model_version is null
    or model_version::text ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
  )
  not valid;

alter table public.processes
  validate constraint processes_model_version_format_check;

create index processes_model_owner_version_idx
  on public.processes using btree (
    model_id,
    (coalesce(model_version, version))
  )
  where model_id is not null;

create or replace trigger processes_set_modified_at_trigger
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
  model_id,
  model_version
on public.processes
for each row
execute function private.update_modified_at();

create or replace trigger process_derivative_rebuild_primary_update_fence
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
  model_id,
  model_version
on public.processes
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

commit;
