begin;

create or replace function pg_temp.required_process_owner_version_replace(
  source_text text,
  old_text text,
  new_text text,
  replacement_label text
) returns text
language plpgsql
as $$
declare
  first_position integer := strpos(source_text, old_text);
  remainder text;
begin
  if first_position = 0 then
    raise exception 'required Process owner-version replacement did not apply: %', replacement_label;
  end if;

  remainder := substr(source_text, first_position + length(old_text));
  if strpos(remainder, old_text) > 0 then
    raise exception 'Process owner-version replacement is ambiguous: %', replacement_label;
  end if;

  return replace(source_text, old_text, new_text);
end;
$$;

do $$
declare
  collect_targets constant regprocedure :=
    'api.cmd_review_collect_dataset_targets(jsonb,boolean)'::regprocedure;
  resolve_targets constant regprocedure :=
    'private.review_resolve_current_reference_targets_v1(uuid[])'::regprocedure;
  assert_closure constant regprocedure :=
    'private.cmd_review_assert_lifecycle_closure(jsonb,text,uuid)'::regprocedure;
  legacy_approve constant regprocedure :=
    'private.cmd_review_approve_issue304_legacy(text,uuid,jsonb)'::regprocedure;
  definition text;
begin
  definition := pg_get_functiondef(collect_targets);
  definition := pg_temp.required_process_owner_version_replace(
    definition,
    'model_process.version = v_current.dataset_version',
    'coalesce(model_process.model_version, model_process.version) = v_current.dataset_version',
    'review target collection'
  );
  execute definition;

  definition := pg_get_functiondef(resolve_targets);
  definition := pg_temp.required_process_owner_version_replace(
    definition,
    'model_process.version = current_target.data_version',
    'coalesce(model_process.model_version, model_process.version) = current_target.data_version',
    'current reference target resolution'
  );
  execute definition;

  definition := pg_get_functiondef(assert_closure);
  definition := pg_temp.required_process_owner_version_replace(
    definition,
    'model_process.version = v_current.dataset_version',
    'coalesce(model_process.model_version, model_process.version) = v_current.dataset_version',
    'lifecycle closure assertion'
  );
  execute definition;

  definition := pg_get_functiondef(legacy_approve);
  definition := pg_temp.required_process_owner_version_replace(
    definition,
    'model_process.version = v_review.data_version',
    'coalesce(model_process.model_version, model_process.version) = v_review.data_version',
    'legacy review approval compatibility'
  );
  execute definition;
end;
$$;

comment on function api.cmd_review_collect_dataset_targets(jsonb, boolean) is
  'Collects exact review targets; LifecycleModel result membership uses model_id plus coalesce(model_version, process version).';

comment on function private.review_resolve_current_reference_targets_v1(uuid[]) is
  'Resolves current review references using exact Process owner versions with legacy same-version fallback.';

comment on function private.cmd_review_assert_lifecycle_closure(jsonb, text, uuid) is
  'Asserts lifecycle closure using exact Process owner versions with legacy same-version fallback.';

comment on function private.cmd_review_approve_issue304_legacy(text, uuid, jsonb) is
  'Legacy approval compatibility path; Process model membership uses exact owner-version semantics.';

commit;
