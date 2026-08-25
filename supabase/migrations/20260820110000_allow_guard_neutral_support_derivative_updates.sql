-- FlowProperty and UnitGroup support snapshots protect identity, ownership,
-- state, payload, and modified_at. Their Edge-owned asynchronous derivative
-- columns are not part of that proof, so those writebacks can safely bypass
-- the scope-wide actor fence without weakening protected fields.

-- This is the single current internal trigger implementation, not a
-- version-selectable external API. Preserve its OID, dependencies, owner, and
-- grants while giving it a stable canonical name.
alter function private.dataset_flow_identity_active_fence_v2()
  rename to dataset_flow_identity_active_fence;

drop trigger if exists dataset_flow_identity_flowproperty_active_fence
  on public.flowproperties;
drop trigger if exists dataset_flow_identity_flowproperty_delete_active_fence
  on public.flowproperties;

create trigger dataset_flow_identity_flowproperty_active_fence
before update on public.flowproperties
for each row
when (
  (to_jsonb(new) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
  is distinct from
  (to_jsonb(old) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
)
execute function private.dataset_flow_identity_active_fence();

create trigger dataset_flow_identity_flowproperty_delete_active_fence
before delete on public.flowproperties
for each row
execute function private.dataset_flow_identity_active_fence();

comment on trigger dataset_flow_identity_flowproperty_active_fence
  on public.flowproperties is
  'Fail-closed Step 3 actor fence for FlowProperty updates that change any support-guard-relevant or future non-derivative column; extracted_md, embedding_ft, embedding_ft_at, and search_text updates bypass the actor fence.';

comment on trigger dataset_flow_identity_flowproperty_delete_active_fence
  on public.flowproperties is
  'Fail-closed Step 3 actor fence for every FlowProperty delete.';

drop trigger if exists dataset_flow_identity_unitgroup_active_fence
  on public.unitgroups;
drop trigger if exists dataset_flow_identity_unitgroup_delete_active_fence
  on public.unitgroups;

create trigger dataset_flow_identity_unitgroup_active_fence
before update on public.unitgroups
for each row
when (
  (to_jsonb(new) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
  is distinct from
  (to_jsonb(old) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at',
    'search_text'
  ]::text[])
)
execute function private.dataset_flow_identity_active_fence();

create trigger dataset_flow_identity_unitgroup_delete_active_fence
before delete on public.unitgroups
for each row
execute function private.dataset_flow_identity_active_fence();

comment on trigger dataset_flow_identity_unitgroup_active_fence
  on public.unitgroups is
  'Fail-closed Step 3 actor fence for UnitGroup updates that change any support-guard-relevant or future non-derivative column; extracted_md, embedding_ft, embedding_ft_at, and search_text updates bypass the actor fence.';

comment on trigger dataset_flow_identity_unitgroup_delete_active_fence
  on public.unitgroups is
  'Fail-closed Step 3 actor fence for every UnitGroup delete.';
