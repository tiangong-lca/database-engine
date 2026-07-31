-- The Step 3 actor fence protects identity-capture guards across flows,
-- processes, flow properties, and unit groups. Flow derivative workers only
-- update extracted_md and the full-text embedding columns, none of which
-- participate in the captured Flow guard. Avoid taking the owner-wide fence
-- for those guard-neutral updates while keeping every other present or future
-- Flow column protected by default.

drop trigger if exists dataset_flow_identity_flow_active_fence
  on public.flows;
drop trigger if exists dataset_flow_identity_flow_insert_delete_active_fence
  on public.flows;

create trigger dataset_flow_identity_flow_active_fence
before update on public.flows
for each row
when (
  (to_jsonb(new) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at'
  ]::text[])
  is distinct from
  (to_jsonb(old) - array[
    'extracted_md',
    'embedding_ft',
    'embedding_ft_at'
  ]::text[])
)
execute function private.dataset_flow_identity_active_fence_v2();

create trigger dataset_flow_identity_flow_insert_delete_active_fence
before insert or delete on public.flows
for each row
execute function private.dataset_flow_identity_active_fence_v2();

comment on trigger dataset_flow_identity_flow_active_fence
  on public.flows is
  'Fail-closed Step 3 actor fence for Flow updates that change any identity-guard-relevant or future non-derivative column; extracted_md, embedding_ft, and embedding_ft_at updates bypass the owner-wide fence.';

comment on trigger dataset_flow_identity_flow_insert_delete_active_fence
  on public.flows is
  'Fail-closed Step 3 actor fence for every Flow insert or delete.';
