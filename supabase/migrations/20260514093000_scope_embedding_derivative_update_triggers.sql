drop trigger if exists flows_json_sync_trigger on public.flows;
drop trigger if exists flows_set_modified_at_trigger on public.flows;
drop trigger if exists lifecyclemodels_json_sync_trigger on public.lifecyclemodels;
drop trigger if exists lifecyclemodels_set_modified_at_trigger on public.lifecyclemodels;
drop trigger if exists processes_json_sync_trigger on public.processes;
drop trigger if exists processes_set_modified_at_trigger on public.processes;

create trigger flows_json_sync_trigger
before insert or update of json_ordered on public.flows
for each row
execute function public.flows_sync_jsonb_version();

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
  reviews,
  embedding_flag
on public.flows
for each row
execute function public.update_modified_at();

create trigger lifecyclemodels_json_sync_trigger
before insert or update of json_ordered on public.lifecyclemodels
for each row
execute function public.lifecyclemodels_sync_jsonb_version();

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
  reviews,
  embedding_flag
on public.lifecyclemodels
for each row
execute function public.update_modified_at();

create trigger processes_json_sync_trigger
before insert or update of json_ordered on public.processes
for each row
execute function public.processes_sync_jsonb_version();

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
  embedding_flag,
  model_id
on public.processes
for each row
execute function public.update_modified_at();
