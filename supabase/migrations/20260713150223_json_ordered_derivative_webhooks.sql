-- Dataset commands treat json_ordered as the writable source document and
-- synchronize json in a BEFORE trigger. PostgreSQL column-specific UPDATE
-- triggers only consider columns named by the original UPDATE statement, so
-- the markdown webhook must explicitly cover both writable representations.

drop trigger if exists flow_extract_md_trigger_update on public.flows;
create trigger flow_extract_md_trigger_update
after update of json, json_ordered on public.flows
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_flow_embedding_ft', '1000');

drop trigger if exists lifecyclemodel_extract_md_trigger_update on public.lifecyclemodels;
create trigger lifecyclemodel_extract_md_trigger_update
after update of json, json_ordered on public.lifecyclemodels
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_model_embedding_ft', '1000');

drop trigger if exists process_extract_md_trigger_update on public.processes;
create trigger process_extract_md_trigger_update
after update of json, json_ordered on public.processes
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_process_embedding_ft', '1000');
