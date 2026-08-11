create index concurrently if not exists processes_model_id_version_idx
  on public.processes using btree (model_id, version)
  where model_id is not null;
