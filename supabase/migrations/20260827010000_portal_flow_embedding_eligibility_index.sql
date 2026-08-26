create index concurrently flows_portal_embedding_eligible_v1_idx
on public.flows using btree (id, version)
where state_code in (100, 200)
  and embedding_ft is not null;
