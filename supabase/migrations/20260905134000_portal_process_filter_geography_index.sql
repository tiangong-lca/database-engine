-- Database #620: bounded exact-version candidate lookup for selective
-- geography-filtered Portal Process semantic retrieval. The vector remains on
-- public.processes; this index stores only the synchronized facet value and
-- exact source identity.

create index concurrently portal_catalog_facet_process_geography_v1_idx
on private.portal_catalog_facet_rows_v1 (facet_geography)
include (id, version)
where dataset_kind = 'process'
  and state_code in (100, 200)
  and facet_contract_version = 1;
