-- Database #603: bounded exact-version candidate lookup for selective
-- access-level-filtered Portal Flow semantic retrieval.  Keep this separate
-- from geography so either equality predicate has a usable leading key.

create index concurrently portal_catalog_facet_flow_access_level_v1_idx
on private.portal_catalog_facet_rows_v1 (facet_access_level)
include (id, version)
where dataset_kind = 'flow'
  and state_code in (100, 200)
  and facet_contract_version = 1;
