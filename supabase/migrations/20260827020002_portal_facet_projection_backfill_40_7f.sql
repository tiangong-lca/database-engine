-- Issue #531: backfill one bounded UUID quarter of the independent narrow
-- facet projection. Existing Search, Hybrid, and Facets reads are unchanged.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

grant api_internal_executor to postgres;
set role api_internal_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

insert into private.portal_catalog_facet_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  facet_access_level, facet_geography, facet_reference_year,
  facet_process_subtype, facet_source, facet_contract_version
)
select projection.dataset_kind,
  projection.id,
  projection.version,
  projection.state_code,
  projection.modified_at,
  facts.facet_access_level,
  facts.facet_geography,
  facts.facet_reference_year,
  facts.facet_process_subtype,
  facts.facet_source,
  1
from private.portal_catalog_search_rows_v1 as projection
cross join lateral private.portal_catalog_facet_facts_v1(
  projection.dataset_kind,
  projection.card
) as facts
where projection.dataset_kind = 'process'
  and projection.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '80000000-0000-0000-0000-000000000000'::uuid
on conflict (dataset_kind, id, version) do nothing;

insert into private.portal_catalog_facet_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  facet_access_level, facet_geography, facet_reference_year,
  facet_process_subtype, facet_source, facet_contract_version
)
select projection.dataset_kind,
  projection.id,
  projection.version,
  projection.state_code,
  projection.modified_at,
  facts.facet_access_level,
  facts.facet_geography,
  facts.facet_reference_year,
  facts.facet_process_subtype,
  facts.facet_source,
  1
from private.portal_catalog_search_rows_v1 as projection
cross join lateral private.portal_catalog_facet_facts_v1(
  projection.dataset_kind,
  projection.card
) as facts
where projection.dataset_kind = 'flow'
  and projection.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '80000000-0000-0000-0000-000000000000'::uuid
on conflict (dataset_kind, id, version) do nothing;

do $verify_portal_facet_backfill_40_7f_process_count$
begin
  if (
    select count(*)
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '80000000-0000-0000-0000-000000000000'::uuid
  ) <> (
    select count(*)
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'process'
      and facet.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and facet.id < '80000000-0000-0000-0000-000000000000'::uuid
  ) then
    raise exception 'Portal facet projection backfill 40_7f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_40_7f_process_count$;

do $verify_portal_facet_backfill_40_7f_flow_count$
begin
  if (
    select count(*)
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '80000000-0000-0000-0000-000000000000'::uuid
  ) <> (
    select count(*)
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'flow'
      and facet.id >= '40000000-0000-0000-0000-000000000000'::uuid
    and facet.id < '80000000-0000-0000-0000-000000000000'::uuid
  ) then
    raise exception 'Portal facet projection backfill 40_7f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_40_7f_flow_count$;

reset role;
revoke api_internal_executor from postgres;

commit;
