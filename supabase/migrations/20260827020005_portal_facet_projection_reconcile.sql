-- Issue #531: reconcile the independent narrow facet projection behind a
-- short parent-write fence. No public read path changes in this migration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- The parent projection is the authority boundary. Its source-table trigger
-- has already observed finalized NEW values, and the facet trigger writes the
-- child in the same source transaction. This fence closes only backfill gaps.
lock table private.portal_catalog_search_rows_v1
  in share row exclusive mode;
lock table private.portal_catalog_facet_rows_v1
  in share row exclusive mode;

grant api_internal_executor to postgres;
set role api_internal_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

insert into private.portal_catalog_facet_rows_v1 (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  facet_access_level,
  facet_geography,
  facet_reference_year,
  facet_process_subtype,
  facet_source,
  facet_contract_version
)
select
  projection.dataset_kind,
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
where not exists (
  select 1
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.dataset_kind = projection.dataset_kind
    and facet.id = projection.id
    and facet.version = projection.version
)
on conflict (dataset_kind, id, version) do nothing;

do $verify_portal_facet_projection_reconciliation$
begin
  if (
    select count(*)
    from private.portal_catalog_facet_rows_v1
  ) <> (
    select count(*)
    from private.portal_catalog_search_rows_v1
  ) or exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    left join private.portal_catalog_facet_rows_v1 as facet
      on facet.dataset_kind = projection.dataset_kind
     and facet.id = projection.id
     and facet.version = projection.version
    where facet.id is null
       or facet.state_code is distinct from projection.state_code
       or facet.modified_at is distinct from projection.modified_at
       or facet.facet_contract_version is distinct from 1
  ) or exists (
    select 1
    from private.portal_catalog_facet_rows_v1 as facet
    where not exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = facet.dataset_kind
        and projection.id = facet.id
        and projection.version = facet.version
    )
  ) then
    raise exception 'Portal facet projection reconciliation failed'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_projection_reconciliation$;

reset role;
revoke api_internal_executor from postgres;

commit;
