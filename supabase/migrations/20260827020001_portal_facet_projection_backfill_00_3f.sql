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
  and projection.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '40000000-0000-0000-0000-000000000000'::uuid
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
  and projection.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '40000000-0000-0000-0000-000000000000'::uuid
on conflict (dataset_kind, id, version) do nothing;

do $verify_portal_facet_backfill_00_3f_process_parity$
begin
  if exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    cross join lateral private.portal_catalog_facet_facts_v1(
      projection.dataset_kind,
      projection.card
    ) as facts
    left join private.portal_catalog_facet_rows_v1 as facet
      on facet.dataset_kind = projection.dataset_kind
     and facet.id = projection.id
     and facet.version = projection.version
    where projection.dataset_kind = 'process'
      and projection.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '40000000-0000-0000-0000-000000000000'::uuid
      and (
        facet.id is null
        or facet.state_code is distinct from projection.state_code
        or facet.modified_at is distinct from projection.modified_at
        or facet.facet_access_level is distinct from facts.facet_access_level
        or facet.facet_geography is distinct from facts.facet_geography
        or facet.facet_reference_year is distinct from facts.facet_reference_year
        or facet.facet_process_subtype is distinct from facts.facet_process_subtype
        or facet.facet_source is distinct from facts.facet_source
        or facet.facet_contract_version is distinct from 1
      )
  ) then
    raise exception 'Portal facet projection backfill 00_3f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_00_3f_process_parity$;

do $verify_portal_facet_backfill_00_3f_flow_parity$
begin
  if exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    cross join lateral private.portal_catalog_facet_facts_v1(
      projection.dataset_kind,
      projection.card
    ) as facts
    left join private.portal_catalog_facet_rows_v1 as facet
      on facet.dataset_kind = projection.dataset_kind
     and facet.id = projection.id
     and facet.version = projection.version
    where projection.dataset_kind = 'flow'
      and projection.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and projection.id < '40000000-0000-0000-0000-000000000000'::uuid
      and (
        facet.id is null
        or facet.state_code is distinct from projection.state_code
        or facet.modified_at is distinct from projection.modified_at
        or facet.facet_access_level is distinct from facts.facet_access_level
        or facet.facet_geography is distinct from facts.facet_geography
        or facet.facet_reference_year is distinct from facts.facet_reference_year
        or facet.facet_process_subtype is distinct from facts.facet_process_subtype
        or facet.facet_source is distinct from facts.facet_source
        or facet.facet_contract_version is distinct from 1
      )
  ) then
    raise exception 'Portal facet projection backfill 00_3f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_00_3f_flow_parity$;

do $verify_portal_facet_backfill_00_3f_process_extra$
begin
  if exists (
    select 1
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'process'
      and facet.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and facet.id < '40000000-0000-0000-0000-000000000000'::uuid
      and not exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = facet.dataset_kind
          and projection.id = facet.id
          and projection.version = facet.version
      )
  ) then
    raise exception 'Portal facet projection backfill 00_3f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_00_3f_process_extra$;

do $verify_portal_facet_backfill_00_3f_flow_extra$
begin
  if exists (
    select 1
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'flow'
      and facet.id >= '00000000-0000-0000-0000-000000000000'::uuid
    and facet.id < '40000000-0000-0000-0000-000000000000'::uuid
      and not exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = facet.dataset_kind
          and projection.id = facet.id
          and projection.version = facet.version
      )
  ) then
    raise exception 'Portal facet projection backfill 00_3f is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_backfill_00_3f_flow_extra$;

reset role;
revoke api_internal_executor from postgres;

commit;
