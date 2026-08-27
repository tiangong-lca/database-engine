-- Issue #533: build one narrow eligibility index for deterministic public
-- CAS/classification homepage examples. This standalone concurrent statement
-- stores only identity/order keys; it never stores card or example values.

create index concurrently portal_catalog_summary_eligibility_v1_idx
on private.portal_catalog_search_rows_v1 (
  dataset_kind,
  id,
  version desc,
  modified_at desc,
  state_code desc
)
where (
    dataset_kind = 'flow'
    and pg_catalog.jsonb_typeof(card -> 'casNumber') = 'string'
    and card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
  ) or (
    pg_catalog.jsonb_typeof(card -> 'classifications') = 'array'
    and pg_catalog.jsonb_array_length(card -> 'classifications') > 0
  );
