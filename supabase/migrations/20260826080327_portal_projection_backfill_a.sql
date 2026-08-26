-- Bounded UUID-prefix batch; one migration commit caps same-key trigger wait.
begin;

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.backfill_portal_catalog_search_range_v1(
  'a0000000-0000-0000-0000-000000000000'::uuid,
  'b0000000-0000-0000-0000-000000000000'::uuid
);
reset role;
revoke api_internal_executor from postgres;

commit;
