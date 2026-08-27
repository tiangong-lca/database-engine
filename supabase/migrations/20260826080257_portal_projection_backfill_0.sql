-- Bounded UUID-prefix batch; one migration commit caps same-key trigger wait.
begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.backfill_portal_catalog_search_range_v1(
  '00000000-0000-0000-0000-000000000000'::uuid,
  '10000000-0000-0000-0000-000000000000'::uuid
);
reset role;
revoke api_internal_executor from postgres;

commit;
