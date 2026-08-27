-- Issue #539: mirror each public facet version into a locator-free sitemap row
-- with a physical stable hash bucket. The new table/index are empty before
-- backfill, so no live-table concurrent index is required. Exact-version rows
-- avoid a shared mutable winner and therefore add no cross-version writer lock.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_sitemap_rows_prerequisite_guard$
begin
  if pg_catalog.to_regclass(
       'private.portal_sitemap_rows_v1'
     ) is not null
     or pg_catalog.to_regclass(
       'private.portal_sitemap_latest_rows_v1'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_row_v1()'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_projection_contract_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_facet_contract_v1()'
     ) is null
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'portal_public_executor'
         and not role.rolcanlogin
         and not role.rolinherit
         and not role.rolbypassrls
         and not role.rolsuper
     )
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'api_internal_executor'
         and not role.rolcanlogin
         and not role.rolbypassrls
     ) then
    raise exception 'Portal sitemap version projection prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_sitemap_rows_prerequisite_guard$;

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
reset role;
revoke api_internal_executor from postgres;

create table private.portal_sitemap_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  modified_at timestamptz not null,
  shard_no smallint not null
    check (shard_no between 0 and 63),
  contract_version smallint not null
    check (contract_version = 1),
  primary key (dataset_kind, id, version),
  constraint portal_sitemap_rows_source_v1_fk
    foreign key (dataset_kind, id, version)
    references private.portal_catalog_facet_rows_v1(
      dataset_kind,
      id,
      version
    )
    on update restrict
    on delete cascade
);

alter table private.portal_sitemap_rows_v1 owner to postgres;
alter table private.portal_sitemap_rows_v1 enable row level security;
alter table private.portal_sitemap_rows_v1 force row level security;

create policy portal_sitemap_rows_portal_select_v1
on private.portal_sitemap_rows_v1
for select
to portal_public_executor
using (contract_version = 1 and shard_no between 0 and 63);

create policy portal_sitemap_rows_internal_all_v1
on private.portal_sitemap_rows_v1
for all
to api_internal_executor
using (contract_version = 1 and shard_no between 0 and 63)
with check (contract_version = 1 and shard_no between 0 and 63);

revoke all on table private.portal_sitemap_rows_v1
  from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
) on table private.portal_sitemap_rows_v1
  to portal_public_executor;
grant select, insert, update, delete
on table private.portal_sitemap_rows_v1
  to api_internal_executor;

create index portal_sitemap_rows_shard_v1_idx
on private.portal_sitemap_rows_v1 (
  shard_no,
  contract_version,
  dataset_kind,
  id,
  version desc,
  modified_at desc
);

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create function private.sync_portal_sitemap_row_v1()
returns trigger
language plpgsql
volatile
parallel unsafe
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  insert into private.portal_sitemap_rows_v1 (
    dataset_kind,
    id,
    version,
    modified_at,
    shard_no,
    contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.modified_at,
    (
      pg_catalog.get_byte(
        pg_catalog.decode(
          pg_catalog.md5(
            new.dataset_kind || ':'::text || new.id::text
          ),
          'hex'::text
        ),
        0
      ) / 4
    )::smallint,
    1
  )
  on conflict (dataset_kind, id, version) do update
  set modified_at = excluded.modified_at,
      shard_no = excluded.shard_no,
      contract_version = excluded.contract_version
  where (
    portal_sitemap_rows_v1.modified_at,
    portal_sitemap_rows_v1.shard_no,
    portal_sitemap_rows_v1.contract_version
  ) is distinct from (
    excluded.modified_at,
    excluded.shard_no,
    excluded.contract_version
  );
  return null;
end
$function$;

revoke all on function private.sync_portal_sitemap_row_v1()
  from public, anon, authenticated, service_role, portal_public_executor;
grant execute on function private.sync_portal_sitemap_row_v1()
  to postgres;

reset role;
revoke create on schema private from api_internal_executor;

create trigger portal_sitemap_rows_sync_v1
after insert or update of
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  facet_contract_version
on private.portal_catalog_facet_rows_v1
for each row
execute function private.sync_portal_sitemap_row_v1();

set role api_internal_executor;
revoke execute on function private.sync_portal_sitemap_row_v1()
  from postgres;
reset role;
revoke api_internal_executor from postgres;

grant api_internal_executor to postgres;
set role api_internal_executor;

insert into private.portal_sitemap_rows_v1 (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
)
select
  facet.dataset_kind,
  facet.id,
  facet.version,
  facet.modified_at,
  (
    pg_catalog.get_byte(
      pg_catalog.decode(
        pg_catalog.md5(
          facet.dataset_kind || ':'::text || facet.id::text
        ),
        'hex'::text
      ),
      0
    ) / 4
  )::smallint,
  1
from private.portal_catalog_facet_rows_v1 as facet
where facet.state_code in (100, 200)
  and facet.facet_contract_version = 1;

do $verify_portal_sitemap_rows_projection_v1$
begin
  if exists (
    with expected as (
      select
        facet.dataset_kind,
        facet.id,
        facet.version,
        facet.modified_at,
        (
          pg_catalog.get_byte(
            pg_catalog.decode(
              pg_catalog.md5(
                facet.dataset_kind || ':'::text || facet.id::text
              ),
              'hex'::text
            ),
            0
          ) / 4
        )::smallint as shard_no,
        1::smallint as contract_version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
    )
    (select * from expected
     except
     select * from private.portal_sitemap_rows_v1)
    union all
    (select * from private.portal_sitemap_rows_v1
     except
     select * from expected)
  ) then
    raise exception 'Portal sitemap version projection reconciliation failed'
      using errcode = '55000';
  end if;
end
$verify_portal_sitemap_rows_projection_v1$;

reset role;
revoke api_internal_executor from postgres;

comment on table private.portal_sitemap_rows_v1 is
  'Exact public Process/Flow version and stable 64-way sitemap bucket; contains no card, document, actor, credential, or locator.';
comment on index private.portal_sitemap_rows_shard_v1_idx is
  'Latest-version sitemap shard order over the exact-version locator-free projection.';
comment on trigger portal_sitemap_rows_sync_v1
  on private.portal_catalog_facet_rows_v1 is
  'Upserts only the affected exact sitemap version after a governed facet INSERT or UPDATE converges; DELETE follows the exact FK cascade.';

commit;
