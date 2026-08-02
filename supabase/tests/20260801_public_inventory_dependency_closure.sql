begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, pg_catalog;
select plan(16);

select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind in ('r','p')), 49::bigint, 'public physical table inventory is exact after Worker and LCA snapshot-family moves');
select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='v'), 12::bigint, 'public view inventory includes Worker and LCA snapshot-family compatibility views');
select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='m'), 0::bigint, 'public materialized-view inventory is exact');
select is((select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.prokind='f'), 336::bigint, 'public function inventory includes the grouped root-review facades, #337 adapters, and Worker composite bridge');
select is((select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.prokind='p'), 0::bigint, 'public procedure inventory is exact');

select is((
  select count(*)::bigint
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relkind='r'
    and c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
), 4::bigint, 'the exact four Worker relations are private physical tables');
select is((
  select count(*)::bigint
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind in ('r','p')
    and c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
), 0::bigint, 'no Worker control-plane relation remains a public physical table');
select is((
  select count(*)::bigint
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v'
    and c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
    and coalesce(c.reloptions,'{}') @> array['security_invoker=true']
), 4::bigint, 'the exact four public Worker compatibility relations are security-invoker views');
select is((
  select count(*)::bigint
  from pg_class v
  join pg_namespace vn on vn.oid=v.relnamespace
  where vn.nspname='public' and v.relkind='v'
    and v.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
    and exists (
      select 1
      from pg_rewrite rw
      join pg_depend d on d.classid='pg_rewrite'::regclass and d.objid=rw.oid
      join pg_class source on source.oid=d.refobjid
      join pg_namespace sn on sn.oid=source.relnamespace
      where rw.ev_class=v.oid
        and d.refclassid='pg_class'::regclass
        and sn.nspname='private'
        and source.relkind='r'
        and source.relname=v.relname
    )
), 4::bigint, 'every public Worker compatibility view retains an explicit same-name dependency on its private physical source');

select ok(exists(select 1 from pg_constraint c join pg_class r on r.oid=c.conrelid join pg_namespace n on n.oid=r.relnamespace where c.contype='f' and n.nspname='public'), 'foreign-key dependency source is populated');
select ok(exists(select 1 from pg_trigger t join pg_class r on r.oid=t.tgrelid join pg_namespace n on n.oid=r.relnamespace where not t.tgisinternal and n.nspname='public'), 'trigger dependency source is populated');
select ok(exists(select 1 from pg_policy p join pg_class r on r.oid=p.polrelid join pg_namespace n on n.oid=r.relnamespace where n.nspname='public'), 'policy dependency source is populated');
select ok(exists(select 1 from pg_rewrite rw join pg_class v on v.oid=rw.ev_class join pg_namespace n on n.oid=v.relnamespace join pg_depend d on d.classid='pg_rewrite'::regclass and d.objid=rw.oid where n.nspname='public' and v.relkind in ('v','m') and d.refclassid='pg_class'::regclass and d.refobjid<>v.oid), 'rewrite dependency source is populated');
select ok(exists(select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace cross join lateral unnest(coalesce(p.proallargtypes,p.proargtypes::oid[])) a(type_oid) join pg_type t on t.oid=a.type_oid join pg_class c on c.reltype=t.oid where n.nspname='public'), 'composite-signature dependency source is populated');
select ok(exists(select 1 from pg_default_acl d join pg_namespace n on n.oid=d.defaclnamespace where n.nspname='public'), 'public default privileges are inventoried');
select ok(exists(select 1 from pg_roles where rolname='service_role'), 'role-contract inventory can resolve service_role');

select * from finish();
rollback;
