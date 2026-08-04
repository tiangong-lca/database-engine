begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, pg_catalog;
select plan(12);

select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind in ('r','p')), 56::bigint, 'public table inventory is exact');
select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='v'), 5::bigint, 'public view inventory is exact');
select is((select count(*)::bigint from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='m'), 0::bigint, 'public materialized-view inventory is exact');
select is((select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.prokind='f'), 332::bigint, 'public function inventory includes the #337 adapters');
select is((select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.prokind='p'), 0::bigint, 'public procedure inventory is exact');

select ok(exists(select 1 from pg_constraint c join pg_class r on r.oid=c.conrelid join pg_namespace n on n.oid=r.relnamespace where c.contype='f' and n.nspname='public'), 'foreign-key dependency source is populated');
select ok(exists(select 1 from pg_trigger t join pg_class r on r.oid=t.tgrelid join pg_namespace n on n.oid=r.relnamespace where not t.tgisinternal and n.nspname='public'), 'trigger dependency source is populated');
select ok(exists(select 1 from pg_policy p join pg_class r on r.oid=p.polrelid join pg_namespace n on n.oid=r.relnamespace where n.nspname='public'), 'policy dependency source is populated');
select ok(exists(select 1 from pg_rewrite rw join pg_class v on v.oid=rw.ev_class join pg_namespace n on n.oid=v.relnamespace join pg_depend d on d.classid='pg_rewrite'::regclass and d.objid=rw.oid where n.nspname='public' and v.relkind in ('v','m') and d.refclassid='pg_class'::regclass and d.refobjid<>v.oid), 'rewrite dependency source is populated');
select ok(exists(select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace cross join lateral unnest(coalesce(p.proallargtypes,p.proargtypes::oid[])) a(type_oid) join pg_type t on t.oid=a.type_oid join pg_class c on c.reltype=t.oid where n.nspname='public'), 'composite-signature dependency source is populated');
select ok(exists(select 1 from pg_default_acl d join pg_namespace n on n.oid=d.defaclnamespace where n.nspname='public'), 'public default privileges are inventoried');
select ok(exists(select 1 from pg_roles where rolname='service_role'), 'role-contract inventory can resolve service_role');

select * from finish();
rollback;
