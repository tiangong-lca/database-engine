begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(7);

with affected(table_name, policy_name) as (
  values
    ('comments', 'comments select by review participants'),
    ('comments', 'comments update by reviewer or review-admin'),
    ('contacts', 'transitional_update_owner_draft_only'),
    ('sources', 'transitional_update_owner_draft_only'),
    ('unitgroups', 'transitional_update_owner_draft_only'),
    ('flowproperties', 'transitional_update_owner_draft_only'),
    ('flows', 'transitional_update_owner_draft_only'),
    ('processes', 'transitional_update_owner_draft_only'),
    ('lifecyclemodels', 'transitional_update_owner_draft_only'),
    ('lca_package_artifacts', 'lca_package_artifacts_select_own'),
    ('lca_package_request_cache', 'lca_package_request_cache_select_own'),
    ('notifications', 'notifications_insert_sender'),
    ('notifications', 'notifications_select_sender_or_recipient'),
    ('notifications', 'notifications_update_sender'),
    ('notifications', 'notifications_delete_recipient_only'),
    ('reviews', 'reviews select by review participants'),
    ('reviews', 'transitional_reviews_update_submitter_only'),
    ('teams', 'update by owner and admin')
)
select is(
  (
    select count(*)::integer
    from affected a
    join pg_class c on c.relname = a.table_name
    join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'public'
    join pg_policy p on p.polrelid = c.oid and p.polname = a.policy_name
  ),
  18,
  'all advisor-targeted RLS policies exist after rewrite'
);

with affected(table_name, policy_name) as (
  values
    ('comments', 'comments select by review participants'),
    ('comments', 'comments update by reviewer or review-admin'),
    ('contacts', 'transitional_update_owner_draft_only'),
    ('sources', 'transitional_update_owner_draft_only'),
    ('unitgroups', 'transitional_update_owner_draft_only'),
    ('flowproperties', 'transitional_update_owner_draft_only'),
    ('flows', 'transitional_update_owner_draft_only'),
    ('processes', 'transitional_update_owner_draft_only'),
    ('lifecyclemodels', 'transitional_update_owner_draft_only'),
    ('lca_package_artifacts', 'lca_package_artifacts_select_own'),
    ('lca_package_request_cache', 'lca_package_request_cache_select_own'),
    ('notifications', 'notifications_insert_sender'),
    ('notifications', 'notifications_select_sender_or_recipient'),
    ('notifications', 'notifications_update_sender'),
    ('notifications', 'notifications_delete_recipient_only'),
    ('reviews', 'reviews select by review participants'),
    ('reviews', 'transitional_reviews_update_submitter_only'),
    ('teams', 'update by owner and admin')
),
policy_exprs as (
  select
    replace(
      coalesce(pg_get_expr(p.polqual, p.polrelid), '')
      || ' '
      || coalesce(pg_get_expr(p.polwithcheck, p.polrelid), ''),
      '( SELECT auth.uid() AS uid)',
      ''
    ) as stripped_expr
  from affected a
  join pg_class c on c.relname = a.table_name
  join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'public'
  join pg_policy p on p.polrelid = c.oid and p.polname = a.policy_name
)
select is(
  (
    select count(*)::integer
    from policy_exprs
    where stripped_expr like '%auth.uid()%'
  ),
  0,
  'advisor-targeted policies wrap auth.uid() as select initplans'
);

select is(
  (
    select count(*)::integer
    from pg_policy p
    join pg_class c on c.oid = p.polrelid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_roles r on r.oid = any (p.polroles)
    where n.nspname = 'public'
      and c.relname = 'comments'
      and p.polpermissive
      and p.polcmd = 'w'
      and r.rolname = 'authenticated'
  ),
  1,
  'comments has one permissive authenticated UPDATE policy'
);

select is(
  (
    select count(*)::integer
    from pg_policy p
    join pg_class c on c.oid = p.polrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname = 'comments'
      and p.polname in ('comments update by review-admin', 'comments update by reviewer self')
  ),
  0,
  'old split comments UPDATE policies are removed'
);

select is(
  (
    select count(*)::integer
    from pg_policy p
    join pg_class c on c.oid = p.polrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname = 'teams'
      and p.polname in ('insert by authenticated', 'select by owner or public teams', 'update by owner and admin')
  ),
  3,
  'teams keeps the three migration-managed policies'
);

select is(
  (
    select count(*)::integer
    from pg_policy p
    join pg_class c on c.oid = p.polrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname = 'teams'
      and (
        coalesce(pg_get_expr(p.polqual, p.polrelid), '')
        || ' '
        || coalesce(pg_get_expr(p.polwithcheck, p.polrelid), '')
      ) like '%policy_teams_%'
  ),
  0,
  'teams policies do not reference production-only policy_teams helper drift'
);

select ok(
  to_regprocedure('public.policy_teams_insert()') is null
  and to_regprocedure('public.policy_teams_select(uuid,integer,boolean)') is null
  and to_regprocedure('public.policy_teams_update(uuid)') is null,
  'production-only policy_teams helper functions are absent'
);

select * from finish();

rollback;
