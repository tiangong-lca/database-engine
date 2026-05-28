-- Normalize RLS policies flagged by Supabase Performance Advisor.
--
-- This migration preserves the existing authorization semantics while:
-- - wrapping row-independent auth.uid() calls as (select auth.uid())
-- - merging duplicate permissive comments UPDATE policies
-- - removing production-only teams helper-function drift in favor of the
--   checked-in inline policy definitions.

drop policy if exists "comments select by review participants" on public.comments;
create policy "comments select by review participants"
on public.comments
for select
to authenticated
using (
  (select auth.uid()) is not null
  and public.policy_review_can_read(review_id, (select auth.uid()))
  and (
    public.cmd_review_is_review_admin((select auth.uid()))
    or exists (
      select 1
      from public.reviews r
      where r.id = comments.review_id
        and (((r.json -> 'user'::text) ->> 'id'::text)::uuid = (select auth.uid()))
    )
    or reviewer_id = (select auth.uid())
  )
);

drop policy if exists "comments update by review-admin" on public.comments;
drop policy if exists "comments update by reviewer self" on public.comments;
drop policy if exists "comments update by reviewer or review-admin" on public.comments;
create policy "comments update by reviewer or review-admin"
on public.comments
for update
to authenticated
using (
  public.policy_is_current_user_in_roles(
    '00000000-0000-0000-0000-000000000000'::uuid,
    array['review-admin'::text]
  )
  or reviewer_id = (select auth.uid())
)
with check (
  public.policy_is_current_user_in_roles(
    '00000000-0000-0000-0000-000000000000'::uuid,
    array['review-admin'::text]
  )
  or reviewer_id = (select auth.uid())
);

do $$
declare
  dataset_table text;
begin
  foreach dataset_table in array array[
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ]
  loop
    execute format(
      'drop policy if exists transitional_update_owner_draft_only on public.%I',
      dataset_table
    );
    execute format(
      $policy$
      create policy transitional_update_owner_draft_only
      on public.%I
      for update
      to authenticated
      using ((state_code = 0) and (user_id = (select auth.uid())))
      with check ((state_code = 0) and (user_id = (select auth.uid())))
      $policy$,
      dataset_table
    );
  end loop;
end
$$;

drop policy if exists lca_package_artifacts_select_own on public.lca_package_artifacts;
create policy lca_package_artifacts_select_own
on public.lca_package_artifacts
for select
to authenticated
using (
  exists (
    select 1
    from public.lca_package_jobs j
    where j.id = lca_package_artifacts.job_id
      and j.requested_by = (select auth.uid())
  )
);

drop policy if exists lca_package_jobs_select_own on public.lca_package_jobs;
create policy lca_package_jobs_select_own
on public.lca_package_jobs
for select
to authenticated
using (requested_by = (select auth.uid()));

drop policy if exists lca_package_request_cache_select_own on public.lca_package_request_cache;
create policy lca_package_request_cache_select_own
on public.lca_package_request_cache
for select
to authenticated
using (requested_by = (select auth.uid()));

drop policy if exists notifications_insert_sender on public.notifications;
create policy notifications_insert_sender
on public.notifications
for insert
to authenticated
with check ((select auth.uid()) = sender_user_id);

drop policy if exists notifications_select_sender_or_recipient on public.notifications;
create policy notifications_select_sender_or_recipient
on public.notifications
for select
to authenticated
using (
  (select auth.uid()) = sender_user_id
  or (select auth.uid()) = recipient_user_id
);

drop policy if exists notifications_update_sender on public.notifications;
create policy notifications_update_sender
on public.notifications
for update
to authenticated
using ((select auth.uid()) = sender_user_id)
with check ((select auth.uid()) = sender_user_id);

drop policy if exists notifications_delete_recipient_only on public.notifications;
create policy notifications_delete_recipient_only
on public.notifications
for delete
to authenticated
using ((select auth.uid()) = recipient_user_id);

drop policy if exists "reviews select by review participants" on public.reviews;
create policy "reviews select by review participants"
on public.reviews
for select
to authenticated
using (
  (select auth.uid()) is not null
  and public.policy_review_can_read(id, (select auth.uid()))
);

drop policy if exists transitional_reviews_update_submitter_only on public.reviews;
create policy transitional_reviews_update_submitter_only
on public.reviews
for update
to authenticated
using (
  (select auth.uid()) is not null
  and (((json -> 'user'::text) ->> 'id'::text)::uuid = (select auth.uid()))
)
with check (
  (select auth.uid()) is not null
  and (((json -> 'user'::text) ->> 'id'::text)::uuid = (select auth.uid()))
);

drop policy if exists "insert by authenticated" on public.teams;
drop policy if exists "select by owner or public teams" on public.teams;
drop policy if exists "update by owner and admin" on public.teams;

create policy "insert by authenticated"
on public.teams
for insert
to authenticated
with check (
  (
    select count(1)
    from public.roles
    where roles.user_id = (select auth.uid())
      and roles.role <> 'rejected'::text
      and roles.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
  ) = 0
);

create policy "select by owner or public teams"
on public.teams
for select
to authenticated
using (
  is_public
  or rank > 0
  or exists (
    select 1
    from public.roles
    where (roles.team_id = teams.id or roles.team_id = '00000000-0000-0000-0000-000000000000'::uuid)
      and roles.user_id = (select auth.uid())
      and roles.role <> 'rejected'::text
  )
);

create policy "update by owner and admin"
on public.teams
for update
to authenticated
using (
  exists (
    select 1
    from public.roles r
    where r.user_id = (select auth.uid())
      and r.team_id = teams.id
      and r.role::text = any (array['owner'::text, 'admin'::text])
  )
)
with check (
  exists (
    select 1
    from public.roles r
    where r.user_id = (select auth.uid())
      and r.team_id = teams.id
      and r.role::text = any (array['owner'::text, 'admin'::text])
  )
);

drop function if exists public.policy_teams_insert();
drop function if exists public.policy_teams_select(uuid, integer, boolean);
drop function if exists public.policy_teams_update(uuid);
