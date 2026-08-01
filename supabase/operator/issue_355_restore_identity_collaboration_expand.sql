\set ON_ERROR_STOP on

begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

do $preflight$
begin
  if to_regclass('public.comments') is null
     or to_regclass('public.identity_center_users') is null
     or to_regclass('public.reviews') is null
     or to_regprocedure('public.review_scope_checksum_v1(jsonb)') is null
     or to_regprocedure('public.review_validate_scope_history_v1(uuid,jsonb)') is null then
    raise exception 'Issue #355 rollback source contract is incomplete';
  end if;
end
$preflight$;

drop view if exists api.identity_center_processed_events_v1;
drop view if exists api.identity_center_users_v1;
drop view if exists api.notifications_v1;
drop view if exists api.reviews_v1;
drop view if exists api.team_roles_v1;
drop view if exists api.teams_v1;
drop view if exists api.user_profiles_v1;

drop view if exists private.comments;
drop view if exists private.identity_center_processed_events;
drop view if exists private.identity_center_users;
drop view if exists private.notifications;
drop view if exists private.reviews;
drop view if exists private.roles;
drop view if exists private.teams;
drop view if exists private.users;

drop function if exists private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid);
drop function if exists private.review_revision_fingerprint_v1(text,jsonb);
drop function if exists private.review_scope_all_reference_ids_v1(jsonb);
drop function if exists private.review_scope_checksum_v1(jsonb);
drop function if exists private.review_scope_current_items_v1(jsonb);
drop function if exists private.review_scope_current_reference_ids_v1(jsonb);
drop function if exists private.review_scope_current_snapshot_v1(jsonb);
drop function if exists private.review_validate_scope_history_v1(uuid,jsonb);

commit;
