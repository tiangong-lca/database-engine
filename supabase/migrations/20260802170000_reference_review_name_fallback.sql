-- Preserve the immutable review name snapshot when it is present. Reference
-- Reviews created before the Common-schema name normalization stored an empty
-- data.name object, so resolve only those legacy gaps from the exact dataset
-- id and version that the Review already targets.

-- The public RPC must bypass the caller's reviews RLS only long enough to
-- project its deliberately narrow child-row DTO.  Do not run that boundary as
-- the database owner: a dedicated non-login role receives read-only access to
-- reviews/comments plus only the helper functions used below.
do $review_progress_executor$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'review_progress_executor'
  ) then
    create role review_progress_executor
      nologin noinherit nosuperuser nocreatedb nocreaterole nobypassrls;
  end if;
end
$review_progress_executor$;

do $review_progress_executor_contract$
declare
  v_role pg_catalog.pg_roles%rowtype;
begin
  select *
  into strict v_role
  from pg_catalog.pg_roles
  where rolname = 'review_progress_executor';

  if v_role.rolcanlogin
     or v_role.rolinherit
     or v_role.rolsuper
     or v_role.rolcreatedb
     or v_role.rolcreaterole
     or v_role.rolreplication
     or v_role.rolbypassrls then
    raise exception 'review_progress_executor has unsafe role attributes';
  end if;
end
$review_progress_executor_contract$;
grant review_progress_executor to postgres;

grant usage on schema public to review_progress_executor;
grant select on public.reviews, public.comments to review_progress_executor;
grant execute on function public.cmd_review_is_review_admin(pg_catalog.uuid)
  to review_progress_executor;
grant execute on function public.cmd_review_is_review_member(pg_catalog.uuid)
  to review_progress_executor;
grant execute on function public.policy_review_can_read(
  pg_catalog.uuid, pg_catalog.uuid
) to review_progress_executor;
grant execute on function public.cmd_review_get_dataset_row(
  pg_catalog.text, pg_catalog.uuid, pg_catalog.text, pg_catalog.bool
) to review_progress_executor;
grant execute on function public.cmd_review_get_dataset_name(
  pg_catalog.text, pg_catalog.jsonb
) to review_progress_executor;

drop policy if exists review_progress_executor_select on public.reviews;
create policy review_progress_executor_select
on public.reviews
for select
to review_progress_executor
using (true);

drop policy if exists review_progress_executor_select on public.comments;
create policy review_progress_executor_select
on public.comments
for select
to review_progress_executor
using (true);

create or replace function public.qry_root_review_reference_progress_v2(
  p_root_review_id pg_catalog.uuid
)
returns table (
  reference_review_id pg_catalog.uuid,
  target_table pg_catalog.text,
  data_id pg_catalog.uuid,
  data_version pg_catalog.text,
  data_name pg_catalog.jsonb,
  submitted_revision_checksum pg_catalog.text,
  state_code pg_catalog.int4,
  reviewer_count pg_catalog.int4,
  completed_reviewer_count pg_catalog.int4,
  actor_comment_state_code pg_catalog.int4,
  actor_comment_modified_at pg_catalog.timestamptz
)
language plpgsql
stable
security definer
set search_path = pg_catalog, pg_temp
as $$
declare
  v_actor pg_catalog.uuid := coalesce(
    nullif(
      pg_catalog.current_setting('request.jwt.claim.sub', true),
      ''
    ),
    nullif(
      pg_catalog.current_setting('request.jwt.claims', true),
      ''
    )::pg_catalog.jsonb ->> 'sub'
  )::pg_catalog.uuid;
  v_is_admin pg_catalog.bool;
  v_is_member pg_catalog.bool;
begin
  v_is_admin := v_actor is not null
    and public.cmd_review_is_review_admin(v_actor);
  v_is_member := v_actor is not null
    and public.cmd_review_is_review_member(v_actor);

  if not v_is_admin and not v_is_member then
    raise exception using errcode = '42501', message = 'REVIEW_ROLE_REQUIRED';
  end if;

  return query
  select
    reference_review.id,
    reference_review.target_table,
    reference_review.data_id,
    pg_catalog.btrim(reference_review.data_version::text),
    case
      when coalesce(
        reference_review.json #> '{data,name}',
        '{}'::jsonb
      ) <> '{}'::jsonb then reference_review.json #> '{data,name}'
      else public.cmd_review_get_dataset_name(
        reference_review.target_table,
        public.cmd_review_get_dataset_row(
          reference_review.target_table,
          reference_review.data_id,
          reference_review.data_version,
          false
        )
      )
    end,
    reference_review.submitted_revision_checksum,
    reference_review.state_code,
    pg_catalog.jsonb_array_length(
      coalesce(reference_review.reviewer_id, '[]'::jsonb)
    )::integer,
    (
      select pg_catalog.count(*)::integer
      from public.comments as completed_comment
      where completed_comment.review_id = reference_review.id
        and completed_comment.state_code in (1, -3, 2)
    ),
    actor_comment.state_code,
    actor_comment.modified_at
  from public.reviews as root_review
  cross join lateral pg_catalog.unnest(
    coalesce(root_review.current_reference_review_ids, '{}'::uuid[])
  ) with ordinality as current_reference(reference_review_id, ordinal_position)
  join public.reviews as reference_review
    on reference_review.id = current_reference.reference_review_id
    and reference_review.review_kind = 'reference'
  left join lateral (
    select comment_row.state_code, comment_row.modified_at
    from public.comments as comment_row
    where comment_row.review_id = reference_review.id
      and comment_row.reviewer_id = v_actor
    order by comment_row.modified_at desc, comment_row.created_at desc
    limit 1
  ) as actor_comment on true
  where root_review.id = p_root_review_id
    and root_review.review_kind = 'root'
    and (
      v_is_admin
      or (
        v_is_member
        and public.policy_review_can_read(reference_review.id, v_actor)
        and actor_comment.state_code is not null
        and actor_comment.state_code <> -2
      )
    )
  order by
    current_reference.ordinal_position,
    reference_review.target_table,
    reference_review.id;
end;
$$;

revoke all on function public.qry_root_review_reference_progress_v2(
  pg_catalog.uuid
) from public, anon;
grant execute on function public.qry_root_review_reference_progress_v2(
  pg_catalog.uuid
)
  to authenticated, service_role;

comment on function public.qry_root_review_reference_progress_v2(
  pg_catalog.uuid
) is
  'Current Reference Review child rows for Review Management; runs as the non-login read-only review_progress_executor, preserves stored names, and resolves legacy empty names from the exact dataset identity without exposing relation paths or aggregate overview fields.';

-- PostgreSQL requires the creating CREATEROLE principal to administer the
-- target role and temporarily grants CREATE-on-schema for the ownership
-- transfer.  The administrator cannot inherit or SET ROLE to the executor;
-- the schema privilege is removed immediately after the transfer.
grant create on schema public to review_progress_executor;
alter function public.qry_root_review_reference_progress_v2(pg_catalog.uuid)
  owner to review_progress_executor;
revoke create on schema public from review_progress_executor;
revoke review_progress_executor from postgres granted by current_user;
