CREATE OR REPLACE FUNCTION "api"."qry_identity_get_visible_users"("p_user_ids" "uuid"[]) RETURNS TABLE("id" "uuid", "email" "text", "display_name" "text")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_requested_count integer := coalesce(cardinality(p_user_ids), 0);
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  if v_requested_count > 100 then
    raise exception using errcode = '22023', message = 'TOO_MANY_USER_IDS';
  end if;

  return query
  with requested as (
    select distinct requested_id
    from unnest(coalesce(p_user_ids, '{}'::uuid[])) as requested_id
    where requested_id is not null
  ), visible as (
    select requested.requested_id
    from requested
    where requested.requested_id = v_actor
      or exists (
        select 1
        from private.roles as actor_membership
        join private.roles as target_membership
          on target_membership.team_id = actor_membership.team_id
        where actor_membership.user_id = v_actor
          and target_membership.user_id = requested.requested_id
          and actor_membership.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
          and actor_membership.role not in ('rejected', 'is_invited')
          and target_membership.role not in ('rejected', 'is_invited')
      )
      or exists (
        select 1
        from private.reviews as review
        where api.policy_review_can_read(review.id, v_actor)
          and (
            review.target_owner_id = requested.requested_id
            or review.reviewer_id ? requested.requested_id::text
            or review.json -> 'user' ->> 'id' = requested.requested_id::text
          )
      )
      or exists (
        select 1
        from private.teams as team
        join private.roles as owner_membership
          on owner_membership.team_id = team.id
         and owner_membership.role = 'owner'
        where coalesce(team.is_public, false)
          and owner_membership.user_id = requested.requested_id
      )
  )
  select
    profile.id,
    coalesce(
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    )
  from visible
  join private.users as profile on profile.id = visible.requested_id
  left join auth.users as auth_user on auth_user.id = profile.id
  order by profile.id;
end
$$;

ALTER FUNCTION "api"."qry_identity_get_visible_users"("p_user_ids" "uuid"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_identity_get_visible_users"("p_user_ids" "uuid"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_identity_get_visible_users"("p_user_ids" "uuid"[]) TO "authenticated";
