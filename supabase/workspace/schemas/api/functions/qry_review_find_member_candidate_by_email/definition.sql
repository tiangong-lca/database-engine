CREATE OR REPLACE FUNCTION "api"."qry_review_find_member_candidate_by_email"("p_email" "text") RETURNS TABLE("id" "uuid", "email" "text", "display_name" "text", "contact" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_email text := lower(btrim(coalesce(p_email, '')));
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  if v_email = '' or length(v_email) > 320 then
    raise exception using errcode = '22023', message = 'INVALID_EMAIL';
  end if;

  if not exists (
    select 1
    from private.roles
    where user_id = v_actor
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  ) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    auth_user.id,
    coalesce(
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(auth_user.raw_user_meta_data ->> 'email'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(auth_user.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(auth_user.raw_user_meta_data ->> 'email'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    profile.contact
  from auth.users as auth_user
  left join private.users as profile on profile.id = auth_user.id
  where lower(coalesce(
    nullif(btrim(auth_user.email), ''),
    nullif(btrim(auth_user.raw_user_meta_data ->> 'email'), ''),
    nullif(btrim(profile.raw_user_meta_data ->> 'email'), ''),
    ''
  )) = v_email
  order by auth_user.created_at desc nulls last, auth_user.id
  limit 1;
end;
$$;

ALTER FUNCTION "api"."qry_review_find_member_candidate_by_email"("p_email" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_find_member_candidate_by_email"("p_email" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_find_member_candidate_by_email"("p_email" "text") TO "authenticated";
