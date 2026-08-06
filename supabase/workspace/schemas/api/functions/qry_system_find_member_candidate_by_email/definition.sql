CREATE OR REPLACE FUNCTION "api"."qry_system_find_member_candidate_by_email"("p_email" "text") RETURNS TABLE("id" "uuid", "email" "text", "display_name" "text")
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
    select 1 from private.roles
    where user_id = v_actor
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin', 'member')
  ) then
    raise exception using errcode = '42501', message = 'SYSTEM_MANAGER_REQUIRED';
  end if;

  return query
  select
    profile.id,
    coalesce(nullif(btrim(auth_user.email), ''), nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    )
  from private.users as profile
  left join auth.users as auth_user on auth_user.id = profile.id
  where lower(btrim(coalesce(auth_user.email, profile.raw_user_meta_data ->> 'email', ''))) = v_email
  order by auth_user.created_at desc nulls last, profile.id
  limit 1;
end
$$;

ALTER FUNCTION "api"."qry_system_find_member_candidate_by_email"("p_email" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_system_find_member_candidate_by_email"("p_email" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_system_find_member_candidate_by_email"("p_email" "text") TO "authenticated";
