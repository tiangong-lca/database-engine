create or replace function public.qry_team_find_invitable_user_by_email(
  p_team_id uuid,
  p_email text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_email text := lower(btrim(coalesce(p_email, '')));
  v_user_id uuid;
  v_user_email text;
  v_user_meta jsonb := '{}'::jsonb;
  v_requested_team_role text;
  v_other_team_role text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null or v_email = '' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_PAYLOAD',
      'status', 400,
      'message', 'teamId and email are required'
    );
  end if;

  if p_team_id = '00000000-0000-0000-0000-000000000000'::uuid then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TEAM_SCOPE',
      'status', 400,
      'message', 'Use system or review member commands for the zero team scope'
    );
  end if;

  if not public.cmd_membership_is_team_manager(v_actor, p_team_id) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only team owners and admins can look up invitees'
    );
  end if;

  select
    au.id,
    coalesce(nullif(btrim(au.email), ''), nullif(btrim(u.raw_user_meta_data ->> 'email'), '')),
    coalesce(u.raw_user_meta_data, '{}'::jsonb)
  into v_user_id, v_user_email, v_user_meta
  from auth.users as au
  left join public.users as u
    on u.id = au.id
  where lower(btrim(coalesce(au.email, u.raw_user_meta_data ->> 'email', ''))) = v_email
  order by au.created_at desc, au.id
  limit 1;

  if v_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_NOT_FOUND',
      'status', 404,
      'message', 'No registered user was found for this email'
    );
  end if;

  select role
    into v_requested_team_role
  from public.roles
  where user_id = v_user_id
    and team_id = p_team_id;

  if v_requested_team_role = 'rejected' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REINVITE_REQUIRED',
      'status', 409,
      'message', 'Use the reinvite command for rejected members',
      'data', jsonb_build_object(
        'id', v_user_id,
        'user_id', v_user_id,
        'email', coalesce(v_user_email, v_email)
      )
    );
  end if;

  if v_requested_team_role is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_MEMBER_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team membership already exists',
      'data', jsonb_build_object(
        'id', v_user_id,
        'user_id', v_user_id,
        'email', coalesce(v_user_email, v_email)
      )
    );
  end if;

  select role
    into v_other_team_role
  from public.roles
  where user_id = v_user_id
    and team_id <> '00000000-0000-0000-0000-000000000000'::uuid
    and team_id <> p_team_id
    and role <> 'rejected'
  order by
    case when role = 'is_invited' then 0 else 1 end,
    modified_at desc nulls last,
    created_at desc nulls last
  limit 1;

  if v_other_team_role = 'is_invited' then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ALREADY_INVITED_TO_TEAM',
      'status', 409,
      'message', 'This user already has a pending invitation to another team'
    );
  end if;

  if v_other_team_role is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ALREADY_IN_TEAM',
      'status', 409,
      'message', 'This user already belongs to another team'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'id', v_user_id,
      'user_id', v_user_id,
      'email', coalesce(v_user_email, v_email),
      'display_name', coalesce(
        nullif(btrim(v_user_meta ->> 'display_name'), ''),
        nullif(btrim(v_user_meta ->> 'name'), ''),
        nullif(btrim(v_user_email), ''),
        v_email
      )
    )
  );
end;
$$;

alter function public.qry_team_find_invitable_user_by_email(uuid, text) owner to postgres;

revoke all on function public.qry_team_find_invitable_user_by_email(uuid, text) from public;

grant execute on function public.qry_team_find_invitable_user_by_email(uuid, text) to authenticated;
grant execute on function public.qry_team_find_invitable_user_by_email(uuid, text) to service_role;
