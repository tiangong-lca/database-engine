-- Keep the application profile mirror aligned with Supabase Auth without
-- making registered-user lookup depend on the mirror being complete.

alter function private.sync_auth_users_to_public_users()
  rename to sync_auth_users_to_private_users;

create or replace function private.sync_auth_users_to_private_users()
returns trigger
language plpgsql
security definer
set search_path = ''
as $function$
begin
  if tg_op = 'DELETE' then
    delete from private.users
    where id = old.id;

    return old;
  end if;

  if tg_op = 'UPDATE'
     and new.raw_user_meta_data is not distinct from old.raw_user_meta_data then
    return new;
  end if;

  insert into private.users as profile (
    id,
    raw_user_meta_data
  )
  values (
    new.id,
    new.raw_user_meta_data
  )
  on conflict (id) do update
  set raw_user_meta_data = excluded.raw_user_meta_data
  where profile.raw_user_meta_data is distinct from excluded.raw_user_meta_data;

  return new;
end;
$function$;
alter function private.sync_auth_users_to_private_users() owner to postgres;

revoke all on function private.sync_auth_users_to_private_users()
  from public, anon, authenticated, service_role, api_internal_executor;

-- A hosted project may still have an out-of-band trigger attached to the
-- preserved function OID. Remove only triggers that invoke this exact helper
-- before installing the canonical governed trigger.
do $migration$
declare
  v_trigger_name name;
begin
  for v_trigger_name in
    select trigger_record.tgname
    from pg_trigger as trigger_record
    where trigger_record.tgrelid = 'auth.users'::regclass
      and trigger_record.tgfoid =
        'private.sync_auth_users_to_private_users()'::regprocedure
      and not trigger_record.tgisinternal
  loop
    execute format(
      'drop trigger %I on auth.users',
      v_trigger_name
    );
  end loop;
end;
$migration$;

-- Deferral keeps explicit same-transaction profile writes compatible. The
-- mirror is still guaranteed before the Auth transaction commits.
create constraint trigger trg_sync_auth_users_to_private_users
after insert or update or delete
on auth.users
deferrable initially deferred
for each row
execute function private.sync_auth_users_to_private_users();

-- Backfill and reconcile historical Auth identities while preserving
-- application-owned fields such as private.users.contact.
insert into private.users as profile (
  id,
  raw_user_meta_data
)
select
  auth_user.id,
  auth_user.raw_user_meta_data
from auth.users as auth_user
on conflict (id) do update
set raw_user_meta_data = excluded.raw_user_meta_data
where profile.raw_user_meta_data is distinct from excluded.raw_user_meta_data;

create or replace function api.qry_review_find_member_candidate_by_email(
  p_email text
)
returns table (
  id uuid,
  email text,
  display_name text,
  contact jsonb
)
language plpgsql
stable
security definer
set search_path = ''
as $function$
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
$function$;
