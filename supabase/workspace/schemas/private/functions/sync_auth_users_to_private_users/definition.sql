CREATE OR REPLACE FUNCTION "private"."sync_auth_users_to_private_users"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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
$$;

ALTER FUNCTION "private"."sync_auth_users_to_private_users"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."sync_auth_users_to_private_users"() FROM PUBLIC;
