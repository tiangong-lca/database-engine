CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_artifact_lifecycle_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_old_expected_role text := case
    when tg_op = 'UPDATE'
    then public.lcia_scope_closure_artifact_role(old.artifact_type)
  end;
  v_new_expected_role text :=
    public.lcia_scope_closure_artifact_role(new.artifact_type);
begin
  if tg_op = 'INSERT'
     and v_new_expected_role is null
     and new.artifact_role is null then
    return new;
  end if;
  if tg_op = 'UPDATE'
     and v_old_expected_role is null
     and v_new_expected_role is null
     and old.artifact_role is null
     and new.artifact_role is null then
    return new;
  end if;
  if tg_op = 'INSERT' then
    new.artifact_role := coalesce(new.artifact_role, v_new_expected_role);
    new.expires_at := coalesce(
      new.expires_at,
      coalesce(new.created_at, now()) + interval '7 days'
    );
    new.lifecycle_state := coalesce(
      new.lifecycle_state,
      case when new.expires_at <= now() then 'expired' else 'ready' end
    );
  else
    if new.artifact_type is distinct from old.artifact_type
       or new.artifact_role is distinct from old.artifact_role
       or new.created_at is distinct from old.created_at
       or new.expires_at is distinct from old.expires_at then
      raise exception 'scope_closure_artifact_identity_or_expiry_is_immutable'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'deleted'
       and new.lifecycle_state is distinct from 'deleted' then
      raise exception 'scope_closure_artifact_deleted_is_terminal'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'expired'
       and new.lifecycle_state not in ('expired', 'deleted') then
      raise exception 'scope_closure_artifact_cannot_return_to_ready'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'ready'
       and new.lifecycle_state not in ('ready', 'expired') then
      raise exception 'scope_closure_artifact_invalid_transition'
        using errcode = '23514';
    end if;
  end if;
  if v_new_expected_role is null
     or new.artifact_role is distinct from v_new_expected_role
     or new.expires_at is null
     or new.expires_at > coalesce(new.created_at, now()) + interval '7 days'
     or new.lifecycle_state is null then
    raise exception 'invalid_scope_closure_artifact_lifecycle'
      using errcode = '23514';
  end if;
  if new.lifecycle_state = 'ready'
     and (
       nullif(trim(new.storage_bucket), '') is null
       or nullif(trim(new.storage_path), '') is null
       or new.content_type is null
       or new.byte_size is null
       or new.checksum_sha256 is null
     ) then
    raise exception 'scope_closure_artifact_ready_metadata_incomplete'
      using errcode = '23514';
  end if;
  if new.lifecycle_state = 'deleted' then
    new.deleted_at := coalesce(new.deleted_at, now());
    new.storage_bucket := null;
    new.storage_path := null;
  end if;
  return new;
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_artifact_lifecycle_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_artifact_lifecycle_guard"() FROM PUBLIC;
