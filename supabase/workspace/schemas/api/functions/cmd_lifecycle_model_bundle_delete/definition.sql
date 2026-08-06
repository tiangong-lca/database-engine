CREATE OR REPLACE FUNCTION "api"."cmd_lifecycle_model_bundle_delete"("p_model_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_owner uuid;
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  select model.user_id
  into v_owner
  from public.lifecyclemodels as model
  where model.id = p_model_id
    and model.version = p_version
  for update;

  if v_owner is null then
    raise exception using errcode = 'P0002', message = 'MODEL_NOT_FOUND';
  end if;

  if v_owner <> v_actor and not exists (
    select 1
    from private.roles as membership
    where membership.user_id = v_actor
      and membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and membership.role = 'review-admin'
  ) then
    raise exception using errcode = '42501', message = 'FORBIDDEN';
  end if;

  return private.delete_lifecycle_model_bundle(p_model_id, p_version);
end
$$;

ALTER FUNCTION "api"."cmd_lifecycle_model_bundle_delete"("p_model_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lifecycle_model_bundle_delete"("p_model_id" "uuid", "p_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lifecycle_model_bundle_delete"("p_model_id" "uuid", "p_version" "text") TO "authenticated";
