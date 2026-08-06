CREATE OR REPLACE FUNCTION "api"."cmd_lifecycle_model_bundle_save"("p_plan" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_plan jsonb := coalesce(p_plan, '{}'::jsonb);
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;
  v_plan := jsonb_set(v_plan, '{actorUserId}', to_jsonb(v_actor::text), true);
  return private.save_lifecycle_model_bundle(v_plan);
end
$$;

ALTER FUNCTION "api"."cmd_lifecycle_model_bundle_save"("p_plan" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lifecycle_model_bundle_save"("p_plan" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lifecycle_model_bundle_save"("p_plan" "jsonb") TO "authenticated";
