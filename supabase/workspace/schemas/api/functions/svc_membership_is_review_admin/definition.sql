CREATE OR REPLACE FUNCTION "api"."svc_membership_is_review_admin"("p_user_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'ok', true,
    'data', exists (
      select 1
      from private.roles as membership
      where membership.user_id = p_user_id
        and membership.role = 'review-admin'
    )
  )
$$;

ALTER FUNCTION "api"."svc_membership_is_review_admin"("p_user_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_membership_is_review_admin"("p_user_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_membership_is_review_admin"("p_user_id" "uuid") TO "service_role";
