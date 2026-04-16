CREATE OR REPLACE FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$$;

ALTER FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "service_role";
