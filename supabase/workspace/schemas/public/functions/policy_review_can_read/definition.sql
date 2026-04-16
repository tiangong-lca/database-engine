CREATE OR REPLACE FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.reviews as r
    where r.id = p_review_id
      and coalesce(p_actor, auth.uid()) is not null
      and (
        public.cmd_review_is_review_admin(coalesce(p_actor, auth.uid()))
        or ((r.json -> 'user' ->> 'id')::uuid = coalesce(p_actor, auth.uid()))
        or (
          public.cmd_review_is_review_member(coalesce(p_actor, auth.uid()))
          and (
            coalesce(r.reviewer_id, '[]'::jsonb) ? coalesce(p_actor, auth.uid())::text
            or exists (
              select 1
              from public.comments as c
              where c.review_id = r.id
                and c.reviewer_id = coalesce(p_actor, auth.uid())
            )
          )
        )
      )
  )
$$;

ALTER FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "service_role";
