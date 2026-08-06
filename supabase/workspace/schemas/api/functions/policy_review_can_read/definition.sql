CREATE OR REPLACE FUNCTION "api"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select exists (
    select 1
    from private.reviews as r
    where r.id = p_review_id
      and coalesce(p_actor, auth.uid()) is not null
      and (
        api.cmd_review_is_review_admin(coalesce(p_actor, auth.uid()))
        or ((r.json -> 'user' ->> 'id')::uuid = coalesce(p_actor, auth.uid()))
        or (
          api.cmd_review_is_review_member(coalesce(p_actor, auth.uid()))
          and (
            coalesce(r.reviewer_id, '[]'::jsonb) ? coalesce(p_actor, auth.uid())::text
            or exists (
              select 1
              from private.comments as c
              where c.review_id = r.id
                and c.reviewer_id = coalesce(p_actor, auth.uid())
            )
          )
        )
      )
  )
$$;

ALTER FUNCTION "api"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "api_internal_executor";
