CREATE OR REPLACE FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text" DEFAULT 'auto'::"text") RETURNS TABLE("review_id" "uuid", "reviewer_id" "uuid", "state_code" integer, "json" "jsonb", "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with actor as (
    select
      auth.uid() as actor_id,
      public.cmd_review_is_review_admin(auth.uid()) as is_review_admin,
      exists (
        select 1
        from public.reviews as r
        where r.id = p_review_id
          and ((r.json -> 'user' ->> 'id')::uuid = auth.uid())
      ) as is_owner
  )
  select
    c.review_id,
    c.reviewer_id,
    c.state_code,
    coalesce(c.json::jsonb, '{}'::jsonb) as json,
    c.created_at,
    c.modified_at
  from public.comments as c
  cross join actor as a
  where c.review_id = p_review_id
    and public.policy_review_can_read(p_review_id, a.actor_id)
    and (
      a.is_review_admin
      or a.is_owner
      or c.reviewer_id = a.actor_id
    )
    and (
      lower(coalesce(p_scope, 'auto')) not in ('mine', 'self')
      or c.reviewer_id = a.actor_id
    )
  order by c.created_at asc, c.reviewer_id asc
$$;

ALTER FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "service_role";
