CREATE OR REPLACE FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean DEFAULT false) RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  case lower(coalesce(p_sort_by, ''))
    when 'created_at' then
      return 'q.created_at';
    when 'createat' then
      return 'q.created_at';
    when 'deadline' then
      return 'q.deadline';
    when 'state_code' then
      return 'q.state_code';
    when 'statecode' then
      return 'q.state_code';
    when 'comment_modified_at' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    when 'commentmodifiedat' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    else
      return 'q.modified_at';
  end case;

  return 'q.modified_at';
end;
$$;

ALTER FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "service_role";
