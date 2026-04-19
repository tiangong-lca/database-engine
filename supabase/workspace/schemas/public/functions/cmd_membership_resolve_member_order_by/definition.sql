CREATE OR REPLACE FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean DEFAULT false) RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  case lower(coalesce(p_sort_by, ''))
    when 'role' then
      return 'm.role';
    when 'email' then
      return 'm.email';
    when 'display_name' then
      return 'm.display_name';
    when 'modified_at' then
      return 'm.modified_at';
    when 'pendingcount' then
      if p_allow_workload then
        return 'm.pending_count';
      end if;
    when 'pending_count' then
      if p_allow_workload then
        return 'm.pending_count';
      end if;
    when 'reviewedcount' then
      if p_allow_workload then
        return 'm.reviewed_count';
      end if;
    when 'reviewed_count' then
      if p_allow_workload then
        return 'm.reviewed_count';
      end if;
    else
      return 'm.created_at';
  end case;

  return 'm.created_at';
end;
$$;

ALTER FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "service_role";
