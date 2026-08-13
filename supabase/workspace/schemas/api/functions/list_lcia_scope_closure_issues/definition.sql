CREATE OR REPLACE FUNCTION "api"."list_lcia_scope_closure_issues"("p_closure_check_id" "uuid", "p_after_issue_id" "uuid" DEFAULT NULL::"uuid", "p_limit" integer DEFAULT 100) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 200));
begin
  if v_actor is null then
    return api.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not api.lcia_scope_closure_is_manager()
     or not exists (
       select 1
       from private.lcia_scope_closure_checks c
       where c.id = p_closure_check_id
         and c.requested_by = v_actor
     ) then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;

  return jsonb_build_object('ok', true, 'data', (
    with page as (
      select i.*
      from private.lcia_scope_closure_issues i
      where i.closure_check_id = p_closure_check_id
        and (p_after_issue_id is null or i.id > p_after_issue_id)
      order by i.id
      limit v_limit + 1
    ), shown as (
      select * from page order by id limit v_limit
    )
    select jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-issues-page.v1',
      'closureCheckId', p_closure_check_id,
      'issues', coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'issueId', id,
        'severity', severity,
        'blocking', blocking,
        'code', issue_code,
        'title', issue_code,
        'summary', message,
        'suggestedAction', suggested_action,
        'occurrenceCount', occurrence_count,
        'affectedRootCount', affected_root_count
      )) order by id), '[]'::jsonb),
      'totalCount', (
        select count(*)
        from private.lcia_scope_closure_issues
        where closure_check_id = p_closure_check_id
      ),
      'nextCursor', case when exists (select 1 from page offset v_limit)
        then (select id from shown order by id desc limit 1)
        else null
      end
    )
    from shown
  ));
end;
$$;

ALTER FUNCTION "api"."list_lcia_scope_closure_issues"("p_closure_check_id" "uuid", "p_after_issue_id" "uuid", "p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."list_lcia_scope_closure_issues"("p_closure_check_id" "uuid", "p_after_issue_id" "uuid", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."list_lcia_scope_closure_issues"("p_closure_check_id" "uuid", "p_after_issue_id" "uuid", "p_limit" integer) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."list_lcia_scope_closure_issues"("p_closure_check_id" "uuid", "p_after_issue_id" "uuid", "p_limit" integer) TO "authenticated";
