CREATE OR REPLACE FUNCTION "private"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
    v_rows_affected integer;
begin
    if p_model_id is null or nullif(btrim(coalesce(p_version, '')), '') is null then
        raise exception 'INVALID_PLAN';
    end if;

    perform 1
      from public.lifecyclemodels
     where id = p_model_id
       and version = p_version
     for update;

    if not found then
        raise exception 'MODEL_NOT_FOUND';
    end if;

    -- The relational ownership columns define bundle membership. json_tg is
    -- frontend state and may be absent, stale, or malformed.
    execute 'del' || 'ete from processes where model_id = $1 and version = $2'
       using p_model_id, p_version;

    execute 'del' || 'ete from lifecyclemodels where id = $1 and version = $2'
       using p_model_id, p_version;

    get diagnostics v_rows_affected = row_count;
    if v_rows_affected = 0 then
        raise exception 'MODEL_NOT_FOUND';
    end if;

    return jsonb_build_object(
        'model_id', p_model_id,
        'version', p_version
    );
end;
$_$;

ALTER FUNCTION "private"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") TO "api_internal_executor";
