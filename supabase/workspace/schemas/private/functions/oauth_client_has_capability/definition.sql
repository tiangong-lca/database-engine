CREATE OR REPLACE FUNCTION "private"."oauth_client_has_capability"("p_capability_id" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select case
    -- First-party Supabase sessions have no OAuth client_id and retain the
    -- existing auth.uid()-based RLS behavior.
    when nullif(pg_catalog.btrim(coalesce(auth.jwt() ->> 'client_id', '')), '') is null
      then true
    when p_capability_id is null or pg_catalog.btrim(p_capability_id) = ''
      then false
    else exists (
      select 1
      from private.oauth_client_registry as client
      join private.oauth_client_capability_grants as grant_row
        on grant_row.client_id = client.client_id
      where client.client_id = auth.jwt() ->> 'client_id'
        and client.enabled
        and grant_row.capability_id = p_capability_id
        and grant_row.allowed
    )
  end;
$$;

ALTER FUNCTION "private"."oauth_client_has_capability"("p_capability_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."oauth_client_has_capability"("p_capability_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."oauth_client_has_capability"("p_capability_id" "text") TO "authenticated";
