CREATE OR REPLACE FUNCTION "api"."qry_team_get"("p_team_id" "uuid") RETURNS TABLE("id" "uuid", "json" "jsonb", "rank" integer, "is_public" boolean, "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select
    team.id,
    team.json,
    team.rank,
    team.is_public,
    team.created_at,
    team.modified_at
  from private.teams as team
  where auth.uid() is not null
    and team.id = p_team_id
    and (
      coalesce(team.is_public, false)
      or coalesce(team.rank, -1) > 0
      or exists (
        select 1
        from private.roles as membership
        where membership.user_id = auth.uid()
          and (
            membership.team_id = team.id
            or (
              membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
              and membership.role in ('owner', 'admin', 'member')
            )
          )
      )
    )
$$;

ALTER FUNCTION "api"."qry_team_get"("p_team_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_team_get"("p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_team_get"("p_team_id" "uuid") TO "authenticated";
