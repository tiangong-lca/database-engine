CREATE OR REPLACE FUNCTION "api"."qry_team_list"("p_mode" "text", "p_keyword" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10) RETURNS TABLE("id" "uuid", "json" "jsonb", "rank" integer, "is_public" boolean, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "owner_user_id" "uuid", "owner_email" "text", "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_mode text := lower(btrim(coalesce(p_mode, '')));
  v_keyword text := nullif(btrim(coalesce(p_keyword, '')), '');
  v_page integer := greatest(coalesce(p_page, 1), 1);
  v_page_size integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_pattern text;
  v_system_manager boolean;
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  if v_mode not in ('ranked', 'public', 'unranked') then
    raise exception using errcode = '22023', message = 'INVALID_TEAM_LIST_MODE';
  end if;

  if v_keyword is not null and length(v_keyword) > 128 then
    raise exception using errcode = '22023', message = 'TEAM_KEYWORD_TOO_LONG';
  end if;

  select exists (
    select 1
    from private.roles as membership
    where membership.user_id = v_actor
      and membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and membership.role in ('owner', 'admin', 'member')
  ) into v_system_manager;

  if v_mode = 'unranked' and not v_system_manager then
    raise exception using errcode = '42501', message = 'SYSTEM_MANAGER_REQUIRED';
  end if;

  -- Treat PostgREST filter metacharacters as data, not as query syntax.
  v_pattern := case when v_keyword is null then null else
    '%' || replace(replace(replace(v_keyword, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%'
  end;

  return query
  with visible as (
    select team.*
    from private.teams as team
    where case v_mode
      when 'ranked' then coalesce(team.rank, -1) > 0
      when 'public' then coalesce(team.is_public, false)
      when 'unranked' then coalesce(team.rank, -1) = 0
    end
      and (
        v_pattern is null
        or coalesce(team.json #>> '{title,0,#text}', '') ilike v_pattern escape E'\\'
        or coalesce(team.json #>> '{title,1,#text}', '') ilike v_pattern escape E'\\'
      )
  ), enriched as (
    select
      team.id,
      team.json,
      team.rank,
      team.is_public,
      team.created_at,
      team.modified_at,
      owner_membership.user_id as owner_user_id,
      coalesce(
        nullif(btrim(auth_owner.email), ''),
        nullif(btrim(owner_profile.raw_user_meta_data ->> 'email'), '')
      ) as owner_email,
      count(*) over () as total_count
    from visible as team
    left join lateral (
      select membership.user_id
      from private.roles as membership
      where membership.team_id = team.id
        and membership.role = 'owner'
      order by membership.created_at, membership.user_id
      limit 1
    ) as owner_membership on true
    left join auth.users as auth_owner on auth_owner.id = owner_membership.user_id
    left join private.users as owner_profile on owner_profile.id = owner_membership.user_id
  )
  select enriched.*
  from enriched
  order by
    case when v_mode = 'unranked' then enriched.created_at end desc nulls last,
    case when v_mode <> 'unranked' then enriched.rank end asc nulls last,
    enriched.id
  offset (v_page - 1) * v_page_size
  limit v_page_size;
end
$$;

ALTER FUNCTION "api"."qry_team_list"("p_mode" "text", "p_keyword" "text", "p_page" integer, "p_page_size" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_team_list"("p_mode" "text", "p_keyword" "text", "p_page" integer, "p_page_size" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_team_list"("p_mode" "text", "p_keyword" "text", "p_page" integer, "p_page_size" integer) TO "authenticated";
