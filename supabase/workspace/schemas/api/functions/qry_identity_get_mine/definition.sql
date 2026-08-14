CREATE OR REPLACE FUNCTION "api"."qry_identity_get_mine"() RETURNS TABLE("id" "uuid", "email" "text", "display_name" "text", "contact" "jsonb")
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select
    profile.id,
    coalesce(
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    ),
    profile.contact
  from private.users as profile
  left join auth.users as auth_user on auth_user.id = profile.id
  where auth.uid() is not null
    and profile.id = auth.uid()
$$;

ALTER FUNCTION "api"."qry_identity_get_mine"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_identity_get_mine"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_identity_get_mine"() TO "authenticated";
