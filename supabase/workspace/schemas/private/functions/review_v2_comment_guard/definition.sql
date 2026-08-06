CREATE OR REPLACE FUNCTION "private"."review_v2_comment_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if current_setting('app.review_legacy_migration', true) = 'on' then
    return new;
  end if;

  if exists (
    select 1
    from private.reviews as review_row
    where review_row.id = new.review_id
      and review_row.review_kind is null
  ) then
    raise exception using
      errcode = '55000',
      message = 'LEGACY_REVIEW_READ_ONLY';
  end if;

  return new;
end;
$$;

ALTER FUNCTION "private"."review_v2_comment_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_v2_comment_guard"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_v2_comment_guard"() TO "api_internal_executor";
