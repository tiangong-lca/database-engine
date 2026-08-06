CREATE OR REPLACE FUNCTION "private"."review_dataset_content_guard_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if old.state_code in (20, 100)
    and pg_catalog.current_setting('app.review_controlled_write', true)
      is distinct from 'on'
    and (
      to_jsonb(old)
        - array['state_code', 'review_id', 'reviews', 'modified_at']
      is distinct from
      to_jsonb(new)
        - array['state_code', 'review_id', 'reviews', 'modified_at']
    ) then
    raise exception using
      errcode = '55000',
      message = case
        when old.state_code = 100 then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATASET_UNDER_REVIEW_IMMUTABLE'
      end;
  end if;

  return new;
end;
$$;

ALTER FUNCTION "private"."review_dataset_content_guard_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_dataset_content_guard_v1"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_dataset_content_guard_v1"() TO "api_internal_executor";
