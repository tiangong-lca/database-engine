CREATE OR REPLACE FUNCTION "private"."protect_example_dataset_write"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
BEGIN
  IF OLD.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'EXAMPLE_DATASET_READ_ONLY';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

ALTER FUNCTION "private"."protect_example_dataset_write"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."protect_example_dataset_write"() FROM PUBLIC;
