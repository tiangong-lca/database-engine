CREATE OR REPLACE FUNCTION "private"."sync_json_to_jsonb"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb
    THEN
        NEW.json := NEW.json_ordered;
    END IF;
    RETURN NEW;
END;$$;

ALTER FUNCTION "private"."sync_json_to_jsonb"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."sync_json_to_jsonb"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."sync_json_to_jsonb"() TO "service_role";

GRANT ALL ON FUNCTION "private"."sync_json_to_jsonb"() TO "api_internal_executor";
