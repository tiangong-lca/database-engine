CREATE OR REPLACE FUNCTION "public"."sync_json_to_jsonb"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb
    THEN
        NEW.json := NEW.json_ordered;
    END IF;
    RETURN NEW;
END;$$;

ALTER FUNCTION "public"."sync_json_to_jsonb"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "anon";

GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "service_role";
