CREATE OR REPLACE FUNCTION "public"."unitgroups_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version :=  COALESCE(NEW.json->'unitGroupDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion'
		,
					''
        );
    END IF;
    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."unitgroups_sync_jsonb_version"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "anon";

GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "service_role";
