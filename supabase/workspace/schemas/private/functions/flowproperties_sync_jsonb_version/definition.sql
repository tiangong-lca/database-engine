CREATE OR REPLACE FUNCTION "private"."flowproperties_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE( NEW.json->'flowPropertyDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$$;

ALTER FUNCTION "private"."flowproperties_sync_jsonb_version"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."flowproperties_sync_jsonb_version"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."flowproperties_sync_jsonb_version"() TO "service_role";

GRANT ALL ON FUNCTION "private"."flowproperties_sync_jsonb_version"() TO "api_internal_executor";
