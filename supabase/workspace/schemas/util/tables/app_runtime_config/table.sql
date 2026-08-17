CREATE TABLE IF NOT EXISTS "util"."app_runtime_config" (
    "config_key" "text" NOT NULL,
    "config_value" "jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "statement_timestamp"() NOT NULL,
    CONSTRAINT "app_runtime_config_active_reason_check" CHECK (COALESCE(((("config_value" ->> 'phase'::"text") = 'normal'::"text") OR (("config_value" ->> 'reason'::"text") = ANY (ARRAY['release_upgrade'::"text", 'emergency'::"text"]))), false)),
    CONSTRAINT "app_runtime_config_estimated_end_at_check" CHECK (((NOT ("config_value" ? 'estimatedEndAt'::"text")) OR (("config_value" -> 'estimatedEndAt'::"text") = 'null'::"jsonb") OR (("jsonb_typeof"(("config_value" -> 'estimatedEndAt'::"text")) = 'string'::"text") AND (("config_value" ->> 'estimatedEndAt'::"text") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([.]\d+)?(Z|[+-]\d{2}:\d{2})$'::"text")))),
    CONSTRAINT "app_runtime_config_key_not_blank_check" CHECK (("btrim"("config_key") <> ''::"text")),
    CONSTRAINT "app_runtime_config_phase_check" CHECK (COALESCE((("config_value" ->> 'phase'::"text") = ANY (ARRAY['normal'::"text", 'maintenance'::"text", 'verifying'::"text"])), false)),
    CONSTRAINT "app_runtime_config_reason_check" CHECK (((NOT ("config_value" ? 'reason'::"text")) OR (("config_value" -> 'reason'::"text") = 'null'::"jsonb") OR (("config_value" ->> 'reason'::"text") = ANY (ARRAY['release_upgrade'::"text", 'emergency'::"text"])))),
    CONSTRAINT "app_runtime_config_release_id_check" CHECK (((NOT ("config_value" ? 'releaseId'::"text")) OR (("config_value" -> 'releaseId'::"text") = 'null'::"jsonb") OR ("jsonb_typeof"(("config_value" -> 'releaseId'::"text")) = 'string'::"text"))),
    CONSTRAINT "app_runtime_config_schema_version_check" CHECK (COALESCE((("config_value" -> 'schemaVersion'::"text") = '1'::"jsonb"), false)),
    CONSTRAINT "app_runtime_config_target_version_check" CHECK (((NOT ("config_value" ? 'targetVersion'::"text")) OR (("config_value" -> 'targetVersion'::"text") = 'null'::"jsonb") OR ("jsonb_typeof"(("config_value" -> 'targetVersion'::"text")) = 'string'::"text"))),
    CONSTRAINT "app_runtime_config_value_object_check" CHECK (("jsonb_typeof"("config_value") = 'object'::"text"))
);

ALTER TABLE "util"."app_runtime_config" OWNER TO "postgres";

COMMENT ON TABLE "util"."app_runtime_config" IS 'Operational runtime configuration. Values are not Data API relations and are exposed only by fixed api facades.';

ALTER TABLE ONLY "util"."app_runtime_config"
    ADD CONSTRAINT "app_runtime_config_pkey" PRIMARY KEY ("config_key");
