CREATE TABLE IF NOT EXISTS "private"."command_audit_log" (
    "id" bigint NOT NULL,
    "command" "text" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "target_table" "text",
    "target_id" "uuid",
    "target_version" "text",
    "payload" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

ALTER TABLE "private"."command_audit_log" OWNER TO "postgres";

COMMENT ON TABLE "private"."command_audit_log" IS 'Validation-only migration for database-engine preview branch cutover on 2026-04-14.';

ALTER TABLE "private"."command_audit_log" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "private"."command_audit_log_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY "private"."command_audit_log"
    ADD CONSTRAINT "command_audit_log_pkey" PRIMARY KEY ("id");

GRANT ALL ON TABLE "private"."command_audit_log" TO "service_role";

GRANT SELECT ON TABLE "private"."command_audit_log" TO "api_internal_executor";
