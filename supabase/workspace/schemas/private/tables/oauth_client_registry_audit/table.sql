CREATE TABLE IF NOT EXISTS "private"."oauth_client_registry_audit" (
    "id" bigint NOT NULL,
    "client_id" "text" NOT NULL,
    "action" "text" NOT NULL,
    "actor_role" "text",
    "actor_sub" "uuid",
    "before_state" "jsonb",
    "after_state" "jsonb" NOT NULL,
    "changed_at" timestamp with time zone DEFAULT "statement_timestamp"() NOT NULL,
    CONSTRAINT "oauth_client_registry_audit_action_check" CHECK (("action" = ANY (ARRAY['create'::"text", 'replace'::"text", 'disable'::"text", 'enable'::"text"])))
);

ALTER TABLE ONLY "private"."oauth_client_registry_audit" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."oauth_client_registry_audit" OWNER TO "postgres";

ALTER TABLE "private"."oauth_client_registry_audit" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "private"."oauth_client_registry_audit_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY "private"."oauth_client_registry_audit"
    ADD CONSTRAINT "oauth_client_registry_audit_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."oauth_client_registry_audit" ENABLE ROW LEVEL SECURITY;
