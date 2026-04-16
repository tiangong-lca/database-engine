CREATE TABLE IF NOT EXISTS "public"."lifecyclemodels" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "json_tg" "jsonb",
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_text" "text",
    "embedding_at" timestamp with time zone,
    "embedding_flag" smallint,
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    CONSTRAINT "lifecyclemodels_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100])))
);

ALTER TABLE "public"."lifecyclemodels" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lifecyclemodels"
    ADD CONSTRAINT "lifecyclemodels_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."lifecyclemodels" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."lifecyclemodels" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."lifecyclemodels" TO "authenticated";

GRANT ALL ON TABLE "public"."lifecyclemodels" TO "service_role";
