CREATE TABLE IF NOT EXISTS "public"."processes" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "extracted_text" "text",
    "embedding_at" timestamp with time zone,
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "embedding_flag" smallint,
    "model_id" "uuid",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "extracted_md" "text",
    CONSTRAINT "processes_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);

ALTER TABLE "public"."processes" OWNER TO "postgres";

ALTER TABLE ONLY "public"."processes"
    ADD CONSTRAINT "processes_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."processes" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."processes" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."processes" TO "authenticated";

GRANT ALL ON TABLE "public"."processes" TO "service_role";
