CREATE TABLE IF NOT EXISTS "public"."flows" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "embedding_at" timestamp(6) with time zone DEFAULT NULL::timestamp with time zone,
    "extracted_text" "text",
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "embedding_flag" smallint,
    "embedding_ft_at" timestamp with time zone,
    "extracted_md" "text",
    "embedding_ft" "extensions"."vector"(1024),
    CONSTRAINT "flows_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);

ALTER TABLE "public"."flows" OWNER TO "postgres";

ALTER TABLE ONLY "public"."flows"
    ADD CONSTRAINT "flows_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."flows" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flows" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flows" TO "authenticated";

GRANT ALL ON TABLE "public"."flows" TO "service_role";
