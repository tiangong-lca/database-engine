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
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "model_id" "uuid",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "extracted_md" "text",
    "search_text" "text"[],
    "model_version" character(9),
    CONSTRAINT "processes_model_version_format_check" CHECK ((("model_version" IS NULL) OR (("model_version")::"text" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text"))),
    CONSTRAINT "processes_model_version_requires_model_id_check" CHECK ((("model_version" IS NULL) OR ("model_id" IS NOT NULL))),
    CONSTRAINT "processes_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);

ALTER TABLE "public"."processes" OWNER TO "postgres";

ALTER TABLE ONLY "public"."processes"
    ADD CONSTRAINT "processes_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."processes" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."processes" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."processes" TO "authenticated";

GRANT ALL ON TABLE "public"."processes" TO "service_role";

GRANT SELECT ON TABLE "public"."processes" TO "api_internal_executor";

GRANT SELECT("tableoid") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("ctid") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("id") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("json") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("json") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("state_code") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("state_code") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("version") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("modified_at") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("team_id") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("embedding_ft") ON TABLE "public"."processes" TO "portal_public_executor";

GRANT SELECT("embedding_ft") ON TABLE "public"."processes" TO "next_public_search_executor";

GRANT SELECT("search_text") ON TABLE "public"."processes" TO "next_public_search_executor";
