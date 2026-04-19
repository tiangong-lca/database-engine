CREATE TABLE IF NOT EXISTS "public"."contacts" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "embedding" "extensions"."vector"(1536),
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    CONSTRAINT "contacts_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 3, 20, 100])))
);

ALTER TABLE "public"."contacts" OWNER TO "postgres";

ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."contacts" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "authenticated";

GRANT ALL ON TABLE "public"."contacts" TO "service_role";
