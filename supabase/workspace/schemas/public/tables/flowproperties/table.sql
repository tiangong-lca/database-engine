CREATE TABLE IF NOT EXISTS "public"."flowproperties" (
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
    CONSTRAINT "flowproperties_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);

ALTER TABLE "public"."flowproperties" OWNER TO "postgres";

ALTER TABLE ONLY "public"."flowproperties"
    ADD CONSTRAINT "flowproperties_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."flowproperties" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flowproperties" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flowproperties" TO "authenticated";

GRANT ALL ON TABLE "public"."flowproperties" TO "service_role";
