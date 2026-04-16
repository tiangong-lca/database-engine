CREATE TABLE IF NOT EXISTS "public"."unitgroups" (
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
    CONSTRAINT "unitgroups_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 100, 200])))
);

ALTER TABLE "public"."unitgroups" OWNER TO "postgres";

ALTER TABLE ONLY "public"."unitgroups"
    ADD CONSTRAINT "unitgroups_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."unitgroups" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."unitgroups" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."unitgroups" TO "authenticated";

GRANT ALL ON TABLE "public"."unitgroups" TO "service_role";
