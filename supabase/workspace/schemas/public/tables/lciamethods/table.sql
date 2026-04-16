CREATE TABLE IF NOT EXISTS "public"."lciamethods" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."lciamethods" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lciamethods"
    ADD CONSTRAINT "lciamethods_pkey" PRIMARY KEY ("id", "version");

ALTER TABLE "public"."lciamethods" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lciamethods" TO "anon";

GRANT ALL ON TABLE "public"."lciamethods" TO "authenticated";

GRANT ALL ON TABLE "public"."lciamethods" TO "service_role";
