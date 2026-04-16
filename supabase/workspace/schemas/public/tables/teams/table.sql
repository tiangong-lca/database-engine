CREATE TABLE IF NOT EXISTS "public"."teams" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    "rank" integer DEFAULT '-1'::integer,
    "is_public" boolean DEFAULT false
);

ALTER TABLE "public"."teams" OWNER TO "postgres";

ALTER TABLE ONLY "public"."teams"
    ADD CONSTRAINT "teams_pkey" PRIMARY KEY ("id");

ALTER TABLE "public"."teams" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."teams" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."teams" TO "authenticated";

GRANT ALL ON TABLE "public"."teams" TO "service_role";
