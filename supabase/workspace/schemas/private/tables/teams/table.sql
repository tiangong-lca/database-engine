CREATE TABLE IF NOT EXISTS "private"."teams" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    "rank" integer DEFAULT '-1'::integer,
    "is_public" boolean DEFAULT false
);

ALTER TABLE "private"."teams" OWNER TO "postgres";

ALTER TABLE ONLY "private"."teams"
    ADD CONSTRAINT "teams_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."teams" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."teams" TO "service_role";

GRANT SELECT ON TABLE "private"."teams" TO "api_internal_executor";
