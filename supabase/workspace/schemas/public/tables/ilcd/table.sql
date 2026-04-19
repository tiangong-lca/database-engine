CREATE TABLE IF NOT EXISTS "public"."ilcd" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "file_name" character varying(255),
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "modified_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."ilcd" OWNER TO "postgres";

ALTER TABLE ONLY "public"."ilcd"
    ADD CONSTRAINT "ilcd_pkey" PRIMARY KEY ("id");

ALTER TABLE "public"."ilcd" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."ilcd" TO "anon";

GRANT ALL ON TABLE "public"."ilcd" TO "authenticated";

GRANT ALL ON TABLE "public"."ilcd" TO "service_role";
