CREATE TABLE IF NOT EXISTS "private"."lcia_result_publications" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "package_id" "uuid" NOT NULL,
    "publication_series_key" "text" DEFAULT 'global'::"text" NOT NULL,
    "publication_channel" "text" DEFAULT 'public'::"text" NOT NULL,
    "visibility_scope" "text" DEFAULT 'public'::"text" NOT NULL,
    "is_current" boolean DEFAULT false NOT NULL,
    "status" "text" NOT NULL,
    "display_default_impact_category" "text",
    "published_by" "uuid",
    "published_at" timestamp with time zone,
    "unpublished_by" "uuid",
    "unpublished_at" timestamp with time zone,
    "reason" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_result_publications_channel_chk" CHECK (("publication_channel" = 'public'::"text")),
    CONSTRAINT "lcia_result_publications_status_chk" CHECK (("status" = ANY (ARRAY['current'::"text", 'superseded'::"text", 'unpublished'::"text"]))),
    CONSTRAINT "lcia_result_publications_visibility_chk" CHECK (("visibility_scope" = 'public'::"text"))
);

ALTER TABLE "private"."lcia_result_publications" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lcia_result_publications"
    ADD CONSTRAINT "lcia_result_publications_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lcia_result_publications"
    ADD CONSTRAINT "lcia_result_publications_package_id_fkey" FOREIGN KEY ("package_id") REFERENCES "private"."lcia_result_packages"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lcia_result_publications" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lcia_result_publications" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_result_publications" TO "api_internal_executor";
