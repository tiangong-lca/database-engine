CREATE TABLE IF NOT EXISTS "public"."lca_package_export_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "table_name" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "is_seed" boolean DEFAULT false NOT NULL,
    "refs_done" boolean DEFAULT false NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "worker_job_id" "uuid",
    CONSTRAINT "lca_package_export_items_table_chk" CHECK (("table_name" = ANY (ARRAY['contacts'::"text", 'sources'::"text", 'unitgroups'::"text", 'flowproperties'::"text", 'flows'::"text", 'processes'::"text", 'lifecyclemodels'::"text"]))),
    CONSTRAINT "lca_package_export_items_version_chk" CHECK (("length"("btrim"("version")) > 0))
);

ALTER TABLE "public"."lca_package_export_items" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_package_export_items"
    ADD CONSTRAINT "lca_package_export_items_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_package_export_items"
    ADD CONSTRAINT "lca_package_export_items_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "public"."lca_package_export_items" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_package_export_items" TO "anon";

GRANT ALL ON TABLE "public"."lca_package_export_items" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_package_export_items" TO "service_role";
