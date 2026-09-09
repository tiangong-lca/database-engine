CREATE TABLE IF NOT EXISTS "private"."tidas_import_groups_v2" (
    "worker_job_id" "uuid" NOT NULL,
    "root_table" "text" NOT NULL,
    "root_id" "uuid" NOT NULL,
    "root_version" "text" NOT NULL,
    "entries_sha256" "text" NOT NULL,
    "receipt" "jsonb" NOT NULL,
    "committed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "tidas_import_groups_v2_receipt_check" CHECK (("jsonb_typeof"("receipt") = 'object'::"text")),
    CONSTRAINT "tidas_import_groups_v2_root_table_check" CHECK (("root_table" = ANY (ARRAY['processes'::"text", 'lifecyclemodels'::"text"]))),
    CONSTRAINT "tidas_import_groups_v2_root_version_check" CHECK (("root_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text"))
);

ALTER TABLE "private"."tidas_import_groups_v2" OWNER TO "postgres";

ALTER TABLE ONLY "private"."tidas_import_groups_v2"
    ADD CONSTRAINT "tidas_import_groups_v2_pkey" PRIMARY KEY ("worker_job_id", "root_table", "root_id", "root_version");

ALTER TABLE ONLY "private"."tidas_import_groups_v2"
    ADD CONSTRAINT "tidas_import_groups_v2_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "private"."tidas_import_plans_v2"("worker_job_id");

ALTER TABLE "private"."tidas_import_groups_v2" ENABLE ROW LEVEL SECURITY;
