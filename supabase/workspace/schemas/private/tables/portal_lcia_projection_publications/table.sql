CREATE TABLE IF NOT EXISTS "private"."portal_lcia_projection_publications" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "projection_id" "uuid" NOT NULL,
    "lcia_result_publication_id" "uuid" NOT NULL,
    "package_id" "uuid" NOT NULL,
    "package_version" "text" NOT NULL,
    "package_result_hash" "text" NOT NULL,
    "projection_content_hash" "text" NOT NULL,
    "evidence_hash" "text" NOT NULL,
    "source_published_at" timestamp with time zone NOT NULL,
    "idempotency_key" "text" NOT NULL,
    "status" "text" DEFAULT 'finalized'::"text" NOT NULL,
    "finalized_by" "uuid" NOT NULL,
    "finalized_at" timestamp with time zone NOT NULL,
    "revoked_by" "uuid",
    "revoked_at" timestamp with time zone,
    "revoke_reason" "text",
    CONSTRAINT "portal_lcia_projection_publications_hashes_chk" CHECK ((("package_result_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("projection_content_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("evidence_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "portal_lcia_projection_publications_idempotency_chk" CHECK ((("length"("btrim"("idempotency_key")) >= 1) AND ("length"("btrim"("idempotency_key")) <= 256))),
    CONSTRAINT "portal_lcia_projection_publications_status_chk" CHECK (("status" = ANY (ARRAY['finalized'::"text", 'revoked'::"text"]))),
    CONSTRAINT "portal_lcia_projection_publications_terminal_chk" CHECK (((("status" = 'finalized'::"text") AND ("revoked_by" IS NULL) AND ("revoked_at" IS NULL) AND ("revoke_reason" IS NULL)) OR (("status" = 'revoked'::"text") AND ("revoked_by" IS NOT NULL) AND ("revoked_at" IS NOT NULL) AND (NULLIF("btrim"("revoke_reason"), ''::"text") IS NOT NULL))))
);

ALTER TABLE "private"."portal_lcia_projection_publications" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_lcia_projection_publications" IS 'Finalized or revoked binding to an existing current LCIA result publication; rows are never deleted.';

ALTER TABLE ONLY "private"."portal_lcia_projection_publications"
    ADD CONSTRAINT "portal_lcia_projection_publicati_lcia_result_publication_id_key" UNIQUE ("lcia_result_publication_id");

ALTER TABLE ONLY "private"."portal_lcia_projection_publications"
    ADD CONSTRAINT "portal_lcia_projection_publications_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."portal_lcia_projection_publications"
    ADD CONSTRAINT "portal_lcia_projection_publicat_lcia_result_publication_id_fkey" FOREIGN KEY ("lcia_result_publication_id") REFERENCES "private"."lcia_result_publications"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."portal_lcia_projection_publications"
    ADD CONSTRAINT "portal_lcia_projection_publications_package_id_fkey" FOREIGN KEY ("package_id") REFERENCES "private"."lcia_result_packages"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."portal_lcia_projection_publications"
    ADD CONSTRAINT "portal_lcia_projection_publications_projection_id_fkey" FOREIGN KEY ("projection_id") REFERENCES "private"."portal_lcia_projection_headers"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."portal_lcia_projection_publications" ENABLE ROW LEVEL SECURITY;

GRANT SELECT("id") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("projection_id") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("lcia_result_publication_id") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("package_id") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("package_version") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("projection_content_hash") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("evidence_hash") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("source_published_at") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("status") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";

GRANT SELECT("revoked_at") ON TABLE "private"."portal_lcia_projection_publications" TO "portal_public_executor";
