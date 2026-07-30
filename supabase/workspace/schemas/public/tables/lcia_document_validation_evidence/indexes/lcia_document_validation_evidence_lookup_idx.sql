CREATE INDEX "lcia_document_validation_evidence_lookup_idx" ON "public"."lcia_document_validation_evidence" USING "btree" ("dataset_type", "dataset_id", "dataset_version", "canonical_content_hash");
