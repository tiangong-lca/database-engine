CREATE INDEX "lcia_scope_closure_checks_requested_updated_idx" ON "public"."lcia_scope_closure_checks" USING "btree" ("requested_by", "updated_at" DESC, "id" DESC);
