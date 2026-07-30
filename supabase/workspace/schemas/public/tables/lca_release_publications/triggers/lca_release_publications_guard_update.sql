CREATE OR REPLACE TRIGGER "lca_release_publications_guard_update" BEFORE UPDATE ON "public"."lca_release_publications" FOR EACH ROW EXECUTE FUNCTION "public"."lca_release_guard_publication_update"();
