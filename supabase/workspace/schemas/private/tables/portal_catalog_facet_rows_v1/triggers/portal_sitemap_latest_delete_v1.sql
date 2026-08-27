CREATE OR REPLACE TRIGGER "portal_sitemap_latest_delete_v1" BEFORE DELETE ON "private"."portal_catalog_facet_rows_v1" FOR EACH ROW EXECUTE FUNCTION "private"."sync_portal_sitemap_latest_delete_v1"();
