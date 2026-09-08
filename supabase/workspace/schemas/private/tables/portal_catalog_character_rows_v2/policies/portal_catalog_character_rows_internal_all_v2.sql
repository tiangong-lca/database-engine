CREATE POLICY "portal_catalog_character_rows_internal_all_v2" ON "private"."portal_catalog_character_rows_v2" TO "api_internal_executor" USING (true) WITH CHECK (true);
