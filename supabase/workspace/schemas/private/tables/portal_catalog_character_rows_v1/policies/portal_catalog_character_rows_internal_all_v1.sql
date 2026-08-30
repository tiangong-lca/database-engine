CREATE POLICY "portal_catalog_character_rows_internal_all_v1" ON "private"."portal_catalog_character_rows_v1" TO "api_internal_executor" USING (true) WITH CHECK (true);
