CREATE POLICY "portal_catalog_character_rows_portal_select_v2" ON "private"."portal_catalog_character_rows_v2" FOR SELECT TO "portal_public_executor" USING (true);
