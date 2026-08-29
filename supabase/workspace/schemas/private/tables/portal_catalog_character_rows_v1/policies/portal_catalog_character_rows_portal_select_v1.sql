CREATE POLICY "portal_catalog_character_rows_portal_select_v1" ON "private"."portal_catalog_character_rows_v1" FOR SELECT TO "portal_public_executor" USING (true);
