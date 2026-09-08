CREATE POLICY "portal_catalog_projection_contract_internal_select_v2" ON "private"."portal_catalog_projection_contract_v2" FOR SELECT TO "api_internal_executor" USING (("contract_version" = 2));
