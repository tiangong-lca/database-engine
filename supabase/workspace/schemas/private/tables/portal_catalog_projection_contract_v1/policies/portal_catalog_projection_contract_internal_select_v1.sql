CREATE POLICY "portal_catalog_projection_contract_internal_select_v1" ON "private"."portal_catalog_projection_contract_v1" FOR SELECT TO "api_internal_executor" USING (("contract_version" = 1));
