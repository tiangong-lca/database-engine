CREATE POLICY "portal_public_executor_select_unitgroups_v1" ON "public"."unitgroups" FOR SELECT TO "portal_public_executor" USING (("state_code" = ANY (ARRAY[100, 200])));
