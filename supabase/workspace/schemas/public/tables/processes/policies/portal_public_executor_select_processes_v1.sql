CREATE POLICY "portal_public_executor_select_processes_v1" ON "public"."processes" FOR SELECT TO "portal_public_executor" USING (("state_code" = ANY (ARRAY[100, 200])));
