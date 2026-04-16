CREATE POLICY "Enable read access for all users" ON "public"."ilcd" FOR SELECT TO "authenticated" USING (true);
