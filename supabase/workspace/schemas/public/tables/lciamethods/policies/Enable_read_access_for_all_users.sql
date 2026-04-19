CREATE POLICY "Enable read access for all users" ON "public"."lciamethods" FOR SELECT TO "authenticated" USING (true);
