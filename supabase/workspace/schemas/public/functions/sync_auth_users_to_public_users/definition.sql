CREATE OR REPLACE FUNCTION "public"."sync_auth_users_to_public_users"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    -- 处理插入操作
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.users (id, raw_user_meta_data)
        VALUES (NEW.id, NEW.raw_user_meta_data);
    -- 处理更新操作
    ELSIF TG_OP = 'UPDATE' THEN
		IF NEW.raw_user_meta_data != OLD.raw_user_meta_data THEN
			UPDATE public.users
			SET raw_user_meta_data = NEW.raw_user_meta_data
			WHERE id = NEW.id;
    	END IF;
    -- 处理删除操作
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM public.users
        WHERE id = OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."sync_auth_users_to_public_users"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "anon";

GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "service_role";
