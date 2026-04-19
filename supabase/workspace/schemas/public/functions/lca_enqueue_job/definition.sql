CREATE OR REPLACE FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pgmq'
    AS $$
DECLARE
    v_msg_id bigint;
BEGIN
    IF p_queue_name IS NULL OR btrim(p_queue_name) = '' THEN
        RAISE EXCEPTION 'queue name is required';
    END IF;

    SELECT pgmq.send(p_queue_name, p_message)
      INTO v_msg_id;

    RETURN v_msg_id;
END;
$$;

ALTER FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "service_role";
