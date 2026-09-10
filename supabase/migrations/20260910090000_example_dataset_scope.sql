-- Database #636 / workspace #1095: authenticated example datasets.
-- Each existing state set is extended independently; review states are unrelated.
-- Search keeps the existing tg/co paths and fixes ex to -1 before ranking/versioning.
-- Existing signatures, owners and capability grants are preserved.
BEGIN;
GRANT api_internal_executor TO postgres;
GRANT CREATE ON SCHEMA api, private TO api_internal_executor;

CREATE OR REPLACE FUNCTION private.protect_example_dataset_write()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $guard$
BEGIN
  IF OLD.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'EXAMPLE_DATASET_READ_ONLY';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$guard$;
REVOKE ALL ON FUNCTION private.protect_example_dataset_write() FROM PUBLIC;

ALTER TABLE public.contacts DROP CONSTRAINT contacts_state_code_check;
ALTER TABLE public.contacts ADD CONSTRAINT contacts_state_code_check CHECK (state_code IN (-1, 0, 3, 20, 100));
CREATE POLICY authenticated_example_read ON public.contacts
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.contacts
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.sources DROP CONSTRAINT sources_state_code_check;
ALTER TABLE public.sources ADD CONSTRAINT sources_state_code_check CHECK (state_code IN (-1, 0, 20, 100));
CREATE POLICY authenticated_example_read ON public.sources
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.sources
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.lifecyclemodels DROP CONSTRAINT lifecyclemodels_state_code_check;
ALTER TABLE public.lifecyclemodels ADD CONSTRAINT lifecyclemodels_state_code_check CHECK (state_code IN (-1, 0, 20, 100));
CREATE POLICY authenticated_example_read ON public.lifecyclemodels
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.lifecyclemodels
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.unitgroups DROP CONSTRAINT unitgroups_state_code_check;
ALTER TABLE public.unitgroups ADD CONSTRAINT unitgroups_state_code_check CHECK (state_code IN (-1, 0, 20, 100, 200));
CREATE POLICY authenticated_example_read ON public.unitgroups
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.unitgroups
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.flowproperties DROP CONSTRAINT flowproperties_state_code_check;
ALTER TABLE public.flowproperties ADD CONSTRAINT flowproperties_state_code_check CHECK (state_code IN (-1, 0, 20, 100, 200));
CREATE POLICY authenticated_example_read ON public.flowproperties
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.flowproperties
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.flows DROP CONSTRAINT flows_state_code_check;
ALTER TABLE public.flows ADD CONSTRAINT flows_state_code_check CHECK (state_code IN (-1, 0, 20, 100, 200));
CREATE POLICY authenticated_example_read ON public.flows
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.flows
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

ALTER TABLE public.processes DROP CONSTRAINT processes_state_code_check;
ALTER TABLE public.processes ADD CONSTRAINT processes_state_code_check CHECK (state_code IN (-1, 0, 20, 100, 200));
CREATE POLICY authenticated_example_read ON public.processes
  FOR SELECT TO authenticated
  USING (state_code = -1 AND (SELECT auth.uid()) IS NOT NULL);
CREATE TRIGGER protect_example_dataset_write BEFORE UPDATE OR DELETE ON public.processes
  FOR EACH ROW EXECUTE FUNCTION private.protect_example_dataset_write();

CREATE OR REPLACE FUNCTION "api"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid" DEFAULT NULL::"uuid", "p_rule_verification" boolean DEFAULT NULL::boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb", "p_model_version" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_model_version text := nullif(btrim(coalesce(p_model_version, '')), '');
  v_current_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_updated_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table'
    );
  end if;

  if p_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'jsonOrdered is required'
    );
  end if;

  if p_table <> 'processes' and p_model_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_NOT_ALLOWED',
      'status', 400,
      'message', 'modelId is only allowed for process dataset drafts'
    );
  end if;

  if p_table <> 'processes' and v_model_version is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_VERSION_NOT_ALLOWED',
      'status', 400,
      'message', 'modelVersion is only allowed for process dataset drafts'
    );
  end if;

  if v_model_version is not null and p_model_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_REQUIRED_FOR_MODEL_VERSION',
      'status', 400,
      'message', 'modelId is required when modelVersion is provided'
    );
  end if;

  if v_model_version is not null
     and v_model_version !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_MODEL_VERSION',
      'status', 400,
      'message', 'modelVersion must use NN.NN.NNN format'
    );
  end if;

  execute format(
    'select to_jsonb(t) from public.%I as t where t.id = $1 and t.version = $2 for update of t',
    p_table
  )
    into v_current_row
    using p_id, p_version;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_current_row->>'user_id', '')::uuid;
  v_state_code := coalesce((v_current_row->>'state_code')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can save draft changes'
    );
  end if;

  if v_state_code = -1 or v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 403,
      'message', 'Published data cannot be edited through draft save',
      'details', jsonb_build_object(
        'state_code', v_state_code
      )
    );
  end if;

  if v_state_code >= 20 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 403,
      'message', 'Data is under review and cannot be modified',
      'details', jsonb_build_object(
        'state_code', 20,
        'review_state_code', v_state_code
      )
    );
  end if;

  if p_table = 'processes' then
    execute format(
      'update public.%I as t
          set json_ordered = $1::json,
              model_id = coalesce($2, t.model_id),
              model_version = case
                when $2 is null then t.model_version
                when $3 is not null then $3
                when $2 is distinct from t.model_id then null
                else t.model_version
              end,
              rule_verification = $4,
              modified_at = now()
        where t.id = $5
          and t.version = $6
      returning to_jsonb(t)',
      p_table
    )
      into v_updated_row
      using p_json_ordered, p_model_id, v_model_version, p_rule_verification, p_id, p_version;
  else
    execute format(
      'update public.%I as t
          set json_ordered = $1::json,
              rule_verification = $2,
              modified_at = now()
        where t.id = $3
          and t.version = $4
      returning to_jsonb(t)',
      p_table
    )
      into v_updated_row
      using p_json_ordered, p_rule_verification, p_id, p_version;
  end if;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_save_draft',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_updated_row
  );
end;
$_$;

ALTER FUNCTION "api"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb", "p_model_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb", "p_model_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb", "p_model_version" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb", "p_model_version" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_contact_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_keys AS (
      SELECT c.id, c.version, c.created_at, c.modified_at, c.team_id
      FROM public.contacts c
      WHERE ((data_source = 'tg' AND c.state_code = 100) OR (data_source = 'ex' AND c.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR c.team_id = team_id_filter)
      UNION ALL
      SELECT c.id, c.version, c.created_at, c.modified_at, c.team_id
      FROM public.contacts c
      WHERE data_source = 'co'
        AND c.state_code = 200
        AND (team_id_filter IS NULL OR c.team_id = team_id_filter)
      UNION ALL
      SELECT c.id, c.version, c.created_at, c.modified_at, c.team_id
      FROM public.contacts c
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND c.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR c.state_code = state_code_filter)
      UNION ALL
      SELECT c.id, c.version, c.created_at, c.modified_at, c.team_id
      FROM public.contacts c
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND c.team_id = team_id_filter
        AND (state_code_filter IS NULL OR c.state_code = state_code_filter)
    ),
    latest_keys AS (
      SELECT DISTINCT ON (visible_keys.id)
        visible_keys.id,
        visible_keys.version,
        visible_keys.created_at,
        visible_keys.modified_at,
        visible_keys.team_id
      FROM visible_keys
      ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
    ),
    counted_keys AS (
      SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
      FROM latest_keys
    ),
    paged_keys AS (
      SELECT counted_keys.*
      FROM counted_keys
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
        END DESC NULLS LAST,
        counted_keys.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size
    )
    SELECT
      payload.id,
      payload.json,
      payload.version,
      payload.modified_at,
      payload.team_id,
      paged_keys.total_count
    FROM paged_keys
    JOIN public.contacts payload
      ON payload.id = paged_keys.id
     AND payload.version = paged_keys.version
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
      END DESC NULLS LAST,
      paged_keys.id;
END;
$_$;

ALTER FUNCTION "api"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_flow_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  IF flow_type IS NOT NULL THEN
    flow_type_array := string_to_array(flow_type, ',');
  ELSE
    flow_type_array := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  IF filter_condition_jsonb ? 'asInput' THEN
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  ELSE
    as_input := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  IF jsonb_typeof(filter_condition_jsonb->'classification') = 'array' THEN
    classification_filter := filter_condition_jsonb->'classification';
  ELSE
    classification_filter := '[]'::jsonb;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'classification';

  IF filter_condition_jsonb = '{}'::jsonb
    AND flow_type IS NULL
    AND as_input IS NULL
    AND jsonb_array_length(classification_filter) = 0
  THEN
    RETURN QUERY
      WITH visible_keys AS (
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE ((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'co'
          AND f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'my'
          AND normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'te'
          AND team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      ),
      latest_keys AS (
        SELECT DISTINCT ON (visible_keys.id)
          visible_keys.id,
          visible_keys.version,
          visible_keys.created_at,
          visible_keys.modified_at,
          visible_keys.team_id
        FROM visible_keys
        ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
      ),
      counted_keys AS (
        SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
        FROM latest_keys
      ),
      paged_keys AS (
        SELECT counted_keys.*
        FROM counted_keys
        ORDER BY
          CASE
            WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
          END DESC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
          END DESC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
          END DESC NULLS LAST,
          counted_keys.id
        LIMIT normalized_page_size
        OFFSET (normalized_page_current - 1) * normalized_page_size
      )
      SELECT
        payload.id,
        payload.json,
        payload.version,
        payload.modified_at,
        payload.team_id,
        paged_keys.total_count
      FROM paged_keys
      JOIN public.flows payload
        ON payload.id = paged_keys.id
       AND payload.version = paged_keys.version
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
        END DESC NULLS LAST,
        paged_keys.id;
    RETURN;
  END IF;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*
      FROM public.flows f
      WHERE ((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT DISTINCT visible_rows.id
      FROM visible_rows
      WHERE visible_rows.json @> filter_condition_jsonb
        AND (
          flow_type IS NULL
          OR (visible_rows.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
        )
        AND (
          as_input IS NULL
          OR as_input = false
          OR NOT (
            visible_rows.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
          )
        )
        AND (
          jsonb_array_length(classification_filter) = 0
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(classification_filter) AS selected_class(item)
            WHERE
              (
                selected_class.item->>'scope' = 'elementary'
                AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      WHEN 'array' THEN visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                      WHEN 'object' THEN jsonb_build_array(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      ELSE '[]'::jsonb
                    END
                  ) AS category(item)
                  WHERE category.item->>'@catId' = selected_class.item->>'code'
                )
              )
              OR (
                selected_class.item->>'scope' = 'classification'
                AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      WHEN 'array' THEN visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                      WHEN 'object' THEN jsonb_build_array(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      ELSE '[]'::jsonb
                    END
                  ) AS class_item(item)
                  WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                )
              )
          )
        )
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;

ALTER FUNCTION "api"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_keys AS (
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.flowproperties f
      WHERE ((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.flowproperties f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.flowproperties f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.flowproperties f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    latest_keys AS (
      SELECT DISTINCT ON (visible_keys.id)
        visible_keys.id,
        visible_keys.version,
        visible_keys.created_at,
        visible_keys.modified_at,
        visible_keys.team_id
      FROM visible_keys
      ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
    ),
    counted_keys AS (
      SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
      FROM latest_keys
    ),
    paged_keys AS (
      SELECT counted_keys.*
      FROM counted_keys
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
        END DESC NULLS LAST,
        counted_keys.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size
    )
    SELECT
      payload.id,
      payload.json,
      payload.version,
      payload.modified_at,
      payload.team_id,
      paged_keys.total_count
    FROM paged_keys
    JOIN public.flowproperties payload
      ON payload.id = paged_keys.id
     AND payload.version = paged_keys.version
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
      END DESC NULLS LAST,
      paged_keys.id;
END;
$_$;

ALTER FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE ((data_source = 'tg' AND l.state_code = 100) OR (data_source = 'ex' AND l.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'co'
        AND l.state_code = 200
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND l.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND l.team_id = team_id_filter
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;

ALTER FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_process_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "model_version" character, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT p.*
      FROM public.processes p
      WHERE ((data_source = 'tg' AND p.state_code = 100) OR (data_source = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'co'
        AND p.state_code = 200
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND p.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND p.team_id = team_id_filter
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT DISTINCT visible_rows.id
      FROM visible_rows
      WHERE
        coalesce(type_of_data_set_filter, 'all') = 'all'
        OR visible_rows.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id,
        visible_rows.model_id,
        visible_rows.model_version
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.model_id,
      counted_rows.model_version,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;

ALTER FUNCTION "api"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_source_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_keys AS (
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE ((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    latest_keys AS (
      SELECT DISTINCT ON (visible_keys.id)
        visible_keys.id,
        visible_keys.version,
        visible_keys.created_at,
        visible_keys.modified_at,
        visible_keys.team_id
      FROM visible_keys
      ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
    ),
    counted_keys AS (
      SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
      FROM latest_keys
    ),
    paged_keys AS (
      SELECT counted_keys.*
      FROM counted_keys
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
        END DESC NULLS LAST,
        counted_keys.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size
    )
    SELECT
      payload.id,
      payload.json,
      payload.version,
      payload.modified_at,
      payload.team_id,
      paged_keys.total_count
    FROM paged_keys
    JOIN public.sources payload
      ON payload.id = paged_keys.id
     AND payload.version = paged_keys.version
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
      END DESC NULLS LAST,
      paged_keys.id;
END;
$_$;

ALTER FUNCTION "api"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

CREATE OR REPLACE FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_keys AS (
      SELECT u.id, u.version, u.created_at, u.modified_at, u.team_id
      FROM public.unitgroups u
      WHERE ((data_source = 'tg' AND u.state_code = 100) OR (data_source = 'ex' AND u.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
        AND (team_id_filter IS NULL OR u.team_id = team_id_filter)
      UNION ALL
      SELECT u.id, u.version, u.created_at, u.modified_at, u.team_id
      FROM public.unitgroups u
      WHERE data_source = 'co'
        AND u.state_code = 200
        AND (team_id_filter IS NULL OR u.team_id = team_id_filter)
      UNION ALL
      SELECT u.id, u.version, u.created_at, u.modified_at, u.team_id
      FROM public.unitgroups u
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND u.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR u.state_code = state_code_filter)
      UNION ALL
      SELECT u.id, u.version, u.created_at, u.modified_at, u.team_id
      FROM public.unitgroups u
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND u.team_id = team_id_filter
        AND (state_code_filter IS NULL OR u.state_code = state_code_filter)
    ),
    latest_keys AS (
      SELECT DISTINCT ON (visible_keys.id)
        visible_keys.id,
        visible_keys.version,
        visible_keys.created_at,
        visible_keys.modified_at,
        visible_keys.team_id
      FROM visible_keys
      ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
    ),
    counted_keys AS (
      SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
      FROM latest_keys
    ),
    paged_keys AS (
      SELECT counted_keys.*
      FROM counted_keys
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
        END DESC NULLS LAST,
        counted_keys.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size
    )
    SELECT
      payload.id,
      payload.json,
      payload.version,
      payload.modified_at,
      payload.team_id,
      paged_keys.total_count
    FROM paged_keys
    JOIN public.unitgroups payload
      ON payload.id = paged_keys.id
     AND payload.version = paged_keys.version
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
      END DESC NULLS LAST,
      paged_keys.id;
END;
$_$;

ALTER FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "api"."hybrid_search_flow_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)),''),'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
begin
  if v_source not in ('tg','co','my','te','ex')
    or match_count is distinct from 200
    or page_size is null or page_size not between 1 and 100
    or page_current is null or page_current not between 1 and 400
    or match_threshold is null or match_threshold not between 0 and 1
    or lexical_weight is null or lexical_weight not between 0 and 1
    or semantic_weight is null or semantic_weight not between 0 and 1
    or lexical_weight + semantic_weight <= 0
    or rrf_k is null or rrf_k not between 1 and 1000
    or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object' then
    raise exception using errcode='22023',message='invalid version search request';
  end if;
  if v_source in ('my','te','ex') and v_actor is null then return; end if;
  return query
  with lexical as materialized (
    select candidate.*
    from private.lexical_version_candidates_v1('flow',query_text,query_terms,filter_condition,v_source) as candidate
    where lexical_weight > 0
  ), semantic as materialized (
    select candidate.*
    from private.semantic_flow_version_candidates_v1(
      query_embedding,filter_condition::text,match_threshold,200,v_source) as candidate
    where semantic_weight > 0
  ), fused as materialized (
    select coalesce(lexical.id,semantic.id) as id,
      coalesce(lexical.version,semantic.version) as version,
      coalesce(lexical_weight/(rrf_k+lexical.rank),0::double precision)
        + coalesce(semantic_weight/(rrf_k+semantic.rank),0::double precision) as score
    from lexical full outer join semantic
      on semantic.id=lexical.id and semantic.version=lexical.version
  ), hydrated as materialized (
    select source.id,source.json,source.version,source.modified_at,source.team_id,fused.score
    from fused join public.flows as source
      on source.id=fused.id and source.version::text=fused.version
    where (((v_source = 'tg' AND source.state_code = 100) OR (v_source = 'ex' AND source.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
      or (v_source='co' and source.state_code=200)
      or (v_source='my' and source.user_id=v_actor)
      or (v_source='te' and exists(
        select 1 from private.roles as membership
        where membership.user_id=v_actor and membership.team_id=source.team_id
          and membership.role::text in ('admin','member','owner')
      ))
  ), counted as (
    select hydrated.*,count(*) over()::bigint as total_count from hydrated
  )
  select rows.id,rows.json,rows.version,rows.modified_at,rows.team_id,rows.total_count
  from counted as rows
  order by rows.score desc,rows.id,rows.version desc
  limit page_size offset (page_current-1)*page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_flow_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_flow_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_flow_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "api"."hybrid_search_flow_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "authenticated";

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint, "semantic_route" "text", "semantic_candidate_population" integer, "semantic_fallback_used" boolean)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)), ''), 'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_filter jsonb := coalesce(filter_condition, '{}'::jsonb);
  v_residual jsonb;
  v_flow_types text[] := '{}'::text[];
  v_as_input boolean := false;
  v_classification jsonb := '[]'::jsonb;
  v_classification_codes text[] := '{}'::text[];
  v_elementary_codes text[] := '{}'::text[];
  v_query_embedding extensions.vector(1024);
begin
  if v_source not in ('tg', 'co', 'my', 'te', 'ex')
     or query_text is null or pg_catalog.btrim(query_text) = ''
     or match_count is distinct from 200
     or page_size is null or page_size not between 1 and 100
     or page_current is null or page_current not between 1 and 400
     or match_threshold is null or match_threshold not between 0 and 1
     or lexical_weight is null or lexical_weight not between 0 and 1
     or semantic_weight is null or semantic_weight not between 0 and 1
     or lexical_weight + semantic_weight <= 0
     or rrf_k is null or rrf_k not between 1 and 1000
     or pg_catalog.jsonb_typeof(v_filter) is distinct from 'object'
     or (state_code_filter < 0 and not (v_source = 'ex' and state_code_filter = -1))
     or (v_source = 'ex' and state_code_filter is not null and state_code_filter <> -1) then
    raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
  end if;

  if v_filter ? 'flowType' then
    if pg_catalog.jsonb_typeof(v_filter -> 'flowType') <> 'string' then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    select coalesce(pg_catalog.array_agg(value order by value), '{}'::text[])
    into v_flow_types
    from (
      select distinct nullif(pg_catalog.btrim(item), '') as value
      from pg_catalog.regexp_split_to_table(v_filter ->> 'flowType', ',') as item
    ) as values
    where value is not null;
    if pg_catalog.cardinality(v_flow_types) = 0 or exists (
      select 1 from pg_catalog.unnest(v_flow_types) as value
      where value not in ('Elementary flow', 'Product flow', 'Waste flow', 'Other flow')
    ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'asInput' then
    if pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'boolean'
       or (
         pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'string'
         and pg_catalog.lower(v_filter ->> 'asInput') in ('true', 'false')
       ) then
      v_as_input := (v_filter ->> 'asInput')::boolean;
    else
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'classification' then
    if pg_catalog.jsonb_typeof(v_filter -> 'classification') <> 'array'
       or pg_catalog.jsonb_array_length(v_filter -> 'classification') > 50
       or exists (
         select 1
         from pg_catalog.jsonb_array_elements(v_filter -> 'classification') as selected(item)
         where pg_catalog.jsonb_typeof(selected.item) <> 'object'
           or selected.item ->> 'scope' not in ('classification', 'elementary')
           or nullif(pg_catalog.btrim(selected.item ->> 'code'), '') is null
           or pg_catalog.length(selected.item ->> 'code') > 200
       ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    v_classification := v_filter -> 'classification';
    select
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'classification'), '{}'::text[]),
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'elementary'), '{}'::text[])
    into v_classification_codes, v_elementary_codes
    from pg_catalog.jsonb_array_elements(v_classification) as selected(item);
  end if;

  v_residual := v_filter - 'flowType' - 'asInput' - 'classification';

  if v_source in ('my', 'te', 'ex') and v_actor is null then return; end if;
  if v_source = 'te' and (
    team_id_filter is null
    or not private.dataset_search_can_read_team_filter(team_id_filter, v_actor)
  ) then
    return;
  end if;
  if (v_source = 'tg' and state_code_filter is not null and state_code_filter <> 100)
     or (v_source = 'co' and state_code_filter is not null and state_code_filter <> 200) then
    return;
  end if;

  v_query_embedding := query_embedding::extensions.vector(1024);

  return query
  with fused as materialized (
    select candidate.*
    from private.next_hybrid_version_keys_v2(
      'flow', query_text, query_terms, v_query_embedding,
      v_residual, null, v_flow_types, v_as_input,
      v_classification_codes, v_elementary_codes, match_threshold,
      lexical_weight, semantic_weight, rrf_k, v_source,
      state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.flows as source
      on source.id = fused.id
     and source.version::text = fused.version
    where (
        (((v_source = 'tg' AND source.state_code = 100) OR (v_source = 'ex' AND source.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'co' and source.state_code = 200
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'my' and source.user_id = v_actor
          and (state_code_filter is null or source.state_code = state_code_filter))
        or (v_source = 'te' and source.team_id = team_id_filter
          and (state_code_filter is null or source.state_code = state_code_filter)
          and private.dataset_search_can_read_team_filter(team_id_filter, v_actor))
      )
      and (v_residual = '{}'::jsonb or source.json @> v_residual)
      and (
        pg_catalog.cardinality(v_flow_types) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any(v_flow_types)
      )
      and (
        not v_as_input
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality(v_classification_codes) = 0
          and pg_catalog.cardinality(v_elementary_codes) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && v_classification_codes
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && v_elementary_codes
      )
  ), counted as (
    select hydrated.*, pg_catalog.count(*) over()::bigint as total_count
    from hydrated
  )
  select
    rows.id,
    rows.json,
    rows.version,
    rows.modified_at,
    rows.team_id,
    rows.total_count,
    rows.semantic_route,
    rows.semantic_candidate_population,
    rows.semantic_fallback_used
  from counted as rows
  order by rows.score desc, rows.id, rows.version desc
  limit page_size offset (page_current - 1) * page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "authenticated";

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "model_version" character, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)),''),'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
begin
  if v_source not in ('tg','co','my','te','ex')
    or match_count is distinct from 200
    or page_size is null or page_size not between 1 and 100
    or page_current is null or page_current not between 1 and 400
    or match_threshold is null or match_threshold not between 0 and 1
    or lexical_weight is null or lexical_weight not between 0 and 1
    or semantic_weight is null or semantic_weight not between 0 and 1
    or lexical_weight + semantic_weight <= 0
    or rrf_k is null or rrf_k not between 1 and 1000
    or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object' then
    raise exception using errcode='22023',message='invalid version search request';
  end if;
  if v_source in ('my','te','ex') and v_actor is null then return; end if;
  return query
  with lexical as materialized (
    select candidate.*
    from private.lexical_version_candidates_v1('process',query_text,query_terms,filter_condition,v_source) as candidate
    where lexical_weight > 0
  ), semantic as materialized (
    select candidate.*
    from private.semantic_process_version_candidates_v1(
      query_embedding,filter_condition::text,match_threshold,200,v_source) as candidate
    where semantic_weight > 0
  ), fused as materialized (
    select coalesce(lexical.id,semantic.id) as id,
      coalesce(lexical.version,semantic.version) as version,
      coalesce(lexical_weight/(rrf_k+lexical.rank),0::double precision)
        + coalesce(semantic_weight/(rrf_k+semantic.rank),0::double precision) as score
    from lexical full outer join semantic
      on semantic.id=lexical.id and semantic.version=lexical.version
  ), hydrated as materialized (
    select source.id,source.json,source.version,source.modified_at,source.model_id,source.model_version,source.team_id,fused.score
    from fused join public.processes as source
      on source.id=fused.id and source.version::text=fused.version
    where (((v_source = 'tg' AND source.state_code = 100) OR (v_source = 'ex' AND source.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
      or (v_source='co' and source.state_code=200)
      or (v_source='my' and source.user_id=v_actor)
      or (v_source='te' and exists(
        select 1 from private.roles as membership
        where membership.user_id=v_actor and membership.team_id=source.team_id
          and membership.role::text in ('admin','member','owner')
      ))
  ), counted as (
    select hydrated.*,count(*) over()::bigint as total_count from hydrated
  )
  select rows.id,rows.json,rows.version,rows.modified_at,rows.model_id,rows.model_version,rows.team_id,rows.total_count
  from counted as rows
  order by rows.score desc,rows.id,rows.version desc
  limit page_size offset (page_current-1)*page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "authenticated";

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid", "type_of_data_set_filter" "text" DEFAULT NULL::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "model_version" character, "team_id" "uuid", "total_count" bigint, "semantic_route" "text", "semantic_candidate_population" integer, "semantic_fallback_used" boolean)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)), ''), 'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_process_type text := nullif(pg_catalog.btrim(type_of_data_set_filter), '');
  v_query_embedding extensions.vector(1024);
begin
  if v_process_type = 'all' then v_process_type := null; end if;
  if v_source not in ('tg', 'co', 'my', 'te', 'ex')
     or query_text is null or pg_catalog.btrim(query_text) = ''
     or match_count is distinct from 200
     or page_size is null or page_size not between 1 and 100
     or page_current is null or page_current not between 1 and 400
     or match_threshold is null or match_threshold not between 0 and 1
     or lexical_weight is null or lexical_weight not between 0 and 1
     or semantic_weight is null or semantic_weight not between 0 and 1
     or lexical_weight + semantic_weight <= 0
     or rrf_k is null or rrf_k not between 1 and 1000
     or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object'
     or (state_code_filter < 0 and not (v_source = 'ex' and state_code_filter = -1))
     or (v_source = 'ex' and state_code_filter is not null and state_code_filter <> -1)
     or (
       v_process_type is not null
       and v_process_type not in (
         'Unit process, single operation',
         'Unit process, black box',
         'LCI result',
         'Partly terminated system',
         'Avoided product system'
       )
     ) then
    raise exception using errcode = '22023', message = 'invalid Next Process Hybrid V2 request';
  end if;
  if v_source in ('my', 'te', 'ex') and v_actor is null then return; end if;
  if v_source = 'te' and (
    team_id_filter is null
    or not private.dataset_search_can_read_team_filter(team_id_filter, v_actor)
  ) then
    return;
  end if;
  if (v_source = 'tg' and state_code_filter is not null and state_code_filter <> 100)
     or (v_source = 'co' and state_code_filter is not null and state_code_filter <> 200) then
    return;
  end if;

  v_query_embedding := query_embedding::extensions.vector(1024);

  return query
  with fused as materialized (
    select candidate.*
    from private.next_hybrid_version_keys_v2(
      'process', query_text, query_terms, v_query_embedding,
      filter_condition, v_process_type, '{}'::text[], false,
      '{}'::text[], '{}'::text[], match_threshold, lexical_weight,
      semantic_weight, rrf_k, v_source, state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.model_id,
      source.model_version,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.processes as source
      on source.id = fused.id
     and source.version::text = fused.version
    where (
        (((v_source = 'tg' AND source.state_code = 100) OR (v_source = 'ex' AND source.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'co' and source.state_code = 200
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'my' and source.user_id = v_actor
          and (state_code_filter is null or source.state_code = state_code_filter))
        or (v_source = 'te' and source.team_id = team_id_filter
          and (state_code_filter is null or source.state_code = state_code_filter)
          and private.dataset_search_can_read_team_filter(team_id_filter, v_actor))
      )
      and (filter_condition = '{}'::jsonb or source.json @> filter_condition)
      and (
        v_process_type is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = v_process_type
      )
  ), counted as (
    select hydrated.*, pg_catalog.count(*) over()::bigint as total_count
    from hydrated
  )
  select
    rows.id,
    rows.json,
    rows.version,
    rows.modified_at,
    rows.model_id,
    rows.model_version,
    rows.team_id,
    rows.total_count,
    rows.semantic_route,
    rows.semantic_candidate_population,
    rows.semantic_fallback_used
  from counted as rows
  order by rows.score desc, rows.id, rows.version desc
  limit page_size offset (page_current - 1) * page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") TO "authenticated";

RESET ROLE;
CREATE OR REPLACE FUNCTION "api"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank,
			f.id,
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM contacts f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$$;

ALTER FUNCTION "api"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank,
			f.id,
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM flowproperties f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$$;

ALTER FUNCTION "api"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$

DECLARE
	filter_condition_jsonb JSONB;
	flowType TEXT;
	flowTypeArray TEXT[];
	asInput BOOLEAN;
	use_base_name_order boolean := false;
	use_common_category_order boolean := false;
	use_zh_icu_order boolean := false;
	order_by_jsonb jsonb;
	order_key text;
	order_lang text;
	order_dir text;
	order_lang_norm text;
BEGIN
	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:category","order":"asc"}

	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	flowType := NULLIF(btrim(filter_condition_jsonb->>'flowType'), '');
	IF flowType IS NOT NULL THEN
		flowTypeArray := string_to_array(flowType, ',');
	ELSE
		flowTypeArray := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'flowType';

	IF filter_condition_jsonb ? 'asInput' THEN
		asInput := NULLIF(btrim(filter_condition_jsonb->>'asInput'), '')::BOOLEAN;
	ELSE
		asInput := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'asInput';

	-- order_by 解析
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_category_order := (order_key = 'common:category');
	ELSE
		use_base_name_order := false;
		use_common_category_order := false;
		order_lang := 'en';
		order_dir := 'asc';
	END IF;

	order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
	use_zh_icu_order := (order_lang_norm LIKE 'zh%');

	RETURN QUERY
		WITH filtered AS (
			SELECT
				f.id,
				f.json,
				f.version,
				f.modified_at,
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cat.category_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_category_order THEN cat.category_name
				END AS order_value
			FROM flows f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = order_lang
								LIMIT 1
							),
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = 'en'
								LIMIT 1
							),
							COALESCE(
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_category_order THEN COALESCE(
							(
								SELECT string_agg(cat_item->>'#text', ' / ' ORDER BY cat_level ASC)
								FROM (
									SELECT
										cat_item,
										CASE
											WHEN (cat_item->>'@level') ~ '^\\d+$' THEN (cat_item->>'@level')::int
											ELSE 2147483647
										END AS cat_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:elementaryFlowCategorization'
												-> 'common:category'
										)
											WHEN 'array' THEN (
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											ELSE '[]'::jsonb
										END
									) AS cat_item
								) ordered_cat
							),
							''
						)
					END AS category_name
			) cat
			WHERE f.json @> filter_condition_jsonb
				AND f.json &@~ query_text
				AND (
					(((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
					OR (data_source = 'co' AND state_code = 200)
					OR (data_source = 'my' AND user_id = auth.uid())
					OR (
						data_source = 'te'
						AND EXISTS (
							SELECT 1
							FROM roles r
							WHERE r.user_id = auth.uid()
								AND r.team_id = f.team_id
								AND r.role::text IN ('admin', 'member', 'owner')
						)
					)
				)
				AND (
					flowType IS NULL
					OR flowType = ''
					OR (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = ANY(flowTypeArray)
				)
				AND (
					asInput IS NULL
					OR asInput = false
					OR NOT(
						f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text": "Emissions", "@level": "0"}]}}}}}}'
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
	END;

$_$;

ALTER FUNCTION "api"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  filter_condition_jsonb JSONB;
  use_base_name_order boolean := false;
  use_common_class_order boolean := false;
  use_zh_icu_order boolean := false;
  order_by_jsonb jsonb;
  order_key text;
  order_lang text;
  order_dir text;
  order_lang_norm text;
BEGIN
  -- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:class","order":"asc"}

  filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

  IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
    order_by_jsonb := order_by::jsonb;

    order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
    order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
    order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
    IF order_dir NOT IN ('asc', 'desc') THEN
      order_dir := 'asc';
    END IF;

    use_base_name_order := (order_key = 'basename');
    use_common_class_order := (order_key = 'common:class');
  ELSE
    use_base_name_order := false;
    use_common_class_order := false;
    order_lang := 'en';
    order_dir := 'asc';
  END IF;

  order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
  use_zh_icu_order := (order_lang_norm LIKE 'zh%');

  RETURN QUERY
    WITH filtered AS (
      SELECT
        f.id,
        f.json,
        f.version,
        f.modified_at,
        pgroonga_score(f.tableoid, f.ctid) AS score,
        bn.base_name,
        cls.class_name,
        CASE
          WHEN use_base_name_order THEN bn.base_name
          WHEN use_common_class_order THEN cls.class_name
        END AS order_value
      FROM lifecyclemodels f
      CROSS JOIN LATERAL (
        SELECT
          CASE
            WHEN use_base_name_order THEN COALESCE(
              (
                SELECT bn_item->>'#text'
                FROM jsonb_array_elements(
                  CASE jsonb_typeof(
                    f.json
                      -> 'lifeCycleModelDataSet'
                      -> 'lifeCycleModelInformation'
                      -> 'dataSetInformation'
                      -> 'name'
                      -> 'baseName'
                  )
                    WHEN 'array' THEN (
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    WHEN 'object' THEN jsonb_build_array(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    ELSE '[]'::jsonb
                  END
                ) AS bn_item
                WHERE bn_item->>'@xml:lang' = order_lang
                LIMIT 1
              ),
              (
                SELECT bn_item->>'#text'
                FROM jsonb_array_elements(
                  CASE jsonb_typeof(
                    f.json
                      -> 'lifeCycleModelDataSet'
                      -> 'lifeCycleModelInformation'
                      -> 'dataSetInformation'
                      -> 'name'
                      -> 'baseName'
                  )
                    WHEN 'array' THEN (
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    WHEN 'object' THEN jsonb_build_array(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    ELSE '[]'::jsonb
                  END
                ) AS bn_item
                WHERE bn_item->>'@xml:lang' = 'en'
                LIMIT 1
              ),
              COALESCE(
                f.json #>> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName,0,#text}',
                f.json #>> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName,#text}'
              ),
              ''
            )
          END AS base_name
      ) bn
      CROSS JOIN LATERAL (
        SELECT
          CASE
            WHEN use_common_class_order THEN COALESCE(
              (
                SELECT string_agg(cls_item->>'#text', ' / ' ORDER BY cls_level ASC)
                FROM (
                  SELECT
                    cls_item,
                    CASE
                      WHEN (cls_item->>'@level') ~ '^\\d+$' THEN (cls_item->>'@level')::int
                      ELSE 2147483647
                    END AS cls_level
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'classificationInformation'
                        -> 'common:classification'
                        -> 'common:class'
                    )
                      WHEN 'array' THEN (
                        f.json
                          -> 'lifeCycleModelDataSet'
                          -> 'lifeCycleModelInformation'
                          -> 'dataSetInformation'
                          -> 'classificationInformation'
                          -> 'common:classification'
                          -> 'common:class'
                    )
                      WHEN 'object' THEN jsonb_build_array(
                        f.json
                          -> 'lifeCycleModelDataSet'
                          -> 'lifeCycleModelInformation'
                          -> 'dataSetInformation'
                          -> 'classificationInformation'
                          -> 'common:classification'
                          -> 'common:class'
                    )
                      ELSE '[]'::jsonb
                    END
                  ) AS cls_item
                ) ordered_cls
              ),
              ''
            )
          END AS class_name
      ) cls
      WHERE f.json @> filter_condition_jsonb
        AND f.json &@~ query_text
        AND (
          (((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
          OR (data_source = 'co' AND state_code = 200)
          OR (data_source = 'my' AND user_id = auth.uid())
          OR (
            data_source = 'te'
            AND EXISTS (
              SELECT 1
              FROM roles r
              WHERE r.user_id = auth.uid()
                AND r.team_id = f.team_id
                AND r.role::text IN ('admin', 'member', 'owner')
            )
          )
        )
    )
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
          (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
          CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
          CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
          f2.score DESC,
          f2.modified_at DESC,
          f2.id
      ) AS rank,
      f2.id,
      f2.json,
      f2.version,
      f2.modified_at,
      COUNT(*) OVER() AS total_count
    FROM filtered f2
    ORDER BY
      (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
      (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
      CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
      CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
      f2.score DESC,
      f2.modified_at DESC,
      f2.id
    LIMIT page_size
    OFFSET (page_current - 1) * page_size;
END;
$_$;

ALTER FUNCTION "api"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
DECLARE
    filter_condition_jsonb JSONB;
    use_base_name_order boolean := false;
	use_common_class_order boolean := false;
	use_zh_icu_order boolean := false;
    order_by_jsonb jsonb;
    order_key text;
    order_lang text;
    order_dir text;
	order_lang_norm text;
BEGIN
	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:class","order":"asc"}
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_class_order := (order_key = 'common:class');
	ELSE
		use_base_name_order := false;
		use_common_class_order := false;
		order_lang := 'en';
		order_dir := 'asc';
	END IF;

	order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
	use_zh_icu_order := (order_lang_norm LIKE 'zh%');

  RETURN QUERY
		WITH filtered AS (
			SELECT
				f.id,
				f.json,
				f.version,
				f.modified_at,
				f.model_id,
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cls.class_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_class_order THEN cls.class_name
				END AS order_value
			FROM processes f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = order_lang
								LIMIT 1
							),
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = 'en'
								LIMIT 1
							),
							COALESCE(
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_class_order THEN COALESCE(
							(
								SELECT string_agg(cls_item->>'#text', ' / ' ORDER BY cls_level ASC)
								FROM (
									SELECT
										cls_item,
										CASE
											WHEN (cls_item->>'@level') ~ '^\\d+$' THEN (cls_item->>'@level')::int
											ELSE 2147483647
										END AS cls_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:classification'
												-> 'common:class'
										)
											WHEN 'array' THEN (
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											ELSE '[]'::jsonb
										END
									) AS cls_item
								) ordered_cls
							),
							''
						)
					END AS class_name
			) cls
			WHERE f.json @> filter_condition_jsonb
				AND f.json &@~ query_text
				AND (
					(((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
					OR (data_source = 'co' AND state_code = 200)
					OR (data_source = 'my' AND user_id = auth.uid())
					OR (
						data_source = 'te'
						AND EXISTS (
							SELECT 1
							FROM roles r
							WHERE r.user_id = auth.uid()
								AND r.team_id = f.team_id
								AND r.role::text IN ('admin', 'member', 'owner')
						)
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			f2.model_id,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
END;
$_$;

ALTER FUNCTION "api"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank,
			f.id,
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM sources f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((((data_source = 'tg' AND state_code = 100) OR (data_source = 'ex' AND state_code = -1 AND (SELECT auth.uid()) IS NOT NULL))) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$$;

ALTER FUNCTION "api"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
 filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
  SELECT
   RANK () OVER (ORDER BY extensions.pgroonga_score(f.tableoid, f.ctid) DESC) AS rank,
   f.id,
   f.json,
   f.version,
   f.modified_at,
   COUNT(*) OVER() AS total_count
  FROM public.unitgroups f
  WHERE f.json @> filter_condition_jsonb
    AND f.json &@~ query_text
    AND (
         (((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
         OR
         (data_source = 'my' AND f.user_id::text = this_user_id)
        )
  ORDER BY extensions.pgroonga_score(f.tableoid, f.ctid) DESC
  LIMIT page_size
  OFFSET (page_current -1) * page_size;
END;
$$;

ALTER FUNCTION "api"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := case
    when coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(this_user_id)::uuid
    else null::uuid
  end;
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $8'
  end;

  if exact_query_id is not null then
    v_sql := format($sql$
      with matched_ids as (
        select d.id, 1.0::double precision as search_score
        from %1$s d
        where d.id = $1
          and (
            ((($4 = 'tg' AND d.state_code = 100) OR ($4 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($6 is null or d.team_id = $6))
            or ($4 = 'co' and d.state_code = 200 and ($6 is null or d.team_id = $6))
            or ($4 = 'my' and $5 is not null and d.user_id = $5 and ($7 is null or d.state_code = $7))
            or ($4 = 'te' and $6 is not null and d.team_id = $6 and ($7 is null or d.state_code = $7))
          )
          %2$s
        group by d.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select d2.json, d2.version, d2.modified_at, d2.team_id
          from %1$s d2
          where d2.id = matched_ids.id
            and (
              ((($4 = 'tg' AND d2.state_code = 100) OR ($4 = 'ex' AND d2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($6 is null or d2.team_id = $6))
              or ($4 = 'co' and d2.state_code = 200 and ($6 is null or d2.team_id = $6))
              or ($4 = 'my' and $5 is not null and d2.user_id = $5 and ($7 is null or d2.state_code = $7))
              or ($4 = 'te' and $6 is not null and d2.team_id = $6 and ($7 is null or d2.state_code = $7))
            )
          order by d2.version desc, d2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit $2
      offset ($3 - 1) * $2
    $sql$, p_table, json_filter_clause);

    return query execute v_sql
      using exact_query_id, normalized_page_size, normalized_page_current,
            data_source, normalized_this_user_id, team_id_filter, state_code_filter,
            filter_condition_jsonb;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select d.id,
             d.json,
             d.state_code,
             d.team_id,
             d.user_id,
             pgroonga_score(d.tableoid, d.ctid) as search_score
      from %1$s d
      where d.search_text &@~ $1
    ),
    matched_ids as (
      select d.id, max(d.search_score) as search_score
      from text_matches d
      where (
          ((($5 = 'tg' AND d.state_code = 100) OR ($5 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or d.team_id = $7))
          or ($5 = 'co' and d.state_code = 200 and ($7 is null or d.team_id = $7))
          or ($5 = 'my' and $6 is not null and d.user_id = $6 and ($8 is null or d.state_code = $8))
          or ($5 = 'te' and $7 is not null and d.team_id = $7 and ($8 is null or d.state_code = $8))
        )
        %2$s
      group by d.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select d2.json, d2.version, d2.modified_at, d2.team_id
        from %1$s d2
        where d2.id = matched_ids.id
          and (
            ((($5 = 'tg' AND d2.state_code = 100) OR ($5 = 'ex' AND d2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or d2.team_id = $7))
            or ($5 = 'co' and d2.state_code = 200 and ($7 is null or d2.team_id = $7))
            or ($5 = 'my' and $6 is not null and d2.user_id = $6 and ($8 is null or d2.state_code = $8))
            or ($5 = 'te' and $7 is not null and d2.team_id = $7 and ($8 is null or d2.state_code = $8))
          )
        order by d2.version desc, d2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, p_table, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          data_source, normalized_this_user_id, team_id_filter, state_code_filter;
end;
$_$;

ALTER FUNCTION "api"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
  query_embedding_vector  vector(1024);
  filter_condition_jsonb  jsonb;
  flowType                text;
  flowTypeArray           text[];
  asInput                 boolean;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量转 halfvec(384)
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;
  flowType               := filter_condition_jsonb->>'flowType';
  flowTypeArray          := string_to_array(flowType, ',');
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  asInput                := (filter_condition_jsonb->'asInput')::boolean;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  -- 3) 两阶段：先 HNSW 候选，再业务过滤
  RETURN QUERY
  WITH cand AS (
    SELECT
      f.id,
      f.json,
      f.version,
      f.modified_at,
      f.embedding_ft,
      f.state_code,
      f.user_id,
      f.team_id
    FROM public.flows f
    ORDER BY f.embedding_ft <=> query_embedding_vector
    LIMIT candidate_size
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand c
    WHERE
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      AND c.json @> filter_condition_jsonb
      AND (
           (((data_source = 'tg' AND c.state_code = 100) OR (data_source = 'ex' AND c.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
      AND (
        flowType IS NULL
        OR flowType = ''
        OR (c.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = ANY(flowTypeArray)
      )
      AND (
        asInput IS NULL
        OR asInput = false
        OR NOT (
          c.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
        )
      )
  )
  SELECT
  RANK() OVER (ORDER BY f2.dist) AS "rank",
  f2.id,
  f2.json,
  f2.version,
  f2.modified_at,
  COUNT(*) OVER()               AS total_count
FROM final AS f2
ORDER BY f2.dist
LIMIT match_count;
END;
$$;

ALTER FUNCTION "api"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
  query_embedding_vector  vector(1024);
  filter_condition_jsonb  jsonb;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量入参 -> vector(384)
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;

  -- 3) 两阶段：先用向量索引取候选，再应用阈值/过滤/权限，最后排序分页
  RETURN QUERY
  WITH cand AS (
    SELECT
      m.id,
      m.json,
      m.version,
      m.modified_at,
      m.embedding_ft,
      m.state_code,
      m.user_id
    FROM public.lifecyclemodels AS m
    ORDER BY m.embedding_ft <=> query_embedding_vector
    LIMIT candidate_size
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand AS c
    WHERE
      -- 向量阈值（在候选集上应用）
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      -- JSON 过滤
      AND c.json @> filter_condition_jsonb
      -- data_source 访问控制（与原逻辑一致）
      AND (
           (((data_source = 'tg' AND c.state_code = 100) OR (data_source = 'ex' AND c.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
  )
  SELECT
    RANK() OVER (ORDER BY f2.dist) AS "rank",
    f2.id,
    f2.json,
    f2.version,
    f2.modified_at,
    COUNT(*) OVER()               AS total_count
  FROM final AS f2
  ORDER BY f2.dist
  LIMIT match_count;
END;
$$;

ALTER FUNCTION "api"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
DECLARE
  query_embedding_vector  vector(1024);   -- 若列为 halfvec(384)，这里改成 halfvec(384)
  filter_condition_jsonb  jsonb;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量入参转 vector(384)（或 halfvec(384)）
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;

  -- 3) 两阶段：先按相似度取候选（命中向量索引），再在候选上施加全部业务过滤/阈值
  RETURN QUERY
  WITH cand AS (
    SELECT
      p.id,
      p.json,
      p.version,
      p.modified_at,
      p.embedding_ft,
      p.state_code,
      p.user_id
    FROM public.processes AS p
    ORDER BY p.embedding_ft <=> query_embedding_vector
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand AS c
    WHERE
      -- 向量阈值（在候选集上应用）
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      -- JSON 过滤
      AND c.json @> filter_condition_jsonb
      -- data_source 访问控制（保持你原逻辑）
      AND (
           (((data_source = 'tg' AND c.state_code = 100) OR (data_source = 'ex' AND c.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
  )
  SELECT
    RANK() OVER (ORDER BY f2.dist) AS "rank",
    f2.id,
    f2.json,
    f2.version,
    f2.modified_at,
    COUNT(*) OVER()               AS total_count
  FROM final AS f2
  ORDER BY f2.dist
  LIMIT match_count;
END;
$$;

ALTER FUNCTION "api"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_request_key text := nullif(btrim(p_request_key), '');
  v_roots jsonb := '[]'::jsonb;
  v_root_count integer := 0;
  v_exportable_count integer := 0;
  v_request_payload jsonb;
  v_cache private.lca_package_request_cache%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
  v_resolved_job_id uuid;
  v_worker_status text;
begin
  if p_requested_by is null or p_job_id is null or v_request_key is null
     or v_scope not in ('current_user', 'open_data', 'current_user_and_open_data', 'selected_roots')
     or jsonb_typeof(coalesce(p_roots, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object('ok', false, 'code', 'INVALID_PACKAGE_EXPORT_REQUEST', 'status', 400);
  end if;
  if jsonb_array_length(coalesce(p_roots, '[]'::jsonb)) > 500 then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_ROOT_LIMIT_EXCEEDED', 'status', 400);
  end if;
  if v_scope not in ('current_user', 'selected_roots') and not exists (
    select 1 from private.roles
    where user_id = p_requested_by
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin')
  ) then
    return jsonb_build_object('ok', false, 'code', 'EXPORT_SCOPE_FORBIDDEN', 'status', 403);
  end if;

  if exists (
    select 1
    from jsonb_array_elements(coalesce(p_roots, '[]'::jsonb)) as root(value)
    where jsonb_typeof(root.value) <> 'object'
      or root.value ->> 'table' not in (
        'contacts', 'sources', 'unitgroups', 'flowproperties',
        'flows', 'processes', 'lifecyclemodels'
      )
      or coalesce(root.value ->> 'id', '') !~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
      or nullif(btrim(root.value ->> 'version'), '') is null
  ) then
    return jsonb_build_object('ok', false, 'code', 'INVALID_PACKAGE_ROOT', 'status', 400);
  end if;

  with normalized as (
    select distinct
      root.value ->> 'table' as table_name,
      lower(root.value ->> 'id')::uuid as id,
      btrim(root.value ->> 'version') as version
    from jsonb_array_elements(coalesce(p_roots, '[]'::jsonb)) as root(value)
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'table', normalized.table_name,
      'id', normalized.id,
      'version', normalized.version
    ) order by normalized.table_name, normalized.id, normalized.version), '[]'::jsonb),
    count(*)
  into v_roots, v_root_count
  from normalized;

  if (v_scope = 'selected_roots') <> (v_root_count > 0) then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_SCOPE_ROOTS_MISMATCH', 'status', 400);
  end if;

  if v_scope = 'selected_roots' then
    with requested as (
      select
        root.value ->> 'table' as table_name,
        (root.value ->> 'id')::uuid as id,
        root.value ->> 'version' as version
      from jsonb_array_elements(v_roots) as root(value)
    ), datasets as (
      select 'contacts'::text as table_name, id, version, user_id, state_code from public.contacts
      union all select 'sources', id, version, user_id, state_code from public.sources
      union all select 'unitgroups', id, version, user_id, state_code from public.unitgroups
      union all select 'flowproperties', id, version, user_id, state_code from public.flowproperties
      union all select 'flows', id, version, user_id, state_code from public.flows
      union all select 'processes', id, version, user_id, state_code from public.processes
      union all select 'lifecyclemodels', id, version, user_id, state_code from public.lifecyclemodels
    )
    select count(*)
    into v_exportable_count
    from requested
    join datasets using (table_name, id, version)
    where datasets.user_id = p_requested_by
       or datasets.state_code = -1
       or datasets.state_code between 100 and 199;

    if v_exportable_count <> v_root_count then
      return jsonb_build_object('ok', false, 'code', 'ROOT_EXPORT_FORBIDDEN', 'status', 403);
    end if;
  end if;

  v_request_payload := coalesce(p_request_payload, '{}'::jsonb) || jsonb_build_object(
    'scope', v_scope,
    'roots', v_roots
  );

  perform pg_advisory_xact_lock(hashtextextended(
    p_requested_by::text || ':export_package:' || v_request_key, 0
  ));
  select * into v_cache from private.lca_package_request_cache
  where requested_by = p_requested_by and operation = 'export_package' and request_key = v_request_key
  for update;
  if v_cache.id is not null then
    update private.lca_package_request_cache set
      hit_count = hit_count + 1, last_accessed_at = now(), updated_at = now()
    where id = v_cache.id returning * into v_cache;

    -- selected_roots contains exact immutable dataset identities, so its ready
    -- artifact remains reusable. The other scopes describe mutable datasets and
    -- must reach worker_enqueue_job after completion to allocate fresh work.
    if v_scope = 'selected_roots'
       and v_cache.status = 'ready'
       and v_cache.job_id is not null then
      return jsonb_build_object(
        'ok', true,
        'mode', 'cache_hit',
        'job_id', v_cache.job_id,
        'worker_job_id', v_cache.worker_job_id
      );
    end if;

    if v_cache.worker_job_id is not null then
      select * into v_worker from private.worker_jobs where id = v_cache.worker_job_id;
      if v_worker.status in ('queued', 'running', 'waiting', 'stale', 'completed', 'blocked') then
        if v_worker.status = 'blocked' then
          update private.lca_package_request_cache
          set status = 'failed', updated_at = now()
          where id = v_cache.id
          returning * into v_cache;
        end if;

        if v_worker.status <> 'completed' or v_scope = 'selected_roots' then
          return jsonb_build_object(
            'ok', true,
            'mode', case
              when v_worker.status = 'completed' then 'cache_hit'
              when v_worker.status = 'blocked' then 'blocked'
              else 'in_progress'
            end,
            'job_id', v_cache.job_id,
            'worker_job_id', v_cache.worker_job_id
          );
        end if;
      end if;
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => 'tidas.export_package',
    p_payload_json => jsonb_build_object(
      'type', 'export_package', 'job_id', p_job_id, 'requested_by', p_requested_by,
      'scope', v_scope, 'roots', v_roots
    ),
    p_payload_schema_version => 'tidas.export_package.request.v1',
    p_subject_type => 'lca_package_job',
    p_subject_id => p_job_id,
    p_subject_version => v_scope,
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => p_idempotency_key,
    p_request_hash => v_request_key,
    p_queue_key => v_scope,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;
  v_resolved_job_id := coalesce(
    nullif(v_enqueue #>> '{data,payload,job_id}', '')::uuid,
    nullif(v_enqueue #>> '{data,subjectId}', '')::uuid,
    p_job_id
  );
  v_worker_status := v_enqueue #>> '{data,status}';

  insert into private.lca_package_request_cache as cache (
    requested_by, operation, request_key, request_payload, status,
    job_id, worker_job_id, hit_count, last_accessed_at, created_at, updated_at
  ) values (
    p_requested_by, 'export_package', v_request_key, v_request_payload,
    case when v_worker_status = 'blocked' then 'failed' else 'pending' end,
    v_resolved_job_id, v_worker_id, 1, now(), now(), now()
  ) on conflict (requested_by, operation, request_key) do update set
    request_payload = excluded.request_payload,
    status = excluded.status, job_id = excluded.job_id, worker_job_id = excluded.worker_job_id,
    error_code = null, error_message = null,
    hit_count = cache.hit_count + 1, last_accessed_at = now(), updated_at = now()
  returning * into v_cache;

  return jsonb_build_object(
    'ok', true,
    'mode', case
      when v_worker_status = 'blocked' then 'blocked'
      when coalesce((v_enqueue ->> 'reused')::boolean, false) then 'in_progress'
      else 'queued'
    end,
    'job_id', v_cache.job_id,
    'worker_job_id', v_cache.worker_job_id,
    'scope', v_scope,
    'root_count', v_root_count
  );
end
$_$;

ALTER FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") TO "service_role";

CREATE OR REPLACE FUNCTION "private"."hybrid_search_flows_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(lexical_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from api.search_flows_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer,
        query_terms
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_flow_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select f.*
      from public.flows f
      join fused on fused.id = f.id
      where (
        (((data_source = 'tg' AND f.state_code = 100) OR (data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        or (data_source = 'co' and f.state_code = 200)
        or (data_source = 'my' and f.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from private.roles r
            where r.user_id = auth.uid()
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

ALTER FUNCTION "private"."hybrid_search_flows_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."hybrid_search_flows_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."hybrid_search_flows_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."hybrid_search_flows_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."hybrid_search_lifecyclemodels_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(lexical_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from api.search_lifecyclemodels_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer,
        query_terms
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_lifecyclemodel_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select l.*
      from public.lifecyclemodels l
      join fused on fused.id = l.id
      where (
        (((data_source = 'tg' AND l.state_code = 100) OR (data_source = 'ex' AND l.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        or (data_source = 'co' and l.state_code = 200)
        or (data_source = 'my' and l.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from private.roles r
            where r.user_id = auth.uid()
              and r.team_id = l.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

ALTER FUNCTION "private"."hybrid_search_lifecyclemodels_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."hybrid_search_lifecyclemodels_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."hybrid_search_lifecyclemodels_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."hybrid_search_lifecyclemodels_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."hybrid_search_processes_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(lexical_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from api.search_processes_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer,
        'all',
        query_terms
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_process_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select p.*
      from public.processes p
      join fused on fused.id = p.id
      where (
        (((data_source = 'tg' AND p.state_code = 100) OR (data_source = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)))
        or (data_source = 'co' and p.state_code = 200)
        or (data_source = 'my' and p.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from private.roles r
            where r.user_id = auth.uid()
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.model_id,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.model_id,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

ALTER FUNCTION "private"."hybrid_search_processes_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."hybrid_search_processes_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."hybrid_search_processes_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."hybrid_search_processes_v2_impl"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."hybrid_search_simple_dataset_v2"("p_table" "regclass", "query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $_$
declare
  normalized_data_source text;
  normalized_match_count integer;
  semantic_match_count integer;
  normalized_page_size integer;
  normalized_page_current integer;
  candidate_limit integer;
  normalized_rrf_k integer;
  filter_condition_jsonb jsonb;
  escaped_query_terms text[];
  effective_user_id uuid;
  can_read_team_filter boolean;
  visibility_clause text;
  json_filter_clause text;
  text_match_clause text;
  text_weight double precision;
  hybrid_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported hybrid dataset table: %', p_table;
  end if;

  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 20), 1), 200);
  normalized_page_size := least(greatest(coalesce(page_size, 10), 1), 200);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  semantic_match_count := greatest(normalized_match_count, normalized_page_size);
  candidate_limit := least(greatest(normalized_match_count, normalized_page_size) * 10, 5000);
  normalized_rrf_k := greatest(coalesce(rrf_k, 10), 1);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  effective_user_id := private.dataset_search_effective_user_id('');
  can_read_team_filter := private.dataset_search_can_read_team_filter(
    team_id_filter,
    effective_user_id
  );
  text_weight := coalesce(lexical_weight, 0);

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100 and ($5::uuid is null or d.team_id = $5)';
  elsif normalized_data_source = 'ex' then
    if auth.uid() is null then return; end if;
    visibility_clause := 'd.state_code = -1 and ($5::uuid is null or d.team_id = $5)';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200 and ($5::uuid is null or d.team_id = $5)';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $4 and ($6::integer is null or d.state_code = $6)';
  elsif normalized_data_source = 'te' then
    if team_id_filter is null or not can_read_team_filter then
      return;
    end if;
    visibility_clause := 'd.team_id = $5 and ($6::integer is null or d.state_code = $6)';
  else
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;
  text_match_clause := case
    when cardinality(escaped_query_terms) = 0 then 'false'
    else 'd.search_text &@~| $1'
  end;

  hybrid_sql := format(
    $sql$
      with text_rows as materialized (
        select
          d.id,
          pgroonga_score(d.tableoid, d.ctid) as search_score
        from %1$s d
        where %2$s
          and %3$s
          %4$s
      ),
      text_scores as (
        select text_rows.id, max(text_rows.search_score) as search_score
        from text_rows
        group by text_rows.id
      ),
      text_matches as materialized (
        select
          rank() over (
            order by text_scores.search_score desc, text_scores.id
          )::bigint as text_rank,
          text_scores.id as text_id
        from text_scores
        order by text_scores.search_score desc, text_scores.id
        limit $7
      ),
      semantic as materialized (
        select
          candidate.rank as semantic_rank,
          candidate.id as semantic_id
        from private.semantic_simple_dataset_candidates(
          $8, $9, $10, $11, $12, $3, $6, $5
        ) candidate
      ),
      fused_raw as (
        select
          coalesce(text_matches.text_id, semantic.semantic_id) as id,
          coalesce(
            1.0 / ($13 + text_matches.text_rank),
            0.0
          ) * $14
          + coalesce(
            1.0 / ($13 + semantic.semantic_rank),
            0.0
          ) * $15 as score
        from text_matches
        full outer join semantic
          on text_matches.text_id = semantic.semantic_id
      ),
      fused as (
        select fused_raw.id, sum(fused_raw.score) as score
        from fused_raw
        where fused_raw.id is not null
        group by fused_raw.id
      ),
      visible_rows as (
        select d.*, fused.score
        from %1$s d
        join fused on fused.id = d.id
        where %3$s
      ),
      latest_rows as (
        select distinct on (visible_rows.id)
          visible_rows.id,
          visible_rows.json,
          visible_rows.version,
          visible_rows.modified_at,
          visible_rows.team_id,
          visible_rows.score
        from visible_rows
        order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select
        counted_rows.id,
        counted_rows.json,
        counted_rows.version,
        counted_rows.modified_at,
        counted_rows.team_id,
        counted_rows.total_count
      from counted_rows
      order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
      limit $16
      offset ($17 - 1) * $16
    $sql$,
    p_table,
    text_match_clause,
    visibility_clause,
    json_filter_clause
  );

  return query execute hybrid_sql
    using escaped_query_terms, filter_condition_jsonb, normalized_data_source,
          effective_user_id, team_id_filter, state_code_filter, candidate_limit,
          p_table, query_embedding, filter_condition, match_threshold,
          semantic_match_count, normalized_rrf_k, text_weight,
          coalesce(semantic_weight, 0), normalized_page_size,
          normalized_page_current;
end;
$_$;

ALTER FUNCTION "private"."hybrid_search_simple_dataset_v2"("p_table" "regclass", "query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."hybrid_search_simple_dataset_v2"("p_table" "regclass", "query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."hybrid_search_simple_dataset_v2"("p_table" "regclass", "query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."hybrid_search_simple_dataset_v2"("p_table" "regclass", "query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "api_internal_executor";

SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $_$
declare
  v_table text;
  v_scope text;
  v_filters jsonb := coalesce(p_filters,'{}'::jsonb);
  v_terms text[];
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_exact uuid;
  v_flow_types text[];
  v_as_input boolean;
begin
  if p_kind = 'process' then v_table := 'processes';
  elsif p_kind = 'flow' then v_table := 'flows';
  else raise exception using errcode='22023',message='invalid version search request';
  end if;
  -- Preserve the existing lexical scope: Team Hybrid's semantic branch has
  -- actor-checked membership; its old lexical caller has no explicit team selector.
  -- Do not broaden that lexical scope as a side effect of retaining versions.
  if p_source = 'tg' then v_scope := 'source.state_code = 100';
  elsif p_source = 'ex' and auth.uid() is not null then v_scope := 'source.state_code = -1';
  elsif p_source = 'co' then v_scope := 'source.state_code = 200';
  elsif p_source = 'my' and v_actor is not null then v_scope := 'source.user_id = $3';
  else return;
  end if;
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms),0)=0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms),0)=0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query,'')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;
  if p_kind = 'flow' then
    v_flow_types := pg_catalog.string_to_array(nullif(pg_catalog.btrim(v_filters ->> 'flowType'),''),',');
    v_as_input := nullif(pg_catalog.btrim(v_filters ->> 'asInput'),'')::boolean;
    v_filters := v_filters - 'flowType' - 'asInput';
  end if;
  return query execute pg_catalog.format($query$
    with matched as materialized (
      select source.id,source.version::text as version,source.modified_at,
        case when $4 is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid,source.ctid)::double precision end as score
      from public.%I as source
      where %s
        and (($4 is not null and source.id=$4)
          or ($4 is null and source.search_text operator(extensions.&@~|) $1))
        and ($2='{}'::jsonb or source.json @> $2)
        and ($5 is null or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($5))
        and (not coalesce($6,false) or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        ))
      order by score desc,source.modified_at desc,source.id,source.version desc
      limit 200
    )
    select row_number() over(order by matched.score desc,matched.modified_at desc,matched.id,matched.version desc),
      matched.id,matched.version,matched.score
    from matched order by matched.score desc,matched.modified_at desc,matched.id,matched.version desc
  $query$,v_table,v_scope) using v_terms,v_filters,v_actor,v_exact,v_flow_types,v_as_input;
end;
$_$;

ALTER FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") FROM PUBLIC;

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_terms text[];
  v_exact uuid;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te', 'ex')
     or (p_state_code < 0 and not (p_data_source = 'ex' and p_state_code = -1))
     or (p_data_source = 'ex' and p_state_code is not null and p_state_code <> -1) then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  if v_actor is null then return; end if;
  if p_data_source = 'te' and (
    p_team_id is null
    or not private.dataset_search_can_read_team_filter(p_team_id, v_actor)
  ) then
    return;
  end if;

  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_scope_sql := case p_data_source
    when 'ex' then 'source.state_code = -1 and ($2::uuid is null or source.team_id = $2)'
    when 'my' then 'source.user_id = $1 and ($3::integer is null or source.state_code = $3)'
    else 'source.team_id = $2 and ($3::integer is null or source.state_code = $3)'
  end;
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query, '')) ~*
     '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        pg_catalog.cardinality($6::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($6)
      )
      and (
        not coalesce($7::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($8::text[]) = 0
          and pg_catalog.cardinality($9::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $8
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $9
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    with matched as materialized (
      select
        source.id,
        source.version::text as version,
        source.modified_at,
        case when $11::uuid is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid, source.ctid)::double precision
        end as search_score
      from public.%I as source
      where %s
        and (
          ($11::uuid is not null and source.id = $11)
          or ($11::uuid is null and source.search_text operator(extensions.&@~|) $10::text[])
        )
      order by search_score desc, source.modified_at desc,
        source.id, source.version desc
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by matched.search_score desc, matched.modified_at desc,
          matched.id, matched.version desc
      )::bigint,
      matched.id,
      matched.version,
      matched.search_score
    from matched
    order by matched.search_score desc, matched.modified_at desc,
      matched.id, matched.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_terms, v_exact;
end;
$_$;

ALTER FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision, "semantic_route" "text", "candidate_population" integer)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_exact_cutover constant integer := 2000;
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer := 0;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te', 'ex')
     or p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024
     or pg_catalog.jsonb_typeof(v_residual) is distinct from 'object'
     or (p_state_code < 0 and not (p_data_source = 'ex' and p_state_code = -1))
     or (p_data_source = 'ex' and p_state_code is not null and p_state_code <> -1) then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  if v_actor is null then return; end if;
  if p_data_source = 'te' and (
    p_team_id is null
    or not private.dataset_search_can_read_team_filter(p_team_id, v_actor)
  ) then
    return;
  end if;

  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_scope_sql := case p_data_source
    when 'ex' then 'source.state_code = -1 and ($2::uuid is null or source.team_id = $2)'
    when 'my' then 'source.user_id = $1 and ($3::integer is null or source.state_code = $3)'
    else 'source.team_id = $2 and ($3::integer is null or source.state_code = $3)'
  end;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        pg_catalog.cardinality($6::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($6)
      )
      and (
        not coalesce($7::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($8::text[]) = 0
          and pg_catalog.cardinality($9::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $8
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $9
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    from (
      select source.id, source.version::text as version
      from public.%I as source
      where %s
      limit $10
    ) as candidate
  $query$, v_table_name, v_filter_sql);

  execute v_sql
    into v_candidate_ids, v_candidate_versions
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1;
  v_candidate_count := coalesce(pg_catalog.cardinality(v_candidate_ids), 0);

  if v_candidate_count <= v_exact_cutover then
    v_sql := pg_catalog.format($query$
      with candidate_keys as materialized (
        select
          ($11::uuid[])[ordinal] as id,
          ($12::text[])[ordinal] as version
        from pg_catalog.generate_subscripts($11::uuid[], 1) as key(ordinal)
      ), nearest as materialized (
        select
          source.id,
          source.version::text as version,
          source.embedding_ft operator(extensions.<=>) $13::extensions.vector as distance
        from candidate_keys as candidate
        join public.%I as source
          on source.id = candidate.id
         and source.version::text = candidate.version
        where %s
        order by
          (source.embedding_ft operator(extensions.<=>) $13::extensions.vector)
            + 0::double precision,
          source.id,
          source.version::text desc
        limit 200
      )
      select
        pg_catalog.row_number() over (
          order by nearest.distance + 0::double precision,
            nearest.id, nearest.version desc
        )::bigint,
        nearest.id,
        nearest.version,
        nearest.distance,
        'exact'::text,
        $14::integer
      from nearest
      order by nearest.distance + 0::double precision,
        nearest.id, nearest.version desc
    $query$, v_table_name, v_filter_sql);

    return query execute v_sql
      using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1,
        v_candidate_ids, v_candidate_versions, p_query_embedding,
        v_candidate_count;
    return;
  end if;

  v_sql := pg_catalog.format($query$
    with nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $11::extensions.vector as distance
      from public.%I as source
      where %s
      order by source.embedding_ft operator(extensions.<=>) $11::extensions.vector
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by nearest.distance + 0::double precision,
          nearest.id, nearest.version desc
      )::bigint,
      nearest.id,
      nearest.version,
      nearest.distance,
      'hnsw'::text,
      $12::integer
    from nearest
    order by nearest.distance + 0::double precision,
      nearest.id, nearest.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1,
      p_query_embedding, v_candidate_count;
end;
$_$;

ALTER FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."next_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
begin
  if p_data_source in ('tg', 'co') then
    return query
    select candidate.*
    from private.next_public_lexical_version_candidates_v2(
      p_kind, p_query, p_terms, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_team_id
    ) as candidate;
    return;
  end if;

  if p_data_source in ('my', 'te', 'ex') then
    return query
    select candidate.*
    from private.next_actor_lexical_version_candidates_v2(
      p_kind, p_query, p_terms, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate;
    return;
  end if;

  raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
end;
$$;

ALTER FUNCTION "private"."next_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;

RESET ROLE;
SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision, "semantic_route" "text", "candidate_population" integer)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
begin
  if p_data_source in ('tg', 'co') then
    return query
    select candidate.*
    from private.next_public_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_team_id
    ) as candidate;
    return;
  end if;

  if p_data_source in ('my', 'te', 'ex') then
    return query
    select candidate.*
    from private.next_actor_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate;
    return;
  end if;

  raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
end;
$$;

ALTER FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;

RESET ROLE;
CREATE OR REPLACE FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[] DEFAULT NULL::"text"[], "p_data_source" "text" DEFAULT 'tg'::"text", "p_this_user_id" "text" DEFAULT ''::"text", "p_team_id_filter" "uuid" DEFAULT NULL::"uuid", "p_state_code_filter" integer DEFAULT NULL::integer, "p_limit" integer DEFAULT 20) RETURNS TABLE("rank" bigint, "source_entity_kind" "text", "source_id" "uuid", "source_version" character, "source_name" "text", "source_modified_at" timestamp with time zone, "source_team_id" "uuid", "source_json" "jsonb", "matched_by" "text", "matched_entity_table" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '20s'
    AS $_$
declare
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  normalized_limit integer;
  per_entity_limit integer;
  uuid_pattern text;
  normalized_source_entity_kinds text[];
  branches text[] := array[]::text[];
  v_sql text;
begin
  normalized_data_source := coalesce(nullif(lower(btrim(p_data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(p_this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(p_team_id_filter, effective_user_id);
  normalized_limit := least(greatest(coalesce(p_limit, 20), 1), 50);
  per_entity_limit := normalized_limit;
  uuid_pattern := '%' || p_uuid::text || '%';

  if p_source_entity_kinds is not null then
    select array_agg(distinct normalized_kind order by normalized_kind)
    into normalized_source_entity_kinds
    from (
      select case lower(btrim(kind))
        when 'flow' then 'flow'
        when 'flows' then 'flow'
        when 'process' then 'process'
        when 'processes' then 'process'
        when 'lifecyclemodel' then 'lifecyclemodel'
        when 'lifecyclemodels' then 'lifecyclemodel'
        when 'model' then 'lifecyclemodel'
        when 'models' then 'lifecyclemodel'
        when 'source' then 'source'
        when 'sources' then 'source'
        when 'contact' then 'contact'
        when 'contacts' then 'contact'
        when 'unitgroup' then 'unitgroup'
        when 'unitgroups' then 'unitgroup'
        when 'flowproperty' then 'flowproperty'
        when 'flowproperties' then 'flowproperty'
        else null
      end as normalized_kind
      from unnest(p_source_entity_kinds) as requested(kind)
    ) normalized
    where normalized_kind is not null;

    if coalesce(array_length(normalized_source_entity_kinds, 1), 0) = 0 then
      return;
    end if;
  end if;

  if normalized_source_entity_kinds is null or 'process' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          10::integer as entity_rank,
          'process'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('process', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.processes'::text as matched_entity_table
        from public.processes d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flow' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          20::integer as entity_rank,
          'flow'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flow', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flows'::text as matched_entity_table
        from public.flows d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'lifecyclemodel' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          30::integer as entity_rank,
          'lifecyclemodel'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('lifecyclemodel', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.lifecyclemodels'::text as matched_entity_table
        from public.lifecyclemodels d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'source' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          40::integer as entity_rank,
          'source'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('source', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.sources'::text as matched_entity_table
        from public.sources d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'contact' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          50::integer as entity_rank,
          'contact'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('contact', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.contacts'::text as matched_entity_table
        from public.contacts d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'unitgroup' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          60::integer as entity_rank,
          'unitgroup'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('unitgroup', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.unitgroups'::text as matched_entity_table
        from public.unitgroups d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flowproperty' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          70::integer as entity_rank,
          'flowproperty'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flowproperty', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flowproperties'::text as matched_entity_table
        from public.flowproperties d
        where (
            ((($1 = 'tg' AND d.state_code = 100) OR ($1 = 'ex' AND d.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if coalesce(array_length(branches, 1), 0) = 0 then
    return;
  end if;

  v_sql := format($sql$
    with matched_rows as (
      %s
    )
    select
      row_number() over (
        order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
      )::bigint as rank,
      source_entity_kind,
      source_id,
      source_version,
      source_name,
      source_modified_at,
      source_team_id,
      source_json,
      matched_by,
      matched_entity_table
    from matched_rows
    order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
    limit $7
  $sql$, array_to_string(branches, E'\nunion all\n'));

  return query execute v_sql
    using normalized_data_source, effective_user_id, p_team_id_filter, p_state_code_filter,
          can_read_team_filter, uuid_pattern, normalized_limit, per_entity_limit;
end;
$_$;

ALTER FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
  json_filter_clause text;
  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(team_id_filter, effective_user_id);
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := 'where f.search_text &@~| $14';

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  if flow_type is not null then
    flow_type_array := string_to_array(flow_type, ',');
  else
    flow_type_array := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  if filter_condition_jsonb ? 'asInput' then
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  else
    as_input := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  if jsonb_typeof(filter_condition_jsonb->'classification') = 'array' then
    classification_filter := filter_condition_jsonb->'classification';
  else
    classification_filter := '[]'::jsonb;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'classification';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select f.id, 1.0::double precision as search_score
        from public.flows f
        where f.id = exact_query_id
          and f.json @> filter_condition_jsonb
          and (
            (((normalized_data_source = 'tg' AND f.state_code = 100) OR (normalized_data_source = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or f.team_id = team_id_filter))
            or (normalized_data_source = 'co' and f.state_code = 200 and (team_id_filter is null or f.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and f.user_id = effective_user_id and (state_code_filter is null or f.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and f.team_id = team_id_filter and (state_code_filter is null or f.state_code = state_code_filter))
          )
          and (
            flow_type is null
            or (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          and (
            jsonb_array_length(classification_filter) = 0
            or exists (
              select 1
              from jsonb_array_elements(classification_filter) as selected_class(item)
              where
                (
                  selected_class.item->>'scope' = 'elementary'
                  and exists (
                    select 1
                    from jsonb_array_elements(
                      case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        else '[]'::jsonb
                      end
                    ) as category(item)
                    where category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                or (
                  selected_class.item->>'scope' = 'classification'
                  and exists (
                    select 1
                    from jsonb_array_elements(
                      case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        else '[]'::jsonb
                      end
                    ) as class_item(item)
                    where class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        group by f.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select f2.json, f2.version, f2.modified_at, f2.team_id
          from public.flows f2
          where f2.id = matched_ids.id
            and (
              (((normalized_data_source = 'tg' AND f2.state_code = 100) OR (normalized_data_source = 'ex' AND f2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or f2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and f2.state_code = 200 and (team_id_filter is null or f2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and f2.user_id = effective_user_id and (state_code_filter is null or f2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and f2.team_id = team_id_filter and (state_code_filter is null or f2.state_code = state_code_filter))
            )
          order by f2.version desc, f2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and f.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select f.id,
             f.json,
             f.state_code,
             f.team_id,
             f.user_id,
             pgroonga_score(f.tableoid, f.ctid) as search_score
      from public.flows f
      %s
    ),
    matched_ids as (
      select f.id, max(f.search_score) as search_score
      from text_matches f
      where (
          ((($5 = 'tg' AND f.state_code = 100) OR ($5 = 'ex' AND f.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or f.team_id = $7))
          or ($5 = 'co' and f.state_code = 200 and ($7 is null or f.team_id = $7))
          or ($5 = 'my' and $6 is not null and f.user_id = $6 and ($8 is null or f.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and f.team_id = $7 and ($8 is null or f.state_code = $8))
        )
        %s
        and (
          $10 is null
          or (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = any($11)
        )
        and (
          $12 is null
          or $12 = false
          or not (
            f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
          )
        )
        and (
          jsonb_array_length($13) = 0
          or exists (
            select 1
            from jsonb_array_elements($13) as selected_class(item)
            where
              (
                selected_class.item->>'scope' = 'elementary'
                and exists (
                  select 1
                  from jsonb_array_elements(
                    case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                      when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      else '[]'::jsonb
                    end
                  ) as category(item)
                  where category.item->>'@catId' = selected_class.item->>'code'
                )
              )
              or (
                selected_class.item->>'scope' = 'classification'
                and exists (
                  select 1
                  from jsonb_array_elements(
                    case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                      when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      else '[]'::jsonb
                    end
                  ) as class_item(item)
                  where class_item.item->>'@classId' = selected_class.item->>'code'
                )
              )
          )
        )
      group by f.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select f2.json, f2.version, f2.modified_at, f2.team_id
        from public.flows f2
        where f2.id = matched_ids.id
          and (
            ((($5 = 'tg' AND f2.state_code = 100) OR ($5 = 'ex' AND f2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or f2.team_id = $7))
            or ($5 = 'co' and f2.state_code = 200 and ($7 is null or f2.team_id = $7))
            or ($5 = 'my' and $6 is not null and f2.user_id = $6 and ($8 is null or f2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and f2.team_id = $7 and ($8 is null or f2.state_code = $8))
          )
        order by f2.version desc, f2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, flow_type, flow_type_array, as_input, classification_filter,
          escaped_query_terms;
end;
$_$;

ALTER FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."search_lifecyclemodels_latest_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(team_id_filter, effective_user_id);
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := 'where l.search_text &@~| $10';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select l.id, 1.0::double precision as search_score
        from public.lifecyclemodels l
        where l.id = exact_query_id
          and l.json @> filter_condition_jsonb
          and (
            (((normalized_data_source = 'tg' AND l.state_code = 100) OR (normalized_data_source = 'ex' AND l.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or l.team_id = team_id_filter))
            or (normalized_data_source = 'co' and l.state_code = 200 and (team_id_filter is null or l.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and l.user_id = effective_user_id and (state_code_filter is null or l.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and l.team_id = team_id_filter and (state_code_filter is null or l.state_code = state_code_filter))
          )
        group by l.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select l2.json, l2.version, l2.modified_at, l2.team_id
          from public.lifecyclemodels l2
          where l2.id = matched_ids.id
            and (
              (((normalized_data_source = 'tg' AND l2.state_code = 100) OR (normalized_data_source = 'ex' AND l2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or l2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and l2.state_code = 200 and (team_id_filter is null or l2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and l2.user_id = effective_user_id and (state_code_filter is null or l2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and l2.team_id = team_id_filter and (state_code_filter is null or l2.state_code = state_code_filter))
            )
          order by l2.version desc, l2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and l.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select l.id,
             l.json,
             l.state_code,
             l.team_id,
             l.user_id,
             pgroonga_score(l.tableoid, l.ctid) as search_score
      from public.lifecyclemodels l
      %s
    ),
    matched_ids as (
      select l.id, max(l.search_score) as search_score
      from text_matches l
      where (
          ((($5 = 'tg' AND l.state_code = 100) OR ($5 = 'ex' AND l.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or l.team_id = $7))
          or ($5 = 'co' and l.state_code = 200 and ($7 is null or l.team_id = $7))
          or ($5 = 'my' and $6 is not null and l.user_id = $6 and ($8 is null or l.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and l.team_id = $7 and ($8 is null or l.state_code = $8))
        )
        %s
      group by l.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select l2.json, l2.version, l2.modified_at, l2.team_id
        from public.lifecyclemodels l2
        where l2.id = matched_ids.id
          and (
            ((($5 = 'tg' AND l2.state_code = 100) OR ($5 = 'ex' AND l2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or l2.team_id = $7))
            or ($5 = 'co' and l2.state_code = 200 and ($7 is null or l2.team_id = $7))
            or ($5 = 'my' and $6 is not null and l2.user_id = $6 and ($8 is null or l2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and l2.team_id = $7 and ($8 is null or l2.state_code = $8))
          )
        order by l2.version desc, l2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, escaped_query_terms;
end;
$_$;

ALTER FUNCTION "private"."search_lifecyclemodels_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_lifecyclemodels_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_lifecyclemodels_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_lifecyclemodels_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(team_id_filter, effective_user_id);
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := 'where p.extracted_md &@~| $11';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select p.id, 1.0::double precision as search_score
        from public.processes p
        where p.id = exact_query_id
          and p.json @> filter_condition_jsonb
          and (
            (((normalized_data_source = 'tg' AND p.state_code = 100) OR (normalized_data_source = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'co' and p.state_code = 200 and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and p.user_id = effective_user_id and (state_code_filter is null or p.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p.team_id = team_id_filter and (state_code_filter is null or p.state_code = state_code_filter))
          )
          and (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        group by p.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
          from public.processes p2
          where p2.id = matched_ids.id
            and (
              (((normalized_data_source = 'tg' AND p2.state_code = 100) OR (normalized_data_source = 'ex' AND p2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and p2.state_code = 200 and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and p2.user_id = effective_user_id and (state_code_filter is null or p2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p2.team_id = team_id_filter and (state_code_filter is null or p2.state_code = state_code_filter))
            )
          order by p2.version desc, p2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.model_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and p.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select p.id,
             p.json,
             p.state_code,
             p.team_id,
             p.user_id,
             p.model_id,
             pgroonga_score(p.tableoid, p.ctid) as search_score
      from public.processes p
      %s
    ),
    matched_ids as (
      select p.id, max(p.search_score) as search_score
      from text_matches p
      where (
          ((($5 = 'tg' AND p.state_code = 100) OR ($5 = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or p.team_id = $7))
          or ($5 = 'co' and p.state_code = 200 and ($7 is null or p.team_id = $7))
          or ($5 = 'my' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and p.team_id = $7 and ($8 is null or p.state_code = $8))
        )
        %s
        and (
          coalesce($10, 'all') = 'all'
          or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $10
        )
      group by p.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
        from public.processes p2
        where p2.id = matched_ids.id
          and (
            ((($5 = 'tg' AND p2.state_code = 100) OR ($5 = 'ex' AND p2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or p2.team_id = $7))
            or ($5 = 'co' and p2.state_code = 200 and ($7 is null or p2.team_id = $7))
            or ($5 = 'my' and $6 is not null and p2.user_id = $6 and ($8 is null or p2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and p2.team_id = $7 and ($8 is null or p2.state_code = $8))
          )
        order by p2.version desc, p2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.model_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, type_of_data_set_filter, escaped_query_terms;
end;
$_$;

ALTER FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."search_processes_latest_v2_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[], "owner_draft_only" boolean DEFAULT false) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  if owner_draft_only and normalized_data_source <> 'my' then
    return;
  end if;
  effective_user_id := private.dataset_search_effective_user_id(this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(team_id_filter, effective_user_id);
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := 'where p.search_text &@~| $11';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select p.id, 1.0::double precision as search_score
        from public.processes p
        where p.id = exact_query_id
          and p.json @> filter_condition_jsonb
          and (
            (((normalized_data_source = 'tg' AND p.state_code = 100) OR (normalized_data_source = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'co' and p.state_code = 200 and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and p.user_id = effective_user_id and (state_code_filter is null or p.state_code = state_code_filter) and (not owner_draft_only or (p.state_code = 0)))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p.team_id = team_id_filter and (state_code_filter is null or p.state_code = state_code_filter))
          )
          and (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        group by p.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
          from public.processes p2
          where p2.id = matched_ids.id
            and (
              (((normalized_data_source = 'tg' AND p2.state_code = 100) OR (normalized_data_source = 'ex' AND p2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and p2.state_code = 200 and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and p2.user_id = effective_user_id and (state_code_filter is null or p2.state_code = state_code_filter) and (not owner_draft_only or (p2.state_code = 0)))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p2.team_id = team_id_filter and (state_code_filter is null or p2.state_code = state_code_filter))
            )
          order by p2.version desc, p2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.model_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and p.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select p.id,
             p.json,
             p.state_code,
             p.team_id,
             p.user_id,
             p.model_id,
             p.review_id,
             pgroonga_score(p.tableoid, p.ctid) as search_score
      from public.processes p
      %s
    ),
    matched_ids as (
      select p.id, max(p.search_score) as search_score
      from text_matches p
      where (
          ((($5 = 'tg' AND p.state_code = 100) OR ($5 = 'ex' AND p.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or p.team_id = $7))
          or ($5 = 'co' and p.state_code = 200 and ($7 is null or p.team_id = $7))
          or ($5 = 'my' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8) and (not $12 or (p.state_code = 0)))
          or ($5 = 'te' and $7 is not null and $9 and p.team_id = $7 and ($8 is null or p.state_code = $8))
        )
        %s
        and (
          coalesce($10, 'all') = 'all'
          or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $10
        )
      group by p.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
        from public.processes p2
        where p2.id = matched_ids.id
          and (
            ((($5 = 'tg' AND p2.state_code = 100) OR ($5 = 'ex' AND p2.state_code = -1 AND (SELECT auth.uid()) IS NOT NULL)) and ($7 is null or p2.team_id = $7))
            or ($5 = 'co' and p2.state_code = 200 and ($7 is null or p2.team_id = $7))
            or ($5 = 'my' and $6 is not null and p2.user_id = $6 and ($8 is null or p2.state_code = $8) and (not $12 or (p2.state_code = 0)))
            or ($5 = 'te' and $7 is not null and $9 and p2.team_id = $7 and ($8 is null or p2.state_code = $8))
          )
        order by p2.version desc, p2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.model_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, type_of_data_set_filter, escaped_query_terms,
          owner_draft_only;
end;
$_$;

ALTER FUNCTION "private"."search_processes_latest_v2_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_processes_latest_v2_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_processes_latest_v2_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_processes_latest_v2_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."semantic_flow_candidates"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    AS $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  if flow_type is not null then
    flow_type_array := string_to_array(flow_type, ',');
  else
    flow_type_array := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  if filter_condition_jsonb ? 'asInput' then
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  else
    as_input := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'ex' and auth.uid() is not null then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = -1
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_flow_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_flow_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_flow_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_flow_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    AS $$
declare
  query_embedding_vector extensions.vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
begin
  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 200), 1), 200);
  candidate_size := normalized_match_count;
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  if flow_type is not null then
    flow_type_array := string_to_array(flow_type, ',');
  else
    flow_type_array := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  if filter_condition_jsonb ? 'asInput' then
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  else
    as_input := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'ex' and auth.uid() is not null then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = -1
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

RESET ROLE;
CREATE OR REPLACE FUNCTION "private"."semantic_lifecyclemodel_candidates"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.state_code = 100
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'ex' and auth.uid() is not null then
    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.state_code = -1
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.state_code = 200
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.user_id = effective_user_id
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = l.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_lifecyclemodel_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_lifecyclemodel_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_lifecyclemodel_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_lifecyclemodel_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."semantic_process_candidates"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    AS $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'ex' and auth.uid() is not null then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = -1
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_process_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_process_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_process_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_process_candidates"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";

SET LOCAL ROLE api_internal_executor;
CREATE OR REPLACE FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    AS $$
declare
  query_embedding_vector extensions.vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
begin
  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 200), 1), 200);
  candidate_size := normalized_match_count;
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'ex' and auth.uid() is not null then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = -1
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;

RESET ROLE;
CREATE OR REPLACE FUNCTION "private"."semantic_simple_dataset_candidates"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text", "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    AS $_$
declare
  query_embedding_vector extensions.vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  can_read_team_filter boolean;
  visibility_clause text;
  json_filter_clause text;
  candidate_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported semantic dataset table: %', p_table;
  end if;

  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 20), 1), 200);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - least(greatest(coalesce(match_threshold, 0.5), -1), 1);
  effective_user_id := private.dataset_search_effective_user_id('');
  can_read_team_filter := private.dataset_search_can_read_team_filter(
    team_id_filter,
    effective_user_id
  );

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100 and ($7::uuid is null or d.team_id = $7)';
  elsif normalized_data_source = 'ex' then
    if auth.uid() is null then return; end if;
    visibility_clause := 'd.state_code = -1 and ($7::uuid is null or d.team_id = $7)';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200 and ($7::uuid is null or d.team_id = $7)';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $5 and ($8::integer is null or d.state_code = $8)';
  elsif normalized_data_source = 'te' then
    if team_id_filter is null or not can_read_team_filter then
      return;
    end if;
    visibility_clause := 'd.team_id = $7 and ($8::integer is null or d.state_code = $8)';
  else
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  candidate_sql := format(
    $sql$
      with candidates as materialized (
        select
          d.id as candidate_id,
          d.embedding_ft <=> $1 as candidate_distance
        from %1$s d
        where d.embedding_ft is not null
          and %2$s
          %3$s
        order by d.embedding_ft <=> $1
        limit $3
      ),
      deduplicated as (
        select
          candidates.candidate_id,
          min(candidates.candidate_distance) as candidate_distance
        from candidates
        where candidates.candidate_distance < $4
        group by candidates.candidate_id
      )
      select
        rank() over (
          order by deduplicated.candidate_distance, deduplicated.candidate_id
        )::bigint,
        deduplicated.candidate_id,
        deduplicated.candidate_distance
      from deduplicated
      order by deduplicated.candidate_distance, deduplicated.candidate_id
      limit $6
    $sql$,
    p_table,
    visibility_clause,
    json_filter_clause
  );

  return query execute candidate_sql
    using query_embedding_vector, filter_condition_jsonb, candidate_size,
          threshold_distance, effective_user_id, normalized_match_count,
          team_id_filter, state_code_filter;
end;
$_$;

ALTER FUNCTION "private"."semantic_simple_dataset_candidates"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_simple_dataset_candidates"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_candidates"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_candidates"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text", "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  visibility_clause text;
  search_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported semantic dataset table: %', p_table;
  end if;

  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id('');
  can_read_team_filter := private.dataset_search_can_read_team_filter(
    team_id_filter,
    effective_user_id
  );

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100 and ($9::uuid is null or d.team_id = $9)';
  elsif normalized_data_source = 'ex' then
    if auth.uid() is null then return; end if;
    visibility_clause := 'd.state_code = -1 and ($9::uuid is null or d.team_id = $9)';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200 and ($9::uuid is null or d.team_id = $9)';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $7 and ($8::integer is null or d.state_code = $8)';
  elsif normalized_data_source = 'te' then
    if team_id_filter is null or not can_read_team_filter then
      return;
    end if;
    visibility_clause := 'd.team_id = $9 and ($8::integer is null or d.state_code = $8)';
  else
    return;
  end if;

  search_sql := format(
    $sql$
      with semantic as materialized (
        select candidate.rank, candidate.id, candidate.distance
        from private.semantic_simple_dataset_candidates(
          $1, $2, $3, $4, $5, $6, $8, $9
        ) candidate
      ),
      visible_rows as (
        select
          d.id,
          d.json,
          d.version,
          d.modified_at,
          semantic.rank as semantic_rank,
          semantic.distance
        from %1$s d
        join semantic on semantic.id = d.id
        where %2$s
      ),
      latest_rows as (
        select distinct on (visible_rows.id)
          visible_rows.id,
          visible_rows.json,
          visible_rows.version,
          visible_rows.modified_at,
          visible_rows.semantic_rank,
          visible_rows.distance
        from visible_rows
        order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select
        counted_rows.semantic_rank,
        counted_rows.id,
        counted_rows.json,
        counted_rows.version,
        counted_rows.modified_at,
        counted_rows.total_count
      from counted_rows
      order by counted_rows.semantic_rank, counted_rows.distance, counted_rows.id
    $sql$,
    p_table,
    visibility_clause
  );

  return query execute search_sql
    using p_table, query_embedding, filter_condition, match_threshold,
          match_count, normalized_data_source, effective_user_id,
          state_code_filter, team_id_filter;
end;
$_$;

ALTER FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "api_internal_executor";

REVOKE CREATE ON SCHEMA api, private FROM api_internal_executor;
REVOKE api_internal_executor FROM postgres;
NOTIFY pgrst, 'reload schema';
COMMIT;
