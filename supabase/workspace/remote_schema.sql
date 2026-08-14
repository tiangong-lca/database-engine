


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "public";


ALTER SCHEMA "public" OWNER TO "pg_database_owner";


COMMENT ON SCHEMA "public" IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."contacts" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "contacts_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 3, 20, 100])))
);


ALTER TABLE "public"."contacts" OWNER TO "postgres";


COMMENT ON COLUMN "public"."contacts"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."flowproperties" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "flowproperties_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."flowproperties" OWNER TO "postgres";


COMMENT ON COLUMN "public"."flowproperties"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."flows" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "embedding_ft_at" timestamp with time zone,
    "extracted_md" "text",
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "flows_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."flows" OWNER TO "postgres";


COMMENT ON COLUMN "public"."flows"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."lifecyclemodels" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "json_tg" "jsonb",
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "lifecyclemodels_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100])))
);


ALTER TABLE "public"."lifecyclemodels" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lifecyclemodels"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."processes" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "model_id" "uuid",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "extracted_md" "text",
    "search_text" "text"[],
    CONSTRAINT "processes_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."processes" OWNER TO "postgres";


COMMENT ON COLUMN "public"."processes"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."sources" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "sources_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100])))
);


ALTER TABLE "public"."sources" OWNER TO "postgres";


COMMENT ON COLUMN "public"."sources"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."unitgroups" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "search_text" "text"[],
    CONSTRAINT "unitgroups_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."unitgroups" OWNER TO "postgres";


COMMENT ON COLUMN "public"."unitgroups"."search_text" IS 'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';



CREATE TABLE IF NOT EXISTS "public"."ilcd" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "file_name" character varying(255),
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "modified_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."ilcd" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lciamethods" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."lciamethods" OWNER TO "postgres";


ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."flowproperties"
    ADD CONSTRAINT "flowproperties_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."flows"
    ADD CONSTRAINT "flows_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."ilcd"
    ADD CONSTRAINT "ilcd_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lciamethods"
    ADD CONSTRAINT "lciamethods_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."lifecyclemodels"
    ADD CONSTRAINT "lifecyclemodels_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."processes"
    ADD CONSTRAINT "processes_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."sources"
    ADD CONSTRAINT "sources_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."unitgroups"
    ADD CONSTRAINT "unitgroups_pkey" PRIMARY KEY ("id", "version");



CREATE INDEX "contacts_created_at_idx" ON "public"."contacts" USING "btree" ("created_at" DESC);



CREATE INDEX "contacts_embedding_ft_hnsw_idx" ON "public"."contacts" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "contacts_extracted_md_pgroonga" ON "public"."contacts" USING "pgroonga" ("extracted_md");



CREATE INDEX "contacts_json_dataversion" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "contacts_json_email" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'contactInformation'::"text") -> 'dataSetInformation'::"text") ->> 'email'::"text")));



CREATE INDEX "contacts_json_idx" ON "public"."contacts" USING "gin" ("json");



CREATE INDEX "contacts_search_text_pgroonga" ON "public"."contacts" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "contacts_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "contacts_team_id_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "contacts_user_id_created_at_idx" ON "public"."contacts" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "contacts_user_id_id_version_modified_at_latest_idx" ON "public"."contacts" USING "btree" ("user_id", "id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "state_code");



CREATE INDEX "contacts_user_id_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE UNIQUE INDEX "file_name_index" ON "public"."ilcd" USING "btree" ("file_name");



CREATE INDEX "flowproperties_created_at_idx" ON "public"."flowproperties" USING "btree" ("created_at" DESC);



CREATE INDEX "flowproperties_embedding_ft_hnsw_idx" ON "public"."flowproperties" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "flowproperties_extracted_md_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("extracted_md");



CREATE INDEX "flowproperties_json_dataversion" ON "public"."flowproperties" USING "btree" (((((("json" -> 'flowPropertyDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flowproperties_json_idx" ON "public"."flowproperties" USING "gin" ("json");



CREATE INDEX "flowproperties_json_refobjectid" ON "public"."flowproperties" USING "btree" ((((((("json" -> 'flowPropertyDataSet'::"text") -> 'flowPropertiesInformation'::"text") -> 'quantitativeReference'::"text") -> 'referenceToReferenceUnitGroup'::"text") ->> '@refObjectId'::"text")));



CREATE INDEX "flowproperties_modified_at_idx" ON "public"."flowproperties" USING "btree" ("modified_at");



CREATE INDEX "flowproperties_search_text_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "flowproperties_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flowproperties_team_id_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flowproperties_user_id_created_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "flowproperties_user_id_id_version_modified_at_latest_idx" ON "public"."flowproperties" USING "btree" ("user_id", "id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "state_code");



CREATE INDEX "flowproperties_user_id_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_composite_idx" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")), "state_code", "modified_at" DESC);



CREATE INDEX "flows_created_at_idx" ON "public"."flows" USING "btree" ("created_at" DESC);



CREATE INDEX "flows_embedding_ft_hnsw_idx" ON "public"."flows" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "flows_extracted_md_pgroonga" ON "public"."flows" USING "pgroonga" ("extracted_md");



CREATE INDEX "flows_json_casnumber" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'dataSetInformation'::"text") ->> 'CASNumber'::"text")));



CREATE INDEX "flows_json_dataversion" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flows_json_locationofsupply" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'geography'::"text") ->> 'locationOfSupply'::"text")));



CREATE INDEX "flows_json_ordered_alias_flowproperty_gin_idx" ON "public"."flows" USING "gin" ("private"."dataset_alias_jsonb_array_v1"((("json_ordered")::"jsonb" #> '{flowDataSet,flowProperties,flowProperty}'::"text"[])) "jsonb_path_ops");



CREATE INDEX "flows_json_typeofdataset" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")));



CREATE INDEX "flows_modified_at_idx" ON "public"."flows" USING "btree" ("modified_at");



CREATE INDEX "flows_not_emissions_idx" ON "public"."flows" USING "btree" ("state_code", "modified_at" DESC) WHERE (NOT ("json" @> '{"flowDataSet": {"flowInformation": {"dataSetInformation": {"classificationInformation": {"common:elementaryFlowCategorization": {"common:category": [{"#text": "Emissions", "@level": "0"}]}}}}}}'::"jsonb"));



CREATE INDEX "flows_public_latest_keys_cover_idx" ON "public"."flows" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id") WHERE ("state_code" = 100);



CREATE INDEX "flows_review_id_idx" ON "public"."flows" USING "btree" ("review_id");



CREATE INDEX "flows_search_text_pgroonga" ON "public"."flows" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "flows_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_state_code_idx" ON "public"."flows" USING "btree" ("state_code");



CREATE INDEX "flows_team_id_idx" ON "public"."flows" USING "btree" ("team_id");



CREATE INDEX "flows_team_id_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_user_id_created_at_idx" ON "public"."flows" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "flows_user_id_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "ilcd_created_at_idx" ON "public"."ilcd" USING "btree" ("created_at" DESC);



CREATE INDEX "ilcd_json_idx" ON "public"."ilcd" USING "gin" ("json");



CREATE INDEX "ilcd_modified_at_idx" ON "public"."ilcd" USING "btree" ("modified_at");



CREATE INDEX "ilcd_user_id_created_at_idx" ON "public"."ilcd" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lciamethods_created_at_idx" ON "public"."lciamethods" USING "btree" ("created_at" DESC);



CREATE INDEX "lciamethods_json_dataversion" ON "public"."lciamethods" USING "btree" (((((("json" -> 'LCIAMethodDataSetDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "lciamethods_json_idx" ON "public"."lciamethods" USING "gin" ("json");



CREATE INDEX "lciamethods_json_pgroonga" ON "public"."lciamethods" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "lciamethods_modified_at_idx" ON "public"."lciamethods" USING "btree" ("modified_at");



CREATE INDEX "lciamethods_user_id_created_at_idx" ON "public"."lciamethods" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lifecyclemodels_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("created_at" DESC);



CREATE INDEX "lifecyclemodels_embedding_ft_hnsw_idx" ON "public"."lifecyclemodels" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "lifecyclemodels_extracted_md_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("extracted_md");



CREATE INDEX "lifecyclemodels_json_dataversion" ON "public"."lifecyclemodels" USING "btree" (((((("json" -> 'lifeCycleModelDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "lifecyclemodels_json_idx" ON "public"."lifecyclemodels" USING "gin" ("json");



CREATE INDEX "lifecyclemodels_json_tg_idx" ON "public"."lifecyclemodels" USING "gin" ("json_tg");



CREATE INDEX "lifecyclemodels_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("modified_at");



CREATE INDEX "lifecyclemodels_public_latest_keys_cover_idx" ON "public"."lifecyclemodels" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id") WHERE ("state_code" = 100);



CREATE INDEX "lifecyclemodels_search_text_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "lifecyclemodels_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "lifecyclemodels_team_id_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "lifecyclemodels_user_id_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lifecyclemodels_user_id_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "processes_created_at_idx" ON "public"."processes" USING "btree" ("created_at" DESC);



CREATE INDEX "processes_embedding_ft_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "processes_embedding_ft_tg_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops") WHERE (("state_code" = 100) AND ("embedding_ft" IS NOT NULL));



CREATE INDEX "processes_extracted_md_pgroonga" ON "public"."processes" USING "pgroonga" ("extracted_md");



CREATE INDEX "processes_json_dataversion" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "processes_json_exchange_gin_idx" ON "public"."processes" USING "gin" ((((("json" -> 'processDataSet'::"text") -> 'exchanges'::"text") -> 'exchange'::"text")));



CREATE INDEX "processes_json_location" ON "public"."processes" USING "btree" ((((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'geography'::"text") -> 'locationOfOperationSupplyOrProduction'::"text") ->> '@location'::"text")));



CREATE INDEX "processes_json_ordered_alias_exchange_gin_idx" ON "public"."processes" USING "gin" ("private"."dataset_alias_jsonb_array_v1"((("json_ordered")::"jsonb" #> '{processDataSet,exchanges,exchange}'::"text"[])) "jsonb_path_ops");



CREATE INDEX "processes_json_referenceyear" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'time'::"text") ->> 'common:referenceYear'::"text")));



CREATE INDEX "processes_model_id_version_idx" ON "public"."processes" USING "btree" ("model_id", "version") WHERE ("model_id" IS NOT NULL);



CREATE INDEX "processes_modified_at_idx" ON "public"."processes" USING "btree" ("modified_at");



CREATE INDEX "processes_public_latest_keys_cover_idx" ON "public"."processes" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "model_id") WHERE ("state_code" = 100);



CREATE INDEX "processes_review_id_idx" ON "public"."processes" USING "btree" ("review_id");



CREATE INDEX "processes_rule_verification_idx" ON "public"."processes" USING "btree" ("rule_verification");



CREATE INDEX "processes_search_text_pgroonga" ON "public"."processes" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "processes_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "processes_state_code_idx" ON "public"."processes" USING "btree" ("state_code");



CREATE INDEX "processes_team_id_idx" ON "public"."processes" USING "btree" ("team_id");



CREATE INDEX "processes_team_id_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "processes_user_id_created_at_idx" ON "public"."processes" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "processes_user_id_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "sources_created_at_idx" ON "public"."sources" USING "btree" ("created_at" DESC);



CREATE INDEX "sources_embedding_ft_hnsw_idx" ON "public"."sources" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "sources_extracted_md_pgroonga" ON "public"."sources" USING "pgroonga" ("extracted_md");



CREATE INDEX "sources_json_dataversion" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "sources_json_idx" ON "public"."sources" USING "gin" ("json");



CREATE INDEX "sources_json_publicationtype" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'publicationType'::"text")));



CREATE INDEX "sources_json_sourcecitation" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'sourceCitation'::"text")));



CREATE INDEX "sources_modified_at_idx" ON "public"."sources" USING "btree" ("modified_at");



CREATE INDEX "sources_search_text_pgroonga" ON "public"."sources" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "sources_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "sources_team_id_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "sources_user_id_created_at_idx" ON "public"."sources" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "sources_user_id_id_version_modified_at_latest_idx" ON "public"."sources" USING "btree" ("user_id", "id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "state_code");



CREATE INDEX "sources_user_id_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_created_at_idx" ON "public"."unitgroups" USING "btree" ("created_at" DESC);



CREATE INDEX "unitgroups_embedding_ft_hnsw_idx" ON "public"."unitgroups" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "unitgroups_extracted_md_pgroonga" ON "public"."unitgroups" USING "pgroonga" ("extracted_md");



CREATE INDEX "unitgroups_json_dataversion" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "unitgroups_json_idx" ON "public"."unitgroups" USING "gin" ("json");



CREATE INDEX "unitgroups_json_referencetoreferenceunit" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'unitGroupInformation'::"text") -> 'quantitativeReference'::"text") ->> 'referenceToReferenceUnit'::"text")));



CREATE INDEX "unitgroups_modified_at_idx" ON "public"."unitgroups" USING "btree" ("modified_at");



CREATE INDEX "unitgroups_search_text_pgroonga" ON "public"."unitgroups" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');



CREATE INDEX "unitgroups_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_team_id_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_user_id_created_at_idx" ON "public"."unitgroups" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "unitgroups_user_id_id_version_modified_at_latest_idx" ON "public"."unitgroups" USING "btree" ("user_id", "id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "state_code");



CREATE INDEX "unitgroups_user_id_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE OR REPLACE TRIGGER "contact_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "contact_dataset_extraction_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."contacts" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "contact_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."contacts" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('contacts_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "contacts_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."contacts_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "contacts_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews" ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "dataset_flow_identity_flow_active_fence" BEFORE UPDATE ON "public"."flows" FOR EACH ROW WHEN ((("to_jsonb"("new".*) - ARRAY['extracted_md'::"text", 'embedding_ft'::"text", 'embedding_ft_at'::"text", 'search_text'::"text"]) IS DISTINCT FROM ("to_jsonb"("old".*) - ARRAY['extracted_md'::"text", 'embedding_ft'::"text", 'embedding_ft_at'::"text", 'search_text'::"text"]))) EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence_v2"();



COMMENT ON TRIGGER "dataset_flow_identity_flow_active_fence" ON "public"."flows" IS 'Fail-closed Step 3 actor fence. Only extracted_md, embedding_ft, embedding_ft_at, and asynchronous search_text projection updates bypass the owner-wide fence.';



CREATE OR REPLACE TRIGGER "dataset_flow_identity_flow_insert_delete_active_fence" BEFORE INSERT OR DELETE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence_v2"();



COMMENT ON TRIGGER "dataset_flow_identity_flow_insert_delete_active_fence" ON "public"."flows" IS 'Fail-closed Step 3 actor fence for every Flow insert or delete.';



CREATE OR REPLACE TRIGGER "dataset_flow_identity_flowproperty_active_fence" BEFORE DELETE OR UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence_v2"();



CREATE OR REPLACE TRIGGER "dataset_flow_identity_process_active_fence" BEFORE INSERT OR DELETE OR UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence_v2"();



CREATE OR REPLACE TRIGGER "dataset_flow_identity_unitgroup_active_fence" BEFORE DELETE OR UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence_v2"();



CREATE OR REPLACE TRIGGER "flow_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "flow_derivative_rebuild_primary_delete_fence" BEFORE DELETE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();



CREATE OR REPLACE TRIGGER "flow_derivative_rebuild_primary_update_fence" BEFORE UPDATE OF "id", "json", "created_at", "json_ordered", "user_id", "state_code", "version", "modified_at", "team_id", "review_id", "rule_verification", "reviews" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();



CREATE OR REPLACE TRIGGER "flow_derivative_rebuild_stage_embedding" BEFORE UPDATE OF "embedding_ft", "embedding_ft_at" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."stage_dataset_derivative_rebuild_write"('embedding');



CREATE OR REPLACE TRIGGER "flow_derivative_rebuild_stage_markdown" BEFORE UPDATE OF "extracted_md" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."stage_dataset_derivative_rebuild_write"('markdown');



CREATE OR REPLACE TRIGGER "flow_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."flows" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('flows_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."flows" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "flowproperties_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "private"."flowproperties_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flowproperties_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews" ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "flowproperty_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "flowproperty_dataset_extraction_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."flowproperties" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "flowproperty_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."flowproperties" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('flowproperties_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "flows_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "private"."flows_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flows_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "ilcd_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "private"."sync_json_to_jsonb"();



CREATE OR REPLACE TRIGGER "ilcd_set_modified_at_trigger" BEFORE UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lcia_scope_closure_candidate_hash_refresh" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "state_code", "json", "json_ordered" ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"();



CREATE OR REPLACE TRIGGER "lciamethods_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "private"."lciamethods_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lciamethods_set_modified_at_trigger" BEFORE UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lifecyclemodel_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('lifecyclemodels_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_insert" AFTER INSERT ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodels_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "private"."lifecyclemodels_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lifecyclemodels_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "json_tg", "team_id", "rule_verification", "reviews" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "process_derivative_rebuild_primary_delete_fence" BEFORE DELETE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();



CREATE OR REPLACE TRIGGER "process_derivative_rebuild_primary_update_fence" BEFORE UPDATE OF "id", "json", "created_at", "json_ordered", "user_id", "state_code", "version", "modified_at", "team_id", "review_id", "rule_verification", "reviews", "model_id" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();



CREATE OR REPLACE TRIGGER "process_derivative_rebuild_stage_embedding" BEFORE UPDATE OF "embedding_ft", "embedding_ft_at" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."stage_dataset_derivative_rebuild_write"('embedding');



CREATE OR REPLACE TRIGGER "process_derivative_rebuild_stage_markdown" BEFORE UPDATE OF "extracted_md" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."stage_dataset_derivative_rebuild_write"('markdown');



CREATE OR REPLACE TRIGGER "process_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."processes" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('processes_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."processes" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "processes_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."processes_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "processes_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews", "model_id" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();



CREATE OR REPLACE TRIGGER "source_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "source_dataset_extraction_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."sources" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "source_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."sources" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('sources_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "sources_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "private"."sources_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "sources_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews" ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE OR REPLACE TRIGGER "unitgroup_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "unitgroup_dataset_extraction_trigger_update" AFTER UPDATE OF "json", "json_ordered" ON "public"."unitgroups" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "unitgroup_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."unitgroups" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('unitgroups_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "unitgroups_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."unitgroups_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "unitgroups_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews" ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();



CREATE POLICY "Enable delete for users based on user_id" ON "public"."contacts" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."flowproperties" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."flows" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."lifecyclemodels" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."processes" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."sources" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."unitgroups" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."contacts" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."flowproperties" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."flows" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."lifecyclemodels" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."processes" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."sources" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."unitgroups" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable read access for all users" ON "public"."ilcd" FOR SELECT TO "authenticated" USING (true);



CREATE POLICY "Enable read access for all users" ON "public"."lciamethods" FOR SELECT TO "authenticated" USING (true);



CREATE POLICY "Enable read access for authenticated users" ON "public"."contacts" FOR SELECT USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "contacts"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "contacts"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("contacts"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("contacts"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."flowproperties" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "flowproperties"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "flowproperties"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("flowproperties"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("flowproperties"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."flows" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "flows"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "flows"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("flows"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("flows"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."lifecyclemodels" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "lifecyclemodels"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "lifecyclemodels"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("lifecyclemodels"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("lifecyclemodels"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."processes" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "processes"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "processes"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("processes"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("processes"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."sources" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "sources"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "sources"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("sources"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("sources"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."unitgroups" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = "unitgroups"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "unitgroups"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("unitgroups"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("unitgroups"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



ALTER TABLE "public"."contacts" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."flowproperties" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."flows" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ilcd" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lciamethods" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lifecyclemodels" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."processes" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."sources" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "transitional_update_owner_draft_only" ON "public"."contacts" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flowproperties" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flows" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."lifecyclemodels" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."processes" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."sources" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."unitgroups" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



ALTER TABLE "public"."unitgroups" ENABLE ROW LEVEL SECURITY;


GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";
GRANT USAGE ON SCHEMA "public" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."contacts" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."contacts" TO "authenticated";
GRANT ALL ON TABLE "public"."contacts" TO "service_role";
GRANT SELECT ON TABLE "public"."contacts" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."flowproperties" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."flowproperties" TO "authenticated";
GRANT ALL ON TABLE "public"."flowproperties" TO "service_role";
GRANT SELECT ON TABLE "public"."flowproperties" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."flows" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."flows" TO "authenticated";
GRANT ALL ON TABLE "public"."flows" TO "service_role";
GRANT SELECT ON TABLE "public"."flows" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."lifecyclemodels" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."lifecyclemodels" TO "authenticated";
GRANT ALL ON TABLE "public"."lifecyclemodels" TO "service_role";
GRANT SELECT ON TABLE "public"."lifecyclemodels" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."processes" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."processes" TO "authenticated";
GRANT ALL ON TABLE "public"."processes" TO "service_role";
GRANT SELECT ON TABLE "public"."processes" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."sources" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."sources" TO "authenticated";
GRANT ALL ON TABLE "public"."sources" TO "service_role";
GRANT SELECT ON TABLE "public"."sources" TO "api_internal_executor";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."unitgroups" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE ON TABLE "public"."unitgroups" TO "authenticated";
GRANT ALL ON TABLE "public"."unitgroups" TO "service_role";
GRANT SELECT ON TABLE "public"."unitgroups" TO "api_internal_executor";



GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE "public"."ilcd" TO "anon";
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE "public"."ilcd" TO "authenticated";
GRANT ALL ON TABLE "public"."ilcd" TO "service_role";
GRANT SELECT ON TABLE "public"."ilcd" TO "api_internal_executor";



GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE "public"."lciamethods" TO "anon";
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE "public"."lciamethods" TO "authenticated";
GRANT ALL ON TABLE "public"."lciamethods" TO "service_role";
GRANT SELECT ON TABLE "public"."lciamethods" TO "api_internal_executor";



ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";







