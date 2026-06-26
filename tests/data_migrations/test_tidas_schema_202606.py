import copy
import unittest

from scripts.data_migrations.tidas_schema_202606 import runner as migrate


class TidasSchema202606MigrationTests(unittest.TestCase):
    def test_normalize_version_common_shapes(self):
        self.assertEqual(migrate.normalize_version("1"), "01.00.000")
        self.assertEqual(migrate.normalize_version("1.0"), "01.00.000")
        self.assertEqual(migrate.normalize_version("01.02"), "01.02")
        self.assertEqual(migrate.normalize_version("01.02.003"), "01.02.003")
        self.assertEqual(migrate.normalize_version("v3"), "03.00.000")
        self.assertIsNone(migrate.normalize_version("2025-06"))

    def test_migrates_global_reference_types_and_versions(self):
        doc = {
            "flowDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": "1.0"}
                },
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "@type": "flowproperties data set",
                            "@refObjectId": "00000000-0000-0000-0000-000000000001",
                            "@version": "1",
                        }
                    }
                },
            }
        }

        result = migrate.migrate_document(
            "flows",
            copy.deepcopy(doc),
            type_aliases={"flowproperties data set": "flow property data set"},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version="00.00.001",
        )

        ref = result.document["flowDataSet"]["flowProperties"]["flowProperty"][
            "referenceToFlowPropertyDataSet"
        ]
        self.assertEqual(ref["@type"], "flow property data set")
        self.assertEqual(ref["@version"], "01.00.000")
        self.assertEqual(
            result.document["flowDataSet"]["administrativeInformation"][
                "publicationAndOwnership"
            ]["common:dataSetVersion"],
            "01.00.000",
        )
        self.assertEqual(result.status, "planned")
        self.assertGreaterEqual(len(result.changes), 3)

    def test_ambiguous_reference_type_requires_manual_review(self):
        doc = {
            "unitGroupDataSet": {
                "reference": {
                    "@type": "2322333333333333",
                    "@refObjectId": "00000000-0000-0000-0000-000000000001",
                }
            }
        }

        result = migrate.migrate_document(
            "unitgroups",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        self.assertEqual(result.status, "manual_required")
        self.assertEqual(
            result.document["unitGroupDataSet"]["reference"]["@type"],
            "2322333333333333",
        )
        self.assertTrue(any(issue["rule"] == "A1_reference_type" for issue in result.issues))

    def test_missing_dataset_version_uses_approved_initial_version_when_no_collision(self):
        doc = {
            "flowDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {}
                }
            }
        }

        result = migrate.migrate_document(
            "flows",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={
                "flows": {
                    "00000000-0000-0000-0000-000000000010": {""}
                }
            },
            row_id="00000000-0000-0000-0000-000000000010",
        )

        publication = result.document["flowDataSet"]["administrativeInformation"][
            "publicationAndOwnership"
        ]
        self.assertEqual(publication["common:dataSetVersion"], "01.00.000")
        self.assertEqual(result.status, "planned")
        self.assertFalse(any(issue["rule"] == "A2_dataset_version" for issue in result.issues))

    def test_missing_dataset_version_creates_missing_publication_container(self):
        doc = {"flowPropertyDataSet": {}}

        result = migrate.migrate_document(
            "flowproperties",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={
                "flowproperties": {
                    "00000000-0000-0000-0000-000000000013": {""}
                }
            },
            row_id="00000000-0000-0000-0000-000000000013",
        )

        publication = result.document["flowPropertyDataSet"]["administrativeInformation"][
            "publicationAndOwnership"
        ]
        self.assertEqual(publication["common:dataSetVersion"], "01.00.000")
        self.assertEqual(result.status, "planned")

    def test_dataset_version_parent_container_type_mismatch_stays_manual(self):
        doc = {"flowPropertyDataSet": {"administrativeInformation": "bad"}}

        result = migrate.migrate_document(
            "flowproperties",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={
                "flowproperties": {
                    "00000000-0000-0000-0000-000000000014": {""}
                }
            },
            row_id="00000000-0000-0000-0000-000000000014",
        )

        self.assertEqual(result.status, "manual_required")
        self.assertTrue(any(issue["rule"] == "A2_dataset_version" for issue in result.issues))

    def test_prefixed_root_key_is_normalized_before_dataset_version_backfill(self):
        doc = {"f:flowDataSet": {}}

        result = migrate.migrate_document(
            "flows",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={"flows": {"00000000-0000-0000-0000-000000000015": {""}}},
            row_id="00000000-0000-0000-0000-000000000015",
        )

        self.assertNotIn("f:flowDataSet", result.document)
        self.assertIn("flowDataSet", result.document)
        self.assertEqual(
            result.document["flowDataSet"]["administrativeInformation"][
                "publicationAndOwnership"
            ]["common:dataSetVersion"],
            "01.00.000",
        )
        self.assertEqual(result.status, "planned")
        self.assertTrue(any(change["rule"] == "A9_dataset_root_key" for change in result.changes))

    def test_empty_json_dataset_is_marked_for_delete(self):
        result = migrate.migrate_document(
            "sources",
            {},
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={"sources": {"00000000-0000-0000-0000-000000000016": {""}}},
            row_id="00000000-0000-0000-0000-000000000016",
        )

        self.assertEqual(result.status, "delete_planned")
        self.assertTrue(any(change["rule"] == "D1_empty_json_delete" for change in result.changes))

    def test_missing_dataset_version_remains_manual_when_default_version_collides(self):
        doc = {
            "flowDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {}
                }
            }
        }

        result = migrate.migrate_document(
            "flows",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={
                "flows": {
                    "00000000-0000-0000-0000-000000000010": {"", "01.00.000"}
                }
            },
            row_id="00000000-0000-0000-0000-000000000010",
        )

        self.assertEqual(result.status, "manual_required")
        self.assertTrue(any(issue["rule"] == "A2_dataset_version" for issue in result.issues))

    def test_empty_dataset_version_uses_approved_initial_version_when_no_collision(self):
        doc = {
            "processDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": ""}
                }
            }
        }

        result = migrate.migrate_document(
            "processes",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="",
            version_index={
                "processes": {
                    "00000000-0000-0000-0000-000000000011": {""}
                }
            },
            row_id="00000000-0000-0000-0000-000000000011",
        )

        self.assertEqual(
            result.document["processDataSet"]["administrativeInformation"][
                "publicationAndOwnership"
            ]["common:dataSetVersion"],
            "01.00.000",
        )
        self.assertEqual(result.status, "planned")

    def test_comma_dataset_version_is_normalized_when_no_collision(self):
        doc = {
            "processDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": "01,01.000"}
                }
            }
        }

        result = migrate.migrate_document(
            "processes",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            current_version="01,01.000",
            version_index={
                "processes": {
                    "00000000-0000-0000-0000-000000000012": {"01,01.000"}
                }
            },
            row_id="00000000-0000-0000-0000-000000000012",
        )

        self.assertEqual(
            result.document["processDataSet"]["administrativeInformation"][
                "publicationAndOwnership"
            ]["common:dataSetVersion"],
            "01.01.000",
        )
        self.assertEqual(result.status, "planned")

    def test_reference_type_is_repaired_from_approved_path_mapping_not_value_alias(self):
        doc = {
            "processDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                },
                "modellingAndValidation": {
                    "complianceDeclarations": {
                        "compliance": {
                            "common:referenceToComplianceSystem": {
                                "@type": "Compliance system",
                                "@refObjectId": "00000000-0000-0000-0000-000000000020",
                                "@version": "01.00.000",
                            }
                        }
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "processes",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        ref = result.document["processDataSet"]["modellingAndValidation"][
            "complianceDeclarations"
        ]["compliance"]["common:referenceToComplianceSystem"]
        self.assertEqual(ref["@type"], "source data set")
        self.assertEqual(result.status, "planned")
        self.assertFalse(any(issue["rule"] == "A1_reference_type" for issue in result.issues))

    def test_untrusted_reference_type_value_stays_manual_on_unknown_path(self):
        doc = {
            "processDataSet": {
                "someUnknownReference": {
                    "@type": "Compliance system",
                    "@refObjectId": "00000000-0000-0000-0000-000000000020",
                    "@version": "01.00.000",
                }
            }
        }

        result = migrate.migrate_document(
            "processes",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        self.assertEqual(result.status, "manual_required")
        self.assertEqual(
            result.document["processDataSet"]["someUnknownReference"]["@type"],
            "Compliance system",
        )
        self.assertTrue(any(issue["rule"] == "A1_reference_type" for issue in result.issues))

    def test_invalid_reference_version_is_filled_from_unique_target_dataset_version(self):
        doc = {
            "lifeCycleModelDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                },
                "lifeCycleModelInformation": {
                    "technology": {
                        "processes": {
                            "processInstance": {
                                "referenceToProcess": {
                                    "@type": "process data set",
                                    "@refObjectId": "00000000-0000-0000-0000-000000000030",
                                    "@version": "         ",
                                }
                            }
                        }
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "lifecyclemodels",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            version_index={
                "processes": {
                    "00000000-0000-0000-0000-000000000030": {"01.02.003"}
                }
            },
        )

        ref = result.document["lifeCycleModelDataSet"]["lifeCycleModelInformation"][
            "technology"
        ]["processes"]["processInstance"]["referenceToProcess"]
        self.assertEqual(ref["@version"], "01.02.003")
        self.assertEqual(result.status, "planned")
        self.assertFalse(any(issue["rule"] == "A2_reference_version" for issue in result.issues))

    def test_invalid_reference_version_stays_manual_when_target_has_multiple_versions(self):
        doc = {
            "lifeCycleModelDataSet": {
                "administrativeInformation": {
                    "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                },
                "lifeCycleModelInformation": {
                    "technology": {
                        "processes": {
                            "processInstance": {
                                "referenceToProcess": {
                                    "@type": "process data set",
                                    "@refObjectId": "00000000-0000-0000-0000-000000000030",
                                    "@version": "",
                                }
                            }
                        }
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "lifecyclemodels",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
            version_index={
                "processes": {
                    "00000000-0000-0000-0000-000000000030": {"01.00.000", "01.02.003"}
                }
            },
        )

        ref = result.document["lifeCycleModelDataSet"]["lifeCycleModelInformation"][
            "technology"
        ]["processes"]["processInstance"]["referenceToProcess"]
        self.assertEqual(ref["@version"], "01.02.003")
        self.assertEqual(result.status, "planned")
        self.assertFalse(any(issue["rule"] == "A2_reference_version" for issue in result.issues))

    def test_reference_type_is_repaired_from_dataset_reference_field_name(self):
        doc = {
            "flowDataSet": {
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "@type": "asdaaaaaaaaaaaaaaa",
                            "@refObjectId": "00000000-0000-0000-0000-000000000050",
                            "@version": "01.00.000",
                        }
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "flows",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        ref = result.document["flowDataSet"]["flowProperties"]["flowProperty"][
            "referenceToFlowPropertyDataSet"
        ]
        self.assertEqual(ref["@type"], "flow property data set")
        self.assertFalse(any(issue["rule"] == "A1_reference_type" for issue in result.issues))

    def test_version_lookup_collection_ignores_invalid_reference_uuid(self):
        rows = [
            {
                "id": "00000000-0000-0000-0000-000000000040",
                "json": {
                    "flowDataSet": {
                        "administrativeInformation": {
                            "dataEntryBy": {
                                "common:referenceToDataSetFormat": {
                                    "@type": "source data set",
                                    "@refObjectId": "",
                                    "@version": "",
                                }
                            }
                        }
                    }
                },
            }
        ]

        lookup_ids = migrate.collect_version_lookup_ids(
            table="flows",
            rows=rows,
            type_aliases={},
        )

        self.assertEqual(
            lookup_ids,
            {"flows": {"00000000-0000-0000-0000-000000000040"}},
        )

    def test_lcia_boolean_and_uncertainty_key_migration(self):
        doc = {
            "LCIAMethodDataSet": {
                "modellingAndValidation": {
                    "LCIAMethod": {
                        "normalisationAndWeighting": {
                            "normalisation": "true",
                            "weighting": "false",
                        },
                        "characterisationFactors": {
                            "factor": [
                                {"uncertaintyType": "normal"},
                            ]
                        },
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "lciamethods",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        naw = result.document["LCIAMethodDataSet"]["modellingAndValidation"][
            "LCIAMethod"
        ]["normalisationAndWeighting"]
        factor = result.document["LCIAMethodDataSet"]["modellingAndValidation"][
            "LCIAMethod"
        ]["characterisationFactors"]["factor"][0]
        self.assertIs(naw["normalisation"], True)
        self.assertIs(naw["weighting"], False)
        self.assertNotIn("uncertaintyType", factor)
        self.assertEqual(factor["uncertaintyDistributionType"], "normal")

    def test_process_location_codes_are_rewritten_on_location_fields_only(self):
        doc = {
            "processDataSet": {
                "processInformation": {
                    "geography": {
                        "locationOfOperationSupplyOrProduction": {
                            "@location": "CN-HK",
                            "#text": "CN-HK should remain in prose",
                        },
                        "subLocationOfOperationSupplyOrProduction": {
                            "@location": "CN-TW"
                        },
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "processes",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        geography = result.document["processDataSet"]["processInformation"]["geography"]
        self.assertEqual(
            geography["locationOfOperationSupplyOrProduction"]["@location"], "HK"
        )
        self.assertEqual(
            geography["subLocationOfOperationSupplyOrProduction"]["@location"], "TW"
        )
        self.assertEqual(
            geography["locationOfOperationSupplyOrProduction"]["#text"],
            "CN-HK should remain in prose",
        )

    def test_lifecycle_model_integer_and_missing_versions(self):
        doc = {
            "lifeCycleModelDataSet": {
                "lifeCycleModelInformation": {
                    "quantitativeReference": {
                        "referenceToReferenceProcess": "12"
                    },
                    "technology": {
                        "processes": {
                            "processInstance": {
                                "connections": {
                                    "outputExchange": {
                                        "@flowUUID": "00000000-0000-0000-0000-000000000001",
                                        "downstreamProcess": {
                                            "@id": "1",
                                            "@flowUUID": "00000000-0000-0000-0000-000000000002",
                                        },
                                    }
                                }
                            }
                        }
                    },
                }
            }
        }

        result = migrate.migrate_document(
            "lifecyclemodels",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version="00.00.001",
        )

        root = result.document["lifeCycleModelDataSet"]["lifeCycleModelInformation"]
        self.assertEqual(root["quantitativeReference"]["referenceToReferenceProcess"], 12)
        output = root["technology"]["processes"]["processInstance"]["connections"][
            "outputExchange"
        ]
        self.assertEqual(output["@version"], "00.00.001")
        self.assertEqual(output["downstreamProcess"]["@version"], "00.00.001")

    def test_lifecycle_model_leading_zero_reference_process_is_manual(self):
        doc = {
            "lifeCycleModelDataSet": {
                "lifeCycleModelInformation": {
                    "quantitativeReference": {
                        "referenceToReferenceProcess": "007"
                    }
                }
            }
        }

        result = migrate.migrate_document(
            "lifecyclemodels",
            copy.deepcopy(doc),
            type_aliases={},
            lcia_review_map=migrate.LciaReviewMap(scope={}, method={}),
            unresolved_lifecycle_version=None,
        )

        self.assertEqual(result.status, "manual_required")
        self.assertEqual(
            result.document["lifeCycleModelDataSet"]["lifeCycleModelInformation"][
                "quantitativeReference"
            ]["referenceToReferenceProcess"],
            "007",
        )


if __name__ == "__main__":
    unittest.main()
