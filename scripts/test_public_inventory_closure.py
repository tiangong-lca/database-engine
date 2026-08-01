#!/usr/bin/env python3
"""Offline unit tests for the public inventory closure builder."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import public_inventory_closure as inventory


class PublicInventoryClosureTest(unittest.TestCase):
    def test_target_normalization_is_fail_safe(self) -> None:
        row = {"object_key": "public.old", "object_type": "table", "object_name": "old", "candidate_target": "private_or_retire"}
        self.assertEqual(inventory.target_for(row), "private")
        row["candidate_target"] = "util_or_retire"
        self.assertEqual(inventory.target_for(row), "util")

    def test_core_table_is_always_retained(self) -> None:
        row = {"object_key": "public.processes", "object_type": "table", "object_name": "processes", "candidate_target": "private"}
        self.assertEqual(inventory.target_for(row), "public")

    def test_dynamic_and_regclass_body_dependencies_are_recorded(self) -> None:
        catalog = {
            "relations": [{"object_key": "public.jobs", "object_name": "jobs"}],
            "routines": [{
                "object_key": "public.read_jobs()", "object_name": "read_jobs",
                "definition": "create function public.read_jobs() returns void language plpgsql as $$ begin execute format('select * from public.jobs'); perform to_regclass('public.jobs'); end $$;",
            }],
        }
        edges = inventory.body_dependencies(copy.deepcopy(catalog))
        self.assertEqual(edges[0]["source_key"], "public.read_jobs()")
        self.assertEqual(edges[0]["target_key"], "public.jobs")
        self.assertEqual(edges[0]["dependency_name"], "routine-body-dynamic")

    def test_dependency_plan_groups_cycles_and_reverses_contract_order(self) -> None:
        objects = [
            {"objectKey": "public.a", "migrationBatch": "a"},
            {"objectKey": "public.b", "migrationBatch": "b"},
            {"objectKey": "public.c", "migrationBatch": "c"},
        ]
        edges = [
            {"source_key": "public.a", "target_key": "public.b"},
            {"source_key": "public.b", "target_key": "public.a"},
            {"source_key": "public.c", "target_key": "public.a"},
        ]
        plan = inventory.dependency_plan({"public.a", "public.b", "public.c"}, edges, objects)
        expand = plan["expandDependencyGroups"]
        self.assertEqual(expand[0]["objects"], ["public.a", "public.b"])
        self.assertTrue(expand[0]["cyclic"])
        self.assertEqual(expand[1]["objects"], ["public.c"])
        self.assertEqual(plan["contractDependencyGroups"][0]["objects"], ["public.c"])


if __name__ == "__main__":
    unittest.main()
