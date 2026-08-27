#!/usr/bin/env python3
"""Verify Search and Hybrid share one exhaustive public-card context."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = REPO_ROOT / "contracts" / "portal"
CONTEXT_REF = "./portal.common-types.v1.schema.json#/$defs/PublicCardContext"
CONTEXT_KEYS = ["reference", "functionalUnit", "technology", "source", "quality"]


def load(name: str) -> dict[str, Any]:
    return json.loads((CONTRACT_DIR / name).read_text(encoding="utf-8"))


def require_exhaustive_object(schema: dict[str, Any], label: str) -> None:
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        raise RuntimeError(f"{label} must be an exhaustive object")
    properties = schema.get("properties")
    required = schema.get("required")
    if not isinstance(properties, dict) or not isinstance(required, list):
        raise RuntimeError(f"{label} must declare properties and required keys")
    if set(properties) != set(required):
        raise RuntimeError(f"{label} must require every declared property")


def main() -> int:
    common = load("portal.common-types.v1.schema.json")
    search = load("portal.public-search-page.v1.schema.json")
    hybrid = load("portal.public-hybrid-candidate-page.v1.schema.json")

    search_item = search["$defs"]["SearchItem"]
    hybrid_item = hybrid["$defs"]["Candidate"]
    context = common["$defs"]["PublicCardContext"]
    reference = common["$defs"]["PublicCardReference"]
    quality = common["$defs"]["PublicCardQuality"]

    for schema, label in (
        (search_item, "SearchItem"),
        (hybrid_item, "Hybrid Candidate"),
        (context, "PublicCardContext"),
        (reference, "PublicCardReference"),
        (quality, "PublicCardQuality"),
    ):
        require_exhaustive_object(schema, label)

    search_card = {
        key: value
        for key, value in search_item["properties"].items()
        if key != "match"
    }
    hybrid_card = {
        key: value
        for key, value in hybrid_item["properties"].items()
        if key != "match"
    }
    if search_card != hybrid_card:
        raise RuntimeError("Search and Hybrid card properties differ outside match")
    if set(search_item["required"]) - {"match"} != set(hybrid_item["required"]) - {
        "match"
    }:
        raise RuntimeError("Search and Hybrid card required keys differ outside match")
    if search_card.get("context") != {"$ref": CONTEXT_REF}:
        raise RuntimeError("Search and Hybrid must use the shared PublicCardContext ref")

    if context["required"] != CONTEXT_KEYS or list(context["properties"]) != CONTEXT_KEYS:
        raise RuntimeError("PublicCardContext has drifted from its exact ordered key set")
    if reference["properties"]["kind"].get("enum") != [
        "reference_product",
        "reference_flow_property",
    ]:
        raise RuntimeError("PublicCardReference kind allowlist drifted")
    if quality["required"] != ["reviewStatus"]:
        raise RuntimeError("PublicCardQuality must expose only reviewStatus")

    print(
        "PASS: Search and Hybrid share one exhaustive five-field public-card "
        "context and differ only in match"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
