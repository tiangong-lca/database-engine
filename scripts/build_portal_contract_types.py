#!/usr/bin/env python3
"""Generate deterministic TypeScript modules from Portal JSON Schemas."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = REPO_ROOT / "contracts" / "portal"
OUTPUT_DIR = SCHEMA_DIR / "generated"
TOOL_VERSION = "15.0.4"
GENERATED_SUFFIX = ".d.ts"


def output_name(schema: Path) -> str:
    suffix = ".schema.json"
    if not schema.name.endswith(suffix):
        raise ValueError(f"unsupported Portal schema name: {schema.name}")
    return schema.name[: -len(suffix)] + GENERATED_SUFFIX


def compile_schema(schema: Path, output: Path) -> None:
    subprocess.run(
        [
            "npx",
            "--yes",
            "--package",
            f"json-schema-to-typescript@{TOOL_VERSION}",
            "json2ts",
            "--input",
            str(schema),
            "--output",
            str(output),
            "--cwd",
            str(SCHEMA_DIR),
            "--no-enableConstEnums",
            "--unknownAny",
            "--unreachableDefinitions",
            "--maxItems=-1",
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    generated = output.read_text(encoding="utf-8").rstrip() + "\n"
    provenance = (
        f"// Source: ../{schema.name}\n"
        f"// Generator: json-schema-to-typescript@{TOOL_VERSION}\n"
    )
    output.write_text(provenance + generated, encoding="utf-8")


def render_all(destination: Path) -> dict[str, str]:
    schemas = sorted(SCHEMA_DIR.glob("*.schema.json"), key=lambda path: path.name)
    if not schemas:
        raise RuntimeError(f"no Portal JSON Schemas found under {SCHEMA_DIR}")

    rendered: dict[str, str] = {}
    for schema in schemas:
        generated = destination / output_name(schema)
        compile_schema(schema, generated)
        rendered[generated.name] = generated.read_text(encoding="utf-8")
    return rendered


def check_generated(rendered: dict[str, str]) -> int:
    actual_names = (
        {path.name for path in OUTPUT_DIR.glob(f"*{GENERATED_SUFFIX}")}
        if OUTPUT_DIR.is_dir()
        else set()
    )
    expected_names = set(rendered)
    drifted = sorted(
        name
        for name, expected in rendered.items()
        if not (OUTPUT_DIR / name).is_file()
        or (OUTPUT_DIR / name).read_text(encoding="utf-8") != expected
    )
    unexpected = sorted(actual_names - expected_names)
    if drifted or unexpected:
        if drifted:
            print("Portal generated TypeScript drift: " + ", ".join(drifted))
        if unexpected:
            print("Unexpected Portal generated TypeScript: " + ", ".join(unexpected))
        return 1
    print(
        f"Portal generated TypeScript is current ({len(expected_names)} modules, "
        f"json-schema-to-typescript@{TOOL_VERSION})"
    )
    return 0


def write_generated(rendered: dict[str, str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    expected_names = set(rendered)
    for existing in OUTPUT_DIR.glob(f"*{GENERATED_SUFFIX}"):
        if existing.name not in expected_names:
            existing.unlink()
    for name, content in rendered.items():
        (OUTPUT_DIR / name).write_text(content, encoding="utf-8")
    print(
        f"Wrote {len(rendered)} Portal TypeScript modules with "
        f"json-schema-to-typescript@{TOOL_VERSION}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate committed TypeScript modules from the exhaustive Portal "
            "JSON Schemas."
        )
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail on generated drift without modifying the checkout",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="portal-contract-types-") as temp_dir:
        rendered = render_all(Path(temp_dir))
    if args.check:
        return check_generated(rendered)
    write_generated(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
