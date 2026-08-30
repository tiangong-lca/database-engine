#!/usr/bin/env python3
"""Validate the checked-in Supabase password-recovery email contract."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
import tomllib


CONFIRMATION_URL = "{{ .ConfirmationURL }}"
EXPECTED_CONTENT_PATH = "./supabase/templates/recovery.html"


class AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []
        self._active_anchor: dict[str, str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        anchor = {"href": dict(attrs).get("href") or "", "text": ""}
        self.anchors.append(anchor)
        self._active_anchor = anchor

    def handle_data(self, data: str) -> None:
        if self._active_anchor is not None:
            self._active_anchor["text"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a":
            self._active_anchor = None


def validate(repo_root: Path) -> list[str]:
    errors: list[str] = []
    config_path = repo_root / "supabase" / "config.toml"
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    recovery_config = config.get("auth", {}).get("email", {}).get("template", {}).get("recovery")
    if not isinstance(recovery_config, dict):
        return ["supabase/config.toml must define [auth.email.template.recovery]"]

    if recovery_config.get("content_path") != EXPECTED_CONTENT_PATH:
        errors.append(f"recovery content_path must be {EXPECTED_CONTENT_PATH}")
    if not isinstance(recovery_config.get("subject"), str) or not recovery_config["subject"].strip():
        errors.append("recovery subject must be a non-empty string")

    template_path = repo_root / EXPECTED_CONTENT_PATH.removeprefix("./")
    if not template_path.is_file():
        errors.append(f"recovery template is missing: {template_path.relative_to(repo_root)}")
        return errors

    template = template_path.read_text(encoding="utf-8")
    parser = AnchorParser()
    parser.feed(template)
    confirmation_anchors = [
        anchor for anchor in parser.anchors if anchor["href"].strip() == CONFIRMATION_URL
    ]
    if not any(anchor["text"].strip() != CONFIRMATION_URL for anchor in confirmation_anchors):
        errors.append("recovery template must have a button-style ConfirmationURL link")
    if not any(anchor["text"].strip() == CONFIRMATION_URL for anchor in confirmation_anchors):
        errors.append("recovery template must show ConfirmationURL as a copyable link")
    if "type=magiclink" in template or "{{ .TokenHash }}" in template:
        errors.append("recovery template must not hand-build a magic-link URL")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root (defaults to the parent of scripts/)",
    )
    args = parser.parse_args()

    errors = validate(args.root.resolve())
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Auth recovery email template contract is valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
