#!/usr/bin/env python3
"""Resolve one fail-closed loopback database target for Issue #355 probes."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
TARGET_ENV = "IDENTITY_COLLABORATION_DATABASE_URL"


def supabase_command(*args: str) -> list[str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(args)
    return command


def local_status() -> dict[str, str]:
    result = subprocess.run(
        supabase_command("status", "--output", "json"), cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def _require_loopback(db_url: str) -> None:
    host = urlparse(db_url).hostname
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("Issue #355 probes require an explicit loopback database target")


def _database_identity(db_url: str) -> str:
    result = subprocess.run(
        [
            "psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c",
            "select (pg_control_system()).system_identifier || ':' || current_database()",
        ],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    return result.stdout.strip()


def resolve_target(*, require_status: bool = True) -> tuple[str, dict[str, str] | None]:
    status = local_status() if require_status else None
    explicit = os.environ.get(TARGET_ENV) or os.environ.get("DATABASE_URL")
    if explicit is None:
        if status is None:
            raise SystemExit(f"set {TARGET_ENV} to one loopback database URL")
        explicit = status["DB_URL"]
    _require_loopback(explicit)
    if status is not None:
        status_url = status["DB_URL"]
        _require_loopback(status_url)
        if _database_identity(explicit) != _database_identity(status_url):
            raise SystemExit(
                "Issue #355 explicit database target does not match the selected Supabase stack"
            )
    return explicit, status
