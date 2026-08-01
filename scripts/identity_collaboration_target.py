#!/usr/bin/env python3
"""Resolve one fail-closed loopback database target for Issue #355 probes."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
TARGET_ENV = "IDENTITY_COLLABORATION_DATABASE_URL"
VERIFIED_ENV = "DATABASE_CONTRACT_TARGET_VERIFIED"


@dataclass(frozen=True)
class TargetContext:
    database_url: str
    rest_url: str
    anon_key: str
    service_role_key: str
    jwt_secret: str
    workdir: str | None
    identity: str


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


def resolve_target() -> TargetContext:
    status = local_status()
    explicit = os.environ.get(TARGET_ENV) or os.environ.get("DATABASE_URL")
    if explicit is None:
        explicit = status["DB_URL"]
    _require_loopback(explicit)
    status_url = status["DB_URL"]
    _require_loopback(status_url)
    explicit_identity = _database_identity(explicit)
    if explicit_identity != _database_identity(status_url):
        raise SystemExit(
            "database contract target does not match the selected Supabase stack"
        )
    return TargetContext(
        database_url=explicit,
        rest_url=status["REST_URL"].rstrip("/"),
        anon_key=status["ANON_KEY"],
        service_role_key=status["SERVICE_ROLE_KEY"],
        jwt_secret=status["JWT_SECRET"],
        workdir=os.environ.get("SUPABASE_WORKDIR"),
        identity=explicit_identity,
    )


def apply_target_environment(target: TargetContext) -> None:
    os.environ["DATABASE_URL"] = target.database_url
    os.environ[TARGET_ENV] = target.database_url
    os.environ["SUPABASE_REST_URL"] = target.rest_url
    os.environ["SUPABASE_ANON_KEY"] = target.anon_key
    os.environ["SUPABASE_SERVICE_ROLE_KEY"] = target.service_role_key
    os.environ["SUPABASE_JWT_SECRET"] = target.jwt_secret
    os.environ[VERIFIED_ENV] = target.identity
    if target.workdir:
        os.environ["SUPABASE_WORKDIR"] = target.workdir
    else:
        os.environ.pop("SUPABASE_WORKDIR", None)


def verified_database_url() -> str:
    database_url = os.environ.get(TARGET_ENV) or os.environ.get("DATABASE_URL")
    if database_url and os.environ.get(VERIFIED_ENV):
        _require_loopback(database_url)
        return database_url
    return resolve_target().database_url
