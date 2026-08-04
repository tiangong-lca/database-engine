#!/usr/bin/env python3
"""Apply only the reviewed PostgREST config to one persistent Supabase branch."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "supabase/config.toml"
DEFAULT_API_BASE = "https://api.supabase.com"
ALLOWED_FIELDS = ("db_schema", "db_extra_search_path", "max_rows")
REQUIRED_EXPOSED_SCHEMAS = ("api", "public", "graphql_public")
FORBIDDEN_INTERNAL_SCHEMAS = frozenset({"private", "util", "archive"})


class ConfigError(RuntimeError):
    """Fail-closed configuration or remote reconciliation error."""


def _string_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name} must be a non-empty string list")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ConfigError(f"{name} must contain only non-empty strings")
    result = [item.strip() for item in value]
    if len(result) != len(set(result)):
        raise ConfigError(f"{name} must not contain duplicates")
    return result


def load_desired_config(path: Path, project_ref: str) -> dict[str, Any]:
    with path.open("rb") as handle:
        document = tomllib.load(handle)

    root_api = document.get("api")
    if not isinstance(root_api, dict):
        raise ConfigError("config.toml must contain an [api] table")

    matches: list[dict[str, Any]] = []
    remotes = document.get("remotes", {})
    if not isinstance(remotes, dict):
        raise ConfigError("config.toml [remotes] must be a table")
    for remote in remotes.values():
        if isinstance(remote, dict) and remote.get("project_id") == project_ref:
            matches.append(remote)
    if len(matches) != 1:
        raise ConfigError(
            f"project ref must match exactly one [remotes.*] binding; matches={len(matches)}"
        )

    effective_api = dict(root_api)
    remote_api = matches[0].get("api", {})
    if not isinstance(remote_api, dict):
        raise ConfigError("remote api override must be a table")
    effective_api.update(remote_api)

    schemas = _string_list(effective_api.get("schemas"), "api.schemas")
    if tuple(schemas) != REQUIRED_EXPOSED_SCHEMAS:
        raise ConfigError(
            "api.schemas must be exactly api, public, graphql_public in reviewed order"
        )

    search_path = _string_list(
        effective_api.get("extra_search_path"), "api.extra_search_path"
    )
    forbidden = sorted(FORBIDDEN_INTERNAL_SCHEMAS.intersection(search_path))
    if forbidden:
        raise ConfigError(f"internal schemas cannot enter extra_search_path: {forbidden}")

    max_rows = effective_api.get("max_rows")
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0:
        raise ConfigError("api.max_rows must be a positive integer")

    return {
        "db_schema": ",".join(schemas),
        "db_extra_search_path": ",".join(search_path),
        "max_rows": max_rows,
    }


def _canonical(field: str, value: Any) -> Any:
    if field in {"db_schema", "db_extra_search_path"}:
        if not isinstance(value, str):
            return ()
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if field == "max_rows":
        return value if isinstance(value, int) and not isinstance(value, bool) else None
    raise ConfigError(f"unreviewed PostgREST field: {field}")


def changed_fields(desired: dict[str, Any], remote: dict[str, Any]) -> list[str]:
    if set(desired) != set(ALLOWED_FIELDS):
        raise ConfigError("desired config contains fields outside the allowlist")
    return [
        field
        for field in ALLOWED_FIELDS
        if _canonical(field, desired[field]) != _canonical(field, remote.get(field))
    ]


class ManagementClient:
    def __init__(
        self,
        project_ref: str,
        access_token: str,
        api_base: str = DEFAULT_API_BASE,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        if not access_token:
            raise ConfigError("SUPABASE_ACCESS_TOKEN is required")
        self.url = f"{api_base.rstrip('/')}/v1/projects/{project_ref}/postgrest"
        self.access_token = access_token
        self.opener = opener

    def _request(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "database-engine-postgrest-config-gate/1.0",
            },
        )
        try:
            with self.opener(request, timeout=30) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise ConfigError(
                f"Management API {method} failed with HTTP {exc.code}"
            ) from None
        except urllib.error.URLError:
            raise ConfigError(f"Management API {method} transport failed") from None
        if not body:
            return {}
        result = json.loads(body)
        if not isinstance(result, dict):
            raise ConfigError(f"Management API {method} returned a non-object response")
        return result

    def get(self) -> dict[str, Any]:
        return self._request("GET")

    def patch(self, payload: dict[str, Any]) -> None:
        if not payload or not set(payload).issubset(ALLOWED_FIELDS):
            raise ConfigError("PATCH payload is empty or contains unreviewed fields")
        self._request("PATCH", payload)


def reconcile(
    desired: dict[str, Any],
    get_remote: Callable[[], dict[str, Any]],
    patch_remote: Callable[[dict[str, Any]], None],
    apply: bool,
) -> tuple[str, ...]:
    drift = changed_fields(desired, get_remote())
    if not drift:
        return ()
    if not apply:
        raise ConfigError(f"PostgREST config drift: {','.join(drift)}")

    patch_remote({field: desired[field] for field in drift})
    remaining = changed_fields(desired, get_remote())
    if remaining:
        raise ConfigError(f"PostgREST config readback mismatch: {','.join(remaining)}")
    return tuple(drift)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-ref", required=True)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    try:
        desired = load_desired_config(args.config, args.project_ref)
        client = ManagementClient(
            args.project_ref,
            os.environ.get("SUPABASE_ACCESS_TOKEN", ""),
            args.api_base,
        )
        changed = reconcile(desired, client.get, client.patch, args.apply)
    except (ConfigError, OSError, json.JSONDecodeError) as exc:
        print(f"PostgREST config gate failed: {exc}", file=sys.stderr)
        return 1

    if changed:
        print(f"PostgREST config applied and verified: {','.join(changed)}")
    else:
        print("PostgREST config already matches the reviewed allowlist")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
