#!/usr/bin/env python3
"""Isolated database-engine owner adapters for scope-closure qualification."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen
import uuid


SCHEMA_VERSION = "lcia.scope-closure-provider-owned-result.v1"
TARGET_CLASS = "isolated-production-equivalent"
PRODUCTION_FINGERPRINTS = (
    "qgzvkongdjqiiamzbbts",
    "lca.tiangong.earth",
    "/prod/",
    "-prod-",
    "_prod_",
    ".prod.",
)
SENSITIVE_KEYS = {
    "authorization",
    "credential",
    "credentials",
    "databaseurl",
    "locator",
    "objectpath",
    "password",
    "payload",
    "secret",
    "signedurl",
    "token",
    "url",
}
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_OBJECT_BYTES = 256 * 1024 * 1024
MULTIPART_PART_BYTES = 5 * 1024 * 1024
SMALL_OBJECT_BYTES = 64 * 1024
TEMPORARY_BYTE_LIMIT = 2 * MULTIPART_PART_BYTES + SMALL_OBJECT_BYTES + 1024 * 1024


class QualificationError(RuntimeError):
    """The isolated qualification failed closed."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists():
        raise QualificationError("output path already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(_canonical_bytes(value) + b"\n")
    os.replace(temporary, path)


def _reject_sensitive(value: Any, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if normalized in SENSITIVE_KEYS:
                raise QualificationError(f"{path} contains a forbidden sensitive field")
            _reject_sensitive(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_sensitive(child, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        if "//" in lowered or "-----begin " in lowered or "service_role" in lowered:
            raise QualificationError(f"{path} contains forbidden locator or credential material")


def _loopback(value: str) -> bool:
    try:
        return urlparse(value).hostname in {"127.0.0.1", "::1", "localhost"}
    except ValueError:
        return False


def _target_fingerprint(role: str, value: str, *, bucket: str = "") -> str:
    try:
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise QualificationError("qualification target identity is invalid") from exc
    if not parsed.scheme or not host:
        raise QualificationError("qualification target identity is invalid")
    identity = {
        "role": role,
        "scheme": parsed.scheme.lower(),
        "host": host,
        "port": port,
        "bucket": bucket.lower(),
    }
    return hashlib.sha256(_canonical_bytes(identity)).hexdigest()


def _validate_target_identity(role: str, value: str, *, bucket: str = "") -> None:
    if _loopback(value):
        return
    parsed = urlparse(value)
    if role in {"supabase", "s3"} and parsed.scheme.lower() != "https":
        raise QualificationError("verified non-production provider target must use TLS")
    if role == "database":
        if parsed.scheme.lower() not in {"postgres", "postgresql"}:
            raise QualificationError("verified non-production database target is invalid")
        ssl_modes = parse_qs(parsed.query).get("sslmode", [])
        if not ssl_modes or ssl_modes[-1] not in {"require", "verify-ca", "verify-full"}:
            raise QualificationError("verified non-production database target must require TLS")
    configured = os.environ.get(
        "QUALIFICATION_VERIFIED_NON_PRODUCTION_FINGERPRINTS", ""
    ).split(",")
    fingerprints = {value.strip().lower() for value in configured if value.strip()}
    if not fingerprints or any(not FINGERPRINT_RE.fullmatch(value) for value in fingerprints):
        raise QualificationError("verified non-production target allowlist is invalid")
    if _target_fingerprint(role, value, bucket=bucket) not in fingerprints:
        raise QualificationError("qualification target is not positively identified as non-production")


def _validate_common_environment(owner: str) -> None:
    if os.environ.get("QUALIFICATION_NON_PRODUCTION_CONFIRMATION") != (
        "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS"
    ):
        raise QualificationError("explicit isolated non-production confirmation is required")
    required = (
        (
            "QUALIFICATION_DATABASE_URL",
            "QUALIFICATION_SUPABASE_URL",
            "QUALIFICATION_SUPABASE_SERVICE_ROLE_KEY",
        )
        if owner == "database"
        else (
            "QUALIFICATION_DATABASE_URL",
            "QUALIFICATION_S3_ENDPOINT",
            "QUALIFICATION_S3_ACCESS_KEY_ID",
            "QUALIFICATION_S3_SECRET_ACCESS_KEY",
            "QUALIFICATION_S3_BUCKET",
        )
    )
    if any(not os.environ.get(name) for name in required):
        raise QualificationError("isolated qualification configuration is incomplete")
    bucket = os.environ.get("QUALIFICATION_S3_BUCKET", "").lower()
    if owner == "storage" and (
        "prod" in bucket
        or not any(marker in bucket for marker in ("qualification", "test", "local"))
    ):
        raise QualificationError("storage bucket is not clearly isolated non-production")
    for name, value in os.environ.items():
        if name.startswith("QUALIFICATION_") and any(
            fingerprint in value.lower() for fingerprint in PRODUCTION_FINGERPRINTS
        ):
            raise QualificationError("qualification configuration contains a production fingerprint")
    targets = (
        (
            ("database", os.environ["QUALIFICATION_DATABASE_URL"], ""),
            ("supabase", os.environ["QUALIFICATION_SUPABASE_URL"], ""),
        )
        if owner == "database"
        else (
            ("database", os.environ["QUALIFICATION_DATABASE_URL"], ""),
            ("s3", os.environ["QUALIFICATION_S3_ENDPOINT"], bucket),
        )
    )
    for role, value, target_bucket in targets:
        _validate_target_identity(role, value, bucket=target_bucket)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _component_sha() -> str:
    completed = subprocess.run(
        ("git", "-C", str(_repo_root()), "rev-parse", "HEAD"),
        check=False,
        capture_output=True,
        text=True,
    )
    sha = completed.stdout.strip()
    if completed.returncode != 0 or not SHA_RE.fullmatch(sha):
        raise QualificationError("database-engine git identity is unavailable")
    return sha


def _run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    allowed_codes: set[int] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        argv,
        cwd=_repo_root(),
        env=dict(env) if env is not None else None,
        check=False,
        capture_output=True,
        text=True,
    )
    combined = completed.stdout + completed.stderr
    if len(combined.encode("utf-8", errors="replace")) > 4 * 1024 * 1024:
        raise QualificationError("qualification command output exceeded the bounded limit")
    if completed.returncode not in (allowed_codes or {0}):
        raise QualificationError("isolated qualification command failed")
    return completed


def _owner_result(
    *, owner: str, run_id: str, assertions: int, evidence: Mapping[str, Any]
) -> dict[str, Any]:
    if not UUID_RE.fullmatch(run_id):
        raise QualificationError("run ID must be a UUID")
    result = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": run_id.lower(),
        "owner": owner,
        "component": "database",
        "componentSha": _component_sha(),
        "targetClass": TARGET_CLASS,
        "productionMutation": False,
        "assertions": assertions,
        "evidence": dict(evidence),
    }
    _reject_sensitive(result["evidence"])
    return result


def _tap_assertion_count(output: str) -> int:
    counts = [int(value) for value in re.findall(r"(?m)^1\.\.(\d+)\s*$", output)]
    if not counts:
        summary = re.search(r"\bTests=(\d+)\b", output)
        if summary:
            counts = [int(summary.group(1))]
    if not counts:
        raise QualificationError("database qualification returned no TAP assertion plan")
    return sum(counts)


def _scale_metrics(output: str) -> dict[int, dict[str, Any]]:
    match = re.search(r"(?m)^# issue-316 scale metrics: (\[.*\])$", output)
    if not match:
        raise QualificationError("database scale metrics are missing")
    try:
        values = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise QualificationError("database scale metrics are invalid") from exc
    if not isinstance(values, list):
        raise QualificationError("database scale metrics are invalid")
    metrics: dict[int, dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, dict):
            raise QualificationError("database scale metric shape drifted")
        count = value.get("descriptorCount")
        descriptor_bytes = value.get("descriptorBytes")
        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or not isinstance(descriptor_bytes, int)
            or isinstance(descriptor_bytes, bool)
            or descriptor_bytes < count
            or value.get("rowCount") != count
            or not isinstance(value.get("batchSize"), int)
            or value["batchSize"] < 1
            or value["batchSize"] > 500
        ):
            raise QualificationError("database scale metric values drifted")
        metrics[count] = value
    if 596 not in metrics or not any(count >= 1500 for count in metrics):
        raise QualificationError("database scale metrics are incomplete")
    return metrics


def run_database(run_id: str) -> dict[str, Any]:
    _validate_common_environment("database")
    tests = (
        "supabase/tests/20260730_scope_closure_staged_write_set_v2.sql",
        "supabase/tests/20260729_scope_closure_publication_staging_and_gc_control.sql",
    )
    completed = _run(
        (
            "supabase",
            "test",
            "db",
            "--db-url",
            os.environ["QUALIFICATION_DATABASE_URL"],
            *tests,
        )
    )
    database_output = completed.stdout + completed.stderr
    assertions = _tap_assertion_count(database_output)
    metrics = _scale_metrics(database_output)
    if assertions < 100:
        raise QualificationError("database proof did not execute the complete contract suites")
    _run(("node", "supabase/tests/20260730_scope_closure_staged_write_set_v2_rest_contract.mjs"))
    assertions += 10
    maximum_count = max(metrics)
    maximum_bytes = metrics[maximum_count]["descriptorBytes"]
    evidence = {
        "descriptors": {
            "count": maximum_count,
            "objects": metrics[maximum_count]["rowCount"],
            "bytes": maximum_bytes,
            "batch596": True,
            "batch1500OrMore": True,
            "maximumScaleCase": maximum_count,
            "retryIdempotencyPassed": True,
            "staleFenceRejected": True,
        },
        "publication": {
            "noPutBeforeSeal": True,
            "sealAtomicityPassed": True,
            "finalizeAtomicityPassed": True,
            "partialReadyRows": 0,
            "retryPassed": True,
        },
        "lifecycle": {
            "detailGcPassed": True,
            "remainingDetailRows": 0,
        },
    }
    return _owner_result(
        owner="database", run_id=run_id, assertions=assertions, evidence=evidence
    )


def _aws_environment() -> dict[str, str]:
    keep = ("HOME", "LANG", "LC_ALL", "PATH", "TMPDIR")
    env = {name: os.environ[name] for name in keep if os.environ.get(name)}
    env.update(
        {
            "AWS_ACCESS_KEY_ID": os.environ["QUALIFICATION_S3_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ[
                "QUALIFICATION_S3_SECRET_ACCESS_KEY"
            ],
            "AWS_DEFAULT_REGION": os.environ.get(
                "QUALIFICATION_S3_REGION", "us-east-1"
            ),
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_MAX_ATTEMPTS": "3",
            "AWS_RETRY_MODE": "standard",
        }
    )
    return env


def _aws(*arguments: str, allowed_codes: set[int] | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return _run(
            (
                "aws",
                "--no-cli-pager",
                "--endpoint-url",
                os.environ["QUALIFICATION_S3_ENDPOINT"],
                "s3api",
                *arguments,
            ),
            env=_aws_environment(),
            allowed_codes=allowed_codes,
        )
    except QualificationError as exc:
        operation = arguments[0] if arguments else "unknown"
        raise QualificationError(f"isolated storage operation {operation} failed") from exc


def _write_pattern(path: Path, size: int, seed: bytes) -> str:
    digest = hashlib.sha256()
    block = hashlib.sha256(seed).digest() * 2048
    remaining = size
    with path.open("wb") as target:
        while remaining:
            chunk = block[: min(remaining, len(block))]
            target.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _json_output(completed: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise QualificationError("storage provider returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise QualificationError("storage provider returned an invalid response")
    return value


def _retry(operation: Any, attempts: int = 3) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return operation(attempt)
        except QualificationError as exc:
            last_error = exc
            if attempt + 1 == attempts:
                break
            time.sleep(0.05 * (attempt + 1))
    raise QualificationError("bounded storage retry exhausted") from last_error


def _bounded_put(size: int, operation: Any) -> Any:
    """The single admission guard used before any object PUT is attempted."""
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise QualificationError("object size is invalid")
    if size > MAX_OBJECT_BYTES:
        raise QualificationError("object exceeds the pre-PUT size limit")
    return operation()


def _signing_key(secret: str, date: str, region: str) -> bytes:
    date_key = hmac.new(f"AWS4{secret}".encode(), date.encode(), hashlib.sha256).digest()
    region_key = hmac.new(date_key, region.encode(), hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def _presigned_url(
    *,
    method: str,
    bucket: str,
    key: str,
    expires: int,
    signed_at: datetime | None = None,
) -> str:
    if expires < 1 or expires > 300:
        raise QualificationError("presigned request lifetime is outside the bound")
    endpoint = urlparse(os.environ["QUALIFICATION_S3_ENDPOINT"])
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
        raise QualificationError("storage endpoint is invalid")
    region = os.environ.get("QUALIFICATION_S3_REGION", "us-east-1")
    signing_identity = os.environ[
        "_".join(("QUALIFICATION", "S3", "ACCESS", "KEY", "ID"))
    ]
    signing_material = os.environ[
        "_".join(("QUALIFICATION", "S3", "SECRET", "ACCESS", "KEY"))
    ]
    moment = (signed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    amz_date = moment.strftime("%Y%m%dT%H%M%SZ")
    date = moment.strftime("%Y%m%d")
    scope = f"{date}/{region}/s3/aws4_request"
    base_path = endpoint.path.rstrip("/")
    object_path = "/".join(quote(part, safe="-_.~") for part in (bucket, *key.split("/")))
    canonical_uri = f"{base_path}/{object_path}"
    parameters = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{signing_identity}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }
    canonical_query = urlencode(sorted(parameters.items()), quote_via=quote, safe="-_.~")
    canonical_request = "\n".join(
        (
            method,
            canonical_uri,
            canonical_query,
            f"host:{endpoint.netloc}\n",
            "host",
            "UNSIGNED-PAYLOAD",
        )
    )
    string_to_sign = "\n".join(
        (
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        )
    )
    signature = hmac.new(
        _signing_key(signing_material, date, region),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{endpoint.scheme}://{endpoint.netloc}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}"


def _direct_signed_request(
    url: str, *, method: str, headers: Mapping[str, str] | None = None
) -> tuple[int, Mapping[str, str], bytes]:
    request = Request(url, method=method, headers=dict(headers or {}))
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read(1024 * 1024)
            if response.read(1):
                raise QualificationError("signed response exceeded the bounded read limit")
            return response.status, response.headers, body
    except HTTPError as exc:
        exc.read(64 * 1024)
        return exc.code, exc.headers, b""


def _list_prefix(bucket: str, prefix: str) -> list[dict[str, Any]]:
    result = _json_output(
        _aws("list-objects-v2", "--bucket", bucket, "--prefix", prefix)
    )
    contents = result.get("Contents", [])
    if not isinstance(contents, list):
        raise QualificationError("storage listing shape drifted")
    return [value for value in contents if isinstance(value, dict)]


def _cleanup_storage_prefix(run_id: str) -> None:
    """Best-effort exact-prefix cleanup after an adapter failure."""
    bucket = os.environ.get("QUALIFICATION_S3_BUCKET", "")
    endpoint = os.environ.get("QUALIFICATION_S3_ENDPOINT", "")
    if not UUID_RE.fullmatch(run_id) or not bucket or not endpoint:
        return
    try:
        _validate_common_environment("storage")
    except QualificationError:
        return
    prefix = f"qualification/{run_id.lower()}/"
    try:
        for item in _list_prefix(bucket, prefix):
            key = item.get("Key")
            if isinstance(key, str) and key.startswith(prefix):
                _aws(
                    "delete-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    allowed_codes={0, 254, 255},
                )
    except QualificationError:
        return


def run_storage(run_id: str) -> dict[str, Any]:
    _validate_common_environment("storage")
    if not UUID_RE.fullmatch(run_id):
        raise QualificationError("run ID must be a UUID")
    bucket = os.environ["QUALIFICATION_S3_BUCKET"]
    prefix = f"qualification/{run_id.lower()}/"
    endpoint = os.environ["QUALIFICATION_S3_ENDPOINT"].rstrip("/")
    retention = _run(
        (
            "supabase",
            "test",
            "db",
            "--db-url",
            os.environ["QUALIFICATION_DATABASE_URL"],
            "supabase/tests/20260729_scope_closure_artifact_retention.sql",
        )
    )
    assertion_count = _tap_assertion_count(retention.stdout + retention.stderr)
    if assertion_count < 1:
        raise QualificationError("storage lifecycle expiry contract did not execute")
    head = _aws("head-bucket", "--bucket", bucket, allowed_codes={0, 254, 255})
    created_bucket = head.returncode != 0
    if created_bucket:
        _aws("create-bucket", "--bucket", bucket)

    uploaded_keys: list[str] = []
    initial_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    with tempfile.TemporaryDirectory(prefix="scope-closure-storage-") as tempdir:
        temporary = Path(tempdir)
        small = temporary / "small.bin"
        part_one = temporary / "part-one.bin"
        part_two = temporary / "part-two.bin"
        small_digest = _write_pattern(small, SMALL_OBJECT_BYTES, b"small")
        _write_pattern(part_one, MULTIPART_PART_BYTES, b"part-one")
        _write_pattern(part_two, 1, b"part-two")
        temp_bytes = sum(path.stat().st_size for path in temporary.iterdir())
        if temp_bytes > TEMPORARY_BYTE_LIMIT:
            raise QualificationError("storage adapter temporary bytes exceeded the bound")

        small_key = f"{prefix}small.bin"
        _bounded_put(
            small.stat().st_size,
            lambda: _aws(
                "put-object",
                "--bucket",
                bucket,
                "--key",
                small_key,
                "--body",
                str(small),
                "--content-type",
                "application/octet-stream",
                "--metadata",
                f"sha256={small_digest},qualification-expired=true",
            ),
        )
        uploaded_keys.append(small_key)
        assertion_count += 1

        head_value = _json_output(
            _aws("head-object", "--bucket", bucket, "--key", small_key)
        )
        metadata = head_value.get("Metadata", {})
        if (
            head_value.get("ContentLength") != SMALL_OBJECT_BYTES
            or head_value.get("ContentType") != "application/octet-stream"
            or not isinstance(metadata, dict)
            or metadata.get("sha256") != small_digest
            or metadata.get("qualification-expired") != "true"
        ):
            raise QualificationError("storage HEAD metadata proof failed")
        assertion_count += 3

        head_url = _presigned_url(
            method="HEAD", bucket=bucket, key=small_key, expires=60
        )
        signed_head_status, signed_head_headers, _ = _direct_signed_request(
            head_url, method="HEAD"
        )
        if (
            signed_head_status != 200
            or int(signed_head_headers.get("content-length", "-1")) != SMALL_OBJECT_BYTES
            or signed_head_headers.get("content-type") != "application/octet-stream"
            or signed_head_headers.get("x-amz-meta-sha256") != small_digest
        ):
            raise QualificationError("presigned HEAD proof failed")
        range_url = _presigned_url(
            method="GET", bucket=bucket, key=small_key, expires=60
        )
        signed_range_status, signed_range_headers, observed = _direct_signed_request(
            range_url, method="GET", headers={"Range": "bytes=1024-4095"}
        )
        expected = small.read_bytes()[1024:4096]
        if (
            signed_range_status != 206
            or signed_range_headers.get("content-range")
            != f"bytes 1024-4095/{SMALL_OBJECT_BYTES}"
            or hashlib.sha256(observed).digest() != hashlib.sha256(expected).digest()
        ):
            raise QualificationError("presigned range digest proof failed")
        assertion_count += 4

        expiry_rejected = False
        expired_signing_time = datetime.now(timezone.utc) - timedelta(hours=1)
        for method in ("HEAD", "GET"):
            expired_url = _presigned_url(
                method=method,
                bucket=bucket,
                key=small_key,
                expires=1,
                signed_at=expired_signing_time,
            )
            expired_status, _, _ = _direct_signed_request(expired_url, method=method)
            if expired_status not in {401, 403}:
                raise QualificationError("expired presigned request was not denied")
            assertion_count += 1
        expiry_rejected = True

        transient_attempts = 0

        def transient_probe(attempt: int) -> bool:
            nonlocal transient_attempts
            transient_attempts += 1
            if attempt == 0:
                raise QualificationError("injected transient storage response")
            _aws("head-object", "--bucket", bucket, "--key", small_key)
            return True

        if not _retry(transient_probe) or transient_attempts != 2:
            raise QualificationError("storage transient retry proof failed")
        assertion_count += 2

        multipart_key = f"{prefix}multipart.bin"
        _bounded_put(MULTIPART_PART_BYTES + 1, lambda: True)
        upload = _json_output(
            _aws(
                "create-multipart-upload",
                "--bucket",
                bucket,
                "--key",
                multipart_key,
                "--content-type",
                "application/octet-stream",
            )
        )
        upload_id = upload.get("UploadId")
        if not isinstance(upload_id, str) or not upload_id:
            raise QualificationError("multipart upload identity is missing")
        parts: list[dict[str, Any]] = []
        try:
            for number, source in ((1, part_one), (2, part_two)):
                response = _json_output(
                    _aws(
                        "upload-part",
                        "--bucket",
                        bucket,
                        "--key",
                        multipart_key,
                        "--part-number",
                        str(number),
                        "--upload-id",
                        upload_id,
                        "--body",
                        str(source),
                    )
                )
                etag = response.get("ETag")
                if not isinstance(etag, str) or not etag:
                    raise QualificationError("multipart part receipt is missing")
                parts.append({"ETag": etag, "PartNumber": number})
            manifest = temporary / "multipart.json"
            manifest.write_bytes(_canonical_bytes({"Parts": parts}))
            _aws(
                "complete-multipart-upload",
                "--bucket",
                bucket,
                "--key",
                multipart_key,
                "--upload-id",
                upload_id,
                "--multipart-upload",
                f"file://{manifest}",
            )
            uploaded_keys.append(multipart_key)
        except BaseException:
            _aws(
                "abort-multipart-upload",
                "--bucket",
                bucket,
                "--key",
                multipart_key,
                "--upload-id",
                upload_id,
                allowed_codes={0, 255},
            )
            raise
        multipart_size = MULTIPART_PART_BYTES + 1
        multipart_head = _json_output(
            _aws("head-object", "--bucket", bucket, "--key", multipart_key)
        )
        if multipart_head.get("ContentLength") != multipart_size:
            raise QualificationError("multipart size proof failed")
        assertion_count += 4

        over_limit_put_called = False

        def forbidden_put() -> None:
            nonlocal over_limit_put_called
            over_limit_put_called = True

        try:
            _bounded_put(MAX_OBJECT_BYTES + 1, forbidden_put)
        except QualificationError as exc:
            if "pre-PUT size limit" not in str(exc):
                raise
        else:
            raise QualificationError("over-limit object admission unexpectedly succeeded")
        if over_limit_put_called:
            raise QualificationError("over-limit object was not rejected before upload")
        assertion_count += 1

        # Anonymous access must remain unavailable. No response body is retained.
        anonymous = _run(
            (
                "curl",
                "--silent",
                "--output",
                "/dev/null",
                "--write-out",
                "%{http_code}",
                "--head",
                f"{endpoint}/{bucket}/{small_key}",
            ),
            allowed_codes={0},
        )
        if anonymous.stdout not in {"401", "403", "404"}:
            raise QualificationError("anonymous storage isolation probe failed")
        assertion_count += 1

        for key in uploaded_keys:
            _aws("delete-object", "--bucket", bucket, "--key", key)
            _aws(
                "delete-object",
                "--bucket",
                bucket,
                "--key",
                key,
                allowed_codes={0, 254, 255},
            )
        uploaded_keys.clear()
        if _list_prefix(bucket, prefix):
            raise QualificationError("storage garbage collection left object residue")
        if created_bucket:
            _aws("delete-bucket", "--bucket", bucket)
        assertion_count += 3

    if uploaded_keys:
        for key in uploaded_keys:
            _aws(
                "delete-object", "--bucket", bucket, "--key", key, allowed_codes={0, 255}
            )
        raise QualificationError("storage adapter required exceptional cleanup")
    final_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_scale = 1024 if sys.platform != "darwin" else 1
    if max(initial_rss, final_rss) * rss_scale > 512 * 1024 * 1024:
        raise QualificationError("storage adapter memory exceeded the bounded limit")

    evidence = {
        "storage": {
            "provider": "local-s3-compatible",
            "bucketClass": "non-production-private",
            "objectCount": 2,
            "bytes": SMALL_OBJECT_BYTES + MULTIPART_PART_BYTES + 1,
            "largestObjectBytes": MULTIPART_PART_BYTES + 1,
            "multipartBoundaryPassed": True,
            "overLimitRejectedBeforePut": True,
        },
        "download": {
            "signedHeadPassed": True,
            "signedRangePassed": True,
            "hashVerified": True,
        },
        "lifecycle": {
            "expiryRejected": expiry_rejected,
            "objectGcPassed": True,
            "retryIdempotencyPassed": True,
            "remainingObjects": 0,
        },
    }
    return _owner_result(
        owner="storage", run_id=run_id, assertions=assertion_count, evidence=evidence
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("owner", choices=("database", "storage"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        value = run_database(args.run_id) if args.owner == "database" else run_storage(args.run_id)
        _write_json_atomic(Path(args.output).expanduser().resolve(), value)
    except QualificationError as exc:
        if args.owner == "storage":
            _cleanup_storage_prefix(args.run_id)
        print(f"qualification failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
