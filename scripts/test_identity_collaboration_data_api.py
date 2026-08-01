#!/usr/bin/env python3
"""Verify Issue #355 DTO/RPC profiles against local PostgREST."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from identity_collaboration_target import resolve_target

ROOT = Path(__file__).resolve().parents[1]
AUTH_READS = {
    "review_comments_v1": "review_id,reviewer_id,state_code",
    "reviews_v1": "id,state_code,reviewer_id",
    "team_roles_v1": "user_id,team_id,role",
    "teams_v1": "id,rank,is_public",
    "user_profiles_v1": "id,contact,email,display_name",
}
SERVICE_ONLY = {
    "notifications_v1": "id,recipient_user_id,sender_user_id,type",
    "identity_center_processed_events_v1": "event_id,event_type,processed_at",
    "identity_center_users_v1": "keycloak_sub,user_id,status,desired_role",
}


def b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def authenticated_jwt(secret: str) -> str:
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = b64(json.dumps({
        "iss": "supabase-demo",
        "sub": "35500000-0000-4000-8000-000000000001",
        "role": "authenticated",
        "aud": "authenticated",
        "exp": 1983812996,
    }, separators=(",", ":")).encode())
    signature = b64(hmac.new(secret.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest())
    return f"{header}.{payload}.{signature}"


def call(url: str, apikey: str, token: str, profile: str, *, body: dict | None = None) -> tuple[int, object]:
    headers = {"apikey": apikey, "Authorization": f"Bearer {token}", "Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    headers["Content-Profile" if body is not None else "Accept-Profile"] = profile
    request = urllib.request.Request(
        url,
        headers=headers,
        data=None if body is None else json.dumps(body).encode(),
        method="GET" if body is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def main() -> int:
    db_url, status = resolve_target()
    assert status is not None
    rest = status["REST_URL"].rstrip("/")
    anon_key = status["ANON_KEY"]
    service_key = status["SERVICE_ROLE_KEY"]
    jwt_secret = status["JWT_SECRET"]
    auth_token = authenticated_jwt(jwt_secret)
    subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", "notify pgrst, 'reload schema'"],
        cwd=ROOT, check=True, stdout=subprocess.DEVNULL,
    )

    probe = f"{rest}/reviews_v1?select=id&limit=1"
    for _ in range(40):
        if call(probe, service_key, service_key, "api")[0] == 200:
            break
        time.sleep(0.25)
    else:
        raise AssertionError("PostgREST did not reload the Issue #355 API schema")

    for name, columns in AUTH_READS.items():
        url = f"{rest}/{name}?{urllib.parse.urlencode({'select': columns, 'limit': 1})}"
        assert call(url, anon_key, anon_key, "api")[0] in (401, 403)
        status_code, result = call(url, anon_key, auth_token, "api")
        assert status_code == 200 and isinstance(result, list), (name, status_code, result)

    for name, columns in SERVICE_ONLY.items():
        url = f"{rest}/{name}?{urllib.parse.urlencode({'select': columns, 'limit': 1})}"
        assert call(url, anon_key, auth_token, "api")[0] in (401, 403)
        status_code, result = call(url, service_key, service_key, "api")
        assert status_code == 200 and isinstance(result, list), (name, status_code, result)

    rpc_status, rpc_result = call(
        f"{rest}/rpc/qry_notification_get_my_data_count",
        anon_key,
        auth_token,
        "public",
        body={"p_days": 3, "p_last_view_at": None},
    )
    assert rpc_status == 200 and isinstance(rpc_result, int), (rpc_status, rpc_result)

    for profile in ("private", "util", "archive"):
        status_code, result = call(probe, service_key, service_key, profile)
        assert status_code == 406 and isinstance(result, dict) and result.get("code") == "PGRST106"

    print("PASS Issue #355 PostgREST DTO/RPC profiles and schema cache")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
