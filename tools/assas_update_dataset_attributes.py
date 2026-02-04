#!/usr/bin/env python3
"""
Authenticate (cookie session via /auth/basic/login) and update dataset attributes.
Uses same auth approach as tools/assas_data_downloader.py.
"""

import argparse
import getpass
import json
import logging
import os
import sys
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger("assas_update_dataset_attributes")


class AssasAPIClient:
    def __init__(self, base_url: str, verify_ssl: bool = True, base_path: str = ""):
        self.base_url = base_url.rstrip("/")
        self.base_path = (base_path or "").strip()
        if self.base_path and not self.base_path.startswith("/"):
            self.base_path = f"/{self.base_path}"
        self.base_path = self.base_path.rstrip("/")

        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.authenticated = False

    def authenticate(self, username: str, password: str) -> bool:
        # Try both plain and base_path-prefixed login routes
        candidate_login_paths = [
            f"{self.base_path}/auth/basic/login" if self.base_path else None,
        ]
        candidate_login_paths = [p for p in candidate_login_paths if p]

        for login_path in candidate_login_paths:
            auth_url = urljoin(self.base_url, login_path)
            logger.info("Authenticating at: %s", auth_url)

            resp = self.session.post(
                auth_url,
                data={"username": username, "password": password},
                allow_redirects=True,
                timeout=30,
            )
            logger.info("Auth response: HTTP %s", resp.status_code)

            # success heuristic: session cookie present
            session_cookies = [
                c
                for c in self.session.cookies
                if any(k in c.name.lower() for k in ["session", "auth", "login"])
            ]
            if session_cookies:
                self.authenticated = True
                logger.info("Authenticated (cookie: %s)", session_cookies[0].name)
                return True

            # If wrong path (404), try next; otherwise stop.
            if resp.status_code == 404:
                continue

            logger.error("Authentication failed (no session cookie).")
            return False

        logger.error(
            "Authentication failed (login endpoint not found / no session cookie)."
        )
        return False

    def post_json(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        url = urljoin(self.base_url, path)
        return self.session.post(url, json=payload, timeout=30)


def setup_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=numeric, format="%(asctime)s %(levelname)s %(message)s")


def _safe_json(resp: requests.Response) -> Optional[dict]:
    try:
        return resp.json()
    except Exception:
        return None


def _effective_http_status(resp: requests.Response) -> int:
    """
    Some APIs return 200 with {"success": false, ...}. Treat that as error.
    If the payload contains a status code field, prefer it.
    """
    payload = _safe_json(resp)
    if not isinstance(payload, dict):
        return resp.status_code

    # If server uses an embedded status_code, honor it
    for key in ("status_code", "http_status", "code"):
        v = payload.get(key)
        if isinstance(v, int) and 100 <= v <= 599:
            return v

    # If success explicitly false but status is 200, treat as error
    if resp.status_code == 200 and payload.get("success") is False:
        return 400

    return resp.status_code


def _print_response(resp: requests.Response) -> None:
    eff = _effective_http_status(resp)

    # NEW: log response details + JSON (if any)
    logger.info("Response: %s %s", resp.status_code, resp.reason)
    try:
        logger.info("Final URL: %s", resp.url)
    except Exception:
        pass

    payload = _safe_json(resp)
    if payload is not None:
        logger.info("Response JSON:\n%s", json.dumps(payload, indent=2, sort_keys=True))
    else:
        # avoid logging huge HTML pages at INFO
        text = (resp.text or "").strip()
        logger.info("Response body (non-JSON, first 2000 chars): %s", text[:2000])

    # keep console output as before
    print(f"HTTP {resp.status_code} (effective: {eff})")
    if payload is not None:
        print(json.dumps(payload, indent=2))
    else:
        print(resp.text)


def _exit_code_from_status(effective_status: int) -> int:
    # Shell exit codes are 0..255; keep it simple and stable.
    if 200 <= effective_status < 300:
        return 0
    if effective_status in (401, 403):
        return 3
    if effective_status == 404:
        return 4
    if effective_status == 409:
        return 5
    return 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Update dataset attributes via ASSAS API (cookie session auth).")
    p.add_argument(
        "--base-url", 
        default="https://assas.scc.kit.edu", 
        help="e.g. https://assas.scc.kit.edu"
    )
    p.add_argument(
        "--base-path", 
        default=os.getenv("ASSAS_BASE_PATH", "/test"), 
        help="e.g. /test (or set ASSAS_BASE_PATH)"
    )
    p.add_argument(        
        "--no-verify-ssl", 
        action="store_true", 
        help="Disable SSL verification"
    )
    p.add_argument(
        "--loglevel", 
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    p.add_argument(
        "--dataset-id", 
        required=True, 
        help="Dataset UUID"
    )
    p.add_argument(
        "--title", 
        required=True, 
        help="meta_title"
    )
    p.add_argument(
        "--name", 
        required=True,        
        help="meta_name"
    )
    p.add_argument(
        "--description",
        required=True,
        help="meta_description",
    )
    p.add_argument(
        "--username", 
        default=os.getenv("ASSAS_USERNAME"), 
        help="Username (or set ASSAS_USERNAME)"
    )
    p.add_argument(
        "--password", 
        default=os.getenv("ASSAS_PASSWORD"), 
        help="Password (or set ASSAS_PASSWORD)"
        )

    args = p.parse_args()
    setup_logging(args.loglevel)

    username = (args.username or "").strip() or input("Username: ").strip()
    if not username:
        print("Username required", file=sys.stderr)
        return 2

    password = (args.password or "").strip()
    if not password:
        password = getpass.getpass("Password: ").strip()
    if not password:
        print("Password required", file=sys.stderr)
        return 2

    client = AssasAPIClient(
        args.base_url, 
        verify_ssl=not args.no_verify_ssl, 
        base_path=args.base_path)
    if not client.authenticate(username, password):
        return 3

    payload = {
        "meta_title": args.title, 
        "meta_name": args.name, 
        "meta_description": args.description
    }

    base_path = client.base_path  # normalized

    # Try likely prefixes; ONLY retry on 404
    candidate_paths = [
        f"{base_path}/assas_app/datasets/{args.dataset_id}/attributes" \
            if base_path else None,
        f"{base_path}/assas_app/datasets/{args.dataset_id}/attributes"\
            .replace("//", "/"),
    ]
    candidate_paths = [p for p in candidate_paths if p]

    last_resp: Optional[requests.Response] = None
    for path in candidate_paths:
        logger.info("POST %s", path)
        resp = client.post_json(path, payload)
        last_resp = resp

        eff = _effective_http_status(resp)

        # success -> stop
        if 200 <= eff < 300:
            _print_response(resp)
            return 0

        # retry only if endpoint not found
        if resp.status_code == 404:
            continue

        # real error -> stop 
        # (do NOT try other paths, or you hide the real code like 409)
        _print_response(resp)
        return _exit_code_from_status(eff)

    if last_resp is not None:
        _print_response(last_resp)
        return _exit_code_from_status(_effective_http_status(last_resp))

    print("No request was made.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())