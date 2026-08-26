"""Session-tied CSRF for authenticated dashboard mutations.

Not a framework swap. Tokens live in the Flask session and must be submitted
on POST/PUT/PATCH/DELETE to /dashboard/* and /api/admin/*.
Stripe webhooks and public analytics are excluded.
"""

from __future__ import annotations

import secrets
from typing import Optional

from flask import Request, session

SESSION_KEY = "_csrf_token"
FORM_FIELD = "csrf_token"
HEADER_NAME = "X-CSRF-Token"


def get_csrf_token() -> str:
    token = session.get(SESSION_KEY)
    if not isinstance(token, str) or len(token) < 16:
        token = secrets.token_urlsafe(32)
        session[SESSION_KEY] = token
        session.modified = True
    return token


def reset_csrf_token() -> str:
    """Rotate after login so a pre-auth token cannot be reused."""
    token = secrets.token_urlsafe(32)
    session[SESSION_KEY] = token
    session.modified = True
    return token


def _submitted_token(req: Request) -> Optional[str]:
    form_val = req.form.get(FORM_FIELD)
    if form_val:
        return str(form_val)
    header = req.headers.get(HEADER_NAME)
    if header:
        return str(header)
    data = req.get_json(silent=True)
    if isinstance(data, dict) and data.get(FORM_FIELD):
        return str(data.get(FORM_FIELD))
    return None


def csrf_token_valid(req: Request) -> bool:
    expected = session.get(SESSION_KEY)
    submitted = _submitted_token(req)
    if not isinstance(expected, str) or not submitted:
        return False
    if len(expected) < 16 or len(submitted) < 8:
        return False
    try:
        return secrets.compare_digest(expected, submitted)
    except (TypeError, ValueError):
        return False


def path_requires_csrf(path: str) -> bool:
    p = path or ""
    if p.startswith("/webhook"):
        return False
    if p.startswith("/api/analytics"):
        return False
    if p.startswith("/dashboard/"):
        return True
    if p.startswith("/api/admin"):
        return True
    return False
