"""Server-side helpers for storefront analytics (geo, device class, client IP)."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def client_ip_from_request(req: Any) -> str:
    xff = (req.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (getattr(req, "remote_addr", None) or "").strip() or ""


def _is_private_or_local_ip(ip: str) -> bool:
    ip = (ip or "").strip()
    if not ip or ip in ("127.0.0.1", "::1"):
        return True
    if ip.startswith(("10.", "192.168.")):
        return True
    if ip.startswith("fc00:") or ip.startswith("fe80:"):
        return True
    if ip.startswith("172."):
        parts = ip.split(".")
        if len(parts) >= 2 and parts[0] == "172":
            try:
                second = int(parts[1])
                if 16 <= second <= 31:
                    return True
            except ValueError:
                pass
    return False


def ip_fingerprint(ip: str) -> str:
    """SHA-256 prefix of IP for coarse clustering without storing raw IPs."""
    raw = (ip or "").strip().encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:24]


def geo_lookup(ip: str) -> Dict[str, str]:
    """
    Best-effort geo from ip-api.com (no API key).
    Returns keys: country_code, city, region (empty strings if unknown).
    """
    out: Dict[str, str] = {"country_code": "", "city": "", "region": ""}
    ip = (ip or "").strip()
    if _is_private_or_local_ip(ip):
        return out
    try:
        fields = "status,countryCode,city,regionName"
        url = f"http://ip-api.com/json/{ip}?fields={fields}"
        with urlopen(url, timeout=2) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        if data.get("status") == "success":
            cc = (data.get("countryCode") or "").strip().upper()
            out["country_code"] = cc[:8] if cc else ""
            city = (data.get("city") or "").strip()
            out["city"] = city[:128] if city else ""
            reg = (data.get("regionName") or "").strip()
            out["region"] = reg[:128] if reg else ""
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, TypeError):
        pass
    return out


def geo_country_code(ip: str) -> str:
    """Backward-compatible: ISO country code only."""
    return geo_lookup(ip).get("country_code") or ""


def device_class_from_user_agent(ua: str) -> str:
    u = (ua or "").lower()
    if "ipad" in u or ("android" in u and "mobile" not in u) or "tablet" in u:
        return "tablet"
    if (
        "mobile" in u
        or "iphone" in u
        or "ipod" in u
        or "android" in u
        or "webos" in u
        or "blackberry" in u
    ):
        return "mobile"
    return "desktop"
