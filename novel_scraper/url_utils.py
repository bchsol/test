from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from .errors import HostNotAllowed


def normalize_url(url: str, base_url: str, allowed_hosts: tuple[str, ...]) -> str:
    absolute = urljoin(base_url, url)
    parts = urlsplit(absolute)
    host = (parts.hostname or "").lower().rstrip(".")
    if parts.username is not None or parts.password is not None:
        raise HostNotAllowed("인증 정보가 포함된 URL은 허용되지 않습니다")
    if parts.scheme not in {"http", "https"} or host not in allowed_hosts:
        raise HostNotAllowed(f"허용되지 않은 URL 호스트: {absolute}")
    port = parts.port
    if port is not None and port != (443 if parts.scheme == "https" else 80):
        raise HostNotAllowed("표준 HTTP/HTTPS 포트만 허용됩니다")
    netloc = host if port is None else f"{host}:{port}"
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")
    # Keep all identity-bearing query parameters; only canonicalize their ordering.
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)), doseq=True)
    return urlunsplit((parts.scheme.lower(), netloc, path, query, ""))
