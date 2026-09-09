from __future__ import annotations

import email.utils
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

import httpx

from .config import HttpConfig
from .errors import AccessBlocked, HostNotAllowed, NetworkFailure, RateLimited, RequestBudgetExceeded
from .url_utils import normalize_url


@dataclass(slots=True)
class RequestBudget:
    limit: int
    used: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def take(self) -> None:
        with self._lock:
            if self.used >= self.limit:
                raise RequestBudgetExceeded(f"요청 상한 {self.limit}회를 소진했습니다")
            self.used += 1


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (parsed - current).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


class HttpClient:
    def __init__(self, config: HttpConfig, allowed_hosts: tuple[str, ...], budget: RequestBudget, *, transport: httpx.BaseTransport | None = None, sleep=time.sleep, monotonic=time.monotonic, url_policy=None, pace_per_thread: bool = False):
        self.config = config
        self.allowed_hosts = allowed_hosts
        self.budget = budget
        self.url_policy = url_policy
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request_at: float | None = None
        self._request_lock = threading.Lock()
        self._pace_per_thread = pace_per_thread
        self._thread_state = threading.local()
        self._client = httpx.Client(
            timeout=config.timeout_seconds,
            follow_redirects=False,
            headers={"User-Agent": config.user_agent, "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9"},
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _pace(self, last_request_at: float | None) -> None:
        if last_request_at is not None:
            remaining = self.config.min_interval_seconds - (self._monotonic() - last_request_at)
            if remaining > 0:
                self._sleep(remaining)

    def _reserve_request(self) -> None:
        if self._pace_per_thread:
            # Explicit multi-worker crawls pace each worker independently. This
            # permits one simultaneous request per worker while keeping retries
            # and subsequent work on that worker spaced by the configured delay.
            self._pace(getattr(self._thread_state, "last_request_at", None))
            self.budget.take()
            self._thread_state.last_request_at = self._monotonic()
            return
        # Single-worker and non-crawler callers retain one global request gate.
        with self._request_lock:
            self._pace(self._last_request_at)
            self.budget.take()
            self._last_request_at = self._monotonic()

    def reserve_external_request(self) -> None:
        """Apply this client's pace and budget to a browser-owned navigation."""
        self._reserve_request()

    def _one(self, url: str) -> httpx.Response:
        retryable_status = {500, 502, 503, 504}
        for attempt in range(self.config.max_retries + 1):
            self._reserve_request()
            try:
                with self._client.stream("GET", url) as streamed:
                    body = bytearray()
                    for chunk in streamed.iter_bytes():
                        body.extend(chunk)
                        if len(body) > 5 * 1024 * 1024:
                            raise NetworkFailure("응답 본문이 5 MiB 제한을 초과했습니다")
                    headers = dict(streamed.headers)
                    # iter_bytes() already decoded gzip/br. Reusing those wire headers
                    # on a new Response would decompress the same bytes a second time.
                    headers.pop("content-encoding", None)
                    headers.pop("transfer-encoding", None)
                    headers["content-length"] = str(len(body))
                    response = httpx.Response(streamed.status_code, headers=headers, content=bytes(body), request=streamed.request)
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                if attempt >= self.config.max_retries:
                    raise NetworkFailure(f"네트워크 오류 재시도 한도 초과: {exc}") from exc
                self._sleep(min(2**attempt, self.config.max_retry_after_seconds))
                continue
            if response.status_code in {401, 402, 403}:
                raise AccessBlocked(f"HTTP {response.status_code}: 자동 수집을 중단합니다")
            if response.status_code == 429:
                delay = parse_retry_after(response.headers.get("Retry-After"))
                if delay is None:
                    delay = float(2**attempt)
                if delay > self.config.max_retry_after_seconds or attempt >= self.config.max_retries:
                    raise RateLimited(f"HTTP 429: 허용 대기/재시도 한도를 초과했습니다 (Retry-After={delay:.1f}s)")
                self._sleep(delay)
                continue
            if response.status_code in retryable_status:
                if attempt >= self.config.max_retries:
                    raise NetworkFailure(f"HTTP {response.status_code}: 서버 오류 재시도 한도 초과")
                self._sleep(min(2**attempt, self.config.max_retry_after_seconds))
                continue
            if response.status_code in {301, 302, 303, 307, 308}:
                return response
            if response.status_code >= 400:
                raise NetworkFailure(f"HTTP {response.status_code}: 요청 실패")
            return response
        raise AssertionError("unreachable")

    def get(self, url: str) -> httpx.Response:
        current = normalize_url(url, url, self.allowed_hosts)
        for _ in range(4):
            if self.url_policy is not None:
                self.url_policy(current)
            response = self._one(current)
            if response.status_code not in {301, 302, 303, 307, 308}:
                return response
            location = response.headers.get("Location")
            if not location:
                raise NetworkFailure("Location 없는 리디렉션 응답")
            target = urljoin(current, location)
            host = (urlsplit(target).hostname or "").lower().rstrip(".")
            if host not in self.allowed_hosts:
                raise HostNotAllowed(f"허용 호스트 밖으로 리디렉션됨: {target}")
            current = normalize_url(target, current, self.allowed_hosts)
        raise NetworkFailure("리디렉션 상한(3회)을 초과했습니다")
