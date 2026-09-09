class ScraperError(Exception):
    """Base error with a stable machine-readable reason."""

    reason = "scraper_error"


class ConfigError(ScraperError):
    reason = "config_error"


class RequestBudgetExceeded(ScraperError):
    reason = "request_budget_exceeded"


class AccessBlocked(ScraperError):
    reason = "access_blocked"


class DailyVerificationRequired(AccessBlocked):
    reason = "daily_verification_required"


class SecurityVerificationRequired(AccessBlocked):
    """A temporary site-wide viewer verification gate, not a locked chapter."""

    reason = "security_verification_required"


class RateLimited(ScraperError):
    reason = "rate_limited"


class NetworkFailure(ScraperError):
    reason = "network_failure"


class HostNotAllowed(ScraperError):
    reason = "host_not_allowed"


class ParserMismatch(ScraperError):
    reason = "parser_mismatch"


class ContentRestricted(ScraperError):
    reason = "content_restricted"


class BlockPageDetected(AccessBlocked):
    reason = "block_page_detected"


def raise_for_access_notice(message: str) -> None:
    """Classify a visible viewer notice without treating temporary gates as locks."""
    cleaned = " ".join(message.split())
    if "일일 조회 인증" in cleaned or "일반 소설 뷰어에서 인증" in cleaned:
        raise DailyVerificationRequired(cleaned[:240])
    if "본문 보안 검증" in cleaned or "광고 검증" in cleaned:
        raise SecurityVerificationRequired(cleaned[:240])
    raise AccessBlocked(cleaned[:240] or "원본에서 회차 접근을 확인해 주세요")
