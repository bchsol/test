from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .errors import ConfigError


@dataclass(frozen=True, slots=True)
class SiteConfig:
    start_url: str
    allowed_hosts: tuple[str, ...]
    permission_confirmed: bool = False
    allowed_asset_hosts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StorageConfig:
    database_path: Path


@dataclass(frozen=True, slots=True)
class HttpConfig:
    min_interval_seconds: float = 3.0
    timeout_seconds: float = 20.0
    max_retries: int = 2
    max_retry_after_seconds: float = 60.0
    user_agent: str = "NovelMetadataCollector/0.1"


@dataclass(frozen=True, slots=True)
class CrawlConfig:
    max_works: int = 20
    max_requests: int = 50
    update_overlap_pages: int = 2
    concurrency: int = 1
    body_concurrency: int = 2
    browser_concurrency: int = 2


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    path: Path
    level: str = "INFO"


@dataclass(frozen=True, slots=True)
class ExportConfig:
    csv_utf8_bom: bool = True
    protect_csv_formulas: bool = True


@dataclass(frozen=True, slots=True)
class Config:
    site: SiteConfig
    storage: StorageConfig
    http: HttpConfig
    crawl: CrawlConfig
    logging: LoggingConfig
    export: ExportConfig
    source_path: Path


def _resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def load_config(path: str | Path) -> Config:
    source = Path(path).resolve()
    try:
        raw = tomllib.loads(source.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"설정 파일을 읽을 수 없습니다: {exc}") from exc

    try:
        site = raw["site"]
        start_url = str(site["start_url"])
        hosts = tuple(str(x).lower().rstrip(".") for x in site["allowed_hosts"])
        asset_hosts = tuple(str(x).lower().rstrip(".") for x in site.get("allowed_asset_hosts", []))
        split = urlsplit(start_url)
        if split.scheme not in {"http", "https"} or not split.hostname:
            raise ValueError("start_url은 http(s) 절대 URL이어야 합니다")
        if split.hostname.lower().rstrip(".") not in hosts:
            raise ValueError("start_url 호스트가 allowed_hosts에 없습니다")
        if not hosts:
            raise ValueError("allowed_hosts가 비어 있습니다")

        base = source.parent
        storage = raw.get("storage", {})
        http = raw.get("http", {})
        crawl = raw.get("crawl", {})
        logging_raw = raw.get("logging", {})
        export = raw.get("export", {})

        http_config = HttpConfig(
            min_interval_seconds=float(http.get("min_interval_seconds", 3.0)),
            timeout_seconds=float(http.get("timeout_seconds", 20.0)),
            max_retries=int(http.get("max_retries", 2)),
            max_retry_after_seconds=float(http.get("max_retry_after_seconds", 60.0)),
            user_agent=str(http.get("user_agent", "NovelMetadataCollector/0.1")),
        )
        crawl_config = CrawlConfig(
            max_works=int(crawl.get("max_works", 20)),
            max_requests=int(crawl.get("max_requests", 50)),
            update_overlap_pages=int(crawl.get("update_overlap_pages", 2)),
            concurrency=int(crawl.get("concurrency", 1)),
            body_concurrency=int(crawl.get("body_concurrency", 2)),
            browser_concurrency=int(crawl.get("browser_concurrency", 2)),
        )
        if min(http_config.min_interval_seconds, http_config.timeout_seconds) < 0:
            raise ValueError("시간 제한 값은 음수일 수 없습니다")
        if min(http_config.max_retries, crawl_config.max_works, crawl_config.max_requests) < 0:
            raise ValueError("횟수 제한 값은 음수일 수 없습니다")
        if not 1 <= crawl_config.concurrency <= 8:
            raise ValueError("crawl.concurrency는 1~8 범위여야 합니다")
        if not 1 <= crawl_config.body_concurrency <= 8:
            raise ValueError("crawl.body_concurrency는 1~8 범위여야 합니다")
        if not 1 <= crawl_config.browser_concurrency <= 4:
            raise ValueError("crawl.browser_concurrency는 1~4 범위여야 합니다")

        permission_confirmed = site.get("permission_confirmed", False)
        if not isinstance(permission_confirmed, bool):
            raise ValueError("site.permission_confirmed는 true 또는 false여야 합니다")

        return Config(
            site=SiteConfig(start_url=start_url, allowed_hosts=hosts, permission_confirmed=permission_confirmed, allowed_asset_hosts=asset_hosts),
            storage=StorageConfig(_resolve(base, str(storage.get("database_path", "data/novels.sqlite3")))),
            http=http_config,
            crawl=crawl_config,
            logging=LoggingConfig(
                _resolve(base, str(logging_raw.get("path", "logs/novel_scraper.log"))),
                str(logging_raw.get("level", "INFO")).upper(),
            ),
            export=ExportConfig(
                bool(export.get("csv_utf8_bom", True)),
                bool(export.get("protect_csv_formulas", True)),
            ),
            source_path=source,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"잘못된 설정: {exc}") from exc
