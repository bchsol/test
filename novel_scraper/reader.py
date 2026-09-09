"""Loopback-only, on-demand novel reader with a persistent parsed-content cache."""
from __future__ import annotations

import dataclasses
import gzip
import json
import logging
import mimetypes
import re
import sqlite3
import threading
import time
import webbrowser
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit, unquote

from .browser_reader import BrowserReader
from .config import Config
from .crawler import robots_allows, robots_url
from .errors import AccessBlocked, ConfigError, NetworkFailure, ParserMismatch, ScraperError
from .http_client import HttpClient, RequestBudget
from .parsers import parse_detail_page, parse_list_page
from .reader_parser import static_chapter

LOG = logging.getLogger(__name__)
MAX_CACHE_BYTES = 100 * 1024 * 1024
MAX_CACHE_ENTRIES = 2000


class ReaderBusy(ScraperError):
    reason = "reader_busy"


class BodyNotArchived(ScraperError):
    reason = "body_not_archived"


def page_number(value: str | int) -> int:
    if not re.fullmatch(r"[1-9]\d{0,4}", str(value)) or int(value) > 10000:
        raise ValueError("페이지는 1~10000 사이 정수여야 합니다")
    return int(value)


def site_id(value: str) -> str:
    if not re.fullmatch(r"\d{1,20}", value):
        raise ValueError("잘못된 작품 또는 회차 번호입니다")
    return value


class ReaderService:
    def __init__(
        self,
        config: Config,
        *,
        online: bool = False,
        transport=None,
        browser=None,
        max_requests: int | None = None,
        reset_budget_per_load: bool = True,
        parallel_requests: bool = False,
        cache_enabled: bool = True,
        browser_workers: int = 1,
    ):
        self.config = config
        self.online = online
        self._lock = threading.Lock()
        self._robots_lock = threading.Lock()
        self._parallel_requests = parallel_requests
        self._cache_enabled = cache_enabled
        self._robots_text: str | None = None
        self._robots_at = 0.0
        self._browser = browser or BrowserReader(
            config.site.allowed_hosts,
            workers=browser_workers,
        )
        request_limit = config.crawl.max_requests if max_requests is None else max_requests
        if request_limit < 1:
            raise ConfigError("요청 상한은 1 이상이어야 합니다")
        self._reset_budget_per_load = reset_budget_per_load
        self._request_limit = request_limit
        self._client = HttpClient(
            config.http,
            config.site.allowed_hosts,
            RequestBudget(request_limit),
            transport=transport,
            pace_per_thread=parallel_requests,
        )
        self._client.url_policy = self._check_url
        self.database = config.storage.database_path
        self.archive_root = (config.source_path.parent / 'data' / 'pc_archive').resolve()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS reader_cache (
                url TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                fetched_at REAL NOT NULL, bytes INTEGER NOT NULL)""")
        parts = urlsplit(config.site.start_url)
        self.origin = urlunsplit((parts.scheme, parts.netloc, "", "", ""))

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.database, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def health(self) -> dict:
        with self._db() as db:
            count, chapters = db.execute("SELECT COUNT(*), COALESCE(SUM(kind='chapter'),0) FROM reader_cache").fetchone()
        return {"service": "novel-reader", "version": 1, "online": self.online,
                "source_host": urlsplit(self.origin).hostname,
                "cache_entries": count, "cached_chapters": chapters,
                "requests_used": self._client.budget.used,
                "request_limit": self._client.budget.limit}

    def request_usage(self) -> dict[str, int]:
        return {"used": self._client.budget.used, "limit": self._client.budget.limit}

    @staticmethod
    def _library_page(value: str | int, *, maximum: int = 10000) -> int:
        if not re.fullmatch(r"[1-9]\d*", str(value)) or int(value) > maximum:
            raise ValueError(f"페이지는 1~{maximum} 사이 정수여야 합니다")
        return int(value)

    @staticmethod
    def _library_work(row) -> dict:
        try:
            genres = json.loads(row["genres_json"] or "[]")
        except (TypeError, ValueError):
            genres = []
        keys = set(row.keys())
        chapter_count = int(row["chapter_count"]) if "chapter_count" in keys else 0
        discovered_count = int(row["discovered_chapter_count"]) if "discovered_chapter_count" in keys else chapter_count
        return {
            "id": str(row["site_id"] or row["id"]),
            "title": row["title"],
            "author": row["author"],
            "genres": genres if isinstance(genres, list) else [],
            "serial_status": row["serial_status"],
            "platform": row["platform"],
            "displayed_chapter_count": row["displayed_chapter_count"],
            "update_raw": row["update_raw"],
            "cover_image_url": row["cover_image_url"],
            # chapter_count means successfully archived local bodies, not metadata rows.
            "chapter_count": chapter_count,
            "discovered_chapter_count": discovered_count,
            "fully_archived": bool(row["fully_archived"]) if "fully_archived" in keys else False,
            "first_seen_at": row["first_seen_at"],
            "last_checked_at": row["last_checked_at"],
        }

    def _attach_archive(self, db: sqlite3.Connection) -> None:
        """Attach the persistent body archive, or an empty compatible DB when absent."""
        archive_db = self.archive_root / 'archive.sqlite3'
        if archive_db.is_file():
            db.execute("ATTACH DATABASE ? AS archive", (str(archive_db),))
            table = db.execute(
                "SELECT 1 FROM archive.sqlite_master WHERE type='table' AND name='archive_chapters'"
            ).fetchone()
            if table:
                return
            db.execute("DETACH DATABASE archive")
        db.execute("ATTACH DATABASE ':memory:' AS archive")
        db.execute(
            """CREATE TABLE archive.archive_chapters (
                   work_id TEXT NOT NULL, chapter_id TEXT NOT NULL,
                   PRIMARY KEY(work_id, chapter_id)
               )"""
        )

    def _chapter_archive_path(self, work_id: str, chapter_id: str) -> Path:
        work_id = site_id(work_id)
        chapter_id = site_id(chapter_id)
        root = self.archive_root
        path = (root / 'works' / work_id / 'chapters' / f'{chapter_id}.txt.gz').resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise BodyNotArchived("로컬 본문 경로가 올바르지 않습니다") from exc
        return path

    def _chapter_is_archived(self, work_id: str, chapter_id: str) -> bool:
        return self._chapter_archive_path(work_id, chapter_id).is_file()

    def library_catalog(self, page=1, *, page_size=48, query="", genre="") -> dict:
        page = self._library_page(page)
        page_size = self._library_page(page_size, maximum=100)
        query = str(query).strip()[:100]
        genre = str(genre).strip()[:60]
        clauses: list[str] = []
        params: list[object] = []
        if query:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("w.title LIKE ? ESCAPE '\\'")
            params.append(f"%{escaped}%")
        if genre:
            clauses.append("EXISTS (SELECT 1 FROM json_each(w.genres_json) WHERE value=?)")
            params.append(genre)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._db() as db:
            db.row_factory = sqlite3.Row
            self._attach_archive(db)
            total = int(db.execute(f"SELECT COUNT(*) FROM works w{where}", params).fetchone()[0])
            rows = db.execute(
                f"""SELECT w.*,
                       (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) discovered_chapter_count,
                       (SELECT COUNT(*) FROM chapters c
                          JOIN archive.archive_chapters ac
                            ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                         WHERE c.work_id=w.id AND c.site_id IS NOT NULL) chapter_count,
                       CASE WHEN
                         (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) > 0
                         AND (SELECT COUNT(*) FROM chapters c
                                JOIN archive.archive_chapters ac
                                  ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                               WHERE c.work_id=w.id AND c.site_id IS NOT NULL)
                             = (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL)
                         AND (w.displayed_chapter_count IS NULL OR w.displayed_chapter_count <= 0
                              OR (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) >= w.displayed_chapter_count)
                       THEN 1 ELSE 0 END fully_archived
                    FROM works w{where}
                    ORDER BY w.last_checked_at DESC, w.id DESC LIMIT ? OFFSET ?""",
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            chapters_total = int(db.execute(
                """SELECT COUNT(*) FROM archive.archive_chapters ac
                     JOIN works w ON w.site_id=ac.work_id
                     JOIN chapters c ON c.work_id=w.id AND c.site_id=ac.chapter_id"""
            ).fetchone()[0])
            body_works = int(db.execute(
                """SELECT COUNT(DISTINCT w.id) FROM archive.archive_chapters ac
                     JOIN works w ON w.site_id=ac.work_id
                     JOIN chapters c ON c.work_id=w.id AND c.site_id=ac.chapter_id"""
            ).fetchone()[0])
            works_total = int(db.execute(
                """SELECT COUNT(*) FROM (
                       SELECT w.id, w.displayed_chapter_count,
                              COUNT(c.id) discovered,
                              SUM(CASE WHEN ac.chapter_id IS NOT NULL THEN 1 ELSE 0 END) archived
                         FROM works w
                         JOIN chapters c ON c.work_id=w.id AND c.site_id IS NOT NULL
                         LEFT JOIN archive.archive_chapters ac
                           ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                        WHERE w.site_id IS NOT NULL
                        GROUP BY w.id
                       HAVING discovered > 0
                          AND archived = discovered
                          AND (w.displayed_chapter_count IS NULL OR w.displayed_chapter_count <= 0
                               OR discovered >= w.displayed_chapter_count)
                   )"""
            ).fetchone()[0])
            genres = [
                str(row[0])
                for row in db.execute(
                    "SELECT DISTINCT value FROM works, json_each(works.genres_json) WHERE typeof(value)='text' ORDER BY value"
                ).fetchall()
            ]
        pages = max(1, (total + page_size - 1) // page_size)
        return {
            "works": [self._library_work(row) for row in rows],
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total": total,
            "stats": {"works": works_total, "chapters": chapters_total, "body_works": body_works},
            "genres": genres,
            "source": "sqlite",
        }

    def library_detail(self, work_id: str, page=1, *, page_size=100) -> dict:
        work_id = site_id(work_id)
        page = self._library_page(page)
        page_size = self._library_page(page_size, maximum=100)
        with self._db() as db:
            db.row_factory = sqlite3.Row
            self._attach_archive(db)
            work = db.execute(
                """SELECT w.*,
                       (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) discovered_chapter_count,
                       (SELECT COUNT(*) FROM chapters c
                          JOIN archive.archive_chapters ac
                            ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                         WHERE c.work_id=w.id AND c.site_id IS NOT NULL) chapter_count,
                       CASE WHEN
                         (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) > 0
                         AND (SELECT COUNT(*) FROM chapters c
                                JOIN archive.archive_chapters ac
                                  ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                               WHERE c.work_id=w.id AND c.site_id IS NOT NULL)
                             = (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL)
                         AND (w.displayed_chapter_count IS NULL OR w.displayed_chapter_count <= 0
                              OR (SELECT COUNT(*) FROM chapters c WHERE c.work_id=w.id AND c.site_id IS NOT NULL) >= w.displayed_chapter_count)
                       THEN 1 ELSE 0 END fully_archived
                    FROM works w WHERE w.site_id=? OR (w.site_id IS NULL AND w.id=?)
                    ORDER BY w.site_id IS NOT NULL DESC LIMIT 1""",
                (work_id, work_id),
            ).fetchone()
            if work is None:
                raise ValueError("수집 DB에서 작품을 찾지 못했습니다")
            chapters = db.execute(
                """SELECT c.site_id,c.normalized_url,c.title,c.display_order,c.chapter_number,c.date_raw,c.date_normalized,
                          CASE WHEN ac.chapter_id IS NOT NULL THEN 1 ELSE 0 END archive_registered
                     FROM chapters c
                     LEFT JOIN archive.archive_chapters ac
                       ON ac.work_id=? AND ac.chapter_id=c.site_id
                    WHERE c.work_id=? ORDER BY c.display_order,c.id LIMIT ? OFFSET ?""",
                (str(work["site_id"] or work["id"]), work["id"], page_size, (page - 1) * page_size),
            ).fetchall()
        value = self._library_work(work)
        public_work_id = str(work["site_id"] or work["id"])
        value["chapters"] = [
            {
                "id": str(row["site_id"] or ""),
                "title": row["title"],
                "display_order": row["display_order"],
                "chapter_number": row["chapter_number"],
                "date_raw": row["date_raw"],
                "date_normalized": row["date_normalized"],
                "archived": bool(row["archive_registered"]) and bool(row["site_id"])
                and self._chapter_is_archived(public_work_id, str(row["site_id"])),
            }
            for row in chapters
        ]
        value["page"] = page
        value["page_size"] = page_size
        # The list still includes pending metadata rows, so paginate by discovered rows.
        value["pages"] = max(1, (value["discovered_chapter_count"] + page_size - 1) // page_size)
        value["source"] = "sqlite"
        return value

    def library_chapter(self, work_id: str, chapter_id: str) -> dict:
        work_id = site_id(work_id)
        chapter_id = site_id(chapter_id)
        with self._db() as db:
            db.row_factory = sqlite3.Row
            self._attach_archive(db)
            work = db.execute(
                "SELECT id,site_id,title FROM works WHERE site_id=? OR (site_id IS NULL AND id=?) ORDER BY site_id IS NOT NULL DESC LIMIT 1",
                (work_id, work_id),
            ).fetchone()
            if work is None:
                raise BodyNotArchived("수집 DB에서 작품을 찾지 못했습니다")
            chapter = db.execute(
                """SELECT c.id,c.site_id,c.title,c.display_order,c.chapter_number,c.date_raw,
                          CASE WHEN ac.chapter_id IS NOT NULL THEN 1 ELSE 0 END archive_registered
                     FROM chapters c
                     LEFT JOIN archive.archive_chapters ac
                       ON ac.work_id=? AND ac.chapter_id=c.site_id
                    WHERE c.work_id=? AND c.site_id=? LIMIT 1""",
                (work_id, work["id"], chapter_id),
            ).fetchone()
            if chapter is None:
                raise BodyNotArchived("수집 DB에서 회차를 찾지 못했습니다")
            ordered = db.execute(
                """SELECT c.site_id,c.title,
                          CASE WHEN ac.chapter_id IS NOT NULL THEN 1 ELSE 0 END archive_registered
                     FROM chapters c
                     LEFT JOIN archive.archive_chapters ac
                       ON ac.work_id=? AND ac.chapter_id=c.site_id
                    WHERE c.work_id=? AND c.site_id IS NOT NULL ORDER BY c.display_order,c.id""",
                (work_id, work["id"]),
            ).fetchall()

        path = self._chapter_archive_path(work_id, chapter_id)
        if not chapter["archive_registered"] or not path.is_file():
            raise BodyNotArchived("아직 이 회차의 로컬 본문이 저장되지 않았습니다. resume --continuous 수집을 계속 실행해 주세요")
        try:
            packed = path.read_bytes()
            if len(packed) > 16 * 1024 * 1024:
                raise BodyNotArchived("저장된 로컬 본문 파일이 비정상적으로 큽니다")
            raw = gzip.decompress(packed)
            if len(raw) > 32 * 1024 * 1024:
                raise BodyNotArchived("압축 해제된 로컬 본문이 비정상적으로 큽니다")
            text = raw.decode('utf-8')
        except (OSError, EOFError, UnicodeError) as exc:
            raise BodyNotArchived("저장된 로컬 본문 파일을 읽을 수 없습니다") from exc
        paragraphs = [value.strip() for value in re.split(r'\n\s*\n', text) if value.strip()]
        if not paragraphs:
            raise BodyNotArchived("저장된 로컬 본문이 비어 있습니다")

        archived = [
            (str(row["site_id"]), row["title"])
            for row in ordered
            if row["archive_registered"] and row["site_id"] and self._chapter_is_archived(work_id, str(row["site_id"]))
        ]
        current_index = next((index for index, item in enumerate(archived) if item[0] == chapter_id), None)
        previous = archived[current_index - 1] if current_index is not None and current_index > 0 else None
        following = archived[current_index + 1] if current_index is not None and current_index + 1 < len(archived) else None
        return {
            "work_id": work_id,
            "work_title": work["title"],
            "chapter_id": chapter_id,
            "title": chapter["title"],
            "chapter_number": chapter["chapter_number"],
            "date_raw": chapter["date_raw"],
            "paragraphs": paragraphs,
            "previous": {"id": previous[0], "title": previous[1]} if previous else None,
            "next": {"id": following[0], "title": following[1]} if following else None,
            "storage": "local_txt_gz",
        }

    def _cached(self, url: str):
        if not self._cache_enabled:
            return None
        with self._db() as db:
            row = db.execute("SELECT payload,fetched_at FROM reader_cache WHERE url=?", (url,)).fetchone()
        if row:
            try:
                value = json.loads(row[0])
                if isinstance(value, dict):
                    return value, row[1]
            except (ValueError, TypeError):
                pass
        return None

    def _save(self, url: str, kind: str, value: dict, now: float):
        if not self._cache_enabled:
            return
        payload = json.dumps(value, ensure_ascii=False)
        size = len(payload.encode('utf-8'))
        if size > 4 * 1024 * 1024:
            raise NetworkFailure("저장할 회차 데이터가 너무 큽니다")
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO reader_cache VALUES(?,?,?,?,?)", (url, kind, payload, now, size))
            count, total = db.execute("SELECT COUNT(*), COALESCE(SUM(bytes),0) FROM reader_cache").fetchone()
            if count > MAX_CACHE_ENTRIES or total > MAX_CACHE_BYTES:
                for old_url, old_size in db.execute("SELECT url,bytes FROM reader_cache ORDER BY fetched_at").fetchall():
                    if count <= MAX_CACHE_ENTRIES and total <= MAX_CACHE_BYTES:
                        break
                    db.execute("DELETE FROM reader_cache WHERE url=?", (old_url,))
                    count -= 1
                    total -= old_size

    @staticmethod
    def _with_cache(value: dict, at: float, hit: bool, *, stale=False, warning=None):
        return {**value, "cache": {"hit": hit, "stale": stale,
                "stored_at": datetime.fromtimestamp(at, timezone.utc).isoformat(), "warning": warning}}

    def _check_url(self, url: str):
        if url == robots_url(self.config.site.start_url):
            return
        if self._robots_text is None or not robots_allows(self._robots_text, url, self.config.http.user_agent):
            raise AccessBlocked("robots.txt에서 해당 페이지의 자동 요청을 허용하지 않습니다")

    def _prepare_request(self):
        if self._reset_budget_per_load:
            self._client.budget = RequestBudget(max(4, self._request_limit))
        if self._robots_text is None or time.monotonic() - self._robots_at > 3600:
            # Parallel collectors share one robots refresh instead of every worker
            # spending a request on robots.txt at the same time.
            with self._robots_lock:
                if self._robots_text is None or time.monotonic() - self._robots_at > 3600:
                    response = self._client.get(robots_url(self.config.site.start_url))
                    # Validate the policy itself before keeping it. A maintenance page is not allow-all.
                    robots_allows(response.text, self.config.site.start_url, self.config.http.user_agent)
                    self._robots_text = response.text
                    self._robots_at = time.monotonic()

    def _fetch_and_cache(self, url: str, kind: str, parse, *, refresh: bool, cached) -> dict:
        # Recheck the cache after a serial reader waited for its lock. Parallel
        # collectors intentionally skip this duplicate-prevention lock because
        # their work queue already assigns one chapter to one worker.
        if not self._parallel_requests:
            second = self._cached(url)
            ttl = float('inf') if kind == 'chapter' else 900
            if second and not refresh and time.time() - second[1] < ttl:
                return self._with_cache(*second, True)
        self._prepare_request()
        response = self._client.get(url)
        if urlsplit(str(response.url)).path != urlsplit(url).path:
            raise AccessBlocked("원본에서 다른 페이지로 이동했습니다. 로그인·인증 상태를 확인해 주세요")
        value = parse(response.content, str(response.url))
        at = time.time()
        self._save(url, kind, value, at)
        return self._with_cache(value, at, False)

    def _load(self, url: str, kind: str, parse, *, refresh=False) -> dict:
        cached = self._cached(url)
        ttl = float('inf') if kind == 'chapter' else 900
        if cached and not refresh and (time.time() - cached[1] < ttl or not self.online):
            return self._with_cache(*cached, True, stale=(time.time() - cached[1] >= ttl))
        if not self.online:
            raise ConfigError("오프라인 모드에 저장된 페이지가 없습니다. Start-Novel.cmd로 온라인 읽기 모드를 실행하세요")
        if self._parallel_requests:
            try:
                return self._fetch_and_cache(url, kind, parse, refresh=refresh, cached=cached)
            except ScraperError as exc:
                if cached:
                    return self._with_cache(*cached, True, stale=True, warning=str(exc))
                raise
        if not self._lock.acquire(blocking=False):
            raise ReaderBusy("다른 페이지를 불러오는 중입니다. 완료 후 다시 눌러 주세요")
        try:
            return self._fetch_and_cache(url, kind, parse, refresh=refresh, cached=cached)
        except ScraperError as exc:
            if cached:
                return self._with_cache(*cached, True, stale=True, warning=str(exc))
            raise
        finally:
            self._lock.release()

    def catalog(self, page=1, *, refresh=False):
        page = page_number(page)
        parts = urlsplit(self.config.site.start_url)
        query = dict(parse_qsl(parts.query))
        if page > 1:
            query['page'] = str(page)
        url = urlunsplit(parts._replace(query=urlencode(query)))
        def parse(html, final_url):
            listing = parse_list_page(
                html,
                final_url,
                self.config.site.allowed_hosts,
                self.config.site.allowed_asset_hosts,
                pagination_url=url,
            )
            works = []
            for work in listing.works:
                works.append({**dataclasses.asdict(work), "id": work.site_id,
                              "normalized_url": work.url, "author": None})
            return {"works": works, "page": page, "has_next": listing.next_url is not None,
                    "source_url": final_url}
        return self._load(url, 'catalog', parse, refresh=refresh)

    def detail(self, work_id: str, page=1, *, refresh=False):
        work_id = site_id(work_id)
        page = page_number(page)
        url = f'{self.origin}/novel/{work_id}' + (f'?epage={page}' if page > 1 else '')
        def parse(html, final_url):
            detail = parse_detail_page(html, final_url, self.config.site.allowed_hosts)
            value = dataclasses.asdict(detail)
            value['id'] = detail.site_id
            for chapter in value['chapters']:
                chapter['id'] = chapter['site_id']
            return value
        return self._load(url, 'detail', parse, refresh=refresh)

    def chapter(self, work_id: str, chapter_id: str, *, refresh=False):
        url = f'{self.origin}/novel/{site_id(work_id)}/{site_id(chapter_id)}'
        def parse(html, final_url):
            try:
                value = static_chapter(html, final_url, self.config.site.allowed_hosts)
            except ParserMismatch:
                # Some episode types do not expose the usual content host in the
                # initial HTML. Let the ordinary browser make the final decision;
                # it can distinguish a rendered page from a paid/access gate.
                value = None
            if value is None:
                # Browser renders the ordinary public page with its own site scripts.
                # No direct protected API, unlock, session-proof or decryption reimplementation.
                self._client.reserve_external_request()
                value = self._browser.read(final_url)
            return value
        return self._load(url, 'chapter', parse, refresh=refresh)

    def close(self):
        self._client.close()
        self._browser.close()


def make_server(service: ReaderService, static_root: Path, port: int = 8787) -> ThreadingHTTPServer:
    root = static_root.resolve()
    class Handler(BaseHTTPRequestHandler):
        server_version = 'NovelReader/1'
        def log_message(self, fmt, *args):
            LOG.info(fmt, *args)

        def _respond(self, status, data: bytes, content_type: str):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' https: data:; connect-src 'self'; font-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _json(self, status, value):
            self._respond(status, json.dumps(value, ensure_ascii=False).encode('utf-8'), 'application/json; charset=utf-8')

        def do_GET(self):
            bound_port = self.server.server_address[1]
            allowed = {f'127.0.0.1:{bound_port}', f'localhost:{bound_port}'}
            if self.headers.get('Host', '') not in allowed:
                self._json(403, {'error': {'code':'host_rejected', 'message':'로컬 주소로만 접근할 수 있습니다'}})
                return
            origin = self.headers.get('Origin')
            if origin and origin not in {f'http://{host}' for host in allowed}:
                self._json(403, {'error': {'code':'origin_rejected', 'message':'외부 사이트 요청은 허용되지 않습니다'}})
                return
            path = urlsplit(self.path).path
            if path.startswith('/api/'):
                if path != '/api/health' and self.headers.get('X-Novel-Client') != 'local-reader':
                    self._json(403, {'error': {'code':'client_required', 'message':'서재 화면에서 요청해 주세요'}})
                    return
                try:
                    query = parse_qs(urlsplit(self.path).query)
                    refresh = query.get('refresh', ['0'])[0] == '1'
                    if path == '/api/health':
                        value = service.health()
                    elif path == '/api/library':
                        value = service.library_catalog(
                            query.get('page', ['1'])[0],
                            page_size=query.get('page_size', ['48'])[0],
                            query=query.get('q', [''])[0],
                            genre=query.get('genre', [''])[0],
                        )
                    elif match := re.fullmatch(r'/api/library/works/(\d{1,20})', path):
                        value = service.library_detail(
                            match[1],
                            query.get('page', ['1'])[0],
                            page_size=query.get('page_size', ['100'])[0],
                        )
                    elif match := re.fullmatch(r'/api/library/chapters/(\d{1,20})/(\d{1,20})', path):
                        value = service.library_chapter(match[1], match[2])
                    elif path == '/api/catalog':
                        value = service.catalog(query.get('page', ['1'])[0], refresh=refresh)
                    elif match := re.fullmatch(r'/api/works/(\d{1,20})', path):
                        value = service.detail(match[1], query.get('page', ['1'])[0], refresh=refresh)
                    elif match := re.fullmatch(r'/api/chapters/(\d{1,20})/(\d{1,20})', path):
                        value = service.chapter(match[1], match[2], refresh=refresh)
                    else:
                        self._json(404, {'error': {'code':'not_found', 'message':'없는 API 주소입니다'}})
                        return
                    self._json(200, value)
                except ValueError as exc:
                    self._json(400, {'error': {'code':'invalid_request', 'message':str(exc)}})
                except ScraperError as exc:
                    status = 409 if isinstance(exc, ReaderBusy) else 404 if isinstance(exc, BodyNotArchived) else 403 if isinstance(exc, AccessBlocked) else 503
                    self._json(status, {'error': {'code':exc.reason, 'message':str(exc)}})
                except Exception:
                    LOG.exception('Reader request failed')
                    self._json(500, {'error': {'code':'internal_error', 'message':'로컬 처리 오류입니다. logs/novel_scraper.log를 확인해 주세요'}})
                return
            relative = unquote(path).lstrip('/') or 'index.html'
            file = (root / relative).resolve()
            if not file.is_relative_to(root) or not file.is_file() or file.suffix not in {'.html','.js','.css','.svg','.png','.ico','.woff','.woff2','.json'}:
                self._respond(404, b'Not found', 'text/plain; charset=utf-8')
                return
            mime = mimetypes.guess_type(file.name)[0] or 'application/octet-stream'
            if file.suffix == '.js':
                mime = 'text/javascript'
            self._respond(200, file.read_bytes(), mime)

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    server.daemon_threads = False
    return server


def serve(config: Config, *, online=False, port=8787, open_browser=False):
    root = config.source_path.parent / 'site' / 'dist-local'
    if not (root / 'index.html').is_file():
        raise ConfigError('웹 화면 빌드가 없습니다. Start-Novel.cmd 또는 site 폴더에서 npm run build를 실행하세요')
    if not 1 <= port <= 65535:
        raise ConfigError('포트는 1~65535 범위여야 합니다')
    service = ReaderService(config, online=online)
    try:
        server = make_server(service, root, port)
    except OSError as exc:
        service.close()
        raise ConfigError(f'{port} 포트를 열 수 없습니다. 이미 실행 중인 서재 또는 다른 프로그램을 확인하세요') from exc
    url = f'http://127.0.0.1:{port}'
    print(f'Novel Reader: {url} ({"online" if online else "offline"})', flush=True)
    print('Stop: Ctrl+C. Only requested pages are loaded; cached chapters remain available offline.', flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.close()
    return 0
