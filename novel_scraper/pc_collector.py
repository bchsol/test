"""Persistent PC-side collector for public novel pages.

The collector intentionally reuses ReaderService, so it follows the same host,
robots, access-gate and ordinary-browser rules as the local reader.  It stores
only parsed text and metadata; advertising markup and remote scripts are never
written into the archive.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .config import Config
from .errors import (
    AccessBlocked,
    ConfigError,
    DailyVerificationRequired,
    ParserMismatch,
    RateLimited,
    RequestBudgetExceeded,
    ScraperError,
    SecurityVerificationRequired,
)
from .reader import ReaderService, site_id
from .storage import Storage


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS archive_works (
    work_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    author TEXT,
    genres_json TEXT NOT NULL DEFAULT '[]',
    serial_status TEXT,
    displayed_chapter_count INTEGER,
    discovered_chapter_count INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_chapters (
    work_id TEXT NOT NULL REFERENCES archive_works(work_id) ON DELETE CASCADE,
    chapter_id TEXT NOT NULL,
    chapter_number REAL,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    date_raw TEXT,
    relative_path TEXT NOT NULL,
    paragraphs INTEGER NOT NULL,
    characters INTEGER NOT NULL,
    raw_bytes INTEGER NOT NULL,
    gzip_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY(work_id, chapter_id)
);
CREATE INDEX IF NOT EXISTS archive_chapters_work_number
    ON archive_chapters(work_id, chapter_number, chapter_id);
CREATE TABLE IF NOT EXISTS archive_failures (
    work_id TEXT NOT NULL,
    chapter_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    terminal INTEGER NOT NULL DEFAULT 0,
    last_failed_at TEXT NOT NULL,
    PRIMARY KEY(work_id, chapter_id)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_positive(value: int, label: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if value < minimum:
        raise ConfigError(f"{label} 값은 {minimum} 이상이어야 합니다")
    return value


def choose_evenly(rows: list[dict], limit: int) -> list[dict]:
    """Choose a deterministic oldest-to-newest spread without duplicate indices."""
    if limit <= 0 or limit >= len(rows):
        return list(rows)
    if limit == 1:
        return [rows[len(rows) // 2]]
    indexes = []
    for pos in range(limit):
        index = round(pos * (len(rows) - 1) / (limit - 1))
        if index not in indexes:
            indexes.append(index)
    return [rows[index] for index in indexes]


def sample_candidates(rows: list[dict], limit: int) -> list[dict]:
    """Return interior quantiles first, then nearby fallbacks.

    Very newest episodes are commonly temporarily locked.  Size sampling should
    measure readable text rather than fail merely because an endpoint is gated.
    Full collection still attempts every chapter when --all-chapters is used.
    """
    if limit <= 0 or not rows:
        return []
    if limit >= len(rows):
        return list(rows)
    anchors: list[int] = []
    for pos in range(limit):
        index = round((pos + 1) * (len(rows) - 1) / (limit + 1))
        if index not in anchors:
            anchors.append(index)
    ordered = list(anchors)
    radius = 1
    while len(ordered) < len(rows):
        added = False
        for anchor in anchors:
            for index in (anchor - radius, anchor + radius):
                if 0 <= index < len(rows) and index not in ordered:
                    ordered.append(index)
                    added = True
        if not added and radius > len(rows):
            break
        radius += 1
    for index in range(len(rows)):
        if index not in ordered:
            ordered.append(index)
    return [rows[index] for index in ordered]


@dataclass(slots=True)
class SaveResult:
    raw_bytes: int
    gzip_bytes: int
    paragraphs: int
    characters: int
    relative_path: str
    existed: bool = False


class ArchiveStore:
    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "works").mkdir(exist_ok=True)
        (self.root / "reports").mkdir(exist_ok=True)
        self.database = self.root / "archive.sqlite3"
        with self._db() as db:
            db.executescript(SCHEMA)
            # Older versions treated a site-wide daily verification gate as a
            # permanently locked chapter. Repair those rows without deleting
            # any successfully archived content.
            db.execute(
                """UPDATE archive_failures
                   SET reason='daily_verification_required', terminal=0
                   WHERE message LIKE '%일일 조회 인증%'
                      OR message LIKE '%일반 소설 뷰어에서 인증%'"""
            )
            # Viewer security/ad verification is also a temporary, site-wide
            # gate. Older versions incorrectly made each affected chapter a
            # permanent skip; retain the history but make it resumable.
            db.execute(
                """UPDATE archive_failures
                   SET reason='security_verification_required', terminal=0
                   WHERE message LIKE '%본문 보안 검증%'
                      OR message LIKE '%광고 검증%'"""
            )

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.database, timeout=20)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def save_work(self, work: dict, discovered: int) -> None:
        work_id = site_id(str(work.get("id") or work.get("site_id") or ""))
        title = str(work.get("title") or "").strip()
        source_url = str(work.get("url") or work.get("normalized_url") or "").strip()
        if not title or not source_url:
            raise ConfigError("작품 메타데이터에 제목 또는 원본 주소가 없습니다")
        now = utc_now()
        payload = json.dumps(work, ensure_ascii=False, sort_keys=True)
        genres = work.get("genres") if isinstance(work.get("genres"), list) else []
        with self._db() as db:
            db.execute(
                """INSERT INTO archive_works(
                       work_id,title,source_url,author,genres_json,serial_status,
                       displayed_chapter_count,discovered_chapter_count,metadata_json,
                       first_seen_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(work_id) DO UPDATE SET
                     title=excluded.title, source_url=excluded.source_url,
                     author=excluded.author, genres_json=excluded.genres_json,
                     serial_status=excluded.serial_status,
                     displayed_chapter_count=excluded.displayed_chapter_count,
                     discovered_chapter_count=excluded.discovered_chapter_count,
                     metadata_json=excluded.metadata_json, updated_at=excluded.updated_at""",
                (
                    work_id,
                    title,
                    source_url,
                    work.get("author"),
                    json.dumps(genres, ensure_ascii=False),
                    work.get("serial_status"),
                    work.get("displayed_chapter_count"),
                    discovered,
                    payload,
                    now,
                    now,
                ),
            )
        work_dir = self.root / "works" / work_id
        work_dir.mkdir(parents=True, exist_ok=True)
        metadata = {
            "work_id": work_id,
            "title": title,
            "source_url": source_url,
            "author": work.get("author"),
            "genres": genres,
            "serial_status": work.get("serial_status"),
            "displayed_chapter_count": work.get("displayed_chapter_count"),
            "discovered_chapter_count": discovered,
            "updated_at": now,
        }
        self._atomic_write(work_dir / "meta.json", json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"))

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temp.write_bytes(data)
            os.replace(temp, path)
        finally:
            if temp.exists():
                temp.unlink()

    def chapter_record(self, work_id: str, chapter_id: str):
        with self._db() as db:
            return db.execute(
                "SELECT * FROM archive_chapters WHERE work_id=? AND chapter_id=?",
                (work_id, chapter_id),
            ).fetchone()

    def has_chapter(self, work_id: str, chapter_id: str) -> bool:
        row = self.chapter_record(work_id, chapter_id)
        if row is None:
            return False
        path = (self.root / row["relative_path"]).resolve()
        try:
            path.relative_to(self.root)
        except ValueError:
            return False
        return path.is_file() and path.stat().st_size == int(row["gzip_bytes"])

    def clear_failure(self, work_id: str, chapter_id: str) -> None:
        with self._db() as db:
            db.execute("DELETE FROM archive_failures WHERE work_id=? AND chapter_id=?", (work_id, chapter_id))

    def failure_is_terminal(self, work_id: str, chapter_id: str) -> bool:
        with self._db() as db:
            row = db.execute(
                "SELECT terminal FROM archive_failures WHERE work_id=? AND chapter_id=?",
                (work_id, chapter_id),
            ).fetchone()
        return bool(row and int(row["terminal"]))

    def record_failure(self, work_id: str, chapter_id: str, reason: str, message: str, *, terminal: bool = False) -> bool:
        now = utc_now()
        with self._db() as db:
            previous = db.execute(
                "SELECT attempts,terminal FROM archive_failures WHERE work_id=? AND chapter_id=?",
                (work_id, chapter_id),
            ).fetchone()
            attempts = (int(previous["attempts"]) if previous else 0) + 1
            final_terminal = terminal or bool(previous and int(previous["terminal"])) or attempts >= 3
            db.execute(
                """INSERT INTO archive_failures(work_id,chapter_id,reason,message,attempts,terminal,last_failed_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(work_id,chapter_id) DO UPDATE SET
                     reason=excluded.reason,message=excluded.message,attempts=excluded.attempts,
                     terminal=excluded.terminal,last_failed_at=excluded.last_failed_at""",
                (work_id, chapter_id, reason, message, attempts, 1 if final_terminal else 0, now),
            )
        return final_terminal

    def record_retryable_failure(self, work_id: str, chapter_id: str, reason: str, message: str) -> None:
        now = utc_now()
        with self._db() as db:
            previous = db.execute(
                "SELECT attempts FROM archive_failures WHERE work_id=? AND chapter_id=?",
                (work_id, chapter_id),
            ).fetchone()
            attempts = (int(previous["attempts"]) if previous else 0) + 1
            db.execute(
                """INSERT INTO archive_failures(work_id,chapter_id,reason,message,attempts,terminal,last_failed_at)
                   VALUES(?,?,?,?,?,0,?)
                   ON CONFLICT(work_id,chapter_id) DO UPDATE SET
                     reason=excluded.reason,message=excluded.message,attempts=excluded.attempts,
                     terminal=0,last_failed_at=excluded.last_failed_at""",
                (work_id, chapter_id, reason, message, attempts, now),
            )

    def save_chapter(self, work_id: str, chapter: dict, content: dict) -> SaveResult:
        work_id = site_id(work_id)
        chapter_id = site_id(str(chapter.get("id") or chapter.get("site_id") or content.get("chapter_id") or ""))
        paragraphs = content.get("paragraphs")
        if not isinstance(paragraphs, list) or not paragraphs:
            raise ConfigError("저장할 본문 문단이 없습니다")
        clean = [str(value).strip() for value in paragraphs if str(value).strip()]
        if not clean:
            raise ConfigError("저장할 본문 문단이 없습니다")
        text = "\n\n".join(clean).rstrip() + "\n"
        raw = text.encode("utf-8")
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        relative = Path("works") / work_id / "chapters" / f"{chapter_id}.txt.gz"
        target = self.root / relative
        self._atomic_write(target, compressed)
        digest = hashlib.sha256(raw).hexdigest()
        now = utc_now()
        title = str(chapter.get("title") or content.get("title") or chapter_id).strip()
        source_url = str(chapter.get("url") or content.get("url") or "").strip()
        with self._db() as db:
            db.execute(
                """INSERT INTO archive_chapters(
                       work_id,chapter_id,chapter_number,title,source_url,date_raw,
                       relative_path,paragraphs,characters,raw_bytes,gzip_bytes,sha256,collected_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(work_id,chapter_id) DO UPDATE SET
                     chapter_number=excluded.chapter_number,title=excluded.title,
                     source_url=excluded.source_url,date_raw=excluded.date_raw,
                     relative_path=excluded.relative_path,paragraphs=excluded.paragraphs,
                     characters=excluded.characters,raw_bytes=excluded.raw_bytes,
                     gzip_bytes=excluded.gzip_bytes,sha256=excluded.sha256,
                     collected_at=excluded.collected_at""",
                (
                    work_id,
                    chapter_id,
                    chapter.get("chapter_number"),
                    title,
                    source_url,
                    chapter.get("date_raw"),
                    relative.as_posix(),
                    len(clean),
                    len(text),
                    len(raw),
                    len(compressed),
                    digest,
                    now,
                ),
            )
        self.clear_failure(work_id, chapter_id)
        return SaveResult(len(raw), len(compressed), len(clean), len(text), relative.as_posix())

    def status(self) -> dict:
        with self._db() as db:
            works = int(db.execute("SELECT COUNT(*) FROM archive_works").fetchone()[0])
            row = db.execute(
                "SELECT COUNT(*),COALESCE(SUM(raw_bytes),0),COALESCE(SUM(gzip_bytes),0),COALESCE(SUM(characters),0) FROM archive_chapters"
            ).fetchone()
        chapters, raw_bytes, gzip_bytes, characters = (int(value) for value in row)
        actual_disk = self.database.stat().st_size
        for path in (self.root / "works").rglob("*"):
            if path.is_file():
                actual_disk += path.stat().st_size
        return {
            "root": str(self.root),
            "database": str(self.database),
            "works": works,
            "chapters": chapters,
            "characters": characters,
            "raw_text_bytes": raw_bytes,
            "gzip_text_bytes": gzip_bytes,
            "actual_archive_bytes": actual_disk,
            "compression_ratio": (gzip_bytes / raw_bytes) if raw_bytes else None,
        }

    def write_report(self, report: dict) -> Path:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        target = self.root / "reports" / f"collect-{stamp}.json"
        self._atomic_write(target, payload)
        self._atomic_write(self.root / "reports" / "latest.json", payload)
        return target


class PcCollector:
    def __init__(
        self,
        config: Config,
        store: ArchiveStore,
        *,
        chapter_delay: float | None = None,
        max_requests: int | None = None,
        workers: int = 1,
        browser_workers: int = 1,
    ):
        self.config = config
        self.store = store
        if not 1 <= workers <= 8:
            raise ConfigError("본문 병렬 작업 수는 1~8 범위여야 합니다")
        if not 1 <= browser_workers <= 4:
            raise ConfigError("본문 브라우저 병렬 작업 수는 1~4 범위여야 합니다")
        if browser_workers > workers:
            raise ConfigError("본문 브라우저 작업 수는 본문 작업 수보다 클 수 없습니다")
        self.workers = workers
        self.browser_workers = browser_workers
        self.service = ReaderService(
            config,
            online=True,
            max_requests=max_requests,
            reset_budget_per_load=False,
            parallel_requests=workers > 1,
            cache_enabled=False,
            browser_workers=browser_workers,
        )
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="novel-body") if workers > 1 else None
        self.chapter_delay = config.http.min_interval_seconds if chapter_delay is None else chapter_delay
        if self.chapter_delay < 0:
            raise ConfigError("회차 요청 간격은 음수일 수 없습니다")

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
        self.service.close()

    def _fetch_and_store_chapter(self, work_id: str, chapter: dict, *, refresh: bool = False) -> SaveResult:
        chapter_id = str(chapter["id"])
        content = self.service.chapter(work_id, chapter_id, refresh=refresh)
        saved = self.store.save_chapter(work_id, chapter, content)
        if self.chapter_delay:
            time.sleep(self.chapter_delay)
        return saved

    def _chapter_batch(self, work_id: str, chapters: list[dict], *, refresh: bool = False):
        """Run one bounded chapter batch and return (chapter, saved, error) rows."""
        rows = self._chapter_jobs_batch([(work_id, chapter) for chapter in chapters], refresh=refresh)
        return [(chapter, saved, error) for _, chapter, saved, error in rows]

    def _chapter_jobs_batch(self, jobs: list[tuple[str, dict]], *, refresh: bool = False):
        """Run chapter jobs from one or many works through the shared worker pool."""
        if not jobs:
            return []
        if self._executor is None:
            result = []
            for work_id, chapter in jobs:
                try:
                    result.append((work_id, chapter, self._fetch_and_store_chapter(work_id, chapter, refresh=refresh), None))
                except Exception as exc:
                    result.append((work_id, chapter, None, exc))
            return result
        futures = [
            (work_id, chapter, self._executor.submit(self._fetch_and_store_chapter, work_id, chapter, refresh=refresh))
            for work_id, chapter in jobs
        ]
        result = []
        for work_id, chapter, future in futures:
            try:
                result.append((work_id, chapter, future.result(), None))
            except Exception as exc:
                result.append((work_id, chapter, None, exc))
        return result

    def catalog_work_ids(self, limit: int) -> list[str]:
        _safe_positive(limit, "표본 작품 수")
        result: list[str] = []
        page = 1
        while len(result) < limit:
            data = self.service.catalog(page)
            for work in data.get("works", []):
                value = work.get("id")
                if value and str(value) not in result:
                    result.append(site_id(str(value)))
                    if len(result) >= limit:
                        break
            if len(result) >= limit or not data.get("has_next"):
                break
            page += 1
        if not result:
            raise ConfigError("목록에서 표본 작품을 찾지 못했습니다")
        return result

    def work_chapters(self, work_id: str) -> tuple[dict, list[dict]]:
        work_id = site_id(work_id)
        first = self.service.detail(work_id, 1)
        work = {key: value for key, value in first.items() if key not in {"chapters", "cache", "next_url", "episode_page", "last_page"}}
        chapters: list[dict] = []
        seen: set[str] = set()
        last_page = max(1, int(first.get("last_page") or 1))
        details = {1: first}
        if last_page > 1:
            if self._executor is None:
                for page in range(2, last_page + 1):
                    details[page] = self.service.detail(work_id, page)
            else:
                futures = {
                    page: self._executor.submit(self.service.detail, work_id, page)
                    for page in range(2, last_page + 1)
                }
                for page, future in futures.items():
                    details[page] = future.result()
        for page in range(1, last_page + 1):
            detail = details[page]
            for chapter in detail.get("chapters", []):
                chapter_id = chapter.get("id") or chapter.get("site_id")
                if not chapter_id or str(chapter_id) in seen:
                    continue
                row = dict(chapter)
                row["id"] = site_id(str(chapter_id))
                row.setdefault("url", row.get("normalized_url"))
                chapters.append(row)
                seen.add(str(chapter_id))
        # Chronological order makes sampling and subsequent upload order predictable.
        chapters.sort(
            key=lambda row: (
                row.get("chapter_number") is None,
                float(row.get("chapter_number") or 0),
                str(row.get("id")),
            )
        )
        work["id"] = work_id
        return work, chapters

    def collect(
        self,
        work_ids: Iterable[str],
        *,
        chapters_per_work: int,
        all_chapters: bool = False,
        refresh: bool = False,
        project_works: int = 13000,
        max_errors: int = 5,
    ) -> dict:
        _safe_positive(chapters_per_work, "작품당 표본 회차 수")
        _safe_positive(project_works, "예측 작품 수")
        _safe_positive(max_errors, "최대 오류 수")
        work_ids = list(work_ids)
        started = utc_now()
        failures: list[dict] = []
        work_reports: list[dict] = []
        selected_sizes: list[tuple[int, int]] = []
        chapter_totals: list[int] = []
        fetched = skipped = 0
        halted = False

        for raw_work_id in work_ids:
            work_id = site_id(str(raw_work_id))
            try:
                work, chapters = self.work_chapters(work_id)
                self.store.save_work(work, len(chapters))
                chapter_totals.append(len(chapters))
                candidates = chapters if all_chapters else sample_candidates(chapters, chapters_per_work)
                target_successes = len(chapters) if all_chapters else min(chapters_per_work, len(chapters))
                report = {
                    "work_id": work_id,
                    "title": work.get("title"),
                    "discovered_chapters": len(chapters),
                    "target_chapters": target_successes,
                    "attempted_chapters": 0,
                    "archived": 0,
                    "skipped_existing": 0,
                    "access_blocked": 0,
                    "raw_bytes": 0,
                    "gzip_bytes": 0,
                    "failures": [],
                }
                attempts_allowed = len(candidates) if all_chapters else min(len(candidates), target_successes + max_errors)
                cursor = 0
                while cursor < attempts_allowed:
                    if not all_chapters and report["archived"] >= target_successes:
                        break
                    needed = attempts_allowed - cursor if all_chapters else max(1, target_successes - report["archived"])
                    batch_size = min(self.workers, attempts_allowed - cursor, needed)
                    batch = candidates[cursor:cursor + batch_size]
                    cursor += len(batch)
                    fetch_batch: list[dict] = []
                    for chapter in batch:
                        chapter_id = str(chapter["id"])
                        report["attempted_chapters"] += 1
                        if not refresh and self.store.has_chapter(work_id, chapter_id):
                            row = self.store.chapter_record(work_id, chapter_id)
                            assert row is not None
                            raw_bytes = int(row["raw_bytes"])
                            gzip_bytes = int(row["gzip_bytes"])
                            report["skipped_existing"] += 1
                            skipped += 1
                            report["archived"] += 1
                            report["raw_bytes"] += raw_bytes
                            report["gzip_bytes"] += gzip_bytes
                            selected_sizes.append((raw_bytes, gzip_bytes))
                        else:
                            fetch_batch.append(chapter)

                    for chapter, saved, error in self._chapter_batch(work_id, fetch_batch, refresh=refresh):
                        chapter_id = str(chapter["id"])
                        if error is None:
                            assert saved is not None
                            fetched += 1
                            report["archived"] += 1
                            report["raw_bytes"] += saved.raw_bytes
                            report["gzip_bytes"] += saved.gzip_bytes
                            selected_sizes.append((saved.raw_bytes, saved.gzip_bytes))
                            continue
                        if isinstance(error, (DailyVerificationRequired, SecurityVerificationRequired)):
                            self.store.record_retryable_failure(work_id, chapter_id, error.reason, str(error))
                            item = {"work_id": work_id, "chapter_id": chapter_id, "reason": error.reason, "message": str(error)}
                            failures.append(item)
                            report["failures"].append(item)
                            halted = True
                        elif isinstance(error, AccessBlocked):
                            item = {"work_id": work_id, "chapter_id": chapter_id, "reason": error.reason, "message": str(error)}
                            failures.append(item)
                            report["failures"].append(item)
                            report["access_blocked"] += 1
                        elif isinstance(error, (RateLimited, RequestBudgetExceeded)):
                            item = {"work_id": work_id, "chapter_id": chapter_id, "reason": error.reason, "message": str(error)}
                            failures.append(item)
                            report["failures"].append(item)
                            halted = True
                        elif isinstance(error, (ScraperError, OSError, ValueError)):
                            item = {
                                "work_id": work_id,
                                "chapter_id": chapter_id,
                                "reason": getattr(error, "reason", error.__class__.__name__),
                                "message": str(error),
                            }
                            failures.append(item)
                            report["failures"].append(item)
                        else:
                            raise error
                    if halted or len(report["failures"]) >= max_errors:
                        break
                work_reports.append(report)
                if halted:
                    break
            except ConfigError:
                raise
            except (DailyVerificationRequired, SecurityVerificationRequired) as exc:
                failures.append({
                    "work_id": work_id,
                    "reason": exc.reason,
                    "message": str(exc),
                })
                halted = True
                break
            except (AccessBlocked, RateLimited, RequestBudgetExceeded) as exc:
                failures.append({
                    "work_id": work_id,
                    "reason": exc.reason,
                    "message": str(exc),
                })
                halted = True
                break
            except (ScraperError, OSError, ValueError) as exc:
                failures.append({
                    "work_id": work_id,
                    "reason": getattr(exc, "reason", exc.__class__.__name__),
                    "message": str(exc),
                })
                continue

        raw_total = sum(raw for raw, _ in selected_sizes)
        gzip_total = sum(packed for _, packed in selected_sizes)
        average_raw = statistics.fmean(raw for raw, _ in selected_sizes) if selected_sizes else 0.0
        average_gzip = statistics.fmean(packed for _, packed in selected_sizes) if selected_sizes else 0.0
        average_chapters = statistics.fmean(chapter_totals) if chapter_totals else 0.0
        projected_chapters = round(project_works * average_chapters)
        projected_gzip = round(projected_chapters * average_gzip)
        projected_raw = round(projected_chapters * average_raw)
        usage = self.service.request_usage()
        report = {
            "status": "failed" if not selected_sizes else "partial" if failures else "completed",
            "started_at": started,
            "finished_at": utc_now(),
            "archive": self.store.status(),
            "run": {
                "works_requested": len(work_ids),
                "works_measured": len(chapter_totals),
                "chapters_fetched": fetched,
                "chapters_reused": skipped,
                "access_blocked_chapters": sum(work["access_blocked"] for work in work_reports),
                "requests_used": usage["used"],
                "request_limit": usage["limit"],
                "halted": halted,
                "works_with_archived_samples": sum(1 for work in work_reports if work["archived"] > 0),
                "failures": failures,
                "raw_bytes": raw_total,
                "gzip_bytes": gzip_total,
                "compression_ratio": (gzip_total / raw_total) if raw_total else None,
                "average_raw_bytes_per_chapter": average_raw,
                "average_gzip_bytes_per_chapter": average_gzip,
                "average_discovered_chapters_per_work": average_chapters,
            },
            "projection": {
                "works": project_works,
                "estimated_chapters": projected_chapters,
                "estimated_raw_bytes": projected_raw,
                "estimated_gzip_bytes": projected_gzip,
                "estimated_gzip_gib": projected_gzip / (1024**3),
                "recommended_with_25pct_margin_gib": projected_gzip * 1.25 / (1024**3),
                "note": "표본 작품의 회차 수와 표본 본문 압축 크기를 단순 평균한 추정치입니다.",
            },
            "works": work_reports,
        }
        report_path = self.store.write_report(report)
        report["report_path"] = str(report_path)
        return report

    def collect_from_metadata(self, metadata: Storage) -> dict:
        """Archive missing chapter bodies already discovered by the metadata crawler."""
        started = utc_now()
        archive_alias = "body_archive_status"
        attached = {str(row[1]) for row in metadata.conn.execute("PRAGMA database_list")}
        if archive_alias not in attached:
            metadata.conn.execute(
                f"ATTACH DATABASE ? AS {archive_alias}",
                (str(self.store.database),),
            )

        def archive_counts() -> tuple[int, int]:
            row = metadata.conn.execute(
                f"""SELECT
                       COUNT(ac.chapter_id),
                       COALESCE(SUM(CASE
                           WHEN ac.chapter_id IS NULL AND af.terminal=1 THEN 1 ELSE 0
                       END), 0)
                   FROM chapters c
                   JOIN works w ON w.id=c.work_id
                   LEFT JOIN {archive_alias}.archive_chapters ac
                     ON ac.work_id=w.site_id AND ac.chapter_id=c.site_id
                   LEFT JOIN {archive_alias}.archive_failures af
                     ON af.work_id=w.site_id AND af.chapter_id=c.site_id
                   WHERE w.site_id IS NOT NULL AND c.site_id IS NOT NULL"""
            ).fetchone()
            return int(row[0]), int(row[1])

        total_candidates = int(
            metadata.conn.execute(
                """SELECT COUNT(*) FROM chapters c JOIN works w ON w.id=c.work_id
                   WHERE w.site_id IS NOT NULL AND c.site_id IS NOT NULL"""
            ).fetchone()[0]
        )
        works = metadata.conn.execute(
            """SELECT w.*,COUNT(c.id) discovered_chapters
               FROM works w JOIN chapters c ON c.work_id=w.id
               WHERE w.site_id IS NOT NULL AND c.site_id IS NOT NULL
               GROUP BY w.id ORDER BY w.id"""
        ).fetchall()
        reused, terminal_skips = archive_counts()
        fetched = 0
        failures: list[dict] = []
        halted = False
        halt_reason: str | None = None
        jobs: list[tuple[str, dict]] = []

        def flush_jobs() -> None:
            nonlocal fetched, terminal_skips, halted, halt_reason
            if not jobs:
                return
            batch = list(jobs)
            jobs.clear()
            for job_work_id, chapter, saved, error in self._chapter_jobs_batch(batch):
                chapter_id = str(chapter["id"])
                if error is None:
                    assert saved is not None
                    fetched += 1
                    continue
                if isinstance(error, (DailyVerificationRequired, SecurityVerificationRequired)):
                    self.store.record_retryable_failure(job_work_id, chapter_id, error.reason, str(error))
                    halted = True
                    halt_reason = error.reason
                    failures.append({
                        "work_id": job_work_id,
                        "chapter_id": chapter_id,
                        "reason": error.reason,
                        "message": str(error),
                        "terminal": False,
                    })
                    continue
                if isinstance(error, AccessBlocked):
                    self.store.record_failure(job_work_id, chapter_id, error.reason, str(error), terminal=True)
                    terminal_skips += 1
                    failures.append({
                        "work_id": job_work_id,
                        "chapter_id": chapter_id,
                        "reason": error.reason,
                        "message": str(error),
                        "terminal": True,
                    })
                    continue
                if isinstance(error, (RateLimited, RequestBudgetExceeded)):
                    halted = True
                    halt_reason = error.reason
                    failures.append({
                        "work_id": job_work_id,
                        "chapter_id": chapter_id,
                        "reason": error.reason,
                        "message": str(error),
                        "terminal": False,
                    })
                    continue
                if isinstance(error, (ScraperError, OSError, ValueError)):
                    reason = getattr(error, "reason", error.__class__.__name__)
                    final_terminal = self.store.record_failure(
                        job_work_id,
                        chapter_id,
                        reason,
                        str(error),
                        terminal=isinstance(error, ParserMismatch),
                    )
                    if final_terminal:
                        terminal_skips += 1
                    failures.append({
                        "work_id": job_work_id,
                        "chapter_id": chapter_id,
                        "reason": reason,
                        "message": str(error),
                        "terminal": final_terminal,
                    })
                    continue
                raise error

        for work_row in works:
            work_id = site_id(str(work_row["site_id"]))
            try:
                genres = json.loads(work_row["genres_json"] or "[]")
            except (TypeError, ValueError):
                genres = []
            work = {
                "id": work_id,
                "title": work_row["title"],
                "url": work_row["normalized_url"],
                "author": work_row["author"],
                "genres": genres if isinstance(genres, list) else [],
                "serial_status": work_row["serial_status"],
                "displayed_chapter_count": work_row["displayed_chapter_count"],
            }
            self.store.save_work(work, int(work_row["discovered_chapters"]))
            chapters = metadata.conn.execute(
                f"""SELECT c.site_id,c.normalized_url,c.title,c.display_order,
                           c.chapter_number,c.date_raw,c.date_normalized
                    FROM chapters c
                    LEFT JOIN {archive_alias}.archive_chapters ac
                      ON ac.work_id=? AND ac.chapter_id=c.site_id
                    LEFT JOIN {archive_alias}.archive_failures af
                      ON af.work_id=? AND af.chapter_id=c.site_id
                    WHERE c.work_id=? AND c.site_id IS NOT NULL
                      AND ac.chapter_id IS NULL
                      AND COALESCE(af.terminal, 0)=0
                    ORDER BY c.display_order,c.id""",
                (work_id, work_id, work_row["id"]),
            ).fetchall()
            for row in chapters:
                chapter_id = site_id(str(row["site_id"]))
                chapter = {
                    "id": chapter_id,
                    "title": row["title"],
                    "url": row["normalized_url"],
                    "display_order": row["display_order"],
                    "chapter_number": row["chapter_number"],
                    "date_raw": row["date_raw"],
                    "date_normalized": row["date_normalized"],
                }
                jobs.append((work_id, chapter))
                if len(jobs) >= self.workers:
                    flush_jobs()
                    if halted:
                        break
            if halted:
                break

        if jobs and not halted:
            flush_jobs()

        archived_now, terminal_now = archive_counts()
        pending = max(0, total_candidates - archived_now - terminal_now)
        usage = self.service.request_usage()
        status = "completed" if pending == 0 else "partial"
        return {
            "status": status,
            "reason": None if status == "completed" else halt_reason or "pending_bodies",
            "started_at": started,
            "finished_at": utc_now(),
            "total_chapters": total_candidates,
            "archived_chapters": archived_now,
            "terminal_skips": terminal_now,
            "pending": pending,
            "fetched_this_batch": fetched,
            "reused_this_batch": reused,
            "requests_used": usage["used"],
            "request_limit": usage["limit"],
            "workers": self.workers,
            "browser_workers": self.browser_workers,
            "failures": failures,
            "archive": self.store.status(),
        }


def resolve_archive_root(config: Config, output_dir: Path | str) -> Path:
    path = Path(output_dir)
    return path.resolve() if path.is_absolute() else (config.source_path.parent / path).resolve()


def collect_pc(
    config: Config,
    *,
    output_dir: Path | str,
    work_ids: list[str],
    works: int,
    chapters_per_work: int,
    all_chapters: bool,
    refresh: bool,
    project_works: int,
    chapter_delay: float | None,
    max_errors: int,
    max_requests: int,
    workers: int = 1,
    browser_workers: int = 1,
) -> dict:
    root = resolve_archive_root(config, output_dir)
    store = ArchiveStore(root)
    collector = PcCollector(
        config,
        store,
        chapter_delay=chapter_delay,
        max_requests=max_requests,
        workers=workers,
        browser_workers=browser_workers,
    )
    try:
        ids = [site_id(value) for value in work_ids] if work_ids else collector.catalog_work_ids(works)
        return collector.collect(
            ids,
            chapters_per_work=chapters_per_work,
            all_chapters=all_chapters,
            refresh=refresh,
            project_works=project_works,
            max_errors=max_errors,
        )
    finally:
        collector.close()


def pc_status(config: Config, output_dir: Path | str) -> dict:
    return ArchiveStore(resolve_archive_root(config, output_dir)).status()
