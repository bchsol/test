from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .models import WorkDetail, WorkSummary


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    mode TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running','partial','completed','failed','interrupted')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    max_works INTEGER NOT NULL,
    max_requests INTEGER NOT NULL,
    requests_used INTEGER NOT NULL DEFAULT 0,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    error_reason TEXT,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('robots','list','detail')),
    target TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','processing','completed','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, kind, target)
);
CREATE TABLE IF NOT EXISTS works (
    id INTEGER PRIMARY KEY,
    site_id TEXT,
    normalized_url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    author TEXT,
    genres_json TEXT NOT NULL DEFAULT '[]',
    serial_status TEXT,
    platform TEXT,
    displayed_chapter_count INTEGER,
    update_raw TEXT,
    update_normalized TEXT,
    cover_image_url TEXT,
    first_seen_at TEXT NOT NULL,
    last_checked_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS works_site_id_unique ON works(site_id) WHERE site_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS run_works (
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    PRIMARY KEY(run_id, work_id)
);
CREATE TABLE IF NOT EXISTS chapters (
    id INTEGER PRIMARY KEY,
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    site_id TEXT,
    normalized_url TEXT NOT NULL,
    title TEXT NOT NULL,
    display_order INTEGER NOT NULL,
    chapter_number REAL,
    date_raw TEXT,
    date_normalized TEXT,
    first_seen_at TEXT NOT NULL,
    last_checked_at TEXT NOT NULL,
    UNIQUE(work_id, normalized_url)
);
CREATE UNIQUE INDEX IF NOT EXISTS chapters_site_id_unique ON chapters(work_id, site_id) WHERE site_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chapters_work_display_order ON chapters(work_id, display_order, id);
CREATE INDEX IF NOT EXISTS idx_works_last_checked ON works(last_checked_at DESC, id DESC);
"""


class Storage:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(works)")}
        if "cover_image_url" not in columns:
            with self.conn:
                self.conn.execute("ALTER TABLE works ADD COLUMN cover_image_url TEXT")
        self.conn.execute("PRAGMA optimize")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def create_run(self, mode: str, max_works: int, max_requests: int) -> int:
        with self.conn:
            cur = self.conn.execute(
                "INSERT INTO runs(mode,status,started_at,max_works,max_requests) VALUES(?,?,?,?,?)",
                (mode, "running", utc_now(), max_works, max_requests),
            )
        return int(cur.lastrowid)

    def latest_resumable_run(self) -> sqlite3.Row | None:
        # Resume runs copy unfinished tasks without rewriting historical source
        # rows. An older partial run can therefore retain stale pending tasks
        # after a newer run completed them. Only the newest run is the active tip.
        return self.conn.execute(
            """SELECT * FROM runs
               WHERE id=(SELECT MAX(id) FROM runs)
                 AND status IN ('running','partial','interrupted','failed')
                 AND EXISTS (
                     SELECT 1 FROM tasks
                     WHERE run_id=runs.id AND status != 'completed'
                 )"""
        ).fetchone()

    def create_resume_run(self, source_run_id: int, max_requests: int, max_works: int | None = None) -> int:
        source = self.conn.execute("SELECT * FROM runs WHERE id=?", (source_run_id,)).fetchone()
        if source is None:
            raise ValueError(f"실행 {source_run_id}을 찾을 수 없습니다")
        source_limit = int(source["max_works"])
        target_limit = source_limit if max_works is None else max_works
        if target_limit < source_limit:
            raise ValueError(f"재개 작품 한도는 기존 한도 {source_limit}보다 작게 줄일 수 없습니다")
        new_id = self.create_run("resume", target_limit, max_requests)
        now = utc_now()
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO run_works(run_id,work_id) SELECT ?,work_id FROM run_works WHERE run_id=?", (new_id, source_run_id))
            self.conn.execute(
                """INSERT OR IGNORE INTO tasks(run_id,kind,target,status,checkpoint_json,created_at,updated_at)
                   SELECT ?,kind,target,'pending',checkpoint_json,?,? FROM tasks
                   WHERE run_id=? AND status!='completed' AND kind!='robots'""",
                (new_id, now, now, source_run_id),
            )
        return new_id

    def add_task(self, run_id: int, kind: str, target: str, checkpoint: dict | None = None) -> bool:
        now = utc_now()
        with self.conn:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO tasks(run_id,kind,target,status,checkpoint_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, kind, target, "pending", json.dumps(checkpoint or {}, ensure_ascii=False), now, now),
            )
        return cur.rowcount > 0

    def reset_processing(self, run_id: int) -> None:
        with self.conn:
            self.conn.execute("UPDATE tasks SET status='pending', updated_at=? WHERE run_id=? AND status='processing'", (utc_now(), run_id))
            self.conn.execute("UPDATE runs SET status='running', finished_at=NULL, error_reason=NULL, error_message=NULL WHERE id=?", (run_id,))

    def claim_task(self, run_id: int) -> sqlite3.Row | None:
        with self.conn:
            row = self.conn.execute(
                "SELECT * FROM tasks WHERE run_id=? AND status='pending' ORDER BY CASE kind WHEN 'robots' THEN 0 WHEN 'list' THEN 1 ELSE 2 END, id LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            self.conn.execute("UPDATE tasks SET status='processing', attempts=attempts+1, updated_at=? WHERE id=?", (utc_now(), row["id"]))
        return self.conn.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()

    def fail_task(self, task_id: int, reason: str, message: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE tasks SET status='failed', error_reason=?, error_message=?, updated_at=? WHERE id=?", (reason, message, utc_now(), task_id))

    def requeue_task(self, task_id: int, reason: str, message: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE tasks SET status='pending', error_reason=?, error_message=?, updated_at=? WHERE id=?", (reason, message, utc_now(), task_id))

    def complete_task(self, task_id: int, checkpoint: dict | None = None) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE tasks SET status='completed', checkpoint_json=?, error_reason=NULL, error_message=NULL, updated_at=? WHERE id=?",
                (json.dumps(checkpoint or {}, ensure_ascii=False), utc_now(), task_id),
            )

    def skip_task(self, task_id: int, reason: str, message: str) -> None:
        checkpoint = {"skipped": True, "reason": reason}
        with self.conn:
            self.conn.execute(
                """UPDATE tasks SET status='completed', checkpoint_json=?,
                   error_reason=?, error_message=?, updated_at=? WHERE id=?""",
                (json.dumps(checkpoint, ensure_ascii=False), reason, message, utc_now(), task_id),
            )

    def _upsert_summary(self, summary: WorkSummary, now: str) -> int:
        existing = None
        if summary.site_id is not None:
            existing = self.conn.execute("SELECT id FROM works WHERE site_id=?", (summary.site_id,)).fetchone()
        if existing is None:
            existing = self.conn.execute("SELECT id FROM works WHERE normalized_url=?", (summary.url,)).fetchone()
        if existing is not None:
            work_id = int(existing[0])
            self.conn.execute(
                """UPDATE works SET site_id=COALESCE(?,site_id), normalized_url=?, title=?,
                     genres_json=CASE WHEN ?!='[]' THEN ? ELSE genres_json END,
                     platform=COALESCE(?,platform), update_raw=COALESCE(?,update_raw),
                     cover_image_url=COALESCE(?,cover_image_url), last_checked_at=? WHERE id=?""",
                (summary.site_id, summary.url, summary.title, json.dumps(summary.genres, ensure_ascii=False), json.dumps(summary.genres, ensure_ascii=False), summary.platform, summary.update_raw, summary.cover_image_url, now, work_id),
            )
            return work_id
        self.conn.execute(
            """INSERT INTO works(site_id,normalized_url,title,genres_json,platform,update_raw,cover_image_url,first_seen_at,last_checked_at)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(normalized_url) DO UPDATE SET
                 site_id=COALESCE(excluded.site_id,works.site_id), title=excluded.title,
                 genres_json=CASE WHEN excluded.genres_json!='[]' THEN excluded.genres_json ELSE works.genres_json END,
                 platform=COALESCE(excluded.platform,works.platform), update_raw=COALESCE(excluded.update_raw,works.update_raw),
                 cover_image_url=COALESCE(excluded.cover_image_url,works.cover_image_url),
                 last_checked_at=excluded.last_checked_at""",
            (summary.site_id, summary.url, summary.title, json.dumps(summary.genres, ensure_ascii=False), summary.platform, summary.update_raw, summary.cover_image_url, now, now),
        )
        row = self.conn.execute("SELECT id FROM works WHERE normalized_url=?", (summary.url,)).fetchone()
        assert row is not None
        return int(row[0])

    def save_list_and_complete(
        self,
        run_id: int,
        task_id: int,
        works: Iterable[WorkSummary],
        next_url: str | None,
        max_works: int,
        allow_next: bool = True,
        is_last_page_confirmed: bool = False,
    ) -> int:
        now = utc_now()
        added = 0
        with self.conn:
            existing = int(self.conn.execute("SELECT COUNT(*) FROM run_works WHERE run_id=?", (run_id,)).fetchone()[0])
            for summary in works:
                if existing + added >= max_works:
                    break
                work_id = self._upsert_summary(summary, now)
                cur = self.conn.execute("INSERT OR IGNORE INTO run_works(run_id,work_id) VALUES(?,?)", (run_id, work_id))
                if cur.rowcount:
                    added += 1
                self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(run_id,kind,target,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (run_id, "detail", summary.url, "pending", now, now),
                )
            total = int(self.conn.execute("SELECT COUNT(*) FROM run_works WHERE run_id=?", (run_id,)).fetchone()[0])
            if next_url and total < max_works and allow_next:
                self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(run_id,kind,target,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (run_id, "list", next_url, "pending", now, now),
                )
            self.conn.execute(
                "UPDATE tasks SET status='completed', checkpoint_json=?, updated_at=? WHERE id=?",
                (json.dumps({"accepted": added, "next_url": next_url, "last_page_confirmed": is_last_page_confirmed}, ensure_ascii=False), now, task_id),
            )
        return added

    def latest_completed_list_task(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE kind='list' AND status='completed' ORDER BY id DESC LIMIT 1"
        ).fetchone()

    def latest_expandable_completed_run(self, requested_limit: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT r.* FROM runs r
               WHERE r.status='completed'
                 AND (SELECT COUNT(*) FROM run_works rw WHERE rw.run_id=r.id) < ?
               ORDER BY r.id DESC LIMIT 1""",
            (requested_limit,),
        ).fetchone()

    def save_detail_and_complete(self, run_id: int, task_id: int, detail: WorkDetail) -> int:
        now = utc_now()
        with self.conn:
            work = self.conn.execute("SELECT id FROM works WHERE site_id=?", (detail.site_id,)).fetchone() if detail.site_id else None
            if work is None:
                work = self.conn.execute("SELECT id FROM works WHERE normalized_url=?", (detail.url,)).fetchone()
            if work is None:
                cur = self.conn.execute(
                    """INSERT INTO works(site_id,normalized_url,title,author,genres_json,serial_status,platform,displayed_chapter_count,update_raw,update_normalized,first_seen_at,last_checked_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (detail.site_id, detail.url, detail.title, detail.author, json.dumps(detail.genres, ensure_ascii=False), detail.serial_status, detail.platform, detail.displayed_chapter_count, detail.update_raw, detail.update_normalized, now, now),
                )
                work_id = int(cur.lastrowid)
            else:
                work_id = int(work[0])
                genres = json.dumps(detail.genres, ensure_ascii=False)
                self.conn.execute(
                    """UPDATE works SET site_id=COALESCE(?,site_id), normalized_url=?, title=?,
                       author=COALESCE(?,author), genres_json=CASE WHEN ?!='[]' THEN ? ELSE genres_json END,
                       serial_status=COALESCE(?,serial_status), platform=COALESCE(?,platform),
                       displayed_chapter_count=COALESCE(?,displayed_chapter_count),
                       update_raw=COALESCE(?,update_raw), update_normalized=COALESCE(?,update_normalized), last_checked_at=? WHERE id=?""",
                    (detail.site_id, detail.url, detail.title, detail.author, genres, genres, detail.serial_status, detail.platform, detail.displayed_chapter_count, detail.update_raw, detail.update_normalized, now, work_id),
                )
            for chapter in detail.chapters:
                found = self.conn.execute("SELECT id FROM chapters WHERE work_id=? AND site_id=?", (work_id, chapter.site_id)).fetchone() if chapter.site_id else None
                if found is None:
                    found = self.conn.execute("SELECT id FROM chapters WHERE work_id=? AND normalized_url=?", (work_id, chapter.url)).fetchone()
                if found is None:
                    self.conn.execute(
                        "INSERT INTO chapters(work_id,site_id,normalized_url,title,display_order,chapter_number,date_raw,date_normalized,first_seen_at,last_checked_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (work_id, chapter.site_id, chapter.url, chapter.title, chapter.display_order, chapter.chapter_number, chapter.date_raw, chapter.date_normalized, now, now),
                    )
                else:
                    self.conn.execute(
                        """UPDATE chapters SET site_id=COALESCE(?,site_id), normalized_url=?, title=?, display_order=?,
                           chapter_number=?, date_raw=?, date_normalized=?, last_checked_at=? WHERE id=?""",
                        (chapter.site_id, chapter.url, chapter.title, chapter.display_order, chapter.chapter_number, chapter.date_raw, chapter.date_normalized, now, int(found[0])),
                    )
            if detail.next_url:
                self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(run_id,kind,target,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (run_id, "detail", detail.next_url, "pending", now, now),
                )
            self.conn.execute("UPDATE tasks SET status='completed', checkpoint_json=?, updated_at=? WHERE id=?", (json.dumps({"chapters": len(detail.chapters)}, ensure_ascii=False), now, task_id))
        return work_id

    def finish_run(self, run_id: int, status: str, requests_used: int, *, reason: str | None = None, message: str | None = None, checkpoint: dict | None = None) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET status=?, finished_at=?, requests_used=?, error_reason=?, error_message=?, checkpoint_json=? WHERE id=?",
                (status, utc_now(), requests_used, reason, message, json.dumps(checkpoint or {}, ensure_ascii=False), run_id),
            )

    def run_counts(self, run_id: int) -> dict[str, int]:
        row = self.conn.execute(
            """SELECT
                (SELECT COUNT(*) FROM run_works WHERE run_id=?) works,
                (SELECT COUNT(*) FROM tasks WHERE run_id=? AND kind='detail' AND status='completed'
                    AND COALESCE(json_extract(checkpoint_json, '$.skipped'), 0)=0) details,
                (SELECT COUNT(*) FROM tasks WHERE run_id=? AND status='pending') pending,
                (SELECT COUNT(*) FROM tasks WHERE run_id=? AND status='failed') failed,
                (SELECT COUNT(*) FROM tasks WHERE run_id=? AND kind='detail' AND status='completed'
                    AND COALESCE(json_extract(checkpoint_json, '$.skipped'), 0)=1) skipped""",
            (run_id, run_id, run_id, run_id, run_id),
        ).fetchone()
        return dict(row) if row else {"works": 0, "details": 0, "pending": 0, "failed": 0, "skipped": 0}

    def list_page_count(self, run_id: int) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM tasks WHERE run_id=? AND kind='list' AND status='completed'", (run_id,)).fetchone()[0])

    def rows(self, table: str) -> list[sqlite3.Row]:
        if table not in {"works", "chapters"}:
            raise ValueError(table)
        if table == "works":
            return self.conn.execute("SELECT * FROM works ORDER BY id").fetchall()
        return self.conn.execute("SELECT * FROM chapters ORDER BY work_id, display_order, id").fetchall()

    def compact(self) -> dict[str, int]:
        """Reclaim unused SQLite pages without changing collected records."""
        self.conn.commit()
        before = self.path.stat().st_size if self.path.exists() else 0
        self.conn.execute("PRAGMA optimize")
        self.conn.execute("VACUUM")
        after = self.path.stat().st_size if self.path.exists() else 0
        return {"bytes_before": before, "bytes_after": after, "bytes_saved": max(0, before - after)}
