from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path
from typing import Any

from .storage import Storage


FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
WORK_FIELDS = ["id", "site_id", "normalized_url", "title", "author", "genres", "serial_status", "platform", "displayed_chapter_count", "update_raw", "update_normalized", "cover_image_url", "first_seen_at", "last_checked_at"]
CHAPTER_FIELDS = ["id", "work_id", "site_id", "normalized_url", "title", "display_order", "chapter_number", "date_raw", "date_normalized", "first_seen_at", "last_checked_at"]


def protect_csv(value: Any, enabled: bool = True) -> Any:
    if enabled and isinstance(value, str) and value.startswith(FORMULA_PREFIXES):
        return "'" + value
    return value


def _dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    if "genres_json" in result:
        result["genres"] = json.loads(result.pop("genres_json") or "[]")
    return result


def export_csv(storage: Storage, output_dir: Path | str, *, bom: bool = True, protect_formulas: bool = True) -> tuple[Path, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    works_path = output / "works.csv"
    chapters_path = output / "chapters.csv"
    encoding = "utf-8-sig" if bom else "utf-8"
    for path, table in ((works_path, "works"), (chapters_path, "chapters")):
        rows = [_dict(row) for row in storage.rows(table)]
        if table == "works":
            for row in rows:
                row["genres"] = json.dumps(row["genres"], ensure_ascii=False)
        fields = list(rows[0]) if rows else (WORK_FIELDS if table == "works" else CHAPTER_FIELDS)
        with path.open("w", encoding=encoding, newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({key: protect_csv(value, protect_formulas) for key, value in row.items()} for row in rows)
    return works_path, chapters_path


def export_json(
    storage: Storage,
    output_dir: Path | str,
    *,
    gzip_output: bool = False,
    pretty: bool = True,
    filename: str | None = None,
) -> Path:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    default_name = "novels.json.gz" if gzip_output else "novels.json"
    name = filename or default_name
    if Path(name).name != name or not name.endswith(".json.gz" if gzip_output else ".json"):
        raise ValueError("JSON 파일 이름은 경로 없는 .json 또는 .json.gz 이름이어야 합니다")
    path = output / name
    works = [_dict(row) for row in storage.rows("works")]
    chapters = [_dict(row) for row in storage.rows("chapters")]
    by_work: dict[int, list[dict[str, Any]]] = {}
    for chapter in chapters:
        by_work.setdefault(int(chapter["work_id"]), []).append(chapter)
    for work in works:
        work["chapters"] = by_work.get(int(work["id"]), [])
    payload = json.dumps(
        {"works": works},
        ensure_ascii=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    ).encode("utf-8")
    if gzip_output:
        # mtime=0 makes identical exports byte-for-byte reproducible.
        path.write_bytes(gzip.compress(payload, compresslevel=9, mtime=0))
    else:
        path.write_bytes(payload)
    return path
