from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkSummary:
    title: str
    url: str
    site_id: str | None = None
    genres: list[str] = field(default_factory=list)
    platform: str | None = None
    update_raw: str | None = None
    cover_image_url: str | None = None


@dataclass(slots=True)
class Chapter:
    title: str
    url: str
    display_order: int
    site_id: str | None = None
    chapter_number: float | None = None
    date_raw: str | None = None
    date_normalized: str | None = None


@dataclass(slots=True)
class WorkDetail:
    title: str
    url: str
    site_id: str | None = None
    author: str | None = None
    genres: list[str] = field(default_factory=list)
    serial_status: str | None = None
    platform: str | None = None
    displayed_chapter_count: int | None = None
    update_raw: str | None = None
    update_normalized: str | None = None
    chapters: list[Chapter] = field(default_factory=list)
    next_url: str | None = None
    episode_page: int = 1
    last_page: int = 1


@dataclass(slots=True)
class ListPage:
    works: list[WorkSummary]
    next_url: str | None
    is_explicitly_empty: bool = False
    is_last_page_confirmed: bool = False
