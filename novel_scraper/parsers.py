from __future__ import annotations

import json
import re
from datetime import date
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .errors import BlockPageDetected, ContentRestricted, HostNotAllowed, ParserMismatch
from .models import Chapter, ListPage, WorkDetail, WorkSummary
from .url_utils import normalize_url

ADAPTER_VERIFIED_AT = "2026-09-08"
_WORK_PATH = re.compile(r"^/novel/(?P<work>\d+)$")
_CHAPTER_PATH = re.compile(r"^/novel/(?P<work>\d+)/(?P<chapter>\d+)$")
_BLOCK_MARKERS = (
    "captcha",
    "cf-chl-",
    "checking your browser",
    "access denied",
    "unusual traffic",
    "접근이 차단",
)


def _soup(html: str | bytes) -> BeautifulSoup:
    soup = BeautifulSoup(html, "html.parser")
    probe = " ".join((soup.title.get_text(" ", strip=True) if soup.title else "", soup.get_text(" ", strip=True)[:3000])).lower()
    if any(marker in probe for marker in _BLOCK_MARKERS):
        raise BlockPageDetected("HTTP 200 응답에서 CAPTCHA/접근 차단 문구를 감지했습니다")
    return soup


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())
    return value or None


def _site_id(url: str, pattern: re.Pattern[str], group: str) -> str | None:
    match = pattern.fullmatch(urlsplit(url).path)
    return match.group(group) if match else None


def _absolute_date(raw: str | None) -> str | None:
    if not raw:
        return None
    match = re.fullmatch(r"\s*(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\s*", raw)
    if not match:
        return None
    try:
        return date(*(int(x) for x in match.groups())).isoformat()
    except ValueError:
        return None


def _json_item_list(soup: BeautifulSoup) -> tuple[list[dict], int | None, bool]:
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        values = data if isinstance(data, list) else [data]
        for value in values:
            if not isinstance(value, dict):
                continue
            entity = value.get("mainEntity") if value.get("@type") == "CollectionPage" else value
            if isinstance(entity, dict) and entity.get("@type") == "ItemList":
                items = entity.get("itemListElement")
                count = entity.get("numberOfItems")
                return (items if isinstance(items, list) else [], count if isinstance(count, int) else None, True)
    return [], None, False


def parse_list_page(
    html: str | bytes,
    base_url: str,
    allowed_hosts: tuple[str, ...],
    allowed_asset_hosts: tuple[str, ...] = (),
    *,
    pagination_url: str | None = None,
) -> ListPage:
    soup = _soup(html)
    json_items, declared_count, has_item_list = _json_item_list(soup)
    results: list[WorkSummary] = []
    seen: set[str] = set()

    # Verified live structure: each card is a li[date-title] with a /novel/{id} link.
    for card in soup.select("li[date-title]"):
        anchor = card.select_one('a[href^="/novel/"]')
        if anchor is None:
            continue
        try:
            url = normalize_url(str(anchor.get("href", "")), base_url, allowed_hosts)
        except HostNotAllowed:
            continue
        work_id = _site_id(url, _WORK_PATH, "work")
        title = _clean(str(card.get("date-title", ""))) or _clean(card.select_one("span.title").get_text(" ", strip=True) if card.select_one("span.title") else None)
        if not work_id or not title or url in seen:
            continue
        genres = [x.strip() for x in str(card.get("data-genre", "")).split(",") if x.strip()]
        platform_node = card.select_one(".list-platform")
        update_node = card.select_one(".list-date")
        image_node = card.select_one("img.theme-thumb-img[src]")
        image_url = None
        if image_node:
            raw_image = str(image_node.get("src", "")).strip()
            image_parts = urlsplit(raw_image)
            if image_parts.scheme in {"http", "https"} and (image_parts.hostname or "").lower().rstrip(".") in allowed_asset_hosts:
                image_url = raw_image
        results.append(WorkSummary(title, url, work_id, genres, _clean(platform_node.get_text(" ", strip=True) if platform_node else None), _clean(update_node.get_text(" ", strip=True) if update_node else None), image_url))
        seen.add(url)

    # JSON-LD is a semantic fallback and also proves that this is a list page.
    for item in json_items:
        if not isinstance(item, dict):
            continue
        raw_url = item.get("url")
        title = _clean(item.get("name") if isinstance(item.get("name"), str) else None)
        if not isinstance(raw_url, str) or not title:
            continue
        try:
            url = normalize_url(raw_url, base_url, allowed_hosts)
        except HostNotAllowed:
            continue
        work_id = _site_id(url, _WORK_PATH, "work")
        if work_id and url not in seen:
            results.append(WorkSummary(title, url, work_id))
            seen.add(url)

    if not results:
        if has_item_list and declared_count == 0:
            return ListPage([], None, True)
        raise ParserMismatch("작품 목록 영역을 찾지 못했습니다; 차단/오류 페이지 또는 DOM 변경 가능성이 있습니다")

    # Some deployments rewrite ?page=N to an opaque same-host path.  Resolve
    # document links against the final URL, but derive pagination state from
    # the requested URL retained by the caller.
    pagination_base = pagination_url or base_url
    current_page = int(parse_qs(urlsplit(pagination_base).query).get("page", ["1"])[0] or 1)
    next_url = None
    pagination_anchors = soup.select("ul.pagination a[href], .pg_wrap a[href]")
    observed_pages: list[int] = []
    for anchor in pagination_anchors:
        try:
            candidate = normalize_url(str(anchor["href"]), base_url, allowed_hosts)
            page_values = parse_qs(urlsplit(candidate).query).get("page", [])
            if page_values:
                observed_pages.append(int(page_values[0]))
            if urlsplit(candidate).path == urlsplit(pagination_base).path and page_values and int(page_values[0]) == current_page + 1:
                next_url = candidate
                break
        except (HostNotAllowed, ValueError):
            continue
    is_last_page_confirmed = bool(pagination_anchors) and next_url is None and not any(
        page > current_page for page in observed_pages
    )
    return ListPage(results, next_url, is_last_page_confirmed=is_last_page_confirmed)


def parse_detail_page(html: str | bytes, base_url: str, allowed_hosts: tuple[str, ...]) -> WorkDetail:
    soup = _soup(html)
    requested_url = normalize_url(base_url, base_url, allowed_hosts)
    requested_parts = urlsplit(requested_url)
    query = [(key, value) for key, values in parse_qs(requested_parts.query, keep_blank_values=True).items() for value in values if key != "epage"]
    url = urlunsplit((requested_parts.scheme, requested_parts.netloc, requested_parts.path, urlencode(query, doseq=True), ""))
    work_id = _site_id(url, _WORK_PATH, "work")
    if not work_id:
        raise ParserMismatch(f"상세 URL 형식이 아닙니다: {url}")

    title_node = soup.select_one(".theme-detail-title-line")
    title = _clean(title_node.get_text(" ", strip=True) if title_node else None)
    if not title:
        raise ParserMismatch("필수 상세 제목을 찾지 못했습니다")

    info: dict[str, str] = {}
    for row in soup.select(".theme-detail-info-row"):
        label = row.select_one(".theme-detail-info-label")
        value = row.select_one(".theme-detail-info-value")
        if label and value:
            key = _clean(label.get_text(" ", strip=True))
            val = _clean(value.get_text(" ", strip=True))
            if key and val:
                info[key] = val

    chapter_container = soup.select_one("ul.list-body")
    if chapter_container is None:
        if soup.select_one('input[type="password"]') is not None:
            raise ContentRestricted("비밀번호가 필요한 작품이므로 회차 목록 수집에서 제외합니다")
        raise ParserMismatch("회차 목록 컨테이너를 찾지 못했습니다")
    chapters: list[Chapter] = []
    seen: set[str] = set()
    try:
        episode_page = max(1, int(parse_qs(requested_parts.query).get("epage", ["1"])[0]))
    except ValueError:
        raise ParserMismatch("잘못된 회차 페이지 식별자입니다") from None
    order_offset = (episode_page - 1) * 100
    for order, item in enumerate(chapter_container.select("li.list-item"), start=order_offset + 1):
        anchor = item.select_one(".wr-subject a[href]")
        if anchor is None:
            continue
        try:
            chapter_url = normalize_url(str(anchor["href"]), url, allowed_hosts)
        except HostNotAllowed:
            continue
        match = _CHAPTER_PATH.fullmatch(urlsplit(chapter_url).path)
        if not match or match.group("work") != work_id or chapter_url in seen:
            continue
        chapter_title = _clean(anchor.get_text(" ", strip=True))
        if not chapter_title:
            continue
        date_node = item.select_one(".wr-date") or item.select_one(".item-details .fa-clock-o")
        if date_node and "wr-date" not in (date_node.get("class") or []):
            date_node = date_node.parent
        date_raw = _clean(date_node.get_text(" ", strip=True) if date_node else None)
        number_node = item.select_one(".wr-num")
        number_raw = number_node.get_text(strip=True) if number_node else str(item.get("data-index", ""))
        number_match = re.fullmatch(r"(\d+(?:\.\d+)?)", number_raw)
        if number_match is None:
            number_match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*화", chapter_title)
        chapters.append(Chapter(chapter_title, chapter_url, order, match.group("chapter"), float(number_match.group(1)) if number_match else None, date_raw, _absolute_date(date_raw)))
        seen.add(chapter_url)

    displayed_count = None
    for item in soup.select(".theme-detail-meta-item"):
        label = _clean(item.select_one(".theme-detail-meta-label").get_text(" ", strip=True) if item.select_one(".theme-detail-meta-label") else None)
        value = _clean(item.select_one(".theme-detail-meta-value").get_text(" ", strip=True) if item.select_one(".theme-detail-meta-value") else None)
        if label == "최신화" and value:
            match = re.fullmatch(r"(\d+)\s*화", value)
            if match:
                displayed_count = int(match.group(1))
                break

    genres = [x.strip() for x in info.get("장르", "").split(",") if x.strip()]
    next_url = None
    last_page = episode_page
    for anchor in soup.select("ul.pagination a[href], .theme-episode-pager a[href], nav.pg_wrap a[href]"):
        try:
            candidate = normalize_url(str(anchor["href"]), requested_url, allowed_hosts)
            candidate_parts = urlsplit(candidate)
            candidate_page = int(parse_qs(candidate_parts.query).get("epage", ["0"])[0])
            if candidate_parts.path == requested_parts.path and candidate_page >= 1:
                last_page = max(last_page, candidate_page)
                if candidate_page == episode_page + 1:
                    next_url = candidate
        except (HostNotAllowed, ValueError):
            continue

    return WorkDetail(
        title=title,
        url=url,
        site_id=work_id,
        author=info.get("작가"),
        genres=genres,
        serial_status=info.get("발행구분"),
        displayed_chapter_count=displayed_count,
        chapters=chapters,
        next_url=next_url,
        episode_page=episode_page,
        last_page=last_page,
    )
