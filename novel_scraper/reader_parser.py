"""Extract only readable chapter content, never scripts, navigation or adverts."""
from __future__ import annotations

import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from .errors import HostNotAllowed, ParserMismatch, raise_for_access_notice
from .parsers import _soup
from .url_utils import normalize_url

CHAPTER_PATH = re.compile(r"^/novel/(\d+)/(\d+)$")
CONTENT_SELECTOR = "[data-theme-novel-content], .theme-novel-content, #novel_content, #novel-content"


def chapter_metadata(html: str | bytes, url: str, hosts: tuple[str, ...]) -> dict:
    normalized = normalize_url(url, url, hosts)
    match = CHAPTER_PATH.fullmatch(urlsplit(normalized).path)
    if not match:
        raise ParserMismatch("올바른 소설 회차 주소가 아닙니다")
    soup = _soup(html)
    title_node = soup.select_one(".theme-novel-title, .theme-novel-head h3")
    book_node = soup.select_one(".page-title h2")
    if not title_node:
        raise ParserMismatch("회차 제목을 찾지 못했습니다. 원본 사이트 구조가 변경되었을 수 있습니다")
    result = {
        "url": normalized, "work_id": match[1], "chapter_id": match[2],
        "title": title_node.get_text(" ", strip=True),
        "work_title": book_node.get_text(" ", strip=True) if book_node else "",
        "previous_url": None, "next_url": None,
    }
    for anchor in soup.select(".theme-novel-nav a[href]"):
        try:
            target = normalize_url(str(anchor["href"]), normalized, hosts)
        except (ValueError, HostNotAllowed):
            continue
        other = CHAPTER_PATH.fullmatch(urlsplit(target).path)
        if not other or other[1] != match[1] or target == normalized:
            continue
        label = re.sub(r"\s+", "", anchor.get_text())
        if label in {"이전화", "이전", "이전회차"}:
            result["previous_url"] = target
        elif label in {"다음화", "다음", "다음회차"}:
            result["next_url"] = target
    return result


def clean_paragraphs(fragment: str) -> list[str]:
    if len(fragment) > 2_000_000:
        raise ParserMismatch("회차 본문 크기 제한을 초과했습니다")
    soup = BeautifulSoup(fragment, "html.parser")
    for node in soup.select("script, style, iframe, object, embed, form, button, nav, aside, [hidden], [aria-hidden=true], .advertisement, .adsbygoogle, [data-banner-id]"):
        node.decompose()
    for node in soup.select("[style]"):
        style = re.sub(r"\s+", "", str(node.get("style", "")).lower())
        if "display:none" in style or "visibility:hidden" in style:
            node.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    # Text nodes outside <p> are meaningful in imported EPUB markup too. Insert
    # boundaries instead of selecting only <p>, which can silently drop paragraphs.
    for node in soup.find_all(["p", "div", "section", "h1", "h2", "h3", "li"]):
        node.insert_before("\n")
        node.insert_after("\n")
    lines = [re.sub(r"[\t \u00a0]+", " ", line).strip() for line in soup.get_text().splitlines()]
    paragraphs = [line for line in lines if line]
    if not paragraphs or (len(paragraphs) == 1 and ("불러오는 중" in paragraphs[0] or "loading" in paragraphs[0].lower())):
        raise ParserMismatch("표시된 소설 본문이 없습니다")
    return paragraphs


def static_chapter(html: str | bytes, url: str, hosts: tuple[str, ...]) -> dict | None:
    meta = chapter_metadata(html, url, hosts)
    soup = _soup(html)
    host = soup.select_one(CONTENT_SELECTOR)
    if host is None:
        raise ParserMismatch("소설 본문 영역이 없습니다")
    # A loading / verification / purchase notice is not a successful chapter.
    notice = host.select_one(".wr-none")
    if notice:
        text = notice.get_text(" ", strip=True)
        if "불러오는 중" in text:
            return None
        raise_for_access_notice(text)
    meta["paragraphs"] = clean_paragraphs(str(host))
    meta["render_method"] = "html"
    return meta
