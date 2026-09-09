"""One bounded Chromium instance with concurrent pages for rendered chapters."""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import TimeoutError as FutureTimeoutError
from urllib.parse import urlsplit

from .errors import (
    AccessBlocked,
    ConfigError,
    NetworkFailure,
    raise_for_access_notice,
)
from .reader_parser import chapter_metadata, clean_paragraphs


READY = """() => {
  const host = document.querySelector('[data-theme-novel-content], .theme-novel-content');
  if (!host) return false;
  const root = host.shadowRoot || host;
  if ([...root.querySelectorAll('p, .novel-epub-rendered')].some(n => n.textContent.trim().length > 10)) return true;
  const notice = host.querySelector('.wr-none');
  return notice && !notice.textContent.includes('불러오는 중');
}"""
LOG = logging.getLogger(__name__)
BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}
READ_TIMEOUT_SECONDS = 70


_raise_for_access_notice = raise_for_access_notice


class BrowserReader:
    """Render concurrent pages in one isolated Chromium and one event loop."""

    def __init__(
        self,
        hosts: tuple[str, ...],
        channel: str = "chrome",
        workers: int = 1,
    ):
        if not 1 <= workers <= 4:
            raise ConfigError("본문 브라우저 병렬 작업 수는 1~4 범위여야 합니다")
        self.hosts = hosts
        self.channel = channel
        self.workers = workers
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._thread = threading.Thread(target=self._run_loop, name="novel-browser", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise ConfigError("본문 브라우저 이벤트 루프를 시작하지 못했습니다")

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._playwright = None
        self._browser = None
        self._context = None
        self._pages = None
        self._start_lock = asyncio.Lock()
        self._ready.set()
        self._loop.run_forever()
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        if pending:
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        self._loop.close()

    async def _start(self) -> None:
        if self._browser is not None and self._browser.is_connected():
            return
        async with self._start_lock:
            if self._browser is not None and self._browser.is_connected():
                return
            try:
                from playwright.async_api import async_playwright
            except ImportError as exc:
                raise ConfigError('본문 뷰어 의존성이 없습니다. .venv\\Scripts\\python.exe -m pip install -e ".[dev]"를 실행하세요') from exc
            await self._shutdown_objects()
            try:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(channel=self.channel, headless=True, timeout=20000)
                self._context = await self._browser.new_context(accept_downloads=False)

                async def route_request(route):
                    request = route.request
                    if request.resource_type in BLOCKED_RESOURCE_TYPES:
                        await route.abort()
                        return
                    if request.is_navigation_request() and request.frame != request.frame.page.main_frame:
                        await route.abort()
                        return
                    target = urlsplit(request.url)
                    if target.scheme in {"http", "https"} and (target.hostname or "") not in self.hosts:
                        await route.abort()
                        return
                    await route.continue_()

                await self._context.route("**/*", route_request)
                self._pages = asyncio.Queue()
                for _ in range(self.workers):
                    page = await self._context.new_page()
                    page.on("popup", lambda popup: asyncio.create_task(popup.close()))
                    await self._pages.put(page)
            except Exception as exc:
                await self._shutdown_objects()
                raise ConfigError(f"본문용 {self.channel} 브라우저를 시작하지 못했습니다. Chrome 설치를 확인하세요") from exc

    async def _read(self, url: str) -> dict:
        await self._start()
        page = await self._pages.get()
        LOG.info("browser_render_start url=%s", url)
        try:
            from playwright.async_api import Error, TimeoutError
            response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if response is None:
                raise NetworkFailure("회차 페이지 응답이 없습니다")
            if response.status in {401, 402, 403, 429}:
                raise AccessBlocked(f"원본 사이트 HTTP {response.status}: 인증 또는 이용 제한을 원본에서 확인해 주세요")
            if response.status >= 400:
                raise NetworkFailure(f"회차 페이지 HTTP {response.status}")
            if urlsplit(page.url).path != urlsplit(url).path:
                raise AccessBlocked("원본 사이트가 다른 페이지로 이동했습니다. 로그인·인증 상태를 확인해 주세요")
            gate = page.locator('#theme-novel-viewer-data')
            if await gate.count():
                import json
                cfg = json.loads(await gate.text_content() or '{}')
                if cfg.get('paidGate', {}).get('locked'):
                    raise AccessBlocked("유료 또는 잠긴 회차입니다. 원본에서 이용 권한을 확인해 주세요")
            await page.wait_for_function(READY, timeout=30000)
            content = await page.evaluate("""() => {
                const host=document.querySelector('[data-theme-novel-content], .theme-novel-content');
                const root=host && (host.shadowRoot || host);
                return {html:root ? root.innerHTML.slice(0,2000001) : '',
                    notice:host?.querySelector('.wr-none')?.textContent || ''};
            }""")
            if content['notice'] and '불러오는 중' not in content['notice']:
                _raise_for_access_notice(content['notice'])
            result = chapter_metadata(await page.content(), page.url, self.hosts)
            result['paragraphs'] = clean_paragraphs(content['html'])
            result['render_method'] = 'browser'
            LOG.info("browser_render_complete url=%s paragraphs=%s", url, len(result['paragraphs']))
            return result
        except TimeoutError as exc:
            raise NetworkFailure("브라우저에서 본문 표시를 기다리다 시간 초과했습니다. 원본에서 인증·점검 여부를 확인한 뒤 다시 시도하세요") from exc
        except Error as exc:
            raise NetworkFailure("본문 브라우저가 페이지를 열지 못했습니다. 원본 접근 상태를 확인해 주세요") from exc
        finally:
            try:
                await page.goto('about:blank', wait_until='commit', timeout=3000)
            except Exception:
                pass
            await self._pages.put(page)

    def read(self, url: str) -> dict:
        if self._closed:
            raise NetworkFailure("본문 브라우저가 이미 종료되었습니다")
        future = asyncio.run_coroutine_threadsafe(self._read(url), self._loop)
        try:
            return future.result(timeout=READ_TIMEOUT_SECONDS)
        except FutureTimeoutError as exc:
            future.cancel()
            LOG.error("browser_render_timeout url=%s", url)
            raise NetworkFailure("본문 브라우저 작업이 70초를 초과해 중단되었습니다") from exc

    async def _shutdown_objects(self) -> None:
        if self._browser is not None:
            try:
                await asyncio.wait_for(self._browser.close(), timeout=10)
            except Exception as exc:
                LOG.warning("Chromium 종료 경고: %s", exc)
        if self._playwright is not None:
            try:
                await asyncio.wait_for(self._playwright.stop(), timeout=5)
            except Exception as exc:
                LOG.warning("Playwright 종료 경고: %s", exc)
        self._browser = self._context = self._playwright = self._pages = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            future = asyncio.run_coroutine_threadsafe(self._shutdown_objects(), self._loop)
            future.result(timeout=16)
        except Exception as exc:
            LOG.warning("본문 브라우저 종료 경고: %s", exc)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
