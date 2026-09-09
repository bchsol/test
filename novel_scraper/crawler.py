from __future__ import annotations

import json
import logging
import time
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from .config import Config
from .errors import AccessBlocked, ConfigError, ContentRestricted, RequestBudgetExceeded, ScraperError
from .http_client import HttpClient, RequestBudget
from .parsers import parse_detail_page, parse_list_page
from .storage import Storage


@dataclass(slots=True)
class CrawlResult:
    run_id: int
    status: str
    requests_used: int
    works: int
    details: int
    pending: int
    failed: int
    reason: str | None = None
    workers: int = 1
    skipped: int = 0


def robots_url(start_url: str) -> str:
    parts = urlsplit(start_url)
    return urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))


def robots_allows(text: str, start_url: str, user_agent: str) -> bool:
    directives = [line.split("#", 1)[0].strip().lower() for line in text.splitlines()]
    if not any(line.startswith("user-agent:") and line.partition(":")[2].strip() for line in directives):
        raise AccessBlocked("robots.txt 응답이 비어 있거나 유효한 User-agent 지시문이 없습니다")
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url(start_url))
    parser.parse(text.splitlines())
    return parser.can_fetch(user_agent, start_url)


class Crawler:
    def __init__(self, config: Config, storage: Storage, *, transport: httpx.BaseTransport | None = None, sleep=None, monotonic=None, workers: int = 1):
        self.config = config
        self.storage = storage
        self.transport = transport
        self.sleep = sleep
        self.monotonic = monotonic
        if not 1 <= workers <= 8:
            raise ConfigError("병렬 작업 수는 1~8 범위여야 합니다")
        self.workers = workers
        self.log = logging.getLogger(__name__)

    def start(
        self,
        mode: str,
        max_works: int,
        max_requests: int,
        *,
        resume_source: int | None = None,
        recover_list: bool = False,
    ) -> CrawlResult:
        if not self.config.site.permission_confirmed:
            raise ConfigError("수집 허용 여부가 미확인입니다. 이용조건을 직접 확인한 뒤 site.permission_confirmed=true로 설정하십시오")
        if max_works <= 0 or max_requests <= 0:
            raise ConfigError("작품 및 요청 상한은 1 이상이어야 합니다")
        recovery_target = None
        recovery_task_id = None
        if resume_source is not None and recover_list:
            last_list = self.storage.latest_completed_list_task()
            if last_list is None:
                raise ConfigError("복구할 이전 목록 페이지 체크포인트가 없습니다")
            checkpoint = json.loads(last_list["checkpoint_json"] or "{}")
            if checkpoint.get("last_page_confirmed"):
                raise ConfigError("이전 수집에서 사이트의 마지막 목록 페이지까지 확인했습니다")
            parts = urlsplit(last_list["target"])
            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            try:
                query["page"] = str(int(query.get("page", "1")) + 1)
            except ValueError:
                raise ConfigError("이전 목록 페이지 번호를 복구할 수 없습니다") from None
            recovery_target = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
            recovery_task_id = int(last_list["id"])
        if resume_source is None:
            run_id = self.storage.create_run(mode, max_works, max_requests)
            self.storage.add_task(run_id, "list", self.config.site.start_url)
        else:
            run_id = self.storage.create_resume_run(resume_source, max_requests, max_works)
            max_works = int(self.storage.conn.execute("SELECT max_works FROM runs WHERE id=?", (run_id,)).fetchone()[0])
            if recovery_target is not None:
                self.storage.add_task(run_id, "list", recovery_target, {"recovered_from": recovery_task_id})
        self.storage.add_task(run_id, "robots", robots_url(self.config.site.start_url))
        return self._run(run_id, mode, max_works, max_requests)

    def _run(self, run_id: int, mode: str, max_works: int, max_requests: int) -> CrawlResult:
        budget = RequestBudget(max_requests)
        kwargs = {"transport": self.transport}
        if self.sleep is not None:
            kwargs["sleep"] = self.sleep
        if self.monotonic is not None:
            kwargs["monotonic"] = self.monotonic
        client = HttpClient(
            self.config.http,
            self.config.site.allowed_hosts,
            budget,
            pace_per_thread=self.workers > 1,
            **kwargs,
        )
        robots_text = None
        def check_target(url):
            if url == robots_url(self.config.site.start_url):
                return
            if robots_text is None or not robots_allows(robots_text, url, self.config.http.user_agent):
                raise AccessBlocked("robots.txt가 해당 페이지 수집을 허용하지 않습니다")
        client.url_policy = check_target
        final_status = "completed"
        reason = None
        message = None
        active_task = None
        active_batch: set[int] = set()
        executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="novel-detail") if self.workers > 1 else None

        def fetch_detail(task):
            response = client.get(task["target"])
            return parse_detail_page(response.content, str(response.url), self.config.site.allowed_hosts)

        try:
            while (task := self.storage.claim_task(run_id)) is not None:
                if task["kind"] == "detail" and executor is not None:
                    batch = [task]
                    while len(batch) < self.workers:
                        following = self.storage.claim_task(run_id)
                        if following is None:
                            break
                        if following["kind"] != "detail":
                            self.storage.requeue_task(following["id"], "scheduler_order", "병렬 상세 배치 이후 순차 처리")
                            break
                        batch.append(following)
                    active_batch = {int(item["id"]) for item in batch}
                    futures = [(item, executor.submit(fetch_detail, item)) for item in batch]
                    batch_error: tuple[str, str, str] | None = None
                    budget_error: RequestBudgetExceeded | None = None
                    for item, future in futures:
                        task_id = int(item["id"])
                        try:
                            detail = future.result()
                            self.storage.save_detail_and_complete(run_id, task_id, detail)
                        except ContentRestricted as exc:
                            self.storage.skip_task(task_id, exc.reason, str(exc))
                        except RequestBudgetExceeded as exc:
                            self.storage.requeue_task(task_id, exc.reason, str(exc))
                            budget_error = exc
                        except ScraperError as exc:
                            self.storage.fail_task(task_id, exc.reason, str(exc))
                            batch_error = batch_error or ("failed", exc.reason, str(exc))
                        except Exception as exc:
                            self.log.exception("병렬 상세 작업 실패")
                            self.storage.fail_task(task_id, "internal_error", str(exc))
                            batch_error = batch_error or ("failed", "internal_error", str(exc))
                        finally:
                            active_batch.discard(task_id)
                    if batch_error is not None:
                        final_status, reason, message = batch_error
                        break
                    if budget_error is not None:
                        final_status, reason, message = "partial", budget_error.reason, str(budget_error)
                        break
                    continue
                active_task = task
                self.log.info("task=%s target=%s", task["kind"], task["target"])
                response = client.get(task["target"])
                if task["kind"] == "robots":
                    robots_text = response.text
                    allowed = robots_allows(response.text, self.config.site.start_url, self.config.http.user_agent)
                    if not allowed:
                        raise AccessBlocked("robots.txt가 시작 URL 수집을 허용하지 않습니다")
                    self.storage.complete_task(task["id"], {"allowed": True, "robots_url": str(response.url)})
                elif task["kind"] == "list":
                    page = parse_list_page(
                        response.content,
                        str(response.url),
                        self.config.site.allowed_hosts,
                        self.config.site.allowed_asset_hosts,
                        pagination_url=task["target"],
                    )
                    page_limit = self.config.crawl.update_overlap_pages if mode == "update" else 10_000
                    allow_next = self.storage.list_page_count(run_id) + 1 < page_limit
                    self.storage.save_list_and_complete(
                        run_id,
                        task["id"],
                        page.works,
                        page.next_url,
                        max_works,
                        allow_next,
                        page.is_last_page_confirmed,
                    )
                else:
                    try:
                        detail = parse_detail_page(response.content, str(response.url), self.config.site.allowed_hosts)
                    except ContentRestricted as exc:
                        self.storage.skip_task(task["id"], exc.reason, str(exc))
                    else:
                        self.storage.save_detail_and_complete(run_id, task["id"], detail)
                active_task = None
        except RequestBudgetExceeded as exc:
            final_status, reason, message = "partial", exc.reason, str(exc)
            if active_task is not None:
                self.storage.requeue_task(active_task["id"], exc.reason, str(exc))
                active_task = None
        except KeyboardInterrupt:
            final_status, reason, message = "interrupted", "keyboard_interrupt", "사용자 중단"
            self.storage.reset_processing(run_id)
            active_task = None
            active_batch.clear()
        except ScraperError as exc:
            final_status, reason, message = "failed", exc.reason, str(exc)
            if active_task is not None:
                self.storage.fail_task(active_task["id"], exc.reason, str(exc))
        except Exception as exc:
            final_status, reason, message = "failed", "internal_error", str(exc)
            self.log.exception("예상하지 못한 작업 실패")
            if active_task is not None:
                self.storage.fail_task(active_task["id"], reason, message)
        finally:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
            client.close()
            counts = self.storage.run_counts(run_id)
            if final_status == "completed" and counts["pending"]:
                final_status = "partial"
                reason = reason or "pending_tasks"
            self.storage.finish_run(run_id, final_status, budget.used, reason=reason, message=message, checkpoint=counts)
        return CrawlResult(run_id, final_status, budget.used, reason=reason, workers=self.workers, **counts)


def continue_until_terminal(
    crawler: Crawler,
    initial: CrawlResult,
    max_requests: int,
    *,
    pause_seconds: float,
    on_batch=None,
    sleep=time.sleep,
) -> CrawlResult:
    """Resume budget-limited batches until completion or a real stop condition."""
    if max_requests < 2:
        raise ConfigError("연속 실행은 robots 확인 외 작업도 진행할 수 있도록 --max-requests 2 이상이 필요합니다")
    if pause_seconds < 0:
        raise ConfigError("배치 대기 시간은 음수일 수 없습니다")
    result = initial
    resumable_reasons = {"request_budget_exceeded", "pending_tasks"}
    while result.status == "partial" and result.reason in resumable_reasons:
        row = crawler.storage.conn.execute("SELECT max_works FROM runs WHERE id=?", (result.run_id,)).fetchone()
        if row is None:
            raise ConfigError(f"연속 재개할 실행 {result.run_id}을 찾을 수 없습니다")
        if pause_seconds:
            sleep(pause_seconds)
        result = crawler.start(
            "resume",
            max_works=int(row[0]),
            max_requests=max_requests,
            resume_source=result.run_id,
        )
        if on_batch is not None:
            on_batch(result)
    return result
