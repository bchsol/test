"""Deterministic, offline benchmark for the parallel HTTP/parser path."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from .config import HttpConfig
from .errors import ConfigError
from .http_client import HttpClient, RequestBudget
from .parsers import parse_detail_page


def parse_workers(value: str) -> list[int]:
    try:
        workers = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError:
        raise ConfigError("benchmark --workers는 1,2,4 형식의 정수 목록이어야 합니다") from None
    if not workers or any(worker < 1 or worker > 8 for worker in workers):
        raise ConfigError("benchmark 병렬 작업 수는 각각 1~8 범위여야 합니다")
    return list(dict.fromkeys(workers))


def _html(work_id: str) -> str:
    return f'''<div class="theme-detail-title-line">합성 작품 {work_id}</div>
    <ul class="list-body"><li class="list-item"><div class="wr-subject">
    <a href="/novel/{work_id}/{work_id}01">1화</a></div></li></ul>'''


def run_benchmark(workers: list[int], requests: int, latency_ms: float) -> dict:
    if requests < 1 or requests > 1000:
        raise ConfigError("benchmark 요청 수는 1~1000 범위여야 합니다")
    if latency_ms < 0 or latency_ms > 5000:
        raise ConfigError("benchmark 합성 지연은 0~5000ms 범위여야 합니다")
    hosts = ("newtoki1.org",)
    results = []
    baseline = None
    for worker_count in workers:
        def handler(request: httpx.Request) -> httpx.Response:
            if latency_ms:
                time.sleep(latency_ms / 1000)
            work_id = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, text=_html(work_id), request=request)

        config = HttpConfig(
            min_interval_seconds=0,
            timeout_seconds=max(1.0, latency_ms / 1000 + 1),
            max_retries=0,
            max_retry_after_seconds=1,
            user_agent="NovelBenchmark/1.0",
        )
        budget = RequestBudget(requests)
        started = time.perf_counter()
        with HttpClient(config, hosts, budget, transport=httpx.MockTransport(handler)) as client:
            def fetch(index: int) -> str:
                work_id = str(100000 + index)
                url = f"https://newtoki1.org/novel/{work_id}"
                response = client.get(url)
                return parse_detail_page(response.content, str(response.url), hosts).site_id or ""

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                completed = list(executor.map(fetch, range(requests)))
        elapsed = max(time.perf_counter() - started, 1e-9)
        if len(set(completed)) != requests or budget.used != requests:
            raise RuntimeError("병렬 벤치마크 결과 검증에 실패했습니다")
        baseline = elapsed if baseline is None else baseline
        results.append(
            {
                "workers": worker_count,
                "completed": len(completed),
                "requests_used": budget.used,
                "elapsed_seconds": round(elapsed, 4),
                "requests_per_second": round(requests / elapsed, 2),
                "speedup_vs_first": round(baseline / elapsed, 2),
            }
        )
    return {
        "mode": "offline_synthetic",
        "network_used": False,
        "requests_per_case": requests,
        "synthetic_latency_ms": latency_ms,
        "results": results,
        "note": "Offline local comparison of the parallel HTTP/parser path; not a live-site rate or permission test.",
    }
