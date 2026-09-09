from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
import time
from pathlib import Path

from .config import ConfigError, load_config
from .crawler import CrawlResult, Crawler, continue_until_terminal, robots_allows, robots_url
from .errors import ScraperError
from .exporters import export_csv, export_json
from .http_client import HttpClient, RequestBudget
from .parsers import ADAPTER_VERIFIED_AT, parse_list_page
from .storage import Storage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m novel_scraper", description="로컬 소설 뷰어와 공개 소설 메타데이터 수집기")
    parser.add_argument("--config", default="config.toml")
    commands = parser.add_subparsers(dest="command", required=True)
    doctor = commands.add_parser("doctor", help="설정, 접근, robots, 파서 상태 진단")
    doctor.add_argument("--offline", action="store_true")
    for name in ("crawl", "update"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--limit", type=int)
        cmd.add_argument("--max-requests", type=int)
        cmd.add_argument("--continuous", action="store_true", help="요청 예산 부분 완료를 자동 재개하여 끝날 때까지 진행")
        cmd.add_argument("--batch-delay", type=float, help="자동 재개 배치 사이 대기 초. 기본은 요청 최소 간격")
        cmd.add_argument("--workers", type=int, help="목록/상세 메타데이터 병렬 작업 수(1~8). 기본은 crawl.concurrency")
        cmd.add_argument("--body-workers", type=int, help="연속 실행 후 본문 병렬 작업 수(1~8). 기본은 crawl.body_concurrency")
        cmd.add_argument("--browser-workers", type=int, help="JavaScript 본문 렌더링용 Chrome 수(1~4). 기본은 crawl.browser_concurrency")
        if name == "update":
            cmd.add_argument("--full", action="store_true", help="겹침 페이지 제한을 해제하되 작품/요청 상한은 유지")
    resume = commands.add_parser("resume")
    resume.add_argument("--max-requests", type=int)
    resume.add_argument("--limit", type=int, help="기존 실행의 작품 한도를 이 값까지 확대")
    resume.add_argument("--continuous", action="store_true", help="요청 예산 부분 완료를 자동 재개하여 끝날 때까지 진행")
    resume.add_argument("--batch-delay", type=float, help="자동 재개 배치 사이 대기 초. 기본은 요청 최소 간격")
    resume.add_argument("--workers", type=int, help="목록/상세 메타데이터 병렬 작업 수(1~8). 기본은 crawl.concurrency")
    resume.add_argument("--body-workers", type=int, help="연속 실행 후 본문 병렬 작업 수(1~8). 기본은 crawl.body_concurrency")
    resume.add_argument("--browser-workers", type=int, help="JavaScript 본문 렌더링용 Chrome 수(1~4). 기본은 crawl.browser_concurrency")
    benchmark = commands.add_parser("benchmark", help="네트워크 없이 병렬 HTTP·파서 처리량 비교")
    benchmark.add_argument("--workers", default="1,2,4", help="쉼표로 구분한 병렬도(각 1~8)")
    benchmark.add_argument("--requests", type=int, default=24, help="각 병렬도에서 처리할 합성 상세 페이지 수")
    benchmark.add_argument("--latency-ms", type=float, default=100, help="합성 응답 1회의 지연 시간")
    export = commands.add_parser("export")
    export.add_argument("--format", choices=("csv", "json"), required=True)
    export.add_argument("--output-dir", type=Path, required=True)
    export.add_argument("--gzip", action="store_true", help="JSON을 전송용 novels.json.gz로 압축")
    export.add_argument("--compact-json", action="store_true", help="JSON의 공백/들여쓰기를 제거")
    export.add_argument("--filename", help="JSON 출력 파일 이름. 기본은 novels.json 또는 novels.json.gz")
    commands.add_parser("compact", help="레코드를 유지하면서 SQLite의 미사용 공간 회수")
    reader = commands.add_parser("serve", help="광고 없는 로컬 소설 뷰어 실행")
    reader.add_argument("--port", type=int, default=8787)
    reader.add_argument("--online", action="store_true", help="선택한 공개 페이지를 원본에서 불러오기 (인증·유료 제한 우회 없음)")
    reader.add_argument("--open", action="store_true", help="실행 후 기본 브라우저에서 서재 열기")
    pc = commands.add_parser("pc-collect", help="PC에 본문을 txt.gz로 영구 저장하고 실제 용량을 측정")
    pc.add_argument("--output-dir", type=Path, default=Path("data/pc_archive"))
    pc.add_argument("--work-id", action="append", default=[], help="특정 작품 번호. 여러 번 지정 가능")
    pc.add_argument("--works", type=int, default=3, help="work-id를 생략했을 때 목록에서 표본으로 고를 작품 수")
    pc.add_argument("--chapters-per-work", type=int, default=5, help="작품당 고르게 표본 수집할 회차 수")
    pc.add_argument("--all-chapters", action="store_true", help="선택 작품의 모든 회차 저장")
    pc.add_argument("--project-works", type=int, default=13000, help="용량 예측에 사용할 전체 작품 수")
    pc.add_argument("--chapter-delay", type=float, help="본문 회차 저장 후 추가 대기 초. 기본은 http.min_interval_seconds")
    pc.add_argument("--max-errors", type=int, default=5)
    pc.add_argument("--max-requests", type=int, help="실행 전체 상위 페이지 요청 상한. 기본은 crawl.max_requests")
    pc.add_argument("--workers", type=int, help="상세/본문 HTTP 병렬 작업 수(1~8). 기본은 crawl.body_concurrency")
    pc.add_argument("--browser-workers", type=int, help="JavaScript 본문 렌더링용 Chrome 수(1~4). 기본은 crawl.browser_concurrency")
    pc.add_argument("--refresh", action="store_true", help="이미 저장한 표본도 원본에서 다시 불러와 교체")
    status = commands.add_parser("pc-status", help="PC 영구 보관소의 현재 작품/회차/용량 통계")
    status.add_argument("--output-dir", type=Path, default=Path("data/pc_archive"))
    return parser


def configure_logging(config) -> None:
    config.logging.path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, config.logging.level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(config.logging.path, encoding="utf-8"), logging.StreamHandler()],
    )


def doctor(config, offline: bool) -> int:
    report = {
        "config": "ok",
        "python": sys.version.split()[0],
        "database_parent_writable": None,
        "adapter_verified_at": ADAPTER_VERIFIED_AT,
        "adapter_live_verified": False,
        "network": "skipped" if offline else "pending",
        "robots": "unconfirmed" if offline else "pending",
        "parser": "local-ready" if offline else "pending",
        "detail_workers": config.crawl.concurrency,
        "terms": "미확인 (확인한 페이지에서 이용조건 링크를 식별하지 못함)",
        "collection_permission_confirmed": config.site.permission_confirmed,
    }
    config.storage.database_path.parent.mkdir(parents=True, exist_ok=True)
    report["database_parent_writable"] = config.storage.database_path.parent.exists()
    with Storage(config.storage.database_path):
        pass
    if not offline:
        budget = RequestBudget(min(5, config.crawl.max_requests))
        try:
            with HttpClient(config.http, config.site.allowed_hosts, budget) as client:
                robots_response = client.get(robots_url(config.site.start_url))
                allowed = robots_allows(robots_response.text, config.site.start_url, config.http.user_agent)
                report["robots"] = {"status": robots_response.status_code, "start_url_allowed": allowed}
                if not allowed:
                    report["network"] = "stopped"
                    print(json.dumps(report, ensure_ascii=False, indent=2))
                    return 2
                response = client.get(config.site.start_url)
                page = parse_list_page(
                    response.content,
                    str(response.url),
                    config.site.allowed_hosts,
                    config.site.allowed_asset_hosts,
                    pagination_url=config.site.start_url,
                )
                report["network"] = {"status": response.status_code, "final_url": str(response.url), "requests": budget.used}
                report["parser"] = {"status": "ok", "works_on_page": len(page.works), "next_url": page.next_url}
                report["adapter_live_verified"] = True
        except ScraperError as exc:
            report["network"] = {"status": "failed", "reason": exc.reason, "message": str(exc), "requests": budget.used}
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        configure_logging(config)
        if args.command == "doctor":
            return doctor(config, args.offline)
        if args.command == "benchmark":
            from .benchmark import parse_workers, run_benchmark
            logging.getLogger("httpx").setLevel(logging.WARNING)
            report = run_benchmark(parse_workers(args.workers), args.requests, args.latency_ms)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0
        if args.command == "serve":
            from .reader import serve
            return serve(config, online=args.online, port=args.port, open_browser=args.open)
        if args.command in {"pc-collect", "pc-status"}:
            from .pc_collector import collect_pc, pc_status
            if args.command == "pc-status":
                print(json.dumps(pc_status(config, args.output_dir), ensure_ascii=False, indent=2))
                return 0
            report = collect_pc(
                config,
                output_dir=args.output_dir,
                work_ids=args.work_id,
                works=args.works,
                chapters_per_work=args.chapters_per_work,
                all_chapters=args.all_chapters,
                refresh=args.refresh,
                project_works=args.project_works,
                chapter_delay=args.chapter_delay,
                max_errors=args.max_errors,
                max_requests=args.max_requests if args.max_requests is not None else config.crawl.max_requests,
                workers=args.workers if args.workers is not None else config.crawl.body_concurrency,
                browser_workers=args.browser_workers if args.browser_workers is not None else config.crawl.browser_concurrency,
            )
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 2 if report["status"] == "failed" else 0
        with Storage(config.storage.database_path) as storage:
            if args.command == "compact":
                print(json.dumps(storage.compact(), ensure_ascii=False, indent=2))
                return 0
            if args.command == "export":
                if args.format == "csv":
                    if args.gzip or args.compact_json or args.filename:
                        raise ConfigError("--gzip, --compact-json, --filename은 JSON 내보내기에서만 사용할 수 있습니다")
                    paths = export_csv(storage, args.output_dir, bom=config.export.csv_utf8_bom, protect_formulas=config.export.protect_csv_formulas)
                else:
                    paths = (export_json(storage, args.output_dir, gzip_output=args.gzip, pretty=not args.compact_json, filename=args.filename),)
                print(json.dumps({"exported": [str(path.resolve()) for path in paths]}, ensure_ascii=False))
                return 0
            max_requests = args.max_requests if args.max_requests is not None else config.crawl.max_requests
            workers = args.workers if args.workers is not None else config.crawl.concurrency
            crawler = Crawler(config, storage, workers=workers)
            if args.command == "resume":
                source = storage.latest_resumable_run()
                recover_list = False
                result = None
                if source is None and args.limit is not None:
                    candidate = storage.latest_expandable_completed_run(args.limit)
                    if candidate is not None:
                        last_list = storage.latest_completed_list_task()
                        try:
                            checkpoint = json.loads(last_list["checkpoint_json"] or "{}") if last_list is not None else {}
                        except (TypeError, ValueError):
                            checkpoint = {}
                        if checkpoint.get("last_page_confirmed"):
                            works = int(storage.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0])
                            details = int(storage.conn.execute("SELECT COUNT(DISTINCT work_id) FROM chapters").fetchone()[0])
                            result = CrawlResult(
                                run_id=int(candidate["id"]),
                                status="completed",
                                requests_used=0,
                                works=works,
                                details=details,
                                pending=0,
                                failed=0,
                                reason=None,
                                workers=workers,
                            )
                        else:
                            source = candidate
                            recover_list = True
                if source is None and result is None:
                    print("재개할 미완료 작업이 없습니다.")
                    return 1
                if result is None:
                    resume_limit = args.limit if args.limit is not None else int(source["max_works"])
                    result = crawler.start(
                        "resume",
                        resume_limit,
                        max_requests,
                        resume_source=int(source["id"]),
                        recover_list=recover_list,
                    )
            else:
                limit = args.limit if args.limit is not None else config.crawl.max_works
                mode = "update-full" if args.command == "update" and args.full else args.command
                result = crawler.start(mode, limit, max_requests)
            print(json.dumps(dataclasses.asdict(result), ensure_ascii=False, indent=2))
            if args.continuous:
                delay = args.batch_delay if args.batch_delay is not None else config.http.min_interval_seconds
                result = continue_until_terminal(
                    crawler,
                    result,
                    max_requests,
                    pause_seconds=delay,
                    on_batch=lambda batch: print(json.dumps(dataclasses.asdict(batch), ensure_ascii=False, indent=2), flush=True),
                )
                if result.status == "completed":
                    from .pc_collector import ArchiveStore, PcCollector, resolve_archive_root

                    archive = ArchiveStore(resolve_archive_root(config, Path("data/pc_archive")))
                    body_workers = args.body_workers if args.body_workers is not None else config.crawl.body_concurrency
                    browser_workers = args.browser_workers if args.browser_workers is not None else config.crawl.browser_concurrency
                    if browser_workers > body_workers:
                        raise ConfigError("--browser-workers는 --body-workers보다 클 수 없습니다")
                    while True:
                        collector = PcCollector(
                            config,
                            archive,
                            chapter_delay=config.http.min_interval_seconds,
                            max_requests=max_requests,
                            workers=body_workers,
                            browser_workers=browser_workers,
                        )
                        try:
                            body_result = collector.collect_from_metadata(storage)
                        finally:
                            collector.close()
                        print(json.dumps({"body_archive": body_result}, ensure_ascii=False, indent=2), flush=True)
                        if body_result["status"] == "completed":
                            break
                        if body_result["reason"] not in {"request_budget_exceeded", "pending_bodies"}:
                            return 2
                        if body_result["pending"] > 0 and body_result["fetched_this_batch"] == 0 and body_result["reason"] == "pending_bodies":
                            return 2
                        if delay:
                            time.sleep(delay)
            return 0 if result.status == "completed" else 2
    except (ConfigError, ScraperError, OSError, ValueError) as exc:
        print(f"오류: {exc}", file=sys.stderr)
        return 2
