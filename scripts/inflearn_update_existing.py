#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Batch #2: 이미 수집된 강의 정보 업데이트

- inflearn_course_snapshot_raw에서 최신 fetched_at이 오래된 강의부터 우선 갱신
- 1회 배치당 UPDATE_BATCH_SIZE(기본 100)개만 처리
- HTTP/파싱은 ThreadPoolExecutor로 병렬화(WORKERS)
- ClickHouse insert는 메인 스레드에서 일괄 수행
- 진행 상황은 inflearn_crawl_checkpoint에 source='inflearn_update_existing'로 기록
  (sitemap_index=누적 처리 건수, url_index=최근 batch 처리 건수)

주의:
- DB 접속정보(호스트/포트/DB명) 등은 로그에 출력하지 않음
"""

import os, sys, pathlib
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import inflearn_crawl  # noqa: E402

KST = ZoneInfo("Asia/Seoul")


def _get_int(name: str, default: int) -> int:
    v = (os.environ.get(name) or "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default


UPDATE_BATCH_SIZE = _get_int("UPDATE_BATCH_SIZE", 100)
WORKERS = _get_int("WORKERS", 8)


def ch():
    return inflearn_crawl.ch_client()


def pick_urls(client, limit: int):
    db = inflearn_crawl.CH_DATABASE
    # 최신 스냅샷이 오래된 강의부터 limit개 선택
    sql = f"""
    WITH latest AS (
      SELECT
        course_id,
        locale,
        argMax(source_url, fetched_at) AS source_url,
        max(fetched_at) AS last_fetched_at
      FROM {db}.inflearn_course_snapshot_raw
      WHERE status_code = 'OK'
      GROUP BY course_id, locale
    )
    SELECT course_id, locale, source_url, last_fetched_at
    FROM latest
    ORDER BY last_fetched_at ASC
    LIMIT %(lim)s
    """
    res = client.query(sql, parameters={"lim": limit})
    return res.result_rows


def get_progress(client):
    db = inflearn_crawl.CH_DATABASE
    sql = f"""
    SELECT
      ifNull(toInt64(argMax(sitemap_index, updated_at)), 0) AS total_done,
      ifNull(toInt64(argMax(url_index, updated_at)), 0) AS last_batch_done
    FROM {db}.inflearn_crawl_checkpoint
    WHERE source = 'inflearn_update_existing'
    """
    rows = client.query(sql).result_rows
    if not rows:
        return 0, 0
    return int(rows[0][0] or 0), int(rows[0][1] or 0)


def set_progress(client, total_done: int, last_batch_done: int):
    db = inflearn_crawl.CH_DATABASE
    client.insert(
        f"{db}.inflearn_crawl_checkpoint",
        [["inflearn_update_existing", inflearn_crawl.now_dt64(), int(total_done), int(last_batch_done)]],
        column_names=["source", "updated_at", "sitemap_index", "url_index"],
    )


def insert_if_rows(client, table: str, rows, cols):
    if not rows:
        return 0
    if table == "inflearn_course_snapshot_raw":
        return inflearn_crawl.insert_snapshot_rows_json_each_row(rows)
    client.insert(f"{inflearn_crawl.CH_DATABASE}.{table}", rows, column_names=cols)
    return len(rows)


def main():
    client = ch()
    fetched_at = inflearn_crawl.now_dt64()

    total_done, _ = get_progress(client)

    picked = pick_urls(client, UPDATE_BATCH_SIZE)
    print(f"[update] picked={len(picked)} (UPDATE_BATCH_SIZE={UPDATE_BATCH_SIZE})")

    if not picked:
        set_progress(client, total_done, 0)
        print("[update] nothing to do")
        return

    urls = []
    for (course_id, locale, url, last_fetched_at) in picked:
        if url:
            urls.append(str(url))

    # 병렬로 URL 수집/파싱
    results = []
    failed = 0
    with ThreadPoolExecutor(max_workers=max(1, WORKERS)) as ex:
        fut_to_url = {ex.submit(inflearn_crawl.process_course_url, u, fetched_at): u for u in urls}
        for fut in as_completed(fut_to_url):
            url = fut_to_url[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                failed += 1
                print(f"[warn] process_course_url failed url={url} error={type(e).__name__}: {e}")

    # 결과 합치기
    snap_rows, dim_rows, metric_rows, price_rows, curri_rows, inst_rows, map_rows = ([] for _ in range(7))
    for r in results:
        snap_rows.extend(r.get("snapshot_raw") or [])
        dim_rows.extend(r.get("course_dim") or [])
        metric_rows.extend(r.get("metric_fact") or [])
        price_rows.extend(r.get("price_fact") or [])
        curri_rows.extend(r.get("curriculum_unit") or [])
        inst_rows.extend(r.get("instructor_dim") or [])
        map_rows.extend(r.get("course_instructor_map") or [])

    inserted = {
        "snapshot_raw": insert_if_rows(client, "inflearn_course_snapshot_raw", snap_rows,
            ["uuid","fetched_at","course_id","locale","source_url","query_key","query_key_hash","status_code","error_code","payload","payload_hash"]),
        "course_dim": insert_if_rows(client, "inflearn_course_dim", dim_rows,
            ["course_id","locale","fetched_at","slug","en_slug","status","title","description","thumbnail_url",
             "category_main_title","category_main_slug","category_sub_title","category_sub_slug",
             "level_code","is_new","is_best","student_count","like_count","review_count","average_star",
             "lecture_unit_count","preview_unit_count","runtime_sec",
             "provides_certificate","provides_instructor_answer","provides_inquiry",
             "published_at","last_updated_at","keywords","category_slugs","skill_slugs","common_tag_slugs"]),
        "metric_fact": insert_if_rows(client, "inflearn_course_metric_fact", metric_rows,
            ["fetched_at","course_id","locale","student_count","like_count","review_count","average_star",
             "krw_regular_price","krw_pay_price","discount_rate","discount_title","discount_ended_at","metric_hash"]),
        "price_fact": insert_if_rows(client, "inflearn_course_price_fact", price_rows,
            ["fetched_at","course_id","locale","regular_price","pay_price","discount_rate","discount_title","discount_ended_at",
             "krw_regular_price","krw_pay_price","price_hash"]),
        "curriculum_unit": insert_if_rows(client, "inflearn_course_curriculum_unit", curri_rows,
            ["fetched_at","course_id","locale","section_id","section_title","unit_id","unit_title","unit_type",
             "runtime_sec","is_preview","has_video","has_attachment","quiz_id","reading_time","is_challenge_only","unit_hash"]),
        "instructor_dim": insert_if_rows(client, "inflearn_instructor_dim", inst_rows,
            ["instructor_id","fetched_at","name","slug","thumbnail_url","course_count","student_count","review_count","total_star","answer_count","introduce_html"]),
        "course_instructor_map": insert_if_rows(client, "inflearn_course_instructor_map", map_rows,
            ["fetched_at","course_id","instructor_id","role"]),
    }

    # 진행상황 기록
    total_done += len(urls)
    set_progress(client, total_done, len(urls))

    if failed:
        print(f"[warn] failed_urls={failed}")
    print("[inserted]", inserted)
    print("[done]")


if __name__ == "__main__":
    main()
