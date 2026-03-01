# -*- coding: utf-8 -*-
"""
Inflearn crawler utilities for Statground (ClickHouse).

- OLAP only: ClickHouse is NOT SSOT.
- DateTime64 is inserted as timezone-aware datetime (Asia/Seoul).
- UUID 정책: 스냅샷 row는 ETL에서 UUID v7 생성.

This module is shared by:
- inflearn_collect_new.py (신규 강의 수집: sitemap checkpoint 기반)
- inflearn_update_existing.py (기존 강의 갱신: course_dim 기반 라운드로빈)
- inflearn_stats.py (통계 리포트)
- inflearn_optimize.py (ReplacingMergeTree FINAL merge)

"""

import os, re, json, time, random, hashlib, struct
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple, Optional

import requests
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential
import clickhouse_connect
from uuid6 import uuid7

UA = "Mozilla/5.0 (compatible; StatgroundCrawler/1.0; +https://www.statground.net)"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
KST = ZoneInfo("Asia/Seoul")

def _get_env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env: {name}")
    return str(v).strip()

def _parse_int_env(name: str, default: int) -> int:
    raw = str(os.environ.get(name, str(default))).strip()
    m = re.search(r"\d+", raw)
    if not m:
        raise ValueError(f"{name} must be numeric. Got: {raw!r}.")
    return int(m.group(0))

def now_dt64() -> datetime:
    t = datetime.now(tz=KST)
    # keep milliseconds
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)

def parse_dt64(s: str | None):
    if not s:
        return None
    s = str(s).strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST, microsecond=0)
    except Exception:
        return None

def jitter_sleep(min_s: float, max_s: float):
    if max_s <= 0:
        return
    time.sleep(random.uniform(max(0.0, min_s), max_s))

def h64(text: str) -> int:
    d = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    return struct.unpack("<Q", d)[0]

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30))
def http_get(url: str):
    r = SESSION.get(url, timeout=30)
    return r

def parse_sitemap_locs(xml_bytes: bytes) -> List[str]:
    root = etree.fromstring(xml_bytes)
    locs = root.xpath("//sm:url/sm:loc/text()", namespaces=NS)
    return [str(x).strip() for x in locs if x and str(x).strip()]

def parse_course_id_and_locale(url: str) -> Tuple[int, str]:
    # https://www.inflearn.com/course/<slug>?locale=ko
    # https://www.inflearn.com/course/<slug>?locale=en
    # course id는 페이지 HTML 내부의 next-data에서 추출하되, url에서 locale만 캐치
    try:
        m = re.search(r"[?&]locale=([a-zA-Z_-]+)", url)
        locale = (m.group(1) if m else "ko").lower()
        # course_id는 HTML에서 추출될 때 보장되지만, 여기서는 0 반환
        return 0, locale
    except Exception:
        return 0, "ko"

def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def find_api_data(queries: List[Dict[str, Any]], needle: str):
    for q in queries:
        try:
            qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
            if needle in qk:
                return (q.get("state") or {}).get("data")
        except Exception:
            continue
    return None

def to_u8(x) -> int:
    return 1 if x else 0

def ch_client():
    CH_HOST = _get_env("CH_HOST")
    CH_PORT = _parse_int_env("CH_PORT", 40011)
    CH_USER = _get_env("CH_USER", "default")
    CH_PASSWORD = _get_env("CH_PASSWORD", "")
    CH_DATABASE = _get_env("CH_DATABASE", "default")
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
    )

def get_db() -> str:
    return _get_env("CH_DATABASE", "default")

def ensure_checkpoints(ch):
    # table 존재 확인 (없으면 쿼리 실패). 여기서는 별도 생성 안함.
    pass

def get_checkpoint(ch, source: str) -> Tuple[int, int]:
    db = get_db()
    sql = f"""
    SELECT argMax(sitemap_index, updated_at), argMax(url_index, updated_at)
    FROM {db}.inflearn_crawl_checkpoint
    WHERE source = %(source)s
    """
    rows = ch.query(sql, parameters={"source": source}).result_rows
    if not rows or rows[0][0] is None:
        return (0, 0)
    return int(rows[0][0]), int(rows[0][1])

def set_checkpoint(ch, source: str, sitemap_index: int, url_index: int):
    db = get_db()
    ch.insert(
        f"{db}.inflearn_crawl_checkpoint",
        [[source, now_dt64(), int(sitemap_index), int(url_index)]],
        column_names=["source","updated_at","sitemap_index","url_index"],
    )

def latest_metric_tuple(ch, course_id: int, locale: str):
    db = get_db()
    sql = f"""
    SELECT
      argMax(student_count, fetched_at),
      argMax(like_count, fetched_at),
      argMax(review_count, fetched_at),
      argMax(average_star, fetched_at),
      argMax(krw_regular_price, fetched_at),
      argMax(krw_pay_price, fetched_at),
      argMax(discount_rate, fetched_at),
      argMax(discount_title, fetched_at),
      argMax(discount_ended_at, fetched_at)
    FROM {db}.inflearn_course_metric_fact
    WHERE course_id = %(course_id)s AND locale = %(locale)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale}).result_rows
    return tuple(rows[0]) if rows and rows[0] else None

def latest_price_tuple(ch, course_id: int, locale: str):
    db = get_db()
    sql = f"""
    SELECT
      argMax(regular_price, fetched_at),
      argMax(pay_price, fetched_at),
      argMax(discount_rate, fetched_at),
      argMax(discount_title, fetched_at),
      argMax(discount_ended_at, fetched_at),
      argMax(krw_regular_price, fetched_at),
      argMax(krw_pay_price, fetched_at)
    FROM {db}.inflearn_course_price_fact
    WHERE course_id = %(course_id)s AND locale = %(locale)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale}).result_rows
    return tuple(rows[0]) if rows and rows[0] else None

def should_insert_snapshot(ch, course_id: int, locale: str, qk_hash: int, payload_hash: int) -> bool:
    db = get_db()
    sql = f"""
    SELECT argMax(payload_hash, fetched_at)
    FROM {db}.inflearn_course_snapshot_raw
    WHERE course_id = %(course_id)s AND locale = %(locale)s AND query_key_hash = %(qkh)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale, "qkh": qk_hash}).result_rows
    if not rows or rows[0][0] is None:
        return True
    return int(rows[0][0]) != int(payload_hash)

def parse_course_id_from_next_data(nd: Dict[str, Any]) -> int:
    # pageProps.dehydratedState.queries -> queryKey includes /client/api/v1/course/{id}/online/info
    try:
        page_props = (nd.get("props") or {}).get("pageProps") or {}
        queries = ((page_props.get("dehydratedState") or {}).get("queries") or [])
        for q in queries:
            qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
            m = re.search(r"/client/api/v1/course/(\d+)/online/info", qk)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return 0

def parse_locale_from_url(url: str) -> str:
    m = re.search(r"[?&]locale=([a-zA-Z_-]+)", url)
    return (m.group(1) if m else "ko").lower()

def insert_rows(ch, table: str, rows: list, cols: list[str]) -> int:
    if not rows:
        return 0
    db = get_db()
    ch.insert(f"{db}.{table}", rows, column_names=cols)
    return len(rows)
