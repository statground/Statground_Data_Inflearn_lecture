# Inflearn sitemap crawler -> ClickHouse (Statground)
# Fix: use Python datetime objects for DateTime64(3, 'Asia/Seoul') columns (clickhouse-connect requirement)
#
# Required Secrets:
#   CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE
#
# Env:
#   SITEMAP_BASE, SITEMAP_PREFIX, BATCH_SIZE, REQUEST_SLEEP_MIN/MAX

import os, re, json, time, random, urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

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
    raw = os.environ.get(name, str(default))
    raw = str(raw).strip()
    m = re.search(r"\d+", raw)
    if not m:
        raise ValueError(
            f"{name} must be numeric. Got: {raw!r}. "
            f"Set GitHub Secret {name} to digits only (e.g., 40011)."
        )
    return int(m.group(0))

CH_HOST = _get_env("CH_HOST")
CH_PORT = _parse_int_env("CH_PORT", 8123)
CH_USER = _get_env("CH_USER", "default")
CH_PASSWORD = _get_env("CH_PASSWORD", "")
CH_DATABASE = _get_env("CH_DATABASE", "statground_lecture")

SITEMAP_BASE = _get_env("SITEMAP_BASE", "https://cdn.inflearn.com/sitemaps").rstrip("/")
SITEMAP_PREFIX = _get_env("SITEMAP_PREFIX", "sitemap-courseDetail-")

BATCH_SIZE = _parse_int_env("BATCH_SIZE", 800)  # 0 = try unlimited (not recommended)
SLEEP_MIN = float(_get_env("REQUEST_SLEEP_MIN", "0.6"))
SLEEP_MAX = float(_get_env("REQUEST_SLEEP_MAX", "1.3"))

def now_dt64():
    # DateTime64(3) -> millisecond precision
    t = datetime.now(tz=KST)
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)

def jitter_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20))
def http_get(url: str) -> requests.Response:
    r = SESSION.get(url, timeout=30)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"Retryable HTTP {r.status_code}")
    return r

def parse_sitemap_locs(xml_bytes: bytes) -> list[str]:
    root = etree.fromstring(xml_bytes)
    locs = root.xpath(".//sm:loc/text()", namespaces=NS)
    return [x.strip() for x in locs if x and x.strip()]

def extract_next_data(html: str) -> dict | None:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    return json.loads(m.group(1).strip())

def parse_course_id_and_locale(url: str) -> tuple[int | None, str]:
    u = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(u.query)
    cid = None
    if "cid" in qs and qs["cid"]:
        try:
            cid = int(qs["cid"][0])
        except:
            cid = None
    locale = "en" if u.path.startswith("/en/") else "ko"
    return cid, locale

def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

def ensure_checkpoint_table(ch):
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS {CH_DATABASE}.inflearn_crawl_checkpoint
    (
      source LowCardinality(String) COMMENT 'source name (inflearn_courseDetail)',
      updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'updated time (Asia/Seoul)',
      sitemap_index UInt32 COMMENT 'last processed sitemap index',
      url_index UInt32 COMMENT 'offset within the sitemap url list'
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (source)
    COMMENT 'Crawler checkpoint for Inflearn sitemap processing. OLAP only, SSOT 아님';
    """)

def get_checkpoint(ch) -> tuple[int, int]:
    sql = f"""
    SELECT
      argMax(sitemap_index, updated_at) AS sitemap_index,
      argMax(url_index, updated_at) AS url_index
    FROM {CH_DATABASE}.inflearn_crawl_checkpoint
    WHERE source = 'inflearn_courseDetail'
    """
    res = ch.query(sql).result_rows
    if not res or res[0][0] is None:
        return (0, 0)
    return (int(res[0][0]), int(res[0][1]))

def set_checkpoint(ch, sitemap_index: int, url_index: int):
    ch.insert(
        f"{CH_DATABASE}.inflearn_crawl_checkpoint",
        [[ "inflearn_courseDetail", now_dt64(), sitemap_index, url_index ]],
        column_names=["source","updated_at","sitemap_index","url_index"]
    )

def should_insert_snapshot(ch, course_id: int, locale: str, query_key: str, payload_hash: int) -> bool:
    sql = f"""
    SELECT argMax(payload_hash, fetched_at)
    FROM {CH_DATABASE}.inflearn_course_snapshot_raw
    WHERE course_id = %(course_id)s
      AND locale = %(locale)s
      AND query_key = %(query_key)s
    """
    prev = ch.query(sql, parameters={"course_id": course_id, "locale": locale, "query_key": query_key}).result_rows
    if not prev or prev[0][0] is None:
        return True
    return int(prev[0][0]) != int(payload_hash)

def main():
    ch = ch_client()
    ensure_checkpoint_table(ch)

    fetched_at = now_dt64()  # ✅ datetime
    start_sitemap, start_offset = get_checkpoint(ch)
    print(f"[checkpoint] start sitemap_index={start_sitemap}, url_index={start_offset}")

    processed = 0
    sitemap_index = start_sitemap
    offset = start_offset

    while True:
        sm_url = f"{SITEMAP_BASE}/{SITEMAP_PREFIX}{sitemap_index}.xml"
        r = http_get(sm_url)
        if r.status_code == 404:
            print(f"[done] sitemap {sitemap_index} -> 404. Completed all.")
            # reset checkpoint (optional)
            set_checkpoint(ch, 0, 0)
            break
        if r.status_code != 200:
            raise RuntimeError(f"Unexpected status {r.status_code} for sitemap {sm_url}")

        urls = parse_sitemap_locs(r.content)
        if not urls:
            print(f"[stop] sitemap {sitemap_index} has 0 urls")
            set_checkpoint(ch, sitemap_index, 0)
            break

        print(f"[sitemap] {sitemap_index} urls={len(urls)} (starting at offset={offset})")

        i = offset
        while i < len(urls):
            if BATCH_SIZE > 0 and processed >= BATCH_SIZE:
                print(f"[batch] reached BATCH_SIZE={BATCH_SIZE}, saving checkpoint sitemap={sitemap_index}, offset={i}")
                set_checkpoint(ch, sitemap_index, i)
                return

            url = urls[i]
            i += 1

            course_id, locale = parse_course_id_and_locale(url)
            if not course_id:
                continue

            page = http_get(url)
            if page.status_code != 200:
                continue

            nd = extract_next_data(page.text)
            if not nd:
                continue

            page_props = (nd.get("props") or {}).get("pageProps") or {}
            queries = ((page_props.get("dehydratedState") or {}).get("queries") or [])

            rows = []
            for q in queries:
                qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
                payload_obj = (q.get("state") or {}).get("data")
                if payload_obj is None:
                    continue

                payload = json.dumps(payload_obj, ensure_ascii=False)
                payload_hash = (hash(payload) & 0xFFFFFFFFFFFFFFFF)
                query_key_hash = (hash(qk) & 0xFFFFFFFFFFFFFFFF)

                if not should_insert_snapshot(ch, course_id, locale, qk, payload_hash):
                    continue

                rows.append([
                    str(uuid7()),
                    fetched_at,  # ✅ datetime for DateTime64
                    course_id,
                    locale,
                    url,
                    qk,
                    query_key_hash,
                    "OK",
                    None,
                    payload,
                    payload_hash,
                ])

            if rows:
                ch.insert(
                    f"{CH_DATABASE}.inflearn_course_snapshot_raw",
                    rows,
                    column_names=[
                        "uuid","fetched_at","course_id","locale","source_url",
                        "query_key","query_key_hash","status_code","error_code",
                        "payload","payload_hash"
                    ]
                )

            processed += 1
            if processed % 50 == 0:
                print(f"[progress] processed={processed} last_course_id={course_id} sitemap={sitemap_index} offset={i}")

            jitter_sleep()

        sitemap_index += 1
        offset = 0
        set_checkpoint(ch, sitemap_index, 0)
        jitter_sleep()

if __name__ == "__main__":
    main()
