# Inflearn sitemap crawler -> ClickHouse (Statground) [Improved Schema]
# Inserts: snapshot_raw + course_dim + metric_fact + price_fact + curriculum_unit + instructor_dim + course_instructor_map
# Uses stable 64-bit hashes (blake2b) and change-only inserts for metric/price.
# DateTime64 inserted as datetime objects.

import os, re, json, time, random, urllib.parse, hashlib, struct
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential
import clickhouse_connect
from uuid6 import uuid7

UA = "Mozilla/5.0 (compatible; StatgroundCrawler/1.0; +https://www.statground.net)"

# Thread-safe HTTP session (one session per thread)
import threading
_TLS = threading.local()

def get_session() -> "requests.Session":
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        # NOTE:
        # - Do not force `br` here. GitHub Actions runners may receive Brotli-compressed
        #   payloads for sitemap/page requests, but requests/urllib3 will not always decode
        #   them unless an extra Brotli dependency is installed.
        # - Let requests negotiate a safe Accept-Encoding by itself.
        s.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        })
        _TLS.session = s
    return s
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
        raise ValueError(f"{name} must be numeric. Got: {raw!r}. Set GitHub Secret {name} to digits only (e.g., 40011).")
    return int(m.group(0))

def now_dt64():
    t = datetime.now(tz=KST)
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
    time.sleep(random.uniform(min_s, max_s))

def h64(text: str) -> int:
    d = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    return struct.unpack("<Q", d)[0]

def _get_env_any(names: list[str], default: str | None = None) -> str:
    """Return first non-empty env among names.
    Supports both CH_* and CLICKHOUSE_* naming.
    """
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    if default is None:
        raise RuntimeError("Missing required ClickHouse connection environment variables.")
    return str(default).strip()

def _parse_port_any(names: list[str], default: int = 8123) -> int:
    raw = _get_env_any(names, str(default))
    m = re.search(r"\d+", str(raw))
    if not m:
        return default
    return int(m.group(0))

CH_HOST = _get_env_any(["CH_HOST", "CLICKHOUSE_HOST"])
CH_PORT = _parse_port_any(["CH_PORT", "CLICKHOUSE_PORT"], 8123)
CH_USER = _get_env_any(["CH_USER", "CLICKHOUSE_USER"], "default")
CH_PASSWORD = _get_env_any(["CH_PASSWORD", "CLICKHOUSE_PASSWORD"], "")
CH_DATABASE = _get_env_any(["CH_DATABASE", "CLICKHOUSE_DATABASE"], "statground_lecture")
CH_SECURE = _get_env_any(["CH_SECURE", "CLICKHOUSE_SECURE"], "0").lower() in {"1", "true", "yes", "y", "on"}

SITEMAP_BASE = _get_env("SITEMAP_BASE", "https://cdn.inflearn.com/sitemaps").rstrip("/")
SITEMAP_BASE_FALLBACK = _get_env("SITEMAP_BASE_FALLBACK", "https://www.inflearn.com/sitemaps").rstrip("/")
SITEMAP_PREFIX = _get_env("SITEMAP_PREFIX", "sitemap-courseDetail-")

BATCH_SIZE = _parse_int_env("BATCH_SIZE", 600)
SLEEP_MIN = float(_get_env("REQUEST_SLEEP_MIN", "0.6"))
SLEEP_MAX = float(_get_env("REQUEST_SLEEP_MAX", "1.3"))

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20))
def http_get(url: str) -> requests.Response:
    r = get_session().get(url, timeout=30)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"Retryable HTTP {r.status_code}")
    return r

def _preview_bytes(data: bytes, limit: int = 160) -> str:
    return data[:limit].decode("utf-8", errors="replace").replace("\n", " ").replace("\r", " ")


def parse_sitemap_locs(xml_bytes: bytes) -> list[str]:
    raw = xml_bytes or b""
    if raw[:2] == b"\x1f\x8b":
        import gzip
        raw = gzip.decompress(raw)
    raw = raw.lstrip(b"\xef\xbb\xbf\r\n\t ")
    if not raw:
        return []

    parser = etree.XMLParser(recover=True, resolve_entities=False, no_network=True)
    try:
        root = etree.fromstring(raw, parser=parser)
    except etree.XMLSyntaxError as e:
        snippet = _preview_bytes(raw)
        raise ValueError(f"Invalid sitemap XML: {e}. snippet={snippet!r}") from e

    locs = root.xpath(".//sm:loc/text()", namespaces=NS)
    if not locs:
        locs = root.xpath(".//*[local-name()='loc']/text()")
    return [x.strip() for x in locs if x and x.strip()]


def extract_next_data(html: str) -> dict | None:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    return json.loads(m.group(1).strip()) if m else None


def parse_course_id_from_next_data(nd: dict | None) -> int | None:
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
    return None


def parse_locale_from_url(url: str) -> str:
    u = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(u.query)
    locale = ((qs.get("locale") or [None])[0] or "").strip().lower()
    if locale:
        return locale
    return "en" if u.path.startswith("/en/") else "ko"


def parse_course_id_and_locale(url: str) -> tuple[int | None, str]:
    u = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(u.query)
    cid = None
    for key in ("cid", "courseId", "course_id"):
        if key in qs and qs[key]:
            try:
                cid = int(qs[key][0])
                break
            except Exception:
                cid = None
    locale = parse_locale_from_url(url)
    return cid, locale

def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=CH_SECURE,
    )


def _dt64_text(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    dt = dt.astimezone(KST)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def insert_snapshot_rows_json_each_row(rows: list[list]):
    """Insert snapshot_raw rows via JSONEachRow over HTTP.

    Why this path exists:
    - inflearn_course_snapshot_raw.payload is a ClickHouse JSON column.
    - clickhouse-connect 0.7.x predates the new JSON type support and can fail with
      `Invalid version for Object structure serialization` on Native inserts.
    - JSONEachRow is a stable text-format path for JSON columns.
    """
    if not rows:
        return 0

    scheme = "https" if CH_SECURE else "http"
    url = f"{scheme}://{CH_HOST}:{CH_PORT}/"
    query = (
        f"INSERT INTO {CH_DATABASE}.inflearn_course_snapshot_raw "
        f"(uuid, fetched_at, course_id, locale, source_url, query_key, query_key_hash, "
        f"status_code, error_code, payload, payload_hash) FORMAT JSONEachRow"
    )

    body_lines = []
    for row in rows:
        payload_value = json.loads(row[9]) if isinstance(row[9], str) else row[9]
        line = {
            "uuid": row[0],
            "fetched_at": _dt64_text(row[1]),
            "course_id": row[2],
            "locale": row[3],
            "source_url": row[4],
            "query_key": row[5],
            "query_key_hash": row[6],
            "status_code": row[7],
            "error_code": row[8],
            "payload": payload_value,
            "payload_hash": row[10],
        }
        body_lines.append(json.dumps(line, ensure_ascii=False, separators=(",", ":"), default=str))

    resp = requests.post(
        url,
        params={"query": query},
        data=("\n".join(body_lines) + "\n").encode("utf-8"),
        auth=(CH_USER, CH_PASSWORD),
        headers={"Content-Type": "application/x-ndjson; charset=utf-8"},
        timeout=120,
    )
    if resp.status_code != 200:
        snippet = resp.text[:500].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(
            f"snapshot_raw JSONEachRow insert failed: HTTP {resp.status_code}. response={snippet!r}"
        )
    return len(rows)

def get_checkpoint(ch) -> tuple[int, int]:
    sql = f"""
    SELECT argMax(sitemap_index, updated_at), argMax(url_index, updated_at)
    FROM {CH_DATABASE}.inflearn_crawl_checkpoint
    WHERE source = 'inflearn_courseDetail'
    """
    rows = ch.query(sql).result_rows
    if not rows or rows[0][0] is None:
        return (0, 0)
    return int(rows[0][0]), int(rows[0][1])

def set_checkpoint(ch, sitemap_index: int, url_index: int):
    ch.insert(
        f"{CH_DATABASE}.inflearn_crawl_checkpoint",
        [["inflearn_courseDetail", now_dt64(), sitemap_index, url_index]],
        column_names=["source","updated_at","sitemap_index","url_index"],
    )

def latest_metric_tuple(ch, course_id: int, locale: str):
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
    FROM {CH_DATABASE}.inflearn_course_metric_fact
    WHERE course_id = %(course_id)s AND locale = %(locale)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale}).result_rows
    return tuple(rows[0]) if rows and rows[0] else None

def latest_price_tuple(ch, course_id: int, locale: str):
    sql = f"""
    SELECT
      argMax(regular_price, fetched_at),
      argMax(pay_price, fetched_at),
      argMax(discount_rate, fetched_at),
      argMax(discount_title, fetched_at),
      argMax(discount_ended_at, fetched_at),
      argMax(krw_regular_price, fetched_at),
      argMax(krw_pay_price, fetched_at)
    FROM {CH_DATABASE}.inflearn_course_price_fact
    WHERE course_id = %(course_id)s AND locale = %(locale)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale}).result_rows
    return tuple(rows[0]) if rows and rows[0] else None

def should_insert_snapshot(ch, course_id: int, locale: str, qk_hash: int, payload_hash: int) -> bool:
    sql = f"""
    SELECT argMax(payload_hash, fetched_at)
    FROM {CH_DATABASE}.inflearn_course_snapshot_raw
    WHERE course_id = %(course_id)s AND locale = %(locale)s AND query_key_hash = %(qkh)s
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale, "qkh": qk_hash}).result_rows
    if not rows or rows[0][0] is None:
        return True
    return int(rows[0][0]) != int(payload_hash)


def course_exists(ch, course_id: int, locale: str) -> bool:
    """Return True if course_dim already has this course_id+locale."""
    sql = f"""
    SELECT 1
    FROM {CH_DATABASE}.inflearn_course_dim
    WHERE course_id = %(course_id)s AND locale = %(locale)s
    LIMIT 1
    """
    rows = ch.query(sql, parameters={"course_id": course_id, "locale": locale}).result_rows
    return bool(rows)
def find_api_data(queries: list[dict], needle: str):
    for q in queries:
        qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
        if needle in qk:
            return (q.get("state") or {}).get("data")
    return None

def to_u8(x) -> int:
    return 1 if x else 0

def clamp_u8_percent(x) -> int:
    """Clamp discount rate into UInt8-friendly 0..100 range."""
    try:
        v = int(float(x))
    except Exception:
        return 0
    if v < 0:
        return 0
    if v > 100:
        return 100
    return v

def process_course_url(url: str, fetched_at):
    """단일 강의 URL을 처리하여 각 테이블 insert rows를 생성한다.
    - update_existing 배치에서 병렬 호출용
    - snapshot_raw는 매번 생성하여 마지막 수집 시각이 전진하도록 유지
    - metric/price도 매번 생성(테이블이 ReplacingMergeTree라 최신으로 수렴)
    반환 dict keys:
      snapshot_raw, course_dim, metric_fact, price_fact, curriculum_unit, instructor_dim, course_instructor_map
    """
    rows = {
        "snapshot_raw": [],
        "course_dim": [],
        "metric_fact": [],
        "price_fact": [],
        "curriculum_unit": [],
        "instructor_dim": [],
        "course_instructor_map": [],
    }

    course_id_hint, locale = parse_course_id_and_locale(url)

    page = http_get(url)
    if page.status_code != 200:
        return rows

    nd = extract_next_data(page.text)
    if not nd:
        return rows

    course_id = parse_course_id_from_next_data(nd) or course_id_hint
    if not course_id:
        return rows

    page_props = (nd.get("props") or {}).get("pageProps") or {}
    queries = ((page_props.get("dehydratedState") or {}).get("queries") or [])

    # snapshot_raw
    # update_existing은 inflearn_course_snapshot_raw의 최신 fetched_at이 오래된 강의부터
    # 다시 고르는 구조이므로, payload 변경 여부와 무관하게 이번 재수집 결과를 기록해야
    # last_fetched_at이 전진한다.
    for q in queries:
        qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
        payload_obj = (q.get("state") or {}).get("data")
        if payload_obj is None:
            continue
        payload = json.dumps(payload_obj, ensure_ascii=False, separators=(",", ":"))
        payload_hash = h64(payload)
        qk_hash = h64(qk)

        status_code = "OK"
        error_code = None
        if isinstance(payload_obj, dict):
            if payload_obj.get("statusCode") is not None:
                status_code = str(payload_obj.get("statusCode"))
            if payload_obj.get("errorCode") is not None:
                error_code = str(payload_obj.get("errorCode"))

        rows["snapshot_raw"].append([
            str(uuid7()), fetched_at, course_id, locale, url,
            qk, qk_hash, status_code, error_code, payload, payload_hash
        ])

    online_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/online/info")
    meta_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/meta")
    curriculum = find_api_data(queries, f"/client/api/v2/courses/{course_id}/curriculum")
    discounts_best = find_api_data(queries, f"/client/api/v1/discounts/best?courseIds={course_id}")
    contents = find_api_data(queries, f"/client/api/v1/course/{course_id}/contents?lang=") or find_api_data(queries, f"/client/api/v1/course/{course_id}/contents")

    # course_dim + metric_fact + price_fact
    if isinstance(online_info, dict) and isinstance(meta_info, dict):
        d = (online_info.get("data") or {})
        m = (meta_info.get("data") or {})
        category = d.get("category") or {}
        main_c = category.get("main") or {}
        sub_c = category.get("sub") or {}

        level_code = ""
        for lv in d.get("levels") or []:
            if lv.get("isActive"):
                level_code = lv.get("code") or lv.get("title") or ""
                break

        unit = d.get("unitSummary") or {}
        rev = d.get("review") or {}

        # Nullable이 아닌 DateTime64 컬럼 보호: None이면 fetched_at 사용
        published_at = parse_dt64(d.get("publishedAt")) or fetched_at
        last_updated_at = parse_dt64(d.get("lastUpdatedAt")) or fetched_at

        rows["course_dim"].append([
            course_id, locale, fetched_at,
            str(d.get("slug") or ""), str(d.get("enSlug") or ""),
            str(d.get("status") or ""),
            str(d.get("title") or ""), str(d.get("description") or ""),
            str(d.get("thumbnailUrl") or ""),
            str(main_c.get("title") or ""), str(main_c.get("slug") or ""),
            str(sub_c.get("title") or ""), str(sub_c.get("slug") or ""),
            str(level_code),
            to_u8(d.get("isNew")), to_u8(d.get("isBest")),
            int(d.get("studentCount") or 0), int(d.get("likeCount") or 0),
            int(rev.get("count") or 0), float(rev.get("averageStar") or 0.0),
            int(unit.get("lectureUnitCount") or 0), int(unit.get("previewUnitCount") or 0),
            int(unit.get("runtime") or 0),
            to_u8(d.get("providesCertificate")), to_u8(d.get("providesInstructorAnswer")), to_u8(d.get("providesInquiry")),
            published_at, last_updated_at,
            str(m.get("keywords") or ""),
            m.get("categorySlugs") or [],
            m.get("skillSlugs") or [],
            m.get("commonTagsSlugs") or [],
        ])

        pay = d.get("paymentInfo") or {}
        disc = (discounts_best.get("data") if isinstance(discounts_best, dict) else {}) or {}
        discount = pay.get("discount") or {}
        discount_rate = clamp_u8_percent(pay.get("discountRate") or disc.get("discountRate") or 0)
        discount_title = str(discount.get("title") or disc.get("discountTitle") or "")
        discount_ended_at = parse_dt64(discount.get("endedAt"))

        # metric_fact (항상 생성)
        metric_tuple = (
            int(d.get("studentCount") or 0),
            int(d.get("likeCount") or 0),
            int(rev.get("count") or 0),
            float(rev.get("averageStar") or 0.0),
            int(pay.get("krwRegularPrice") or 0),
            int(pay.get("krwPaymentPrice") or 0),
            int(discount_rate),
            discount_title,
            discount_ended_at,
        )
        metric_hash = h64(json.dumps(metric_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
        rows["metric_fact"].append([fetched_at, course_id, locale,
                                    metric_tuple[0], metric_tuple[1], metric_tuple[2], metric_tuple[3],
                                    metric_tuple[4], metric_tuple[5],
                                    metric_tuple[6], metric_tuple[7], metric_tuple[8],
                                    metric_hash])

        # price_fact (항상 생성)
        price_tuple = (
            float(pay.get("regularPrice") or 0.0),
            float(pay.get("payPrice") or 0.0),
            int(discount_rate),
            discount_title,
            discount_ended_at,
            int(pay.get("krwRegularPrice") or 0),
            int(pay.get("krwPaymentPrice") or 0),
        )
        price_hash = h64(json.dumps(price_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
        rows["price_fact"].append([fetched_at, course_id, locale,
                                   price_tuple[0], price_tuple[1],
                                   price_tuple[2], price_tuple[3], price_tuple[4],
                                   price_tuple[5], price_tuple[6],
                                   price_hash])

    # curriculum_unit
    if isinstance(curriculum, dict):
        cdata = curriculum.get("data") or {}
        for sec in cdata.get("curriculum", []) or []:
            section_id = int(sec.get("id") or 0)
            section_title = str(sec.get("title") or "")
            for u in sec.get("units", []) or []:
                unit_id = int(u.get("id") or 0)
                unit_title = str(u.get("title") or "")
                unit_type = str(u.get("type") or "")
                runtime_sec = int(u.get("runtime") or 0)
                is_preview = to_u8(u.get("isPreview"))
                has_video = to_u8(u.get("hasVideo"))
                has_attachment = to_u8(u.get("hasAttachment"))
                quiz_id = u.get("quizId")
                reading_time = u.get("readingTime")
                is_challenge_only = to_u8(u.get("isChallengeOnly"))

                unit_tuple = (section_id, section_title, unit_id, unit_title, unit_type, runtime_sec,
                              is_preview, has_video, has_attachment, quiz_id, reading_time, is_challenge_only)
                unit_hash = h64(json.dumps(unit_tuple, default=str, ensure_ascii=False, separators=(",", ":")))

                rows["curriculum_unit"].append([fetched_at, course_id, locale,
                                                section_id, section_title,
                                                unit_id, unit_title, unit_type,
                                                runtime_sec, is_preview, has_video, has_attachment,
                                                quiz_id, reading_time, is_challenge_only,
                                                unit_hash])

    # instructor_dim & map
    if isinstance(contents, dict):
        c = contents.get("data") or {}
        for mi in c.get("mainInstructors", []) or []:
            instructor_id = int(mi.get("id") or 0)
            if instructor_id <= 0:
                continue
            rows["instructor_dim"].append([instructor_id, fetched_at,
                                           str(mi.get("name") or ""), str(mi.get("slug") or ""),
                                           str(mi.get("thumbnail") or ""),
                                           int(mi.get("courseCount") or 0),
                                           int(mi.get("studentCount") or 0),
                                           int(mi.get("reviewCount") or 0),
                                           int(mi.get("totalStar") or 0),
                                           int(mi.get("answerCount") or 0),
                                           str(mi.get("introduce") or "")])
            rows["course_instructor_map"].append([fetched_at, course_id, instructor_id, "main"])

    return rows

def main():
    ch = ch_client()
    fetched_at = now_dt64()

    sitemap_index, offset = get_checkpoint(ch)
    print(f"[checkpoint] start sitemap_index={sitemap_index}, url_index={offset}")

    processed = 0  # number of fetched/parsed courses in this run (batch limit)
    scanned = 0    # number of URLs scanned
    exists_cache: dict[tuple[int,str], bool] = {}
    snap_rows, dim_rows, metric_rows, price_rows, curri_rows, inst_rows, map_rows = ([] for _ in range(7))
    next_checkpoint_sitemap = sitemap_index
    next_checkpoint_url = offset
    reset_checkpoint_after_insert = False

    while True:
        # Try primary sitemap base first, then fallback (some environments may get 403 from CDN)
        sitemap_urls = []
        for base in (SITEMAP_BASE, SITEMAP_BASE_FALLBACK):
            if not base:  # 안전장치
                continue
            u = f"{base}/{SITEMAP_PREFIX}{sitemap_index}.xml"
            if u not in sitemap_urls:
                sitemap_urls.append(u)

        urls = None
        status_404_count = 0
        last_status = None
        last_error = None
        sm_url = sitemap_urls[0]

        for u in sitemap_urls:
            sm_url = u
            try:
                r = http_get(sm_url)
            except Exception as e:
                last_error = f"fetch_error={type(e).__name__}: {e}"
                continue

            last_status = r.status_code
            if r.status_code == 404:
                status_404_count += 1
                continue
            if r.status_code == 403:
                # Possible CDN/WAF block - try the next base
                time.sleep(3)
                last_error = f"HTTP 403 for {sm_url}"
                continue
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code} for {sm_url}"
                continue

            try:
                urls = parse_sitemap_locs(r.content)
            except Exception as e:
                last_error = f"invalid sitemap response for {sm_url}: {e}"
                print(f"[warn] {last_error}")
                urls = None
                continue

            if urls:
                break

            last_error = f"empty sitemap response for {sm_url}"
            urls = None

        if urls is None:
            if status_404_count == len(sitemap_urls):
                print(f"[done] sitemap {sitemap_index} -> 404")
                reset_checkpoint_after_insert = True
                break
            if last_status == 403:
                # Stop gracefully and keep the current checkpoint so the next run can retry.
                print(f"[warn] sitemap blocked (403). will keep checkpoint sitemap_index={sitemap_index}, url_index=0 after successful inserts")
                next_checkpoint_sitemap, next_checkpoint_url = sitemap_index, 0
                break
            raise RuntimeError(last_error or f"Failed to fetch/parse sitemap {sitemap_index}")

        i = offset
        while i < len(urls):
            if BATCH_SIZE > 0 and processed >= BATCH_SIZE:
                next_checkpoint_sitemap, next_checkpoint_url = sitemap_index, i
                print(f"[batch] limit hit. will save checkpoint after successful inserts sitemap={sitemap_index} offset={i}")
                break

            url = urls[i]; i += 1
            scanned += 1
            course_id_hint, locale = parse_course_id_and_locale(url)

            # Fast-path skip when sitemap URL already exposes the course id.
            if course_id_hint:
                key = (course_id_hint, locale)
                exists = exists_cache.get(key)
                if exists is None:
                    exists = course_exists(ch, course_id_hint, locale)
                    exists_cache[key] = exists
                if exists:
                    continue

            page = http_get(url)
            if page.status_code != 200:
                continue

            nd = extract_next_data(page.text)
            if not nd:
                continue

            course_id = parse_course_id_from_next_data(nd) or course_id_hint
            if not course_id:
                continue

            # Slow-path skip for sitemap URLs without cid hint.
            key = (course_id, locale)
            exists = exists_cache.get(key)
            if exists is None:
                exists = course_exists(ch, course_id, locale)
                exists_cache[key] = exists
            if exists:
                continue

            page_props = (nd.get("props") or {}).get("pageProps") or {}
            queries = ((page_props.get("dehydratedState") or {}).get("queries") or [])

            # snapshot_raw
            for q in queries:
                qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
                payload_obj = (q.get("state") or {}).get("data")
                if payload_obj is None:
                    continue
                payload = json.dumps(payload_obj, ensure_ascii=False, separators=(",", ":"))
                payload_hash = h64(payload)
                qk_hash = h64(qk)
                if not should_insert_snapshot(ch, course_id, locale, qk_hash, payload_hash):
                    continue
                snap_rows.append([str(uuid7()), fetched_at, course_id, locale, url, qk, qk_hash, "OK", None, payload, payload_hash])

            online_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/online/info")
            meta_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/meta")
            curriculum = find_api_data(queries, f"/client/api/v2/courses/{course_id}/curriculum")
            discounts_best = find_api_data(queries, f"/client/api/v1/discounts/best?courseIds={course_id}")
            contents = find_api_data(queries, f"/client/api/v1/course/{course_id}/contents?lang=") or find_api_data(queries, f"/client/api/v1/course/{course_id}/contents")

            if isinstance(online_info, dict) and isinstance(meta_info, dict):
                d = online_info.get("data") or {}
                m = meta_info.get("data") or {}
                category = d.get("category") or {}
                main_c = category.get("main") or {}
                sub_c = category.get("sub") or {}

                level_code = ""
                for lv in d.get("levels") or []:
                    if lv.get("isActive"):
                        level_code = lv.get("code") or lv.get("title") or ""
                        break

                unit = d.get("unitSummary") or {}
                rev = d.get("review") or {}
                published_at = parse_dt64(d.get("publishedAt")) or datetime(1970,1,1,tzinfo=KST)
                last_updated_at = parse_dt64(d.get("lastUpdatedAt")) or datetime(1970,1,1,tzinfo=KST)

                dim_rows.append([
                    course_id, locale, fetched_at,
                    str(d.get("slug") or ""), str(d.get("enSlug") or ""),
                    str(d.get("status") or ""),
                    str(d.get("title") or ""), str(d.get("description") or ""),
                    str(d.get("thumbnailUrl") or ""),
                    str(main_c.get("title") or ""), str(main_c.get("slug") or ""),
                    str(sub_c.get("title") or ""), str(sub_c.get("slug") or ""),
                    str(level_code),
                    to_u8(d.get("isNew")), to_u8(d.get("isBest")),
                    int(d.get("studentCount") or 0), int(d.get("likeCount") or 0),
                    int(rev.get("count") or 0), float(rev.get("averageStar") or 0.0),
                    int(unit.get("lectureUnitCount") or 0), int(unit.get("previewUnitCount") or 0),
                    int(unit.get("runtime") or 0),
                    to_u8(d.get("providesCertificate")), to_u8(d.get("providesInstructorAnswer")), to_u8(d.get("providesInquiry")),
                    published_at, last_updated_at,
                    str(m.get("keywords") or ""),
                    m.get("categorySlugs") or [],
                    m.get("skillSlugs") or [],
                    m.get("commonTagsSlugs") or [],
                ])

                pay = d.get("paymentInfo") or {}
                disc = (discounts_best.get("data") if isinstance(discounts_best, dict) else {}) or {}
                discount = pay.get("discount") or {}
                discount_rate = int(pay.get("discountRate") or disc.get("discountRate") or 0)
                discount_title = str(discount.get("title") or disc.get("discountTitle") or "")
                discount_ended_at = parse_dt64(discount.get("endedAt"))

                # metric_fact change-only
                metric_tuple = (
                    int(d.get("studentCount") or 0),
                    int(d.get("likeCount") or 0),
                    int(rev.get("count") or 0),
                    float(rev.get("averageStar") or 0.0),
                    int(pay.get("krwRegularPrice") or 0),
                    int(pay.get("krwPaymentPrice") or 0),
                    discount_rate,
                    discount_title,
                    discount_ended_at,
                )
                if latest_metric_tuple(ch, course_id, locale) != metric_tuple:
                    metric_hash = h64(json.dumps(metric_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
                    metric_rows.append([fetched_at, course_id, locale,
                                        metric_tuple[0], metric_tuple[1], metric_tuple[2], metric_tuple[3],
                                        metric_tuple[4], metric_tuple[5],
                                        metric_tuple[6], metric_tuple[7], metric_tuple[8],
                                        metric_hash])

                # price_fact change-only
                price_tuple = (
                    float(pay.get("regularPrice") or 0.0),
                    float(pay.get("payPrice") or 0.0),
                    discount_rate,
                    discount_title,
                    discount_ended_at,
                    int(pay.get("krwRegularPrice") or 0),
                    int(pay.get("krwPaymentPrice") or 0),
                )
                if latest_price_tuple(ch, course_id, locale) != price_tuple:
                    price_hash = h64(json.dumps(price_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
                    price_rows.append([fetched_at, course_id, locale,
                                       price_tuple[0], price_tuple[1],
                                       price_tuple[2], price_tuple[3], price_tuple[4],
                                       price_tuple[5], price_tuple[6],
                                       price_hash])

            # curriculum_unit (latest per unit; include locale)
            if isinstance(curriculum, dict):
                cdata = curriculum.get("data") or {}
                for sec in cdata.get("curriculum", []) or []:
                    section_id = int(sec.get("id") or 0)
                    section_title = str(sec.get("title") or "")
                    for u in sec.get("units", []) or []:
                        unit_id = int(u.get("id") or 0)
                        unit_title = str(u.get("title") or "")
                        unit_type = str(u.get("type") or "")
                        runtime_sec = int(u.get("runtime") or 0)
                        is_preview = to_u8(u.get("isPreview"))
                        has_video = to_u8(u.get("hasVideo"))
                        has_attachment = to_u8(u.get("hasAttachment"))
                        quiz_id = u.get("quizId")
                        reading_time = u.get("readingTime")
                        is_challenge_only = to_u8(u.get("isChallengeOnly"))

                        unit_tuple = (section_id, section_title, unit_id, unit_title, unit_type, runtime_sec,
                                      is_preview, has_video, has_attachment, quiz_id, reading_time, is_challenge_only)
                        unit_hash = h64(json.dumps(unit_tuple, default=str, ensure_ascii=False, separators=(",", ":")))

                        curri_rows.append([fetched_at, course_id, locale,
                                           section_id, section_title,
                                           unit_id, unit_title, unit_type,
                                           runtime_sec, is_preview, has_video, has_attachment,
                                           quiz_id, reading_time, is_challenge_only,
                                           unit_hash])

            # instructor_dim & map (Replacing이라 중복 쌓여도 최신으로 수렴)
            if isinstance(contents, dict):
                c = contents.get("data") or {}
                for mi in c.get("mainInstructors", []) or []:
                    instructor_id = int(mi.get("id") or 0)
                    if instructor_id <= 0:
                        continue
                    inst_rows.append([instructor_id, fetched_at,
                                      str(mi.get("name") or ""), str(mi.get("slug") or ""),
                                      str(mi.get("thumbnail") or ""),
                                      int(mi.get("courseCount") or 0),
                                      int(mi.get("studentCount") or 0),
                                      int(mi.get("reviewCount") or 0),
                                      int(mi.get("totalStar") or 0),
                                      int(mi.get("answerCount") or 0),
                                      str(mi.get("introduce") or "")])
                    map_rows.append([fetched_at, course_id, instructor_id, "main"])

            processed += 1
            if processed % 50 == 0:
                print(f"[progress] scanned={scanned} fetched={processed} last_course_id={course_id} sitemap={sitemap_index} offset={i}")
            jitter_sleep(SLEEP_MIN, SLEEP_MAX)

        if reset_checkpoint_after_insert:
            break

        if BATCH_SIZE > 0 and processed >= BATCH_SIZE:
            break

        sitemap_index += 1
        offset = 0
        next_checkpoint_sitemap, next_checkpoint_url = sitemap_index, 0
        jitter_sleep(SLEEP_MIN, SLEEP_MAX)

    def insert_if_rows(table, rows, cols):
        if not rows:
            return 0
        ch.insert(f"{CH_DATABASE}.{table}", rows, column_names=cols)
        return len(rows)

    inserted = {
        "snapshot_raw": insert_snapshot_rows_json_each_row(snap_rows),
        "course_dim": insert_if_rows("inflearn_course_dim", dim_rows,
            ["course_id","locale","fetched_at","slug","en_slug","status","title","description","thumbnail_url",
             "category_main_title","category_main_slug","category_sub_title","category_sub_slug",
             "level_code","is_new","is_best","student_count","like_count","review_count","average_star",
             "lecture_unit_count","preview_unit_count","runtime_sec",
             "provides_certificate","provides_instructor_answer","provides_inquiry",
             "published_at","last_updated_at","keywords","category_slugs","skill_slugs","common_tag_slugs"]),
        "metric_fact": insert_if_rows("inflearn_course_metric_fact", metric_rows,
            ["fetched_at","course_id","locale","student_count","like_count","review_count","average_star",
             "krw_regular_price","krw_pay_price","discount_rate","discount_title","discount_ended_at","metric_hash"]),
        "price_fact": insert_if_rows("inflearn_course_price_fact", price_rows,
            ["fetched_at","course_id","locale","regular_price","pay_price","discount_rate","discount_title","discount_ended_at",
             "krw_regular_price","krw_pay_price","price_hash"]),
        "curriculum_unit": insert_if_rows("inflearn_course_curriculum_unit", curri_rows,
            ["fetched_at","course_id","locale","section_id","section_title","unit_id","unit_title","unit_type",
             "runtime_sec","is_preview","has_video","has_attachment","quiz_id","reading_time","is_challenge_only","unit_hash"]),
        "instructor_dim": insert_if_rows("inflearn_instructor_dim", inst_rows,
            ["instructor_id","fetched_at","name","slug","thumbnail_url","course_count","student_count","review_count","total_star","answer_count","introduce_html"]),
        "course_instructor_map": insert_if_rows("inflearn_course_instructor_map", map_rows,
            ["fetched_at","course_id","instructor_id","role"]),
    }

    if reset_checkpoint_after_insert:
        set_checkpoint(ch, 0, 0)
        print("[checkpoint] reset to sitemap_index=0, url_index=0")
    else:
        set_checkpoint(ch, next_checkpoint_sitemap, next_checkpoint_url)
        print(f"[checkpoint] saved sitemap_index={next_checkpoint_sitemap}, url_index={next_checkpoint_url}")

    print("[inserted]", inserted)
    print("[done]")

if __name__ == "__main__":
    main()
