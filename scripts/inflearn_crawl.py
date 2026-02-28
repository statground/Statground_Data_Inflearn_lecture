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

CH_HOST = _get_env("CH_HOST")
CH_PORT = _parse_int_env("CH_PORT", 8123)
CH_USER = _get_env("CH_USER", "default")
CH_PASSWORD = _get_env("CH_PASSWORD", "")
CH_DATABASE = _get_env("CH_DATABASE", "statground_lecture")

SITEMAP_BASE = _get_env("SITEMAP_BASE", "https://cdn.inflearn.com/sitemaps").rstrip("/")
SITEMAP_PREFIX = _get_env("SITEMAP_PREFIX", "sitemap-courseDetail-")

BATCH_SIZE = _parse_int_env("BATCH_SIZE", 600)
SLEEP_MIN = float(_get_env("REQUEST_SLEEP_MIN", "0.6"))
SLEEP_MAX = float(_get_env("REQUEST_SLEEP_MAX", "1.3"))

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
    return json.loads(m.group(1).strip()) if m else None

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
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
    )

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

def find_api_data(queries: list[dict], needle: str):
    for q in queries:
        qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
        if needle in qk:
            return (q.get("state") or {}).get("data")
    return None

def to_u8(x) -> int:
    return 1 if x else 0

def main():
    ch = ch_client()
    fetched_at = now_dt64()

    sitemap_index, offset = get_checkpoint(ch)
    print(f"[checkpoint] start sitemap_index={sitemap_index}, url_index={offset}")

    processed = 0
    snap_rows, dim_rows, metric_rows, price_rows, curri_rows, inst_rows, map_rows = ([] for _ in range(7))

    while True:
        sm_url = f"{SITEMAP_BASE}/{SITEMAP_PREFIX}{sitemap_index}.xml"
        r = http_get(sm_url)
        if r.status_code == 404:
            print(f"[done] sitemap {sitemap_index} -> 404")
            set_checkpoint(ch, 0, 0)
            break
        if r.status_code != 200:
            raise RuntimeError(f"Unexpected status {r.status_code} for {sm_url}")

        urls = parse_sitemap_locs(r.content)
        if not urls:
            set_checkpoint(ch, sitemap_index, 0)
            break

        i = offset
        while i < len(urls):
            if BATCH_SIZE > 0 and processed >= BATCH_SIZE:
                set_checkpoint(ch, sitemap_index, i)
                print(f"[batch] limit hit. saved checkpoint sitemap={sitemap_index} offset={i}")
                break

            url = urls[i]; i += 1
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
                print(f"[progress] processed={processed} last_course_id={course_id} sitemap={sitemap_index} offset={i}")
            jitter_sleep(SLEEP_MIN, SLEEP_MAX)

        if BATCH_SIZE > 0 and processed >= BATCH_SIZE:
            break

        sitemap_index += 1
        offset = 0
        set_checkpoint(ch, sitemap_index, 0)
        jitter_sleep(SLEEP_MIN, SLEEP_MAX)

    def insert_if_rows(table, rows, cols):
        if not rows:
            return 0
        ch.insert(f"{CH_DATABASE}.{table}", rows, column_names=cols)
        return len(rows)

    inserted = {
        "snapshot_raw": insert_if_rows("inflearn_course_snapshot_raw", snap_rows,
            ["uuid","fetched_at","course_id","locale","source_url","query_key","query_key_hash","status_code","error_code","payload","payload_hash"]),
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

    print("[inserted]", inserted)
    print("[done]")

if __name__ == "__main__":
    main()