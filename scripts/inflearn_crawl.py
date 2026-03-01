# Inflearn sitemap crawler -> ClickHouse (Statground) [Improved Schema]
# Inserts: snapshot_raw + course_dim + metric_fact + price_fact + curriculum_unit + instructor_dim + course_instructor_map
# Uses stable 64-bit hashes (blake2b) and change-only inserts for metric/price.
# DateTime64 inserted as datetime objects.

import os, re, json, time, random, urllib.parse, hashlib, struct, threading, math
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed
import clickhouse_connect
from uuid6 import uuid7

UA = "Mozilla/5.0 (compatible; StatgroundCrawler/1.0; +https://www.statground.net)"
TLS = threading.local()

def _session():
    s = getattr(TLS, "session", None)
    if s is None:
        s = requests.Session()
        TLS.session = s
    return s

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

SITEMAP_BASE = _get_env("SITEMAP_BASE", "https://cdn.inflearn.com/sitemaps").rstrip("/")
SITEMAP_PREFIX = _get_env("SITEMAP_PREFIX", "sitemap-courseDetail-")

BATCH_SIZE = _parse_int_env("BATCH_SIZE", 100)
WORKERS = _parse_int_env("WORKERS", 8)
SLEEP_MIN = float(_get_env("REQUEST_SLEEP_MIN", "0.6"))
SLEEP_MAX = float(_get_env("REQUEST_SLEEP_MAX", "1.3"))

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20))
def http_get(url: str) -> requests.Response:
    r = _session().get(url, timeout=30)
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
    return 
def clamp_u8_percent(x) -> int:
    """ClickHouse UInt8용 할인율(%) 정규화. None/NaN/float/str 모두 안전 처리."""
    try:
        if x is None:
            return 0
        if isinstance(x, bool):
            return 1 if x else 0
        if isinstance(x, (int,)):
            v = x
        else:
            s = str(x).strip()
            if s == "":
                return 0
            v = float(s)
        if not math.isfinite(v):
            return 0
        v = int(round(v))
        if v < 0: v = 0
        if v > 100: v = 100
        return v
    except Exception:
        return 0

None

def to_u8(x) -> int:
    return 1 if x else 0


def process_course_url(url: str, fetched_at):
    """단일 강의 URL을 수집/파싱하여 ClickHouse insert용 row 묶음을 생성한다.
    - 병렬 실행을 위해 ClickHouse 쿼리는 하지 않는다(중복 체크 없음).
    - 실패 시에도 snapshot_raw에 ERROR 상태로 기록할 수 있도록 최소 row를 반환한다.
    """
    snap_rows, dim_rows, metric_rows, price_rows, curri_rows, inst_rows, map_rows = ([] for _ in range(7))
    course_id, locale = parse_course_id_and_locale(url)
    if not course_id:
        return {
            "processed": 0,
            "snapshot_raw": snap_rows,
            "course_dim": dim_rows,
            "metric_fact": metric_rows,
            "price_fact": price_rows,
            "curriculum_unit": curri_rows,
            "instructor_dim": inst_rows,
            "course_instructor_map": map_rows,
        }

    try:
        page = http_get(url)
        if page.status_code != 200:
            # snapshot error only
            snap_rows.append([str(uuid7()), fetched_at, course_id, locale, url, "[]", 0, "HTTP", str(page.status_code), "{}", 0])
            return {
                "processed": 1,
                "snapshot_raw": snap_rows,
                "course_dim": dim_rows,
                "metric_fact": metric_rows,
                "price_fact": price_rows,
                "curriculum_unit": curri_rows,
                "instructor_dim": inst_rows,
                "course_instructor_map": map_rows,
            }

        nd = extract_next_data(page.text)
        if not nd:
            snap_rows.append([str(uuid7()), fetched_at, course_id, locale, url, "[]", 0, "PARSE", "NO_NEXT_DATA", "{}", 0])
            return {
                "processed": 1,
                "snapshot_raw": snap_rows,
                "course_dim": dim_rows,
                "metric_fact": metric_rows,
                "price_fact": price_rows,
                "curriculum_unit": curri_rows,
                "instructor_dim": inst_rows,
                "course_instructor_map": map_rows,
            }

        page_props = (nd.get("props") or {}).get("pageProps") or {}
        queries = ((page_props.get("dehydratedState") or {}).get("queries") or [])

        # snapshot_raw: 모든 query state.data를 저장
        for q in queries:
            qk = json.dumps(q.get("queryKey"), ensure_ascii=False)
            payload_obj = (q.get("state") or {}).get("data")
            if payload_obj is None:
                continue
            payload = json.dumps(payload_obj, ensure_ascii=False, separators=(",", ":"))
            payload_hash = h64(payload)
            qk_hash = h64(qk)
            snap_rows.append([str(uuid7()), fetched_at, course_id, locale, url, qk, qk_hash, "OK", None, payload, payload_hash])

        online_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/online/info")
        meta_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/meta")
        contents = find_api_data(queries, f"/client/api/v1/course/{course_id}/contents")
        price_info = find_api_data(queries, f"/client/api/v1/course/{course_id}/price")

        # course_dim
        if isinstance(meta_info, dict):
            m = meta_info.get("data") or {}
            slug = str(m.get("slug") or "")
            en_slug = str(m.get("enSlug") or "")
            status = str(m.get("status") or "")
            title = str(m.get("title") or "")
            description = str(m.get("description") or "")
            thumbnail_url = str(m.get("thumbnailUrl") or "")

            cat = m.get("category") or {}
            cat_main = cat.get("main") or {}
            cat_sub = cat.get("sub") or {}
            category_main_title = str(cat_main.get("title") or "")
            category_main_slug = str(cat_main.get("slug") or "")
            category_sub_title = str(cat_sub.get("title") or "")
            category_sub_slug = str(cat_sub.get("slug") or "")

            level_code = str(m.get("level") or "")
            is_new = to_u8(m.get("isNew"))
            is_best = to_u8(m.get("isBest"))
            student_count = int(m.get("studentCount") or 0)
            like_count = int(m.get("likeCount") or 0)
            review_count = int(m.get("reviewCount") or 0)
            average_star = float(m.get("averageStar") or 0.0)

            lecture_unit_count = int(m.get("lectureUnitCount") or 0)
            preview_unit_count = int(m.get("previewUnitCount") or 0)
            runtime_sec = int(m.get("runtimeSec") or 0)

            provides_certificate = to_u8(m.get("providesCertificate"))
            provides_instructor_answer = to_u8(m.get("providesInstructorAnswer"))
            provides_inquiry = to_u8(m.get("providesInquiry"))

            published_at = parse_dt64(m.get("publishedAt"))
            last_updated_at = parse_dt64(m.get("lastUpdatedAt"))

            keywords = json.dumps(m.get("keywords") or [], ensure_ascii=False, separators=(",", ":"))
            category_slugs = json.dumps(m.get("categorySlugs") or [], ensure_ascii=False, separators=(",", ":"))
            skill_slugs = json.dumps(m.get("skillSlugs") or [], ensure_ascii=False, separators=(",", ":"))
            common_tag_slugs = json.dumps(m.get("commonTagSlugs") or [], ensure_ascii=False, separators=(",", ":"))

            dim_rows.append([
                course_id, locale, fetched_at,
                slug, en_slug, status, title, description, thumbnail_url,
                category_main_title, category_main_slug,
                category_sub_title, category_sub_slug,
                level_code, is_new, is_best,
                student_count, like_count, review_count, average_star,
                lecture_unit_count, preview_unit_count, runtime_sec,
                provides_certificate, provides_instructor_answer, provides_inquiry,
                published_at, last_updated_at,
                keywords, category_slugs, skill_slugs, common_tag_slugs
            ])

        # metric_fact / price_fact (change-only 목적 hash 유지)
        krw_regular = 0
        krw_pay = 0
        regular_price = 0
        pay_price = 0
        discount_title = ""
        discount_ended_at = None
        discount_rate = 0

        if isinstance(price_info, dict):
            p = price_info.get("data") or {}
            regular_price = int(p.get("regularPrice") or 0)
            pay_price = int(p.get("payPrice") or 0)
            krw_regular = int(p.get("krwRegularPrice") or 0)
            krw_pay = int(p.get("krwPayPrice") or 0)
            discount_title = str(p.get("discountTitle") or "")
            discount_ended_at = parse_dt64(p.get("discountEndedAt"))
            discount_rate = clamp_u8_percent(p.get("discountRate"))

        if isinstance(online_info, dict):
            o = online_info.get("data") or {}
            student_count = int(o.get("studentCount") or 0)
            like_count = int(o.get("likeCount") or 0)
            review_count = int(o.get("reviewCount") or 0)
            average_star = float(o.get("averageStar") or 0.0)

            metric_tuple = (student_count, like_count, review_count, average_star,
                            krw_regular, krw_pay, discount_rate, discount_title, str(discount_ended_at))
            metric_hash = h64(json.dumps(metric_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
            metric_rows.append([
                fetched_at, course_id, locale,
                student_count, like_count, review_count, average_star,
                krw_regular, krw_pay,
                discount_rate, discount_title, discount_ended_at,
                metric_hash
            ])

        price_tuple = (regular_price, pay_price, discount_rate, discount_title, str(discount_ended_at),
                       krw_regular, krw_pay)
        price_hash = h64(json.dumps(price_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
        price_rows.append([
            fetched_at, course_id, locale,
            regular_price, pay_price, discount_rate, discount_title, discount_ended_at,
            krw_regular, krw_pay, price_hash
        ])

        # curriculum_unit
        if isinstance(contents, dict):
            c = contents.get("data") or {}
            # 구조는 기존 코드와 동일하게 처리(가능한 한 안전)
            for sec in c.get("sections", []) or []:
                section_id = int(sec.get("id") or 0)
                section_title = str(sec.get("title") or "")
                for u in sec.get("units", []) or []:
                    unit_id = int(u.get("id") or 0)
                    unit_title = str(u.get("title") or "")
                    unit_type = str(u.get("type") or "")
                    runtime_sec = int(u.get("runtimeSec") or 0)
                    is_preview = to_u8(u.get("isPreview"))
                    has_video = to_u8(u.get("hasVideo"))
                    has_attachment = to_u8(u.get("hasAttachment"))
                    quiz_id = u.get("quizId")
                    reading_time = u.get("readingTime")
                    is_challenge_only = to_u8(u.get("isChallengeOnly"))

                    unit_tuple = (section_id, section_title, unit_id, unit_title, unit_type, runtime_sec,
                                  is_preview, has_video, has_attachment, quiz_id, reading_time, is_challenge_only)
                    unit_hash = h64(json.dumps(unit_tuple, default=str, ensure_ascii=False, separators=(",", ":")))
                    curri_rows.append([
                        fetched_at, course_id, locale,
                        section_id, section_title,
                        unit_id, unit_title, unit_type,
                        runtime_sec, is_preview, has_video, has_attachment,
                        quiz_id, reading_time, is_challenge_only,
                        unit_hash
                    ])

            # instructor_dim & map
            for mi in c.get("mainInstructors", []) or []:
                instructor_id = int(mi.get("id") or 0)
                if instructor_id <= 0:
                    continue
                inst_rows.append([
                    instructor_id, fetched_at,
                    str(mi.get("name") or ""), str(mi.get("slug") or ""),
                    str(mi.get("thumbnail") or ""),
                    int(mi.get("courseCount") or 0),
                    int(mi.get("studentCount") or 0),
                    int(mi.get("reviewCount") or 0),
                    int(mi.get("totalStar") or 0),
                    int(mi.get("answerCount") or 0),
                    str(mi.get("introduce") or "")
                ])
                map_rows.append([fetched_at, course_id, instructor_id, "main"])

        return {
            "processed": 1,
            "snapshot_raw": snap_rows,
            "course_dim": dim_rows,
            "metric_fact": metric_rows,
            "price_fact": price_rows,
            "curriculum_unit": curri_rows,
            "instructor_dim": inst_rows,
            "course_instructor_map": map_rows,
        }

    except Exception as e:
        # 세부 정보 노출 최소화
        snap_rows.append([str(uuid7()), fetched_at, course_id, locale, url, "[]", 0, "ERROR", "EXCEPTION", "{}", 0])
        return {
            "processed": 1,
            "snapshot_raw": snap_rows,
            "course_dim": dim_rows,
            "metric_fact": metric_rows,
            "price_fact": price_rows,
            "curriculum_unit": curri_rows,
            "instructor_dim": inst_rows,
            "course_instructor_map": map_rows,
        }

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
        # 이번 sitemap에서 처리할 URL을 배치 크기만큼만 선택
        remaining = (BATCH_SIZE - processed) if BATCH_SIZE > 0 else (len(urls) - i)
        take = urls[i:i + max(0, remaining)] if remaining > 0 else []
        if not take:
            # 배치 한도에 도달했으면 체크포인트 저장 후 종료
            set_checkpoint(ch, sitemap_index, i)
            print(f"[batch] limit hit. saved checkpoint sitemap={sitemap_index} offset={i}")
            break

        # 병렬 처리(HTTP fetch/parse/build rows)
        with ThreadPoolExecutor(max_workers=max(1, WORKERS)) as ex:
            futs = [ex.submit(process_course_url, u, fetched_at) for u in take]
            done = 0
            for fut in as_completed(futs):
                res = fut.result()
                done += int(res.get("processed") or 0)
                snap_rows.extend(res.get("snapshot_raw") or [])
                dim_rows.extend(res.get("course_dim") or [])
                metric_rows.extend(res.get("metric_fact") or [])
                price_rows.extend(res.get("price_fact") or [])
                curri_rows.extend(res.get("curriculum_unit") or [])
                inst_rows.extend(res.get("instructor_dim") or [])
                map_rows.extend(res.get("course_instructor_map") or [])

        processed += len(take)
        i = i + len(take)
        set_checkpoint(ch, sitemap_index, i)
        print(f"[progress] sitemap={sitemap_index} processed_total={processed} offset={i}")

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