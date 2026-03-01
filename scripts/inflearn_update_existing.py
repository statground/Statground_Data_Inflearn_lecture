#!/usr/bin/env python3
"""Batch #2: update already-collected courses.

Strategy:
- Pick courses whose latest snapshot is oldest (so we refresh stale courses first).
- Re-crawl the course page URL and insert new snapshots/dims/facts as needed.

This reuses helper functions/constants from scripts/inflearn_crawl.py to ensure schema compatibility.
"""

import os, sys, pathlib, json
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import inflearn_crawl  # noqa: E402

KST = ZoneInfo("Asia/Seoul")

def _get_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default

def pick_urls(ch, limit: int):
    # Choose oldest-updated courses first, using latest successful snapshot URL as the canonical URL.
    db = inflearn_crawl.CH_DATABASE
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
    LIMIT {limit}
    """
    r = ch.query(sql)
    return r.result_rows

def main():
    ch = inflearn_crawl.ch_client()
    fetched_at = inflearn_crawl.now_dt64()

    limit = _get_int("UPDATE_BATCH_SIZE", 400)
    rows = pick_urls(ch, limit)
    print(f"[update] picked={len(rows)} (UPDATE_BATCH_SIZE={limit})")

    processed = 0
    snap_rows, dim_rows, metric_rows, price_rows, curri_rows, inst_rows, map_rows = ([] for _ in range(7))

    for course_id, locale, url, last_fetched_at in rows:
        url = str(url or "").strip()
        if not url:
            continue

        # --- the same core parsing logic as inflearn_crawl.main() ---
        page = inflearn_crawl.http_get(url)
        if page.status_code != 200:
            continue

        nd = inflearn_crawl.extract_next_data(page.text)
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
            payload_hash = inflearn_crawl.h64(payload)
            qk_hash = inflearn_crawl.h64(qk)
            if not inflearn_crawl.should_insert_snapshot(ch, int(course_id), str(locale), qk_hash, payload_hash):
                continue
            snap_rows.append([
                str(inflearn_crawl.uuid7()),  # uuid
                fetched_at,
                int(course_id),
                str(locale),
                url,
                qk,
                qk_hash,
                "OK",
                None,
                payload,
                payload_hash
            ])

        online_info = inflearn_crawl.find_api_data(queries, f"/client/api/v1/course/{course_id}/online/info")
        meta_info = inflearn_crawl.find_api_data(queries, f"/client/api/v1/course/{course_id}/meta")
        curriculum = inflearn_crawl.find_api_data(queries, f"/client/api/v2/courses/{course_id}/curriculum")
        discounts_best = inflearn_crawl.find_api_data(queries, f"/client/api/v1/discounts/best?courseIds={course_id}")
        contents = (inflearn_crawl.find_api_data(queries, f"/client/api/v1/course/{course_id}/contents?lang=")
                    or inflearn_crawl.find_api_data(queries, f"/client/api/v1/course/{course_id}/contents"))

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
            published_at = inflearn_crawl.parse_dt64(d.get("publishedAt")) or datetime(1970,1,1,tzinfo=KST)
            last_updated_at = inflearn_crawl.parse_dt64(d.get("lastUpdatedAt")) or datetime(1970,1,1,tzinfo=KST)

            dim_rows.append([
                int(course_id), str(locale), fetched_at,
                str(d.get("slug") or ""), str(d.get("enSlug") or ""),
                str(d.get("status") or ""),
                str(d.get("title") or ""), str(d.get("description") or ""),
                str(d.get("thumbnailUrl") or ""),
                str(main_c.get("title") or ""), str(main_c.get("slug") or ""),
                str(sub_c.get("title") or ""), str(sub_c.get("slug") or ""),
                str(level_code),
                inflearn_crawl.to_u8(d.get("isNew")), inflearn_crawl.to_u8(d.get("isBest")),
                int(d.get("studentCount") or 0), int(d.get("likeCount") or 0),
                int(rev.get("count") or 0), float(rev.get("averageStar") or 0.0),
                int(unit.get("lectureUnitCount") or 0), int(unit.get("previewUnitCount") or 0),
                int(unit.get("runtimeSec") or 0),
                inflearn_crawl.to_u8(m.get("providesCertificate")),
                inflearn_crawl.to_u8(m.get("providesInstructorAnswer")),
                inflearn_crawl.to_u8(m.get("providesInquiry")),
                published_at, last_updated_at,
                json.dumps(d.get("keywords") or [], ensure_ascii=False, separators=(",", ":")),
                json.dumps(d.get("categorySlugs") or [], ensure_ascii=False, separators=(",", ":")),
                json.dumps(d.get("skillSlugs") or [], ensure_ascii=False, separators=(",", ":")),
                json.dumps(d.get("commonTagSlugs") or [], ensure_ascii=False, separators=(",", ":")),
            ])

            # metric_fact (change-only insert)
            krw_reg = int(d.get("regularPrice") or 0)
            krw_pay = int(d.get("payPrice") or 0)
            discount_rate = float(d.get("discountRate") or 0.0)
            discount_title = str(d.get("discountTitle") or "")
            discount_ended_at = inflearn_crawl.parse_dt64(d.get("discountEndedAt")) or datetime(1970,1,1,tzinfo=KST)

            metric_tuple = (
                int(d.get("studentCount") or 0),
                int(d.get("likeCount") or 0),
                int(rev.get("count") or 0),
                float(rev.get("averageStar") or 0.0),
                krw_reg, krw_pay,
                discount_rate, discount_title,
                discount_ended_at
            )
            last_metric = inflearn_crawl.latest_metric_tuple(ch, int(course_id), str(locale))
            if metric_tuple != last_metric:
                metric_hash = inflearn_crawl.h64(json.dumps(metric_tuple, default=str, ensure_ascii=False))
                metric_rows.append([
                    fetched_at, int(course_id), str(locale),
                    metric_tuple[0], metric_tuple[1], metric_tuple[2], metric_tuple[3],
                    metric_tuple[4], metric_tuple[5], metric_tuple[6], metric_tuple[7], metric_tuple[8],
                    metric_hash
                ])

            # price_fact (change-only insert)
            # meta payload usually includes detailed price; fallback to online_info fields
            regular_price = int((m.get("price") or {}).get("regular") or krw_reg)
            pay_price = int((m.get("price") or {}).get("pay") or krw_pay)

            price_tuple = (
                regular_price, pay_price,
                discount_rate, discount_title, discount_ended_at,
                krw_reg, krw_pay
            )
            last_price = inflearn_crawl.latest_price_tuple(ch, int(course_id), str(locale))
            if price_tuple != last_price:
                price_hash = inflearn_crawl.h64(json.dumps(price_tuple, default=str, ensure_ascii=False))
                price_rows.append([
                    fetched_at, int(course_id), str(locale),
                    regular_price, pay_price,
                    discount_rate, discount_title, discount_ended_at,
                    krw_reg, krw_pay,
                    price_hash
                ])

        # curriculum units
        if isinstance(curriculum, dict):
            data = curriculum.get("data") or {}
            sections = data.get("sections") or []
            for sec in sections:
                sec_id = int(sec.get("id") or 0)
                sec_title = str(sec.get("title") or "")
                for u in (sec.get("units") or []):
                    unit_hash = inflearn_crawl.h64(json.dumps(u, ensure_ascii=False, separators=(",", ":")))
                    curri_rows.append([
                        fetched_at, int(course_id), str(locale),
                        sec_id, sec_title,
                        int(u.get("id") or 0),
                        str(u.get("title") or ""),
                        str(u.get("type") or ""),
                        int(u.get("runtimeSec") or 0),
                        inflearn_crawl.to_u8(u.get("isPreview")),
                        inflearn_crawl.to_u8(u.get("hasVideo")),
                        inflearn_crawl.to_u8(u.get("hasAttachment")),
                        int(u.get("quizId") or 0),
                        int(u.get("readingTime") or 0),
                        inflearn_crawl.to_u8(u.get("isChallengeOnly")),
                        unit_hash
                    ])

        # instructor_dim + course_instructor_map
        if isinstance(contents, dict):
            cdata = contents.get("data") or {}
            instructors = cdata.get("instructors") or []
            for inst in instructors:
                inst_id = int(inst.get("id") or 0)
                if inst_id <= 0:
                    continue
                inst_hash = inflearn_crawl.h64(json.dumps(inst, ensure_ascii=False, separators=(",", ":")))
                inst_rows.append([
                    inst_id, fetched_at,
                    str(inst.get("name") or ""),
                    str(inst.get("slug") or ""),
                    str(inst.get("thumbnailUrl") or ""),
                    int(inst.get("courseCount") or 0),
                    int(inst.get("studentCount") or 0),
                    int(inst.get("reviewCount") or 0),
                    float(inst.get("totalStar") or 0.0),
                    int(inst.get("answerCount") or 0),
                    str(inst.get("introduceHtml") or ""),
                ])
                map_rows.append([fetched_at, int(course_id), inst_id, str(inst.get("role") or "")] )

        processed += 1
        inflearn_crawl.jitter_sleep(inflearn_crawl.SLEEP_MIN, inflearn_crawl.SLEEP_MAX)

    # flush inserts (same as inflearn_crawl.py)
    def insert_if_rows(table, rows, cols):
        if not rows:
            return 0
        ch.insert(f"{inflearn_crawl.CH_DATABASE}.{table}", rows, column_names=cols)
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
