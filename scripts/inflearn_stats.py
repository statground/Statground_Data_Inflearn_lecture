#!/usr/bin/env python3
"""Batch #3: generate statistics (hourly/daily/monthly/yearly + cumulative).

Outputs:
- Console
- GitHub Actions Step Summary ($GITHUB_STEP_SUMMARY) if available
"""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import clickhouse_connect

KST = ZoneInfo("Asia/Seoul")

def _get_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default

def ch_client():
    host = os.environ.get("CLICKHOUSE_HOST")
    port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    database = os.environ.get("CLICKHOUSE_DATABASE", "statground_lecture")
    if not host:
        raise RuntimeError("CLICKHOUSE_HOST is required")
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, database=database), database

def md_table(headers, rows):
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)

def q(ch, sql):
    return ch.query(sql).result_rows

def main():
    ch, db = ch_client()
    lookback_days = _get_int("STATS_LOOKBACK_DAYS", 365)
    since = (datetime.now(tz=KST) - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")

    # Latest dim per (course_id, locale)
    latest_dim = f"""
    SELECT
      course_id, locale,
      argMax(title, fetched_at) AS title,
      max(fetched_at) AS last_fetched_at,
      argMax(published_at, fetched_at) AS published_at,
      argMax(student_count, fetched_at) AS student_count,
      argMax(like_count, fetched_at) AS like_count,
      argMax(review_count, fetched_at) AS review_count,
      argMax(average_star, fetched_at) AS average_star
    FROM {db}.inflearn_course_dim
    GROUP BY course_id, locale
    """

    # Latest price per (course_id, locale)
    latest_price = f"""
    SELECT
      course_id, locale,
      argMax(krw_regular_price, fetched_at) AS krw_regular_price,
      argMax(krw_pay_price, fetched_at) AS krw_pay_price,
      argMax(discount_rate, fetched_at) AS discount_rate
    FROM {db}.inflearn_course_price_fact
    GROUP BY course_id, locale
    """

    md = []
    md.append(f"# Inflearn Crawl Stats (lookback {lookback_days}d)")
    md.append(f"- generated_at (KST): {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")

    # Totals
    total_courses = q(ch, f"SELECT uniqExact(cityHash64(toString(course_id), locale)) FROM ({latest_dim})")[0][0]
    total_instructors = q(ch, f"SELECT uniqExact(instructor_id) FROM {db}.inflearn_instructor_dim")[0][0]
    total_snapshots = q(ch, f"SELECT count() FROM {db}.inflearn_course_snapshot_raw WHERE fetched_at >= toDateTime64('{since}',3,'Asia/Seoul')")[0][0]
    md.append("## Totals")
    md.append(md_table(["metric", "value"], [
        ["distinct course(locale) (latest dim)", total_courses],
        ["distinct instructors", total_instructors],
        ["snapshots (last N days)", total_snapshots],
    ]))
    md.append("")

    # Locale breakdown (latest dim)
    loc = q(ch, f"""
      SELECT locale,
        count() AS course_cnt,
        sum(student_count) AS students_sum,
        sum(like_count) AS likes_sum,
        sum(review_count) AS reviews_sum,
        round(avg(average_star), 4) AS avg_star
      FROM ({latest_dim})
      GROUP BY locale
      ORDER BY course_cnt DESC
    """)
    md.append("## Locale breakdown (latest)")
    md.append(md_table(["locale","courses","students_sum","likes_sum","reviews_sum","avg_star"], loc))
    md.append("")

    # Price/Discount breakdown (latest price)
    price = q(ch, f"""
      SELECT
        locale,
        count() AS course_cnt,
        round(avg(krw_regular_price), 0) AS avg_regular,
        round(avg(krw_pay_price), 0) AS avg_pay,
        round(avg(discount_rate), 4) AS avg_discount_rate,
        sum(discount_rate > 0) AS discounted_cnt
      FROM ({latest_price})
      GROUP BY locale
      ORDER BY course_cnt DESC
    """)
    md.append("## Price & Discount (latest)")
    md.append(md_table(["locale","courses","avg_regular_krw","avg_pay_krw","avg_discount_rate","discounted_cnt"], price))
    md.append("")

    # Time-bucket stats by fetched_at (snapshot_raw) - distinct course(locale)
    def time_bucket(bucket_expr: str, label: str, limit: int = 200):
        rows = q(ch, f"""
          SELECT
            {bucket_expr} AS bucket,
            uniqExact(cityHash64(toString(course_id), locale)) AS courses,
            count() AS snapshots
          FROM {db}.inflearn_course_snapshot_raw
          WHERE fetched_at >= toDateTime64('{since}',3,'Asia/Seoul')
          GROUP BY bucket
          ORDER BY bucket DESC
          LIMIT {limit}
        """)
        # cumulative ascending
        rows_asc = list(reversed(rows))
        cum_courses = 0
        cum_snaps = 0
        out = []
        for b,c,s in rows_asc:
            cum_courses += int(c)
            cum_snaps += int(s)
            out.append([b, c, s, cum_courses, cum_snaps])
        return label, list(reversed(out))  # show latest first

    md.append("## Collected-at based stats (snapshot_raw)")
    for bucket_expr,label in [
        ("toStartOfHour(fetched_at)", "Hourly"), 
        ("toDate(fetched_at)", "Daily"), 
        ("toStartOfMonth(fetched_at)", "Monthly"), 
        ("toStartOfYear(fetched_at)", "Yearly"), 
    ]:
        title, rows = time_bucket(bucket_expr, label, limit=240)
        md.append(f"### {title}")
        md.append(md_table(["bucket","courses","snapshots","cum_courses","cum_snapshots"], rows[:120]))
        md.append("")

    # Published-at stats (from latest_dim)
    md.append("## Published-at based stats (course_dim latest)")
    for bucket_expr,label in [
        ("toDate(published_at)", "Daily"), 
        ("toStartOfMonth(published_at)", "Monthly"), 
        ("toStartOfYear(published_at)", "Yearly"), 
    ]:
        rows = q(ch, f"""
          SELECT
            {bucket_expr} AS bucket,
            count() AS courses
          FROM ({latest_dim})
          WHERE published_at >= toDateTime64('{since}',3,'Asia/Seoul')
          GROUP BY bucket
          ORDER BY bucket DESC
          LIMIT 240
        """)
        rows_asc = list(reversed(rows))
        cum = 0
        out = []
        for b,c in rows_asc:
            cum += int(c)
            out.append([b,c,cum])
        md.append(f"### {label}")
        md.append(md_table(["bucket","courses","cum_courses"], list(reversed(out))[:120]))
        md.append("")

    report = "\n".join(md)
    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(report + "\n")

if __name__ == "__main__":
    main()
