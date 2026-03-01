#!/usr/bin/env python3
"""
Batch #3: generate statistics and commit artifacts to GitHub.

Artifacts (committed):
- reports/inflearn/inflearn_stats_latest.md
- reports/inflearn/inflearn_stats_<YYYYMMDD_HH>.md
- reports/inflearn/charts/*.png

Notes:
- Uses ClickHouse as source.
- Produces collected-at (fetched_at) stats and published-at stats.
- Produces total / yearly / monthly / daily / hourly + cumulative.
"""

import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import clickhouse_connect

# matplotlib is used only for generating png charts saved to repo.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


KST = ZoneInfo("Asia/Seoul")

def _parse_port(raw: str, default: int) -> int:
    raw = (raw or "").strip()
    m = re.search(r"\d+", raw)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default


def ch_client():
    # Prefer CH_* (repo secrets), fallback to CLICKHOUSE_*
    host = (os.environ.get("CH_HOST") or os.environ.get("CLICKHOUSE_HOST") or "").strip()
    port_raw = os.environ.get("CH_PORT") or os.environ.get("CLICKHOUSE_PORT") or ""
    port = _parse_port(str(port_raw), 8123)
    user = (os.environ.get("CH_USER") or os.environ.get("CLICKHOUSE_USER") or "default").strip()
    password = os.environ.get("CH_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD") or ""
    database = (os.environ.get("CH_DATABASE") or os.environ.get("CLICKHOUSE_DATABASE") or "default").strip()
    if not host:
        raise RuntimeError("CH_HOST (or CLICKHOUSE_HOST) is required")
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database
    ), database


def _get_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default




def md_table(headers, rows, max_rows: int | None = None):
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for i, r in enumerate(rows):
        if max_rows is not None and i >= max_rows:
            out.append(f"| ... | ... |")
            break
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)


def q(ch, sql):
    return ch.query(sql).result_rows


def save_line_chart(x, y, title: str, out_path: Path, xlabel: str = "", ylabel: str = ""):
    plt.figure()
    plt.plot(x, y)
    plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path)
    plt.close()


def save_hist(values, title: str, out_path: Path, bins: int = 50, xlabel: str = ""):
    plt.figure()
    plt.hist(values, bins=bins)
    plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path)
    plt.close()


def main():
    ch, db = ch_client()

    now = datetime.now(tz=KST)
    lookback_days = _get_int("STATS_LOOKBACK_DAYS", 365)
    since_dt = now - timedelta(days=lookback_days)
    since = since_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Repo paths
    repo_root = Path(__file__).resolve().parents[1]
    report_dir = repo_root / "reports" / "inflearn"
    chart_dir = report_dir / "charts"
    report_dir.mkdir(parents=True, exist_ok=True)
    chart_dir.mkdir(parents=True, exist_ok=True)

    ts_tag = now.strftime("%Y%m%d_%H")
    report_latest = report_dir / "inflearn_stats_latest.md"
    report_hourly = report_dir / f"inflearn_stats_{ts_tag}.md"

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
    md.append("# 인프런 강의 수집 통계")
    md.append(f"- 생성 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    md.append(f"- 조회 범위: 최근 {lookback_days}일 (시작일: {since})")
    md.append("")

    # Totals
    total_courses = q(ch, f"SELECT uniqExact(cityHash64(toString(course_id), locale)) FROM ({latest_dim})")[0][0]
    total_instructors = q(ch, f"SELECT uniqExact(instructor_id) FROM {db}.inflearn_instructor_dim")[0][0]
    total_snapshots = q(ch, f"""
        SELECT count()
        FROM {db}.inflearn_course_snapshot_raw
        WHERE fetched_at >= toDateTime64('{since}',3,'Asia/Seoul')
    """)[0][0]

    md.append("## Totals")
    md.append(md_table(["metric", "value"], [
        ["distinct course(locale) (latest dim)", total_courses],
        ["distinct instructors", total_instructors],
        [f"snapshots (last {lookback_days}d)", total_snapshots],
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
        sum(discount_rate > 0) AS discounted_cnt,
        round(sum(discount_rate > 0) / count(), 4) AS discounted_ratio
      FROM ({latest_price})
      GROUP BY locale
      ORDER BY course_cnt DESC
    """)
    md.append("## Price & Discount (latest)")
    md.append(md_table(["locale","courses","avg_regular_krw","avg_pay_krw","avg_discount_rate","discounted_cnt","discounted_ratio"], price))
    md.append("")

    # -----------------------
    # Collected-at based stats (snapshot_raw)
    # -----------------------
    def time_bucket(bucket_expr: str, label: str, limit: int = 400):
        rows = q(ch, f"""
          SELECT
            {bucket_expr} AS bucket,
            uniqExact(cityHash64(toString(course_id), locale)) AS courses,
            count() AS snapshots
          FROM {db}.inflearn_course_snapshot_raw
          WHERE fetched_at >= toDateTime64('{since}',3,'Asia/Seoul')
          GROUP BY bucket
          ORDER BY bucket ASC
          LIMIT {limit}
        """)
        cum_courses = 0
        cum_snaps = 0
        out = []
        for b, c, s in rows:
            cum_courses += int(c)
            cum_snaps += int(s)
            out.append([b, c, s, cum_courses, cum_snaps])
        return label, out

    md.append("## 수집 시점 기준 통계 (snapshot_raw)")
    collected_series = {}
    for bucket_expr, label, limit in [
        ("toStartOfHour(fetched_at)", "Hourly", 24 * 30),
        ("toDate(fetched_at)", "Daily", 400),
        ("toStartOfMonth(fetched_at)", "Monthly", 200),
        ("toStartOfYear(fetched_at)", "Yearly", 50),
    ]:
        title, rows = time_bucket(bucket_expr, label, limit=limit)
        collected_series[label] = rows
        md.append(f"### {title}")
        # Show latest first in MD
        md.append(md_table(["bucket","courses","snapshots","cum_courses","cum_snapshots"], list(reversed(rows)), max_rows=120))
        md.append("")

    # Charts (collected-at): snapshots time-series
    for label, rows in collected_series.items():
        x = [str(r[0]) for r in rows]
        y = [int(r[2]) for r in rows]
        save_line_chart(x, y, f"Collected-at {label}: snapshots", chart_dir / f"collected_{label.lower()}_snapshots.png", xlabel="time", ylabel="snapshots")

    # -----------------------
    # Published-at based stats (course_dim latest)
    # -----------------------
    md.append("## 개설 시점 기준 통계 (course_dim 최신)")
    published_series = {}
    for bucket_expr, label, limit in [
        ("toDate(published_at)", "Daily", 400),
        ("toStartOfMonth(published_at)", "Monthly", 200),
        ("toStartOfYear(published_at)", "Yearly", 80),
    ]:
        rows = q(ch, f"""
          SELECT
            {bucket_expr} AS bucket,
            count() AS courses
          FROM ({latest_dim})
          WHERE published_at >= toDateTime64('{since}',3,'Asia/Seoul')
          GROUP BY bucket
          ORDER BY bucket ASC
          LIMIT {limit}
        """)
        cum = 0
        out = []
        for b, c in rows:
            cum += int(c)
            out.append([b, c, cum])
        published_series[label] = out
        md.append(f"### {label}")
        md.append(md_table(["bucket","courses","cum_courses"], list(reversed(out)), max_rows=120))
        md.append("")

    # Charts (published-at): courses time-series
    for label, rows in published_series.items():
        x = [str(r[0]) for r in rows]
        y = [int(r[1]) for r in rows]
        save_line_chart(x, y, f"Published-at {label}: new courses", chart_dir / f"published_{label.lower()}_courses.png", xlabel="time", ylabel="courses")

    # -----------------------
    # Distributions (latest)
    # -----------------------
    md.append("## Distributions (latest, top-level)")
    dist_rows = q(ch, f"""
      SELECT
        student_count,
        like_count,
        review_count,
        average_star
      FROM ({latest_dim})
      WHERE last_fetched_at >= toDateTime64('{since}',3,'Asia/Seoul')
      LIMIT 200000
    """)
    students = [int(r[0]) for r in dist_rows if r[0] is not None]
    likes = [int(r[1]) for r in dist_rows if r[1] is not None]
    reviews = [int(r[2]) for r in dist_rows if r[2] is not None]
    stars = [float(r[3]) for r in dist_rows if r[3] is not None]

    md.append(md_table(["metric", "count", "p50", "p90", "p99", "max"], [
        ["student_count", len(students),
         q(ch, f"SELECT quantile(0.5)(student_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.9)(student_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.99)(student_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT max(student_count) FROM ({latest_dim})")[0][0]],
        ["like_count", len(likes),
         q(ch, f"SELECT quantile(0.5)(like_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.9)(like_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.99)(like_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT max(like_count) FROM ({latest_dim})")[0][0]],
        ["review_count", len(reviews),
         q(ch, f"SELECT quantile(0.5)(review_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.9)(review_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.99)(review_count) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT max(review_count) FROM ({latest_dim})")[0][0]],
        ["average_star", len(stars),
         q(ch, f"SELECT quantile(0.5)(average_star) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.9)(average_star) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT quantile(0.99)(average_star) FROM ({latest_dim})")[0][0],
         q(ch, f"SELECT max(average_star) FROM ({latest_dim})")[0][0]],
    ]))
    md.append("")

    # Histograms
    if students:
        save_hist(students, "student_count distribution", chart_dir / "dist_student_count.png", bins=60, xlabel="students")
    if likes:
        save_hist(likes, "like_count distribution", chart_dir / "dist_like_count.png", bins=60, xlabel="likes")
    if reviews:
        save_hist(reviews, "review_count distribution", chart_dir / "dist_review_count.png", bins=60, xlabel="reviews")
    if stars:
        save_hist(stars, "average_star distribution", chart_dir / "dist_average_star.png", bins=40, xlabel="star")

    # Price distributions
    price_rows = q(ch, f"""
      SELECT
        krw_regular_price,
        krw_pay_price,
        discount_rate
      FROM ({latest_price})
      LIMIT 200000
    """)
    reg = [float(r[0]) for r in price_rows if r[0] is not None]
    pay = [float(r[1]) for r in price_rows if r[1] is not None]
    disc = [float(r[2]) for r in price_rows if r[2] is not None]
    if reg:
        save_hist(reg, "regular price (KRW) distribution", chart_dir / "dist_price_regular_krw.png", bins=60, xlabel="KRW")
    if pay:
        save_hist(pay, "pay price (KRW) distribution", chart_dir / "dist_price_pay_krw.png", bins=60, xlabel="KRW")
    if disc:
        save_hist(disc, "discount_rate distribution", chart_dir / "dist_discount_rate.png", bins=50, xlabel="rate")

    # Embed images in MD (markdown 파일 기준 상대경로)
    md.append("## 차트")
    chart_files = sorted([p for p in chart_dir.glob("*.png")])
    for p in chart_files:
        # GitHub는 Markdown 링크를 '현재 md 파일 위치' 기준으로 해석하므로,
        # 레포 루트 기준 경로를 그대로 넣으면 (reports/inflearn/...) 처럼 중복되어 깨질 수 있음.
        rel = os.path.relpath(p, report_latest.parent).replace('\', '/')
        md.append(f"### {p.name}")
        md.append(f"![{p.name}]({rel})")
        md.append("")


    report = "\n".join(md)

    # Write artifacts
    report_latest.write_text(report, encoding="utf-8")
    report_hourly.write_text(report, encoding="utf-8")

    # Console
    print(report)

    # Also write step summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"## Inflearn stats artifacts\n\n")
            f.write(f"- `{report_latest.relative_to(repo_root)}`\n")
            f.write(f"- `{report_hourly.relative_to(repo_root)}`\n")
            f.write(f"- charts: `{chart_dir.relative_to(repo_root)}/`\n\n")


if __name__ == "__main__":
    main()
