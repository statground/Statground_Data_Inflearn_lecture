#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Batch #3: 인프런 강의 통계 리포트 생성(커밋용)

- ClickHouse 조회 → Markdown + PNG 차트 생성 → (Workflow에서) git commit/push
- 긴 테이블(시간/일/월/연 시계열)은 표를 출력하지 않고 **요약 + 차트**만 제공
- 리포트/로그에 DB 접속정보(호스트/포트/DB명 등) 유추 가능한 문구를 출력하지 않음
- 데이터 소스 상태 섹션 출력 금지(요청 반영)

출력물:
- reports/inflearn/inflearn_stats_latest.md
- reports/inflearn/inflearn_stats_YYYYMMDD_HH.md
- reports/inflearn/charts/*.png
"""

from __future__ import annotations

import os
import sys
import re
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import clickhouse_connect

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def env_first(*names: str) -> Optional[str]:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return v
    return None


def parse_port(val: Optional[str], default: int = 8123) -> int:
    if val is None:
        return default
    s = str(val).strip()
    if s == "":
        return default
    m = re.search(r"(\d+)", s)
    if not m:
        return default
    try:
        return int(m.group(1))
    except Exception:
        return default


def fmt_num(x: Any) -> str:
    try:
        if x is None:
            return "0"
        if isinstance(x, bool):
            return "1" if x else "0"
        if isinstance(x, int):
            return f"{x:,}"
        if isinstance(x, float):
            if not math.isfinite(x):
                return "0"
            # 소수점은 상황에 따라
            if abs(x) >= 1000:
                return f"{x:,.0f}"
            return f"{x:,.2f}"
        s = str(x)
        if s.isdigit():
            return f"{int(s):,}"
        return s
    except Exception:
        return str(x)


def set_korean_font():
    # workflow에서 fonts-noto-cjk 설치
    plt.rcParams["font.family"] = "Noto Sans CJK KR"
    plt.rcParams["axes.unicode_minus"] = False


def ch_client():
    host = env_first("CH_HOST", "CLICKHOUSE_HOST")
    if not host:
        raise RuntimeError("CH_HOST(또는 CLICKHOUSE_HOST) 환경변수가 필요합니다.")
    port = parse_port(env_first("CH_PORT", "CLICKHOUSE_PORT"), default=8123)
    user = env_first("CH_USER", "CLICKHOUSE_USER") or "default"
    password = env_first("CH_PASSWORD", "CLICKHOUSE_PASSWORD") or ""
    database = env_first("CH_DATABASE", "CLICKHOUSE_DATABASE") or "default"

    # 접속정보 출력 금지
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
        secure=False,
    )
    return client, database


def q(client, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    res = client.query(sql, parameters=params or {})
    cols = res.column_names
    out: List[Dict[str, Any]] = []
    for row in res.result_rows:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def safe_query(client, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    try:
        return q(client, sql, params=params)
    except Exception:
        return []


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def relpath_for_md(img_path: Path, md_path: Path) -> str:
    # md 파일 기준 상대경로(깨짐 방지)
    return img_path.relative_to(md_path.parent).as_posix()


def plot_line(xs: List[str], ys: List[float], title: str, xlabel: str, ylabel: str, out_path: Path):
    plt.figure(figsize=(10, 4))
    if len(xs) == 0:
        plt.title(title)
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return

    plt.plot(list(range(len(xs))), ys)
    if len(xs) <= 24:
        plt.xticks(list(range(len(xs))), xs, rotation=45, ha="right")
    else:
        step = max(1, len(xs) // 12)
        ticks = list(range(0, len(xs), step))
        labels = [xs[i] for i in ticks]
        plt.xticks(ticks, labels, rotation=45, ha="right")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_hist(values: List[float], title: str, xlabel: str, ylabel: str, out_path: Path, bins: int = 50, logy: bool = False):
    plt.figure(figsize=(10, 4))
    if values:
        plt.hist(values, bins=bins)
        if logy:
            plt.yscale("log")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def summarize_timeseries(rows: List[Dict[str, Any]], value_col: str) -> Dict[str, Any]:
    if not rows:
        return {"total": 0.0, "last": 0.0, "peak": 0.0}
    vals: List[float] = []
    for r in rows:
        v = r.get(value_col)
        try:
            v = 0.0 if v is None else float(v)
        except Exception:
            v = 0.0
        vals.append(v)
    return {"total": sum(vals), "last": vals[-1], "peak": max(vals)}


def md_section_timeseries(md: List[str], title: str, desc: str, chart_rel: str, rows: List[Dict[str, Any]], value_col: str):
    s = summarize_timeseries(rows, value_col)
    md.append(f"## {title}")
    md.append("")
    md.append(desc)
    md.append("")
    md.append(f"- 전체 합계: **{fmt_num(s['total'])}**")
    md.append(f"- 마지막 구간 값: **{fmt_num(s['last'])}**")
    md.append(f"- 최고 피크: **{fmt_num(s['peak'])}**")
    md.append("")
    md.append(f"![{title}]({chart_rel})")
    md.append("")


def md_section_dist(md: List[str], title: str, desc: str, chart_rel: str, bullets: List[str]):
    md.append(f"## {title}")
    md.append("")
    md.append(desc)
    md.append("")
    for b in bullets:
        md.append(f"- {b}")
    md.append("")
    md.append(f"![{title}]({chart_rel})")
    md.append("")


def main():
    set_korean_font()
    client, db = ch_client()

    report_dir = Path("reports") / "inflearn"
    charts_dir = report_dir / "charts"
    ensure_dir(charts_dir)

    ts = now_kst().strftime("%Y%m%d_%H")
    report_latest = report_dir / "inflearn_stats_latest.md"
    report_stamp = report_dir / f"inflearn_stats_{ts}.md"

    lookback_days = int(os.environ.get("STATS_LOOKBACK_DAYS", "365"))
    since_dt = (now_kst() - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")

    # 테이블명(기본값)
    T_SNAPSHOT = os.environ.get("INFLEARN_T_SNAPSHOT", "inflearn_course_snapshot_raw")
    T_DIM = os.environ.get("INFLEARN_T_DIM", "inflearn_course_dim")
    T_METRIC = os.environ.get("INFLEARN_T_METRIC", "inflearn_course_metric_fact")
    T_PRICE = os.environ.get("INFLEARN_T_PRICE", "inflearn_course_price_fact")

    md: List[str] = []
    md.append("# 인프런 강의 수집 통계 리포트")
    md.append("")
    md.append(f"- 생성 시각(KST): **{now_kst().strftime('%Y-%m-%d %H:%M:%S')}**")
    md.append("- 데이터가 길어지는 구간(시간/일/월/연)은 **표를 생략**하고 차트+요약으로 대체합니다.")
    md.append("")

    # -------- 전체 요약 --------
    md.append("## 전체 요약")
    md.append("")
    total_courses = safe_query(client, f"SELECT uniqExact(course_id) AS cnt FROM {db}.{T_DIM}")
    if total_courses:
        md.append(f"- 누적 수집 강의(고유 course_id): **{fmt_num(total_courses[0].get('cnt'))}**")
    locale_top = safe_query(client, f"SELECT locale, uniqExact(course_id) AS cnt FROM {db}.{T_DIM} GROUP BY locale ORDER BY cnt DESC LIMIT 10")
    if locale_top:
        md.append("- 언어별(상위): " + ", ".join([f"{r.get('locale')}:{fmt_num(r.get('cnt'))}" for r in locale_top]))
    md.append("")

    # -------- 수집 시점 기준: fetched_at --------
    hourly = safe_query(client, f"""
SELECT formatDateTime(toStartOfHour(fetched_at), '%Y-%m-%d %H') AS bucket,
       count() AS cnt
FROM {db}.{T_SNAPSHOT}
WHERE fetched_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    daily = safe_query(client, f"""
SELECT formatDateTime(toDate(fetched_at), '%Y-%m-%d') AS bucket,
       count() AS cnt
FROM {db}.{T_SNAPSHOT}
WHERE fetched_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    monthly = safe_query(client, f"""
SELECT formatDateTime(toStartOfMonth(fetched_at), '%Y-%m') AS bucket,
       count() AS cnt
FROM {db}.{T_SNAPSHOT}
WHERE fetched_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    yearly = safe_query(client, f"""
SELECT toString(toYear(fetched_at)) AS bucket,
       count() AS cnt
FROM {db}.{T_SNAPSHOT}
WHERE fetched_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    p_hourly = charts_dir / "collected_hourly_snapshots.png"
    p_daily = charts_dir / "collected_daily_snapshots.png"
    p_monthly = charts_dir / "collected_monthly_snapshots.png"
    p_yearly = charts_dir / "collected_yearly_snapshots.png"

    plot_line([r["bucket"] for r in hourly], [float(r.get("cnt") or 0) for r in hourly], "수집 시점 기준 - 시간별 스냅샷", "시간(YYYY-MM-DD HH)", "스냅샷 수", p_hourly)
    plot_line([r["bucket"] for r in daily], [float(r.get("cnt") or 0) for r in daily], "수집 시점 기준 - 일별 스냅샷", "일자", "스냅샷 수", p_daily)
    plot_line([r["bucket"] for r in monthly], [float(r.get("cnt") or 0) for r in monthly], "수집 시점 기준 - 월별 스냅샷", "월", "스냅샷 수", p_monthly)
    plot_line([r["bucket"] for r in yearly], [float(r.get("cnt") or 0) for r in yearly], "수집 시점 기준 - 연별 스냅샷", "연도", "스냅샷 수", p_yearly)

    md_section_timeseries(md, "수집 시점 기준 - 시간별 스냅샷", "최근 기간(설정값 기준) 시간별 스냅샷 수 추이입니다.", relpath_for_md(p_hourly, report_latest), hourly, "cnt")
    md_section_timeseries(md, "수집 시점 기준 - 일별 스냅샷", "최근 기간(설정값 기준) 일별 스냅샷 수 추이입니다.", relpath_for_md(p_daily, report_latest), daily, "cnt")
    md_section_timeseries(md, "수집 시점 기준 - 월별 스냅샷", "최근 기간(설정값 기준) 월별 스냅샷 수 추이입니다.", relpath_for_md(p_monthly, report_latest), monthly, "cnt")
    md_section_timeseries(md, "수집 시점 기준 - 연별 스냅샷", "최근 기간(설정값 기준) 연별 스냅샷 수 추이입니다.", relpath_for_md(p_yearly, report_latest), yearly, "cnt")

    # -------- 개설 시점 기준: published_at --------
    pub_daily = safe_query(client, f"""
SELECT formatDateTime(toDate(published_at), '%Y-%m-%d') AS bucket,
       count() AS cnt
FROM {db}.{T_DIM}
WHERE published_at IS NOT NULL
  AND published_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    pub_monthly = safe_query(client, f"""
SELECT formatDateTime(toStartOfMonth(published_at), '%Y-%m') AS bucket,
       count() AS cnt
FROM {db}.{T_DIM}
WHERE published_at IS NOT NULL
  AND published_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    pub_yearly = safe_query(client, f"""
SELECT toString(toYear(published_at)) AS bucket,
       count() AS cnt
FROM {db}.{T_DIM}
WHERE published_at IS NOT NULL
  AND published_at >= toDateTime(%(since)s)
GROUP BY bucket
ORDER BY bucket
""", {"since": since_dt})

    p_pub_d = charts_dir / "published_daily_courses.png"
    p_pub_m = charts_dir / "published_monthly_courses.png"
    p_pub_y = charts_dir / "published_yearly_courses.png"
    plot_line([r["bucket"] for r in pub_daily], [float(r.get("cnt") or 0) for r in pub_daily], "개설 시점 기준 - 일별 신규 강의", "일자", "강의 수", p_pub_d)
    plot_line([r["bucket"] for r in pub_monthly], [float(r.get("cnt") or 0) for r in pub_monthly], "개설 시점 기준 - 월별 신규 강의", "월", "강의 수", p_pub_m)
    plot_line([r["bucket"] for r in pub_yearly], [float(r.get("cnt") or 0) for r in pub_yearly], "개설 시점 기준 - 연별 신규 강의", "연도", "강의 수", p_pub_y)

    md_section_timeseries(md, "개설 시점 기준 - 일별 신규 강의", "published_at 기준으로 집계한 일별 신규 강의 수 추이입니다.", relpath_for_md(p_pub_d, report_latest), pub_daily, "cnt")
    md_section_timeseries(md, "개설 시점 기준 - 월별 신규 강의", "published_at 기준으로 집계한 월별 신규 강의 수 추이입니다.", relpath_for_md(p_pub_m, report_latest), pub_monthly, "cnt")
    md_section_timeseries(md, "개설 시점 기준 - 연별 신규 강의", "published_at 기준으로 집계한 연별 신규 강의 수 추이입니다.", relpath_for_md(p_pub_y, report_latest), pub_yearly, "cnt")

    # -------- 분포(최신값 기준) --------
    metric = safe_query(client, f"""
SELECT
  course_id,
  locale,
  argMax(student_count, fetched_at) AS student_count,
  argMax(like_count, fetched_at) AS like_count,
  argMax(review_count, fetched_at) AS review_count,
  argMax(average_star, fetched_at) AS average_star
FROM {db}.{T_METRIC}
GROUP BY course_id, locale
""")

    if metric:
        students = [float(r.get("student_count") or 0) for r in metric]
        likes = [float(r.get("like_count") or 0) for r in metric]
        reviews = [float(r.get("review_count") or 0) for r in metric]
        stars = [float(r.get("average_star") or 0) for r in metric]

        p = charts_dir / "dist_student_count.png"
        plot_hist(students, "수강생 수 분포", "수강생 수", "강의 수", p, bins=60, logy=True)
        md_section_dist(md, "수강생 수 분포", "고유 강의별(최신값 기준) 수강생 수 분포입니다. (y축 로그)", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(students))}**", f"최대: **{fmt_num(max(students) if students else 0)}**"])

        p = charts_dir / "dist_like_count.png"
        plot_hist(likes, "좋아요 수 분포", "좋아요 수", "강의 수", p, bins=60, logy=True)
        md_section_dist(md, "좋아요 수 분포", "고유 강의별(최신값 기준) 좋아요 수 분포입니다. (y축 로그)", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(likes))}**", f"최대: **{fmt_num(max(likes) if likes else 0)}**"])

        p = charts_dir / "dist_review_count.png"
        plot_hist(reviews, "리뷰 수 분포", "리뷰 수", "강의 수", p, bins=60, logy=True)
        md_section_dist(md, "리뷰 수 분포", "고유 강의별(최신값 기준) 리뷰 수 분포입니다. (y축 로그)", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(reviews))}**", f"최대: **{fmt_num(max(reviews) if reviews else 0)}**"])

        p = charts_dir / "dist_average_star.png"
        plot_hist(stars, "평균 별점 분포", "평균 별점", "강의 수", p, bins=40, logy=False)
        md_section_dist(md, "평균 별점 분포", "고유 강의별(최신값 기준) 평균 별점 분포입니다.", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(stars))}**"])

    price = safe_query(client, f"""
SELECT
  course_id,
  locale,
  argMax(krw_regular_price, fetched_at) AS krw_regular_price,
  argMax(krw_pay_price, fetched_at) AS krw_pay_price,
  argMax(discount_rate, fetched_at) AS discount_rate
FROM {db}.{T_PRICE}
GROUP BY course_id, locale
""")

    if price:
        reg = [float(r.get("krw_regular_price") or 0) for r in price]
        pay = [float(r.get("krw_pay_price") or 0) for r in price]
        disc = [float(r.get("discount_rate") or 0) for r in price]

        p = charts_dir / "dist_price_regular_krw.png"
        plot_hist(reg, "정가(KRW) 분포", "정가(KRW)", "강의 수", p, bins=60, logy=True)
        md_section_dist(md, "정가(KRW) 분포", "고유 강의별(최신값 기준) 정가 분포입니다. (y축 로그)", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(reg))}**", f"최대: **{fmt_num(max(reg) if reg else 0)}**"])

        p = charts_dir / "dist_price_pay_krw.png"
        plot_hist(pay, "판매가(KRW) 분포", "판매가(KRW)", "강의 수", p, bins=60, logy=True)
        md_section_dist(md, "판매가(KRW) 분포", "고유 강의별(최신값 기준) 판매가 분포입니다. (y축 로그)", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(pay))}**", f"최대: **{fmt_num(max(pay) if pay else 0)}**"])

        p = charts_dir / "dist_discount_rate.png"
        plot_hist(disc, "할인율 분포", "할인율(%)", "강의 수", p, bins=40, logy=False)
        md_section_dist(md, "할인율 분포", "고유 강의별(최신값 기준) 할인율 분포입니다.", relpath_for_md(p, report_latest),
                        [f"표본 수: **{fmt_num(len(disc))}**"])

    md.append("---")
    md.append("### 메모")
    md.append("- 본 리포트는 GitHub Actions에서 자동 생성됩니다.")
    md.append("- 접속정보(호스트/포트/DB명 등)는 리포트/로그에 출력하지 않습니다.")
    md.append("")

    content = "\n".join(md) + "\n"
    report_latest.write_text(content, encoding="utf-8")
    report_stamp.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # 세부 정보 숨김
        print("통계 리포트 생성 중 오류가 발생했습니다. (세부 정보는 숨김 처리)", file=sys.stderr)
        raise
