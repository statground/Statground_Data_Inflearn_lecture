#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inflearn 통계 리포트 (GitHub Actions)
- md 1개만 덮어쓰기: reports/inflearn/inflearn_stats_latest.md
- 차트는 reports/inflearn/charts/에 생성
- 기준 컬럼은 스키마 그대로 사용 (snapshot_raw: fetched_at 등)
- 데이터 소스 상태(테이블 존재 여부 등) 출력하지 않음
- DB 접속정보 유추 가능한 값(호스트/포트/DB명) 출력하지 않음
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import clickhouse_connect

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


KST = timezone(timedelta(hours=9))


def parse_port(v: Any, default: int = 8123) -> int:
    if v is None:
        return default
    s = str(v).strip()
    if s == "":
        return default
    m = re.search(r"(\d+)", s)
    if not m:
        return default
    try:
        return int(m.group(1))
    except Exception:
        return default


def env_first(*names: str) -> str | None:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def ch_client():
    host = env_first("CH_HOST", "CLICKHOUSE_HOST")
    if not host:
        raise RuntimeError("CH_HOST(또는 CLICKHOUSE_HOST) 환경변수가 필요합니다.")
    port = parse_port(env_first("CH_PORT", "CLICKHOUSE_PORT"), 8123)
    user = env_first("CH_USER", "CLICKHOUSE_USER") or "default"
    password = env_first("CH_PASSWORD", "CLICKHOUSE_PASSWORD") or ""
    database = env_first("CH_DATABASE", "CLICKHOUSE_DATABASE") or "default"
    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database, secure=False
    )
    return client, database


def q(client, sql: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    res = client.query(sql, parameters=params or {})
    cols = res.column_names
    out = []
    for row in res.result_rows:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def fmt_int(x: Any) -> str:
    try:
        return f"{int(x or 0):,}"
    except Exception:
        return "0"


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def set_english_font():
    # Use a default font to avoid Korean glyph dependence; chart text is in English.
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.rcParams["axes.unicode_minus"] = False


def plot_bar(xs: List[str], ys: List[float], title: str, xlabel: str, ylabel: str, out: Path):
    plt.figure(figsize=(10, 4))
    plt.bar(list(range(len(xs))), ys)

    if len(xs) <= 24:
        plt.xticks(list(range(len(xs))), xs, rotation=45, ha="right")
    else:
        step = max(1, len(xs) // 12)
        ticks = list(range(0, len(xs), step))
        plt.xticks(ticks, [xs[i] for i in ticks], rotation=45, ha="right")

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out)
    plt.close()


def main():
    set_english_font()
    client, db = ch_client()

    report_dir = Path("reports") / "inflearn"
    charts_dir = report_dir / "charts"
    ensure_dir(charts_dir)

    report_md = report_dir / "inflearn_stats_latest.md"

    # 테이블명 (환경변수로 오버라이드 가능)
    T_SNAPSHOT = os.environ.get("INFLEARN_T_SNAPSHOT", "inflearn_course_snapshot_raw")
    T_DIM = os.environ.get("INFLEARN_T_DIM", "inflearn_course_dim")
    T_METRIC = os.environ.get("INFLEARN_T_METRIC", "inflearn_course_metric_fact")
    T_PRICE = os.environ.get("INFLEARN_T_PRICE", "inflearn_course_price_fact")

    lookback_days = int(os.environ.get("STATS_LOOKBACK_DAYS", "365"))

    # ✅ since 계산을 "현재시간"이 아니라 "데이터의 최신값" 기준으로 잡아 0 튀는 문제를 줄임
    mx = q(client, f"SELECT max(fetched_at) AS mx FROM {db}.{T_SNAPSHOT}")
    mx_dt = mx[0]["mx"] if mx and mx[0].get("mx") else None
    if mx_dt is None:
        # 데이터가 정말 없는 경우는 그대로 빈 리포트
        now = datetime.now(tz=KST)
        since_dt = now - timedelta(days=lookback_days)
    else:
        since_dt = mx_dt - timedelta(days=lookback_days)

    md: List[str] = []
    md.append("# Inflearn 통계 리포트")
    md.append("")
    md.append(f"- 생성 시각(KST): **{datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')}**")
    md.append(f"- 최근 데이터 기준 Lookback: **{lookback_days}일**")
    md.append("")

    # 1) 수집 시점(fetched_at) 기준: 시간/일/월/년 차트 + 요약
    hourly = q(client, f"""
        SELECT formatDateTime(toStartOfHour(fetched_at), '%%Y-%%m-%%d %%H') AS bucket,
               count() AS cnt
        FROM {db}.{T_SNAPSHOT}
        WHERE fetched_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})
    daily = q(client, f"""
        SELECT formatDateTime(toDate(fetched_at), '%%Y-%%m-%%d') AS bucket,
               count() AS cnt
        FROM {db}.{T_SNAPSHOT}
        WHERE fetched_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})
    monthly = q(client, f"""
        SELECT formatDateTime(toStartOfMonth(fetched_at), '%%Y-%%m') AS bucket,
               count() AS cnt
        FROM {db}.{T_SNAPSHOT}
        WHERE fetched_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})
    yearly = q(client, f"""
        SELECT toString(toYear(fetched_at)) AS bucket,
               count() AS cnt
        FROM {db}.{T_SNAPSHOT}
        WHERE fetched_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})

    def add_ts_section(title: str, rows: List[Dict[str, Any]], png_name: str, xlabel: str):
        xs = [r["bucket"] for r in rows]
        ys = [float(r.get("cnt") or 0) for r in rows]
        out_png = charts_dir / png_name
        if xs:
            plot_bar(xs, ys, title, xlabel, "Snapshots", out_png)
        total = sum(int(r.get("cnt") or 0) for r in rows)
        last = int(rows[-1].get("cnt") or 0) if rows else 0
        peak = max((int(r.get("cnt") or 0) for r in rows), default=0)

        md.append(f"## {title}")
        md.append("")
        md.append(f"- 합계: **{fmt_int(total)}**, 마지막: **{fmt_int(last)}**, 피크: **{fmt_int(peak)}**")
        md.append("")
        if xs:
            md.append(f"![{title}](charts/{png_name})")
            md.append("")

    add_ts_section("Snapshots by Fetch Hour", hourly, "snapshots_hourly.png", "Hour (YYYY-MM-DD HH)")
    add_ts_section("Snapshots by Fetch Day", daily, "snapshots_daily.png", "Date")
    add_ts_section("Snapshots by Fetch Month", monthly, "snapshots_monthly.png", "Month")
    add_ts_section("Snapshots by Fetch Year", yearly, "snapshots_yearly.png", "Year")

    # 2) 개설 시점(published_at) 기준: 일/월/년 (course_dim)
    pub_daily = q(client, f"""
        SELECT formatDateTime(toDate(published_at), '%%Y-%%m-%%d') AS bucket,
               count() AS cnt
        FROM {db}.{T_DIM}
        WHERE published_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})
    pub_monthly = q(client, f"""
        SELECT formatDateTime(toStartOfMonth(published_at), '%%Y-%%m') AS bucket,
               count() AS cnt
        FROM {db}.{T_DIM}
        WHERE published_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})
    pub_yearly = q(client, f"""
        SELECT toString(toYear(published_at)) AS bucket,
               count() AS cnt
        FROM {db}.{T_DIM}
        WHERE published_at >= %(since)s
        GROUP BY bucket
        ORDER BY bucket
    """, {"since": since_dt})

    def add_pub(title: str, rows: List[Dict[str, Any]], png_name: str, xlabel: str):
        xs = [r["bucket"] for r in rows]
        ys = [float(r.get("cnt") or 0) for r in rows]
        out_png = charts_dir / png_name
        if xs:
            plot_bar(xs, ys, title, xlabel, "Courses", out_png)
        total = sum(int(r.get("cnt") or 0) for r in rows)
        md.append(f"## {title}")
        md.append("")
        md.append(f"- 합계: **{fmt_int(total)}**")
        md.append("")
        if xs:
            md.append(f"![{title}](charts/{png_name})")
            md.append("")

    add_pub("New Courses by Publish Day", pub_daily, "published_daily.png", "Date")
    add_pub("New Courses by Publish Month", pub_monthly, "published_monthly.png", "Month")
    add_pub("New Courses by Publish Year", pub_yearly, "published_yearly.png", "Year")

    # 3) 핵심 지표 최신값 분포(요약만 + 차트는 필요 시 추가 가능)
    # (길게 늘어지는 표는 만들지 않음)
    # 여기서는 최소 요약만 유지
    try:
        total_courses = q(client, f"SELECT uniqExact(course_id) AS c FROM {db}.{T_DIM}")[0]["c"]
    except Exception:
        total_courses = 0
    md.append("## 요약")
    md.append("")
    md.append(f"- 누적 강의 수(고유 course_id): **{fmt_int(total_courses)}**")
    md.append("")

    report_md.write_text("\n".join(md) + "\n", encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # 민감정보 노출 방지: 상세 오류 메시지 숨김
        raise