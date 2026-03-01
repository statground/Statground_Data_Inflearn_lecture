#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse OPTIMIZE(FINAL) 실행 스크립트

- ReplacingMergeTree 계열은 merge 시점에 중복이 제거되므로,
  배치가 끝난 뒤 최근 파티션에 한해 OPTIMIZE ... FINAL 을 트리거합니다.
- OPTIMIZE FINAL은 무거운 작업이므로, 최근 N개월(기본 2개월)만 수행합니다.
- 로그에 DB 접속정보(호스트/포트/DB명 등)나 OPTIMIZE SQL을 출력하지 않습니다.
"""

import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import clickhouse_connect

KST = ZoneInfo("Asia/Seoul")

TABLES_PARTITIONED = [
    "inflearn_course_snapshot_raw",
    "inflearn_course_dim",
    "inflearn_course_metric_fact",
    "inflearn_course_price_fact",
    "inflearn_course_curriculum_unit",
    "inflearn_instructor_dim",
    "inflearn_course_instructor_map",
]

TABLES_NON_PARTITIONED = [
    "inflearn_crawl_checkpoint",
]


def _parse_port(raw: str, default: int = 8123) -> int:
    raw = (raw or "").strip()
    if raw == "":
        return default
    m = re.search(r"\d+", raw)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default


def _get_int(name: str, default: int) -> int:
    v = (os.environ.get(name) or "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default


def ch_client():
    host = (os.environ.get("CH_HOST") or os.environ.get("CLICKHOUSE_HOST") or "").strip()
    port = _parse_port(os.environ.get("CH_PORT") or os.environ.get("CLICKHOUSE_PORT") or "", 8123)
    user = (os.environ.get("CH_USER") or os.environ.get("CLICKHOUSE_USER") or "default").strip()
    password = os.environ.get("CH_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD") or ""
    database = (os.environ.get("CH_DATABASE") or os.environ.get("CLICKHOUSE_DATABASE") or "default").strip()
    if not host:
        raise RuntimeError("CH_HOST(또는 CLICKHOUSE_HOST) 환경변수가 필요합니다.")
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, database=database), database


def _recent_partitions_yyyymm(months: int) -> list[str]:
    months = max(0, months)
    now = datetime.now(tz=KST)
    parts: list[str] = []
    for i in range(months):
        y = now.year
        m = now.month - i
        while m <= 0:
            y -= 1
            m += 12
        parts.append(f"{y}{m:02d}")
    return parts


def run():
    ch, db = ch_client()
    months = _get_int("OPTIMIZE_MONTHS", 2)
    parts = _recent_partitions_yyyymm(months)

    # Partitioned tables
    if months > 0 and parts:
        for t in TABLES_PARTITIONED:
            for p in parts:
                try:
                    ch.command(f"OPTIMIZE TABLE {db}.{t} PARTITION {p} FINAL")
                except Exception:
                    # 실패해도 다음으로 진행 (민감정보/SQL/에러 상세 출력 금지)
                    pass

    # Non-partitioned tables
    for t in TABLES_NON_PARTITIONED:
        try:
            ch.command(f"OPTIMIZE TABLE {db}.{t} FINAL")
        except Exception:
            pass


if __name__ == "__main__":
    run()
