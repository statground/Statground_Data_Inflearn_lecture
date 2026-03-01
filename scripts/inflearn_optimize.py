#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse OPTIMIZE FINAL 실행 스크립트

목적:
- ReplacingMergeTree 계열 테이블의 중복 정리를 merge로 유도하기 위해
  배치 종료 후 OPTIMIZE TABLE ... FINAL을 수행한다.

원칙:
- 파티션(월) 있는 테이블은 최근 N개월만 OPTIMIZE
- 파티션 없는(체크포인트 등) 테이블은 전체 FINAL
- 로그에 DB 접속정보/SQL문을 노출하지 않는다.
- 실패하더라도 전체 배치를 망치지 않도록 테이블별로 예외를 삼킨다.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

import clickhouse_connect

KST = timezone(timedelta(hours=9))


def env_first(*names: str):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return v
    return None


def parse_port(val, default=8123) -> int:
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


def ch_client():
    host = env_first("CH_HOST", "CLICKHOUSE_HOST")
    if not host:
        raise RuntimeError("CH_HOST(또는 CLICKHOUSE_HOST) 환경변수가 필요합니다.")
    port = parse_port(env_first("CH_PORT", "CLICKHOUSE_PORT"), default=8123)
    user = env_first("CH_USER", "CLICKHOUSE_USER") or "default"
    password = env_first("CH_PASSWORD", "CLICKHOUSE_PASSWORD") or ""
    database = env_first("CH_DATABASE", "CLICKHOUSE_DATABASE") or "default"
    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database, secure=False
    )
    return client, database


def month_yyyymm(dt: datetime) -> int:
    return dt.year * 100 + dt.month


def recent_partitions(n_months: int) -> list[int]:
    base = datetime.now(tz=KST)
    parts = []
    y = base.year
    m = base.month
    for _ in range(max(1, n_months)):
        parts.append(y * 100 + m)
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return parts


PARTITIONED_TABLES = [
    "inflearn_course_snapshot_raw",
    "inflearn_course_dim",
    "inflearn_course_metric_fact",
    "inflearn_course_price_fact",
    "inflearn_course_curriculum_unit",
    "inflearn_instructor_dim",
    "inflearn_course_instructor_map",
]

CHECKPOINT_TABLES = [
    "inflearn_crawl_checkpoint",
]


def optimize_table_partitioned(client, db: str, table: str, parts: list[int]):
    for p in parts:
        try:
            client.command(f"OPTIMIZE TABLE {db}.{table} PARTITION {p} FINAL")
        except Exception:
            # 출력 최소화(접속정보/SQL 노출 금지)
            pass


def optimize_table_full(client, db: str, table: str):
    try:
        client.command(f"OPTIMIZE TABLE {db}.{table} FINAL")
    except Exception:
        pass


def run():
    client, db = ch_client()
    n_months = int(os.environ.get("OPTIMIZE_MONTHS", "2") or "2")
    parts = recent_partitions(n_months)

    for t in PARTITIONED_TABLES:
        optimize_table_partitioned(client, db, t, parts)

    for t in CHECKPOINT_TABLES:
        optimize_table_full(client, db, t)


if __name__ == "__main__":
    run()
