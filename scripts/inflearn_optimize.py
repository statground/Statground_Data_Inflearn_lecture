#!/usr/bin/env python3
"""OPTIMIZE helper

After inserts, ReplacingMergeTree-family tables will deduplicate on merge.
We trigger OPTIMIZE ... FINAL for:
- partitioned tables: recent N monthly partitions only (default: 2 months)
- non-partitioned (checkpoint): whole table FINAL

Note: OPTIMIZE FINAL is heavy. Keep OPTIMIZE_MONTHS small.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import clickhouse_connect

KST = ZoneInfo("Asia/Seoul")

TABLES = [
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

def run():
    ch, db = ch_client()
    months = max(0, _get_int("OPTIMIZE_MONTHS", 2))
    now = datetime.now(tz=KST)
    parts = []
    for i in range(months):
        y = now.year
        m = now.month - i
        while m <= 0:
            y -= 1
            m += 12
        parts.append(f"{y}{m:02d}")

    # Partitioned tables (assumed PARTITION BY toYYYYMM(fetched_at))
    for t in TABLES:
        if months <= 0:
            continue
        for p in parts:
            sql = f"OPTIMIZE TABLE {db}.{t} PARTITION {p} FINAL"
            try:
                ch.command(sql)
            except Exception as e:

    # Non-partitioned tables
    for t in CHECKPOINT_TABLES:
        sql = f"OPTIMIZE TABLE {db}.{t} FINAL"
        try:
            ch.command(sql)
        except Exception as e:

if __name__ == "__main__":
    run()
