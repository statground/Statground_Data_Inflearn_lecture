#!/usr/bin/env python3
"""OPTIMIZE helper

After inserts, ReplacingMergeTree-family tables will deduplicate on merge.
We trigger OPTIMIZE ... FINAL for:
- partitioned tables: recent N monthly partitions only (default: 2 months)
- non-partitioned (checkpoint): whole table FINAL

Note: OPTIMIZE FINAL is heavy. Keep OPTIMIZE_MONTHS small.
"""

import os
import re
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
            except Exception:
                pass

    # Non-partitioned tables
    for t in CHECKPOINT_TABLES:
        sql = f"OPTIMIZE TABLE {db}.{t} FINAL"
        try:
            ch.command(sql)
        except Exception:
                pass

if __name__ == "__main__":
    run()
