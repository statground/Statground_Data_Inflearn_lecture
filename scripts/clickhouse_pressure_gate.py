#!/usr/bin/env python3
"""Cheap fail-closed preflight for scheduled ClickHouse writers."""

from __future__ import annotations

import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping


PRESSURE_QUERY = """
SELECT
    toUInt64((SELECT sum(value) FROM system.metrics WHERE metric = 'DistributedFilesToInsert')) AS distributed_files,
    toUInt64((SELECT sum(value) FROM system.metrics WHERE metric = 'BrokenDistributedFilesToInsert')) AS broken_distributed_files,
    toUInt64((SELECT sum(value) FROM system.metrics WHERE metric = 'ReadonlyReplica')) AS readonly_metric,
    toUInt64((SELECT count() FROM system.disks WHERE name = 'default')) AS available_samples,
    toUInt64((SELECT sum(free_space) FROM system.disks WHERE name = 'default')) AS available_bytes,
    toUInt64((SELECT count() FROM system.disks WHERE name = 'default')) AS total_samples,
    toUInt64((SELECT sum(total_space) FROM system.disks WHERE name = 'default')) AS total_bytes,
    toUInt64((SELECT count() FROM system.asynchronous_metrics WHERE metric = 'FilesystemMainPathAvailableINodes')) AS available_inode_samples,
    toUInt64((SELECT sum(value) FROM system.asynchronous_metrics WHERE metric = 'FilesystemMainPathAvailableINodes')) AS available_inodes,
    toUInt64((SELECT count() FROM system.asynchronous_metrics WHERE metric = 'FilesystemMainPathTotalINodes')) AS total_inode_samples,
    toUInt64((SELECT sum(value) FROM system.asynchronous_metrics WHERE metric = 'FilesystemMainPathTotalINodes')) AS total_inodes,
    toUInt64((SELECT count() FROM system.asynchronous_metrics WHERE metric = 'OSIOWaitTimeNormalized')) AS iowait_samples,
    toFloat64((SELECT max(value) FROM system.asynchronous_metrics WHERE metric = 'OSIOWaitTimeNormalized')) AS iowait_normalized,
    toUInt64((SELECT countIf(is_readonly != 0 OR is_session_expired != 0) FROM system.replicas)) AS unhealthy_replicas,
    toUInt64((SELECT max(queue_size) FROM system.replicas)) AS max_replica_queue,
    toUInt64((SELECT maxIf(absolute_delay, queue_size > 0) FROM system.replicas)) AS max_replica_delay_seconds,
    toUInt64((SELECT max(parts_to_check) FROM system.replicas)) AS max_parts_to_check
SETTINGS max_threads = 1, max_execution_time = 10
FORMAT JSONEachRow
""".strip()


def _first(env: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = str(env.get(name, "")).strip()
        if value:
            return value
    return ""


def _bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def clickhouse_url(env: Mapping[str, str]) -> str:
    host = _first(env, "CH_HOST", "CLICKHOUSE_HOST")
    if not host:
        raise ValueError("CH_HOST or CLICKHOUSE_HOST is required")
    protocol = _first(env, "CH_PROTOCOL", "CLICKHOUSE_PROTOCOL")
    if not protocol:
        protocol = "https" if _bool(_first(env, "CH_SECURE", "CLICKHOUSE_SECURE")) else "http"
    port = _first(env, "CH_PORT", "CLICKHOUSE_PORT") or "8123"
    configured_path = _first(env, "CH_HTTP_URL_PATH", "CLICKHOUSE_HTTP_URL_PATH")

    if "://" in host:
        parsed = urllib.parse.urlsplit(host)
        path = configured_path or parsed.path or "/"
        netloc = parsed.netloc
        scheme = parsed.scheme
    else:
        path = configured_path or "/"
        netloc = host if urllib.parse.urlsplit("//" + host).port else f"{host}:{port}"
        scheme = protocol
    if not path.startswith("/"):
        path = "/" + path
    return urllib.parse.urlunsplit((scheme, netloc, path, "", ""))


def load_thresholds(env: Mapping[str, str]) -> dict[str, float | int]:
    values: dict[str, float | int] = {
        "max_distributed_files": int(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES", "10000")),
        "min_available_bytes": int(env.get("CLICKHOUSE_PRESSURE_GATE_MIN_AVAILABLE_BYTES", str(100 * 1024**3))),
        "max_used_ratio": float(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_USED_RATIO", "0.90")),
        "min_available_inodes": int(env.get("CLICKHOUSE_PRESSURE_GATE_MIN_AVAILABLE_INODES", "1000000")),
        "max_used_inode_ratio": float(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_USED_INODE_RATIO", "0.95")),
        "max_replica_queue": int(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_REPLICA_QUEUE", "2000")),
        "max_replica_delay_seconds": int(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_REPLICA_DELAY_SECONDS", "900")),
        "max_iowait_normalized": float(env.get("CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED", "0.50")),
    }
    if (
        any(value < 0 for value in values.values())
        or not 0 < values["max_used_ratio"] < 1
        or not 0 < values["max_used_inode_ratio"] < 1
    ):
        raise ValueError("invalid ClickHouse pressure gate thresholds")
    return values


def evaluate(snapshot: Mapping[str, object], thresholds: Mapping[str, float | int]) -> list[str]:
    integer = lambda name: int(float(snapshot.get(name, 0) or 0))
    decimal = lambda name: float(snapshot.get(name, 0) or 0)
    reasons: list[str] = []
    distributed = integer("distributed_files")
    broken_distributed = integer("broken_distributed_files")
    readonly = integer("readonly_metric")
    unhealthy = integer("unhealthy_replicas")
    queue = integer("max_replica_queue")
    delay = integer("max_replica_delay_seconds")
    available = integer("available_bytes")
    total = integer("total_bytes")
    available_inodes = integer("available_inodes")
    total_inodes = integer("total_inodes")

    if distributed > int(thresholds["max_distributed_files"]):
        reasons.append(f"distributed_files={distributed}")
    if broken_distributed > 0:
        reasons.append(f"broken_distributed_files={broken_distributed}")
    if readonly > 0 or unhealthy > 0:
        reasons.append(f"readonly_replicas={max(readonly, unhealthy)}")
    if queue > int(thresholds["max_replica_queue"]):
        reasons.append(f"replica_queue={queue}")
    if queue > 0 and delay > int(thresholds["max_replica_delay_seconds"]):
        reasons.append(f"replica_delay_seconds={delay}")
    if integer("max_parts_to_check") > 0:
        reasons.append(f"parts_to_check={integer('max_parts_to_check')}")
    if integer("available_samples") == 0 or integer("total_samples") == 0 or total <= 0:
        reasons.append("filesystem_metrics_unavailable")
    else:
        used_ratio = max(0.0, min(1.0, 1.0 - available / total))
        if available < int(thresholds["min_available_bytes"]):
            reasons.append(f"available_bytes={available}")
        if used_ratio > float(thresholds["max_used_ratio"]):
            reasons.append(f"used_ratio={used_ratio:.3f}")
    if integer("available_inode_samples") == 0 or integer("total_inode_samples") == 0 or total_inodes <= 0:
        reasons.append("filesystem_inode_metrics_unavailable")
    else:
        used_inode_ratio = max(0.0, min(1.0, 1.0 - available_inodes / total_inodes))
        if available_inodes < int(thresholds["min_available_inodes"]):
            reasons.append(f"available_inodes={available_inodes}")
        if used_inode_ratio > float(thresholds["max_used_inode_ratio"]):
            reasons.append(f"used_inode_ratio={used_inode_ratio:.3f}")
    if integer("iowait_samples") > 0:
        iowait = decimal("iowait_normalized")
        if iowait > float(thresholds["max_iowait_normalized"]):
            reasons.append(f"iowait_normalized={iowait:.3f}")
    return reasons


def fetch_snapshot(env: Mapping[str, str]) -> dict[str, object]:
    user = _first(env, "CH_USER", "CLICKHOUSE_USER")
    password = _first(env, "CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    if not user or not password:
        raise ValueError("CH_USER/CH_PASSWORD or CLICKHOUSE_USER/CLICKHOUSE_PASSWORD is required")
    timeout = float(env.get("CLICKHOUSE_PRESSURE_GATE_TIMEOUT_SECONDS", "15"))
    request = urllib.request.Request(clickhouse_url(env), data=PRESSURE_QUERY.encode(), method="POST")
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", "Basic " + auth)
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    request.add_header("User-Agent", "statground-clickhouse-pressure-gate/1")
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ssl.create_default_context()) as response:
            raw = response.read(64 * 1024)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"ClickHouse pressure query failed status={exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"ClickHouse pressure query transport failed category={exc.__class__.__name__}") from exc
    lines = [line for line in raw.decode("utf-8").splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError("ClickHouse pressure query returned an unexpected row count")
    return json.loads(lines[0])


def main() -> int:
    try:
        thresholds = load_thresholds(os.environ)
        snapshot = fetch_snapshot(os.environ)
        reasons = evaluate(snapshot, thresholds)
    except (ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"::error::ClickHouse write pressure gate failed closed: {exc}", file=sys.stderr)
        return 1
    if reasons:
        print("::error::ClickHouse write pressure gate deferred this run: " + ",".join(reasons), file=sys.stderr)
        return 1
    print(
        "ClickHouse write pressure gate ok "
        f"distributed_files={int(float(snapshot.get('distributed_files', 0) or 0))} "
        f"max_replica_queue={int(float(snapshot.get('max_replica_queue', 0) or 0))} "
        f"available_bytes={int(float(snapshot.get('available_bytes', 0) or 0))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
