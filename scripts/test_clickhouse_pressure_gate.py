import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("clickhouse_pressure_gate.py")
SPEC = importlib.util.spec_from_file_location("clickhouse_pressure_gate", MODULE_PATH)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class ClickHousePressureGateTest(unittest.TestCase):
    def setUp(self):
        self.thresholds = gate.load_thresholds({})
        self.healthy = {
            "endpoint_hostname": "Clickhouse_S1_R1",
            "expected_endpoint_hostname": "Clickhouse_S1_R1",
            "distributed_files": 20,
            "broken_distributed_files": 0,
            "available_samples": 1,
            "available_bytes": 500 * 1024**3,
            "total_samples": 1,
            "total_bytes": 1000 * 1024**3,
            "available_inode_samples": 1,
            "available_inodes": 8_000_000,
            "total_inode_samples": 1,
            "total_inodes": 10_000_000,
            "iowait_samples": 1,
            "iowait_normalized": 0.10,
            "expected_replica_target_count": 2,
            "replica_target_count": 2,
            "expected_local_target_count": 1,
            "local_target_count": 1,
            "invalid_local_target_engines": 0,
            "unhealthy_replicas": 0,
            "max_replica_queue": 12,
            "max_replica_delay_seconds": 4,
            "max_parts_to_check": 0,
        }

    def test_healthy_snapshot_passes(self):
        self.assertEqual(gate.evaluate(self.healthy, self.thresholds), [])

    def test_expected_endpoint_contract_is_strict_and_fail_closed(self):
        self.assertEqual(
            gate.load_expected_endpoint_hostname({gate.ENDPOINT_ENV: "Clickhouse_S1_R1"}),
            "Clickhouse_S1_R1",
        )
        for raw in ("", " Clickhouse_S1_R1", "clickhouse-gateway", "bad/host"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                gate.load_expected_endpoint_hostname({gate.ENDPOINT_ENV: raw})
        mismatch = dict(self.healthy, endpoint_hostname="Clickhouse_S1_R2")
        self.assertIn("endpoint_hostname_mismatch", gate.evaluate(mismatch, self.thresholds))
        missing = dict(self.healthy, endpoint_hostname="")
        self.assertIn("endpoint_hostname_mismatch", gate.evaluate(missing, self.thresholds))

    def test_each_pressure_dimension_blocks(self):
        cases = {
            "unhealthy_replicas": 1,
            "available_bytes": 10,
            "iowait_normalized": 0.75,
            "max_replica_queue": 2001,
            "max_replica_delay_seconds": 901,
            "max_parts_to_check": 1,
        }
        for key, value in cases.items():
            with self.subTest(key=key):
                snapshot = dict(self.healthy, **{key: value})
                self.assertTrue(gate.evaluate(snapshot, self.thresholds))

    def test_unrelated_global_distributed_backlog_is_observability_only(self):
        snapshot = dict(self.healthy, distributed_files=606_448, broken_distributed_files=7)
        self.assertEqual(gate.evaluate(snapshot, self.thresholds), [])

    def test_missing_filesystem_metrics_fails_closed(self):
        snapshot = dict(self.healthy, available_samples=0)
        self.assertIn("filesystem_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, total_inode_samples=0)
        self.assertIn("filesystem_inode_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, iowait_samples=0)
        self.assertIn("iowait_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))

    def test_idle_replica_delay_does_not_block(self):
        snapshot = dict(self.healthy, max_replica_queue=0, max_replica_delay_seconds=9_999_999)
        self.assertEqual(gate.evaluate(snapshot, self.thresholds), [])

    def test_target_contract_is_strict_and_fail_closed(self):
        valid = gate.load_targets(
            {
                gate.TARGET_ENV: (
                    "replica:Data_Lecture_Inflearn_Raw.inflearn_course_snapshot_raw_local,"
                    "local:Data_Lecture_Inflearn_Log.inflearn_direct_insert_outbox"
                )
            }
        )
        self.assertEqual(len(valid), 2)
        for raw in ("", "inflearn_course_snapshot_raw_local", "replica:db.table,", "replica:db.table,replica:db.table"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                gate.load_targets({gate.TARGET_ENV: raw})

        snapshot = dict(self.healthy, replica_target_count=1)
        self.assertIn("replica_targets=1/2", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, local_target_count=0)
        self.assertIn("local_targets=0/1", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, invalid_local_target_engines=1)
        self.assertIn("invalid_local_target_engines=1", gate.evaluate(snapshot, self.thresholds))

    def test_unrelated_legacy_queue_is_excluded_but_target_queue_blocks(self):
        targets = gate.load_targets(
            {gate.TARGET_ENV: "replica:Data_Lecture_Inflearn_Raw.inflearn_course_snapshot_raw_local"}
        )
        query = gate.build_pressure_query(targets)
        self.assertIn("inflearn_course_snapshot_raw_local", query)
        self.assertNotIn("polymarket_market_latest_v2_local", query)
        self.assertIn("WHERE (database, table) IN", query)
        snapshot = dict(self.healthy, max_replica_queue=2001)
        self.assertIn("replica_queue=2001", gate.evaluate(snapshot, self.thresholds))

    def test_query_uses_bounded_system_tables(self):
        targets = gate.load_targets(
            {
                gate.TARGET_ENV: (
                    "replica:Data_Lecture_Inflearn_Raw.inflearn_course_snapshot_raw_local,"
                    "local:Data_Lecture_Inflearn_Log.inflearn_direct_insert_outbox"
                )
            }
        )
        query = gate.build_pressure_query(targets)
        self.assertIn("hostName() AS endpoint_hostname", query)
        self.assertIn("system.metrics", query)
        self.assertIn("system.asynchronous_metrics", query)
        self.assertIn("system.disks", query)
        self.assertIn("system.replicas", query)
        self.assertIn("system.tables", query)
        self.assertNotIn("system.distribution_queue", query)
        self.assertNotIn("ReadonlyReplica", query)
        self.assertIn("maxIf(absolute_delay, queue_size > 0)", query)
        self.assertIn("max_threads = 1", query)

    def test_url_supports_both_env_families(self):
        self.assertEqual(
            gate.clickhouse_url({"CH_HOST": "db.example", "CH_PORT": "8123", "CH_SECURE": "true"}),
            "https://db.example:8123/",
        )
        self.assertEqual(
            gate.clickhouse_url({"CLICKHOUSE_HOST": "http://db.example:9000/base"}),
            "http://db.example:9000/base",
        )

    def test_scheduled_workflow_gates_before_first_writer(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/inflearn_collect_all.yml").read_text()
        gate_step = "run: python3 scripts/clickhouse_pressure_gate.py"
        self.assertEqual(workflow.count(gate_step), 1)
        self.assertLess(workflow.index(gate_step), workflow.index("go run -mod=mod ./cmd/inflearn-collect-new"))
        self.assertEqual(
            workflow.count(
                "CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME: ${{ vars.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME || secrets.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME }}"
            ),
            1,
        )
        self.assertIn('CLICKHOUSE_PRESSURE_GATE_MIN_AVAILABLE_BYTES: "107374182400"', workflow)
        self.assertIn("CLICKHOUSE_PRESSURE_GATE_TARGETS: >-", workflow)
        self.assertIn("replica:Data_Lecture_Inflearn_Raw.inflearn_course_snapshot_raw_local", workflow)
        self.assertIn("replica:Data_Lecture_Inflearn_Service.inflearn_course_display_translation_local", workflow)
        self.assertIn("replica:mirtype_content.provider_practice_content_serving_local", workflow)
        self.assertNotIn("Data_Lecture_Inflearn_Mart.inflearn_raw_snapshot_daily_rollup_local", workflow)
        self.assertIn("local:Data_Lecture_Inflearn_Log.inflearn_direct_insert_outbox", workflow)


if __name__ == "__main__":
    unittest.main()
