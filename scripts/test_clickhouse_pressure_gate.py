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
            "distributed_files": 20,
            "broken_distributed_files": 0,
            "readonly_metric": 0,
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
            "unhealthy_replicas": 0,
            "max_replica_queue": 12,
            "max_replica_delay_seconds": 4,
            "max_parts_to_check": 0,
        }

    def test_healthy_snapshot_passes(self):
        self.assertEqual(gate.evaluate(self.healthy, self.thresholds), [])

    def test_each_pressure_dimension_blocks(self):
        cases = {
            "distributed_files": 10001,
            "broken_distributed_files": 1,
            "readonly_metric": 1,
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

    def test_missing_filesystem_metrics_fails_closed(self):
        snapshot = dict(self.healthy, available_samples=0)
        self.assertIn("filesystem_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, total_inode_samples=0)
        self.assertIn("filesystem_inode_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))

    def test_idle_replica_delay_does_not_block(self):
        snapshot = dict(self.healthy, max_replica_queue=0, max_replica_delay_seconds=9_999_999)
        self.assertEqual(gate.evaluate(snapshot, self.thresholds), [])

    def test_query_uses_constant_cost_system_tables(self):
        self.assertIn("system.metrics", gate.PRESSURE_QUERY)
        self.assertIn("system.asynchronous_metrics", gate.PRESSURE_QUERY)
        self.assertIn("system.disks", gate.PRESSURE_QUERY)
        self.assertIn("system.replicas", gate.PRESSURE_QUERY)
        self.assertNotIn("system.distribution_queue", gate.PRESSURE_QUERY)
        self.assertIn("maxIf(absolute_delay, queue_size > 0)", gate.PRESSURE_QUERY)
        self.assertIn("max_threads = 1", gate.PRESSURE_QUERY)

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
        self.assertIn('CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES: "10000"', workflow)


if __name__ == "__main__":
    unittest.main()
