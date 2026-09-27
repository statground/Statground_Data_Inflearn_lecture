import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("inflearn_workflow_status.py")


class WorkflowStatusTest(unittest.TestCase):
    def run_status(self, **overrides):
        with tempfile.TemporaryDirectory() as temp_dir:
            summary = Path(temp_dir) / "summary"
            env = {
                **os.environ,
                "GITHUB_STEP_SUMMARY": str(summary),
                "INFLEARN_COLLECT_OUTCOME": "success",
                "INFLEARN_UPDATE_OUTCOME": "success",
                "INFLEARN_TRANSLATION_STATUS": "blocked_api_key",
                "INFLEARN_PUBLICATION_ENABLED": "false",
                "INFLEARN_PUBLICATION_OUTCOME": "skipped",
                "INFLEARN_FRESHNESS_OUTCOME": "skipped",
                **overrides,
            }
            completed = subprocess.run(
                [sys.executable, str(SCRIPT)], env=env, text=True, capture_output=True, check=True
            )
            return json.loads(completed.stdout.splitlines()[0]), summary.read_text(), completed.stdout

    def test_green_collection_does_not_claim_translation_or_public_serving(self):
        status, summary, stdout = self.run_status()
        self.assertEqual(status["source_collection"], "collected")
        self.assertEqual(status["display_translation"], "blocked_api_key")
        self.assertEqual(status["public_serving"], "inactive")
        self.assertIn("| Public serving | inactive |", summary)
        self.assertIn("::warning title=Inflearn public serving unverified::", stdout)

    def test_public_serving_requires_refresh_and_freshness_receipts(self):
        status, _, _ = self.run_status(
            INFLEARN_TRANSLATION_STATUS="completed",
            INFLEARN_PUBLICATION_ENABLED="true",
            INFLEARN_PUBLICATION_OUTCOME="success",
            INFLEARN_FRESHNESS_OUTCOME="failure",
        )
        self.assertEqual(status["public_serving"], "failed")
        status, _, _ = self.run_status(
            INFLEARN_TRANSLATION_STATUS="completed",
            INFLEARN_PUBLICATION_ENABLED="true",
            INFLEARN_PUBLICATION_OUTCOME="success",
            INFLEARN_FRESHNESS_OUTCOME="success",
        )
        self.assertEqual(status["public_serving"], "verified")

    def test_incomplete_source_is_not_reported_collected(self):
        status, _, _ = self.run_status(INFLEARN_UPDATE_OUTCOME="failure")
        self.assertEqual(status["source_collection"], "incomplete")


if __name__ == "__main__":
    unittest.main()
