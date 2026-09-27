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
                [sys.executable, str(SCRIPT)], env=env, text=True, capture_output=True, check=False
            )
            return json.loads(completed.stdout.splitlines()[0]), summary.read_text(), completed.stdout, completed.returncode

    def test_green_collection_does_not_claim_translation_or_public_serving(self):
        status, summary, stdout, code = self.run_status()
        self.assertEqual(status["source_collection"], "collected")
        self.assertEqual(status["display_translation"], "blocked_api_key")
        self.assertEqual(status["public_serving"], "inactive")
        self.assertIn("| Public serving | inactive |", summary)
        self.assertIn("::warning title=Inflearn public serving unverified::", stdout)
        self.assertIn("::error title=Inflearn translation blocked::", stdout)
        self.assertEqual(code, 1)

    def test_missing_translation_key_fails_before_publication_refresh(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/inflearn_collect_all.yml").read_text()
        branch = workflow.split('if [ -z "$INFLEARN_TRANSLATION_API_KEY" ]; then', 1)[1].split("\n          fi", 1)[0]
        self.assertIn("translation_status=blocked_api_key", branch)
        self.assertIn("exit 1", branch)
        self.assertIn("::error title=Inflearn translation blocked::", branch)

    def test_public_serving_requires_refresh_and_freshness_receipts(self):
        status, _, _, code = self.run_status(
            INFLEARN_TRANSLATION_STATUS="completed",
            INFLEARN_PUBLICATION_ENABLED="true",
            INFLEARN_PUBLICATION_OUTCOME="success",
            INFLEARN_FRESHNESS_OUTCOME="failure",
        )
        self.assertEqual(status["public_serving"], "failed")
        self.assertEqual(code, 0)
        status, _, _, code = self.run_status(
            INFLEARN_TRANSLATION_STATUS="completed",
            INFLEARN_PUBLICATION_ENABLED="true",
            INFLEARN_PUBLICATION_OUTCOME="success",
            INFLEARN_FRESHNESS_OUTCOME="success",
        )
        self.assertEqual(status["public_serving"], "verified")
        self.assertEqual(code, 0)

    def test_incomplete_source_is_not_reported_collected(self):
        status, _, _, code = self.run_status(INFLEARN_UPDATE_OUTCOME="failure")
        self.assertEqual(status["source_collection"], "incomplete")
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
