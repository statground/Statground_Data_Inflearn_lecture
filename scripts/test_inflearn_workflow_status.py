import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest


SCRIPT = Path(__file__).with_name("inflearn_workflow_status.py")


class WorkflowStatusTest(unittest.TestCase):
    def run_status(self, **overrides):
        with tempfile.TemporaryDirectory() as temp_dir:
            summary = Path(temp_dir) / "summary"
            env = {
                **os.environ,
                "INFLEARN_TRANSLATION_ONLY": "false",
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
        translation_step = workflow.split("      - name: Translate missing display metadata\n", 1)[1].split(
            "\n      - name:", 1
        )[0]
        shell_script = translation_step.split("        run: |\n", 1)[1]
        shell_script = "\n".join(
            line[10:] if line.startswith("          ") else line for line in shell_script.splitlines()
        )
        self.assertLess(
            workflow.index("      - name: Translate missing display metadata\n"),
            workflow.index("      - name: Refresh exact public lecture views serially\n"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output"
            env = {
                **os.environ,
                "CH_HOST": "example.invalid",
                "CH_USER": "test-reader",
                "GITHUB_OUTPUT": str(output),
                **{key: "" for key in (
                    "INFLEARN_TRANSLATION_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY",
                    "GROQ_API_KEY", "CEREBRAS_API_KEY",
                )},
            }
            completed = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", shell_script],
                env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(completed.returncode, 1)
            self.assertIn("::error title=Inflearn translation blocked::", completed.stdout)
            self.assertIn("translation_status=blocked_api_key", output.read_text())

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

    def test_translation_only_reports_the_other_stages_as_not_requested(self):
        status, _, _, code = self.run_status(
            INFLEARN_TRANSLATION_ONLY="true",
            INFLEARN_COLLECT_OUTCOME="skipped",
            INFLEARN_UPDATE_OUTCOME="skipped",
            INFLEARN_TRANSLATION_STATUS="completed",
            INFLEARN_PUBLICATION_ENABLED="true",
        )
        self.assertEqual(status["source_collection"], "not_requested")
        self.assertEqual(status["public_serving"], "not_requested")
        self.assertEqual(code, 0)

    def test_translation_recovery_is_bounded_and_skips_other_write_paths(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/inflearn_collect_all.yml").read_text()
        self.assertIn('if [[ "$INFLEARN_TRANSLATION_ONLY" == true ]]; then\n            export INFLEARN_TRANSLATION_MAX_PER_RUN=12', workflow)
        for name in (
            "Collect new Inflearn courses into ClickHouse", "Update existing Inflearn courses in ClickHouse",
            "Refresh MirType provider practice content", "Refresh exact public lecture views serially",
        ):
            with self.subTest(step=name):
                stage = workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
                self.assertIn("!inputs.translate_only", stage.split("        run:", 1)[0])

    def test_actions_use_only_r_project_provider_secrets(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/inflearn_collect_all.yml").read_text()
        stage = workflow.split("      - name: Translate missing display metadata\n", 1)[1].split("\n      - name:", 1)[0]
        for key in ("OPENROUTER_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY"):
            self.assertIn(f"secrets.{key}", stage)
        for key in ("OPENAI_API_KEY", "INFLEARN_TRANSLATION_API_KEY", "GH_MODELS_API_KEY"):
            self.assertNotIn(f"secrets.{key}", stage)

    def test_publisher_credentials_require_tls_and_the_dedicated_identity(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/inflearn_collect_all.yml").read_text()
        admission = workflow.split("      - name: Validate publication publisher secrets\n", 1)[1].split(
            "\n      - name:", 1
        )[0]
        freshness = workflow.split("      - name: Verify public lecture freshness and refresh health\n", 1)[1].split(
            "\n      - name:", 1
        )[0]
        self.assertIn("id: publisher_admission", admission)
        self.assertIn("if: vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true'", admission)
        self.assertIn("steps.publisher_admission.outcome == 'success'", freshness)
        script = dedent(admission.split("        run: |\n", 1)[1])
        baseline = {
            "CH_SECURE": "true",
            "CH_HOST": "db.example.invalid",
            "PUBLISHER_CH_USER": "inflearn_lecture_generation_publisher",
            "PUBLISHER_CH_PASSWORD": "fixture-value",
            "READER_REFRESH_CONFIG_JSON": "{}",
        }
        for overrides, expected in (
            ({}, 0),
            ({"CH_SECURE": "1"}, 0),
            ({"CH_SECURE": "false"}, 1),
            ({"CH_HOST": "http://db.example.invalid"}, 1),
            ({"PUBLISHER_CH_USER": "collector"}, 1),
            ({"PUBLISHER_CH_PASSWORD": ""}, 1),
        ):
            with self.subTest(overrides=overrides):
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", script],
                    env={**baseline, **overrides}, text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, expected)
                self.assertNotIn("fixture-value", result.stdout + result.stderr)

    def test_incomplete_source_is_not_reported_collected(self):
        status, _, _, code = self.run_status(INFLEARN_UPDATE_OUTCOME="failure")
        self.assertEqual(status["source_collection"], "incomplete")
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
