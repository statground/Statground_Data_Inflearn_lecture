#!/usr/bin/env python3
"""Report source collection, translation, and serving as separate outcomes."""

import json
import os
from pathlib import Path


def stage_status(env):
    collected = (
        env.get("INFLEARN_COLLECT_OUTCOME") == "success"
        and env.get("INFLEARN_UPDATE_OUTCOME") == "success"
    )
    translation = env.get("INFLEARN_TRANSLATION_STATUS", "")
    if translation not in {"completed", "blocked_api_key", "blocked_clickhouse_config"}:
        translation = "failed" if translation == "failure" else "not_verified"

    if env.get("INFLEARN_PUBLICATION_ENABLED") != "true":
        publication = "inactive"
    elif env.get("INFLEARN_PUBLICATION_OUTCOME") == "success" and env.get("INFLEARN_FRESHNESS_OUTCOME") == "success":
        publication = "verified"
    elif "failure" in {env.get("INFLEARN_PUBLICATION_OUTCOME"), env.get("INFLEARN_FRESHNESS_OUTCOME")}:
        publication = "failed"
    else:
        publication = "not_verified"
    return {
        "schema": "statground.inflearn.workflow_stage_status.v1",
        "source_collection": "collected" if collected else "incomplete",
        "display_translation": translation,
        "public_serving": publication,
    }


def main():
    status = stage_status(os.environ)
    print(json.dumps(status, sort_keys=True, separators=(",", ":")))
    if status["display_translation"] != "completed":
        print("::warning title=Inflearn translation incomplete::Collected rows do not prove translated display content is available.")
    if status["public_serving"] != "verified":
        print("::warning title=Inflearn public serving unverified::Collection success does not prove the Web-R lecture catalog was refreshed.")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary_path:
        rows = [
            "## Inflearn pipeline stages",
            "",
            "| Stage | Status |",
            "| --- | --- |",
            f"| Source collection | {status['source_collection']} |",
            f"| Display translation | {status['display_translation']} |",
            f"| Public serving | {status['public_serving']} |",
            "",
            "Collection, translation, and public serving are verified independently.",
            "",
        ]
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("\n".join(rows))


if __name__ == "__main__":
    main()
