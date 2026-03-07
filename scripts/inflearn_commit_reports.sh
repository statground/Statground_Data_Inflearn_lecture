#!/usr/bin/env bash
set -euo pipefail

REPORT_PATH="${REPORT_PATH:-reports/inflearn}"
COMMIT_MESSAGE="${STATS_COMMIT_MESSAGE:-chore(stats): inflearn report update}"
BRANCH_NAME="${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD)}"

# Artifact 업로드 대신 리포트 파일을 직접 커밋한다.
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

git add "${REPORT_PATH}" || true

if git diff --cached --quiet; then
  echo "No report changes."
  exit 0
fi

git commit -m "${COMMIT_MESSAGE}"
git push origin HEAD:"${BRANCH_NAME}"
