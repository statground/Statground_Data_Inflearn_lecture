#!/usr/bin/env python3
"""Batch #1: collect brand-new courses via sitemap scan (checkpointed).

Runs the original sitemap crawler logic in scripts/inflearn_crawl.py.
"""

import os, sys, pathlib

# Ensure repo root on path so we can import scripts/inflearn_crawl.py as a module
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import inflearn_crawl  # noqa: E402

if __name__ == "__main__":
    inflearn_crawl.main()
