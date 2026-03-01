# Statground_Data_Inflearn_lecture
## Workflows (Hourly)

This repo runs 3 hourly GitHub Actions batches:

1) **Collect New Courses** (`inflearn_collect_new.yml`)
- sitemap checkpoint scan for new course pages
- runs `python scripts/inflearn_collect_new.py` (wrapper -> `inflearn_crawl.main()`)

2) **Update Existing Courses** (`inflearn_update_existing.yml`)
- refreshes stale courses first (based on latest snapshot time)
- runs `python scripts/inflearn_update_existing.py`

3) **Stats** (`inflearn_stats.yml`)
- prints stats (collected_at + published_at) with total/yearly/monthly/daily/hourly + cumulative
- writes to GitHub Step Summary

Each workflow ends with `python scripts/inflearn_optimize.py` to trigger `OPTIMIZE ... FINAL` on recent partitions.

### Required secrets / vars

Secrets:
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`

Vars (optional):
- `BATCH_SIZE` (default in code)
- `UPDATE_BATCH_SIZE` (default in code)
- `SLEEP_MIN`, `SLEEP_MAX`
- `SITEMAP_BASE`, `SITEMAP_PREFIX`
- `STATS_LOOKBACK_DAYS`
- `OPTIMIZE_MONTHS` (keep small; default 2)
