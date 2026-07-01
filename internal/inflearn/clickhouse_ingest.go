package inflearn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

var (
	inflearnCourseSnapshotRawColumns = []string{
		"uuid", "fetched_at", "course_id", "locale", "source_url", "query_key",
		"query_key_hash", "status_code", "error_code", "payload_json", "payload_hash",
	}
	inflearnCrawlCheckpointColumns = []string{
		"source", "updated_at", "sitemap_index", "url_index",
	}
	inflearnCourseDimColumns = []string{
		"course_id", "locale", "fetched_at", "slug", "en_slug", "status", "title",
		"description", "thumbnail_url", "category_main_title", "category_main_slug",
		"category_sub_title", "category_sub_slug", "level_code", "is_new", "is_best",
		"student_count", "like_count", "review_count", "average_star", "lecture_unit_count",
		"preview_unit_count", "runtime_sec", "provides_certificate",
		"provides_instructor_answer", "provides_inquiry", "published_at", "last_updated_at",
		"keywords", "category_slugs", "skill_slugs", "common_tag_slugs",
	}
	inflearnCourseMetricFactColumns = []string{
		"fetched_at", "course_id", "locale", "student_count", "like_count",
		"review_count", "average_star", "krw_regular_price", "krw_pay_price",
		"discount_rate", "discount_title", "discount_ended_at", "metric_hash",
	}
	inflearnCoursePriceFactColumns = []string{
		"fetched_at", "course_id", "locale", "regular_price", "pay_price",
		"discount_rate", "discount_title", "discount_ended_at", "krw_regular_price",
		"krw_pay_price", "price_hash",
	}
	inflearnCourseCurriculumUnitColumns = []string{
		"fetched_at", "course_id", "locale", "section_id", "section_title",
		"unit_id", "unit_title", "unit_type", "runtime_sec", "is_preview",
		"has_video", "has_attachment", "quiz_id", "reading_time",
		"is_challenge_only", "unit_hash",
	}
	inflearnInstructorDimColumns = []string{
		"instructor_id", "fetched_at", "name", "slug", "thumbnail_url",
		"course_count", "student_count", "review_count", "total_star",
		"answer_count", "introduce_html",
	}
	inflearnCourseInstructorMapColumns = []string{
		"fetched_at", "course_id", "instructor_id", "role",
	}
)

func (s *Service) UseClickHouseIngest() bool {
	switch strings.ToLower(strings.TrimSpace(s.Cfg.IngestMode)) {
	case "", "clickhouse", "ch", "direct", "db":
		return true
	default:
		return false
	}
}

func (s *Service) ValidateIngest(ctx context.Context) error {
	if s.UseKafkaIngest() {
		return s.ValidateKafkaIngest(ctx)
	}
	return s.ValidateClickHouseIngest(ctx)
}

func (s *Service) ValidateClickHouseIngest(ctx context.Context) error {
	if !s.UseClickHouseIngest() {
		return nil
	}
	if !s.hasClickHouseConfig() {
		return fmt.Errorf("ClickHouse ingest requires CH_HOST/CLICKHOUSE_HOST and CH_USER/CLICKHOUSE_USER")
	}
	rows, err := s.CHQueryRows(ctx, "SELECT 1 AS ok")
	if err != nil {
		return fmt.Errorf("clickhouse preflight failed: %w", err)
	}
	if len(rows) == 0 || asInt(rows[0]["ok"]) != 1 {
		return fmt.Errorf("clickhouse preflight returned an unexpected response")
	}
	fmt.Println("[clickhouse] preflight ok mode=direct")
	return nil
}

func (s *Service) InsertCheckpointClickHouse(ctx context.Context, source string, cp Checkpoint) error {
	row := map[string]any{
		"source":        source,
		"updated_at":    FormatCHTime(NowDT64()),
		"sitemap_index": cp.SitemapIndex,
		"url_index":     cp.URLIndex,
	}
	return s.insertClickHouseRows(ctx, s.Cfg.CHRawDatabase, "inflearn_crawl_checkpoint", inflearnCrawlCheckpointColumns, []map[string]any{row})
}

func (s *Service) InsertCourseRowsClickHouse(ctx context.Context, rows CourseRows) error {
	tasks := []struct {
		database string
		table    string
		columns  []string
		rows     []map[string]any
	}{
		{s.Cfg.CHRawDatabase, "inflearn_course_snapshot_raw", inflearnCourseSnapshotRawColumns, clickHouseSnapshotRows(rows.SnapshotRaw)},
		{s.Cfg.CHServiceDatabase, "inflearn_course_dim", inflearnCourseDimColumns, rows.CourseDim},
		{s.Cfg.CHServiceDatabase, "inflearn_course_metric_fact", inflearnCourseMetricFactColumns, rows.MetricFact},
		{s.Cfg.CHServiceDatabase, "inflearn_course_price_fact", inflearnCoursePriceFactColumns, rows.PriceFact},
		{s.Cfg.CHServiceDatabase, "inflearn_course_curriculum_unit", inflearnCourseCurriculumUnitColumns, rows.CurriculumUnit},
		{s.Cfg.CHServiceDatabase, "inflearn_instructor_dim", inflearnInstructorDimColumns, rows.InstructorDim},
		{s.Cfg.CHServiceDatabase, "inflearn_course_instructor_map", inflearnCourseInstructorMapColumns, rows.CourseInstructor},
	}

	for _, task := range tasks {
		if err := s.insertClickHouseRows(ctx, task.database, task.table, task.columns, task.rows); err != nil {
			return err
		}
	}
	if rows.total() > 0 {
		fmt.Printf("[clickhouse] inserted snapshot=%d course_dim=%d metric=%d price=%d curriculum=%d instructor=%d course_instructor=%d\n",
			len(rows.SnapshotRaw), len(rows.CourseDim), len(rows.MetricFact), len(rows.PriceFact),
			len(rows.CurriculumUnit), len(rows.InstructorDim), len(rows.CourseInstructor))
	}
	return nil
}

func (r CourseRows) total() int {
	return len(r.SnapshotRaw) + len(r.CourseDim) + len(r.MetricFact) + len(r.PriceFact) +
		len(r.CurriculumUnit) + len(r.InstructorDim) + len(r.CourseInstructor)
}

func clickHouseSnapshotRows(rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		normalized := make(map[string]any, len(row)+1)
		for k, v := range row {
			if k == "payload" {
				normalized["payload_json"] = MustJSON(v)
				continue
			}
			normalized[k] = v
		}
		if _, ok := normalized["payload_json"]; !ok {
			normalized["payload_json"] = "{}"
		}
		out = append(out, normalized)
	}
	return out
}

func (s *Service) insertClickHouseRows(ctx context.Context, database, table string, columns []string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	chunkSize := s.Cfg.CHInsertChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}
	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := s.insertClickHouseRowsChunk(ctx, database, table, columns, rows[start:end]); err != nil {
			return fmt.Errorf("clickhouse insert %s.%s rows=%d offset=%d: %w", database, table, end-start, start, err)
		}
	}
	return nil
}

func (s *Service) insertClickHouseRowsChunk(ctx context.Context, database, table string, columns []string, rows []map[string]any) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO %s.%s (%s) SETTINGS insert_distributed_sync = %d FORMAT JSONEachRow\n",
		chIdent(database), chIdent(table), clickHouseColumnList(columns), boolToInt(s.Cfg.CHInsertDistributedSync))
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		if err := enc.Encode(normalizeClickHouseRow(row, columns)); err != nil {
			return err
		}
	}
	timeout := s.Cfg.CHInsertTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	insertCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := s.chPost(insertCtx, strings.TrimSpace(buf.String()), nil, "application/x-ndjson")
	return err
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func clickHouseColumnList(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		out = append(out, chIdent(col))
	}
	return strings.Join(out, ", ")
}

func normalizeClickHouseRow(row map[string]any, columns []string) map[string]any {
	out := make(map[string]any, len(columns))
	for _, col := range columns {
		out[col] = row[col]
	}
	return out
}
