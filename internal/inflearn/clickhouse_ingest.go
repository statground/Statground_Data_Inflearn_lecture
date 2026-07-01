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
	inflearnCourseDisplayTranslationColumns = []string{
		"course_id", "source_locale", "target_language", "source_hash", "translated_at",
		"title", "description", "category_main_title", "category_sub_title", "level_code",
		"keywords", "model", "prompt_hash", "generation_status", "generation_error", "ingested_at",
	}
	inflearnCourseCurriculumDisplayTranslationColumns = []string{
		"course_id", "source_locale", "target_language", "section_id", "unit_id", "source_hash",
		"translated_at", "section_title", "unit_title", "model", "prompt_hash",
		"generation_status", "generation_error", "ingested_at",
	}
	inflearnDirectInsertOutboxColumns = []string{
		"outbox_uuid", "created_at", "target_database", "target_table",
		"target_columns", "rows_json", "row_count", "payload_hash", "source_error",
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
	if s.Cfg.CHDirectOutboxFallback {
		drained, err := s.drainClickHouseDirectOutbox(ctx)
		if err != nil {
			fmt.Printf("[warn] clickhouse direct outbox drain skipped: %s\n", s.sanitizeClickHouseError(err))
		} else if drained > 0 {
			fmt.Printf("[clickhouse] drained direct outbox chunks=%d\n", drained)
		}
	}
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
	payload, err := encodeClickHouseJSONEachRow(columns, rows)
	if err != nil {
		return err
	}
	timeout := s.Cfg.CHInsertTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	insertCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.postClickHouseJSONEachRow(insertCtx, database, table, columns, payload); err == nil {
		return nil
	} else if !isTemporaryClickHouseWriteError(err) {
		return err
	} else {
		if s.Cfg.CHDirectReplicaFallback {
			if fbErr := s.insertClickHouseRowsChunkViaWritableReplica(ctx, database, table, columns, payload, len(rows), err); fbErr == nil {
				return nil
			} else {
				fmt.Printf("[warn] clickhouse writable-replica fallback unavailable target=%s.%s rows=%d error=%s\n",
					database, table, len(rows), s.sanitizeClickHouseError(fbErr))
			}
		}
		if s.Cfg.CHDirectOutboxFallback {
			if outboxErr := s.enqueueClickHouseDirectOutbox(ctx, database, table, columns, payload, len(rows), err); outboxErr == nil {
				return nil
			} else {
				return fmt.Errorf("%w; clickhouse direct outbox enqueue failed: %s", err, s.sanitizeClickHouseError(outboxErr))
			}
		}
		return err
	}
}

func encodeClickHouseJSONEachRow(columns []string, rows []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		if err := enc.Encode(normalizeClickHouseRow(row, columns)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (s *Service) postClickHouseJSONEachRow(ctx context.Context, database, table string, columns []string, payload []byte) error {
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) SETTINGS insert_distributed_sync = %d FORMAT JSONEachRow",
		chIdent(database), chIdent(table), clickHouseColumnList(columns), boolToInt(s.Cfg.CHInsertDistributedSync))
	_, err := s.chPost(ctx, sql, payload, "application/x-ndjson")
	return err
}

func (s *Service) insertClickHouseRowsChunkViaWritableReplica(ctx context.Context, database, table string, columns []string, payload []byte, rowCount int, originalErr error) error {
	hosts, err := s.writableClickHouseReplicaHosts(ctx, database, clickHouseLocalTableName(table))
	if err != nil {
		return err
	}
	if len(hosts) == 0 {
		return fmt.Errorf("no writable replica candidates after %s", s.sanitizeClickHouseError(originalErr))
	}
	timeout := s.Cfg.CHInsertTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	localTable := clickHouseLocalTableName(table)
	var lastErr error
	for _, host := range hosts {
		insertCtx, cancel := context.WithTimeout(ctx, timeout)
		sql := fmt.Sprintf("INSERT INTO FUNCTION remote(%s, %s, %s) (%s) FORMAT JSONEachRow",
			QuoteSQLString(host), QuoteSQLString(database), QuoteSQLString(localTable), clickHouseColumnList(columns))
		_, lastErr = s.chPost(insertCtx, sql, payload, "application/x-ndjson")
		cancel()
		if lastErr == nil {
			fmt.Printf("[clickhouse] writable-replica fallback inserted target=%s.%s local_table=%s host=%s rows=%d\n",
				database, table, localTable, host, rowCount)
			return nil
		}
	}
	return fmt.Errorf("all writable replica candidates failed: %s", s.sanitizeClickHouseError(lastErr))
}

func (s *Service) writableClickHouseReplicaHosts(ctx context.Context, database, localTable string) ([]string, error) {
	cluster := strings.TrimSpace(s.Cfg.CHCluster)
	if cluster == "" {
		cluster = "statground_cluster"
	}
	sql := fmt.Sprintf(`
        SELECT hostName() AS host
        FROM clusterAllReplicas(%s, 'system', 'replicas')
        WHERE database = %s
          AND table = %s
          AND is_readonly = 0
          AND is_session_expired = 0
        ORDER BY absolute_delay ASC, queue_size ASC, host ASC
    `, QuoteSQLString(cluster), QuoteSQLString(database), QuoteSQLString(localTable))
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	hosts := make([]string, 0, len(rows))
	for _, row := range rows {
		host := strings.TrimSpace(asString(row["host"]))
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func (s *Service) enqueueClickHouseDirectOutbox(ctx context.Context, database, table string, columns []string, payload []byte, rowCount int, sourceErr error) error {
	outboxDB := strings.TrimSpace(s.Cfg.CHOutboxDatabase)
	if outboxDB == "" {
		outboxDB = "Data_Lecture_Inflearn_Log"
	}
	outboxTable := strings.TrimSpace(s.Cfg.CHOutboxTable)
	if outboxTable == "" {
		outboxTable = "inflearn_direct_insert_outbox"
	}
	payloadHash := H64(database + "\x1f" + table + "\x1f" + strings.Join(columns, "\x1f") + "\x1f" + string(payload))
	if exists, err := s.pendingClickHouseDirectOutboxExists(ctx, outboxDB, outboxTable, database, table, payloadHash); err == nil && exists {
		fmt.Printf("[clickhouse] direct outbox already has pending chunk target=%s.%s rows=%d hash=%d\n", database, table, rowCount, payloadHash)
		return nil
	}
	now := NowDT64()
	row := map[string]any{
		"outbox_uuid":    UUIDv7String(now),
		"created_at":     FormatCHTime(now),
		"target_database": database,
		"target_table":    table,
		"target_columns":  columns,
		"rows_json":       string(payload),
		"row_count":       rowCount,
		"payload_hash":    payloadHash,
		"source_error":    s.sanitizeClickHouseError(sourceErr),
	}
	body, err := encodeClickHouseJSONEachRow(inflearnDirectInsertOutboxColumns, []map[string]any{row})
	if err != nil {
		return err
	}
	insertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.postClickHouseJSONEachRow(insertCtx, outboxDB, outboxTable, inflearnDirectInsertOutboxColumns, body); err != nil {
		return err
	}
	fmt.Printf("[clickhouse] queued direct outbox target=%s.%s rows=%d hash=%d reason=%s\n",
		database, table, rowCount, payloadHash, s.sanitizeClickHouseError(sourceErr))
	return nil
}

func (s *Service) pendingClickHouseDirectOutboxExists(ctx context.Context, outboxDB, outboxTable, database, table string, payloadHash uint64) (bool, error) {
	sql := fmt.Sprintf(`
        SELECT count() AS c
        FROM %s.%s
        WHERE target_database = %s
          AND target_table = %s
          AND payload_hash = %d
          AND replayed_at IS NULL
        LIMIT 1
    `, chIdent(outboxDB), chIdent(outboxTable), QuoteSQLString(database), QuoteSQLString(table), payloadHash)
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return false, err
	}
	return len(rows) > 0 && asInt64(rows[0]["c"]) > 0, nil
}

func (s *Service) drainClickHouseDirectOutbox(ctx context.Context) (int, error) {
	limit := s.Cfg.CHOutboxReplayLimit
	if limit <= 0 {
		return 0, nil
	}
	outboxDB := strings.TrimSpace(s.Cfg.CHOutboxDatabase)
	if outboxDB == "" {
		outboxDB = "Data_Lecture_Inflearn_Log"
	}
	outboxTable := strings.TrimSpace(s.Cfg.CHOutboxTable)
	if outboxTable == "" {
		outboxTable = "inflearn_direct_insert_outbox"
	}
	sql := fmt.Sprintf(`
        SELECT outbox_uuid, target_database, target_table, target_columns, rows_json, row_count
        FROM %s.%s
        WHERE replayed_at IS NULL
        ORDER BY created_at ASC, outbox_uuid ASC
        LIMIT %d
    `, chIdent(outboxDB), chIdent(outboxTable), limit)
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return 0, err
	}
	drained := 0
	deferredTargets := map[string]struct{}{}
	for _, row := range rows {
		outboxUUID := asString(row["outbox_uuid"])
		database := asString(row["target_database"])
		table := asString(row["target_table"])
		columns := asStringSlice(row["target_columns"])
		payload := []byte(asString(row["rows_json"]))
		rowCount := asInt(row["row_count"])
		if outboxUUID == "" || database == "" || table == "" || len(columns) == 0 || len(payload) == 0 {
			continue
		}
		targetKey := database + "." + table
		if _, ok := deferredTargets[targetKey]; ok {
			continue
		}
		hasWritable, err := s.clickHouseTargetHasWritableReplica(ctx, database, table)
		if err != nil {
			if isTemporaryClickHouseWriteError(err) {
				fmt.Printf("[warn] clickhouse direct outbox replay deferred target=%s rows=%d error=%s\n",
					targetKey, rowCount, s.sanitizeClickHouseError(err))
				deferredTargets[targetKey] = struct{}{}
				continue
			}
			return drained, err
		}
		if !hasWritable {
			fmt.Printf("[warn] clickhouse direct outbox replay deferred target=%s rows=%d reason=no-writable-replica\n",
				targetKey, rowCount)
			deferredTargets[targetKey] = struct{}{}
			continue
		}
		var insertErr error
		if s.Cfg.CHDirectReplicaFallback {
			insertErr = s.insertClickHouseRowsChunkViaWritableReplica(ctx, database, table, columns, payload, rowCount, fmt.Errorf("outbox replay"))
		} else {
			timeout := s.Cfg.CHInsertTimeout
			if timeout <= 0 {
				timeout = 5 * time.Minute
			}
			insertCtx, cancel := context.WithTimeout(ctx, timeout)
			insertErr = s.postClickHouseJSONEachRow(insertCtx, database, table, columns, payload)
			cancel()
		}
		if insertErr != nil {
			fmt.Printf("[warn] clickhouse direct outbox replay failed uuid=%s target=%s.%s rows=%d error=%s\n",
				outboxUUID, database, table, rowCount, s.sanitizeClickHouseError(insertErr))
			_ = s.markClickHouseDirectOutboxReplay(ctx, outboxDB, outboxTable, outboxUUID, false, insertErr)
			continue
		}
		if err := s.markClickHouseDirectOutboxReplay(ctx, outboxDB, outboxTable, outboxUUID, true, nil); err != nil {
			fmt.Printf("[warn] clickhouse direct outbox replay mark failed uuid=%s error=%s\n", outboxUUID, s.sanitizeClickHouseError(err))
		}
		drained++
		fmt.Printf("[clickhouse] direct outbox replayed uuid=%s target=%s.%s rows=%d\n", outboxUUID, database, table, rowCount)
	}
	return drained, nil
}

func (s *Service) clickHouseTargetHasWritableReplica(ctx context.Context, database, table string) (bool, error) {
	hosts, err := s.writableClickHouseReplicaHosts(ctx, database, clickHouseLocalTableName(table))
	if err != nil {
		return false, err
	}
	return len(hosts) > 0, nil
}

func (s *Service) markClickHouseDirectOutboxReplay(ctx context.Context, outboxDB, outboxTable, outboxUUID string, success bool, replayErr error) error {
	var sql string
	if success {
		sql = fmt.Sprintf(`
            ALTER TABLE %s.%s
            UPDATE replay_attempt = replay_attempt + 1,
                   last_replay_at = now64(3, 'Asia/Seoul'),
                   replayed_at = now64(3, 'Asia/Seoul'),
                   replay_error = ''
            WHERE outbox_uuid = toUUID(%s)
            SETTINGS mutations_sync = 1
        `, chIdent(outboxDB), chIdent(outboxTable), QuoteSQLString(outboxUUID))
	} else {
		sql = fmt.Sprintf(`
            ALTER TABLE %s.%s
            UPDATE replay_attempt = replay_attempt + 1,
                   last_replay_at = now64(3, 'Asia/Seoul'),
                   replay_error = %s
            WHERE outbox_uuid = toUUID(%s)
            SETTINGS mutations_sync = 1
        `, chIdent(outboxDB), chIdent(outboxTable), QuoteSQLString(s.sanitizeClickHouseError(replayErr)), QuoteSQLString(outboxUUID))
	}
	markCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := s.chPost(markCtx, strings.TrimSpace(sql), nil, "text/plain; charset=utf-8")
	return err
}

func clickHouseLocalTableName(table string) string {
	table = strings.TrimSpace(table)
	if strings.HasSuffix(table, "_local") {
		return table
	}
	return table + "_local"
}

func isTemporaryClickHouseWriteError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	needles := []string{
		"code: 242",
		"code: 667",
		"code: 999",
		"table_is_read_only",
		"not_initialized",
		"not initialized",
		"not initialized yet",
		"readonly mode",
		"read-only",
		"keeper_exception",
		"coordination error",
		"connection loss",
		"session expired",
		"replica is not active",
		"replica is not ready",
		"replica is not initialized",
		"zookeeper",
		"clickhouse keeper",
		"network_error",
		"socket_timeout",
		"timeout_exceeded",
		"all connection tries failed",
		"context deadline exceeded",
		"i/o timeout",
		"no route to host",
		"connection refused",
		"timeout",
		"temporary",
		"connection reset",
		"broken pipe",
		"eof",
	}
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func (s *Service) sanitizeClickHouseError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	for _, secret := range []string{s.Cfg.CHPassword, s.Cfg.KafkaPassword, s.Cfg.TranslationAPIKey} {
		secret = strings.TrimSpace(secret)
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "***")
		}
	}
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 400 {
		text = text[:400]
	}
	return text
}

func asStringSlice(v any) []string {
	switch x := v.(type) {
	case nil:
		return nil
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			s := strings.TrimSpace(asString(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		parts := splitCSV(x)
		if len(parts) > 0 {
			return parts
		}
		return nil
	default:
		return nil
	}
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
