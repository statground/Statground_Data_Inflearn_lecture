package inflearn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPickUpdateURLsKeepsGlobalFloorAndAddsDistinctPublicPriority(t *testing.T) {
	queries := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		queries = append(queries, query)
		rows := make([]map[string]any, 0, 200)
		for id := 1; id <= 200; id++ {
			row := map[string]any{
				"course_id":            id,
				"locale":               "ko",
				"source_url":           fmt.Sprintf("https://example.test/course/%d", id),
				"public_stale":         0,
				"public_priority_tier": 1,
			}
			if id >= 51 {
				row["public_stale"] = 1
			}
			if id >= 151 {
				row["public_priority_tier"] = 0
			}
			rows = append(rows, row)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicUpdatePriority = true
	svc.Cfg.PublicationV2Enabled = false
	picks, err := svc.pickUpdateURLs(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(picks) != maximumUpdateLimit {
		t.Fatalf("picked=%d, want %d", len(picks), maximumUpdateLimit)
	}
	seen := map[string]bool{}
	for i, pick := range picks {
		key := localCourseKey(pick.CourseID, pick.Locale)
		if seen[key] {
			t.Fatalf("duplicate exact key %s", key)
		}
		seen[key] = true
		wantSelection := "global_oldest"
		if i >= normalGlobalUpdateLimit {
			wantSelection = "public_priority"
		}
		if pick.Selection != wantSelection {
			t.Fatalf("pick[%d] selection=%q, want %q", i, pick.Selection, wantSelection)
		}
	}
	if len(queries) != 1 {
		t.Fatalf("queries=%d, want one shared raw scan", len(queries))
	}
	if got := strings.Count(queries[0], ".inflearn_course_snapshot_raw"); got != 1 {
		t.Fatalf("raw table references=%d, want one shared aggregation", got)
	}
	for _, want := range []string{
		"lecture_publication.v_inflearn_public_update_priority_keys",
		"min(priority_tier) AS priority_tier",
		"public_priority_tier",
		"INTERVAL 24 HOUR",
		"ORDER BY last_fetched_at ASC, course_id ASC, locale ASC",
	} {
		if !strings.Contains(queries[0], want) {
			t.Fatalf("selection query missing %q: %s", want, queries[0])
		}
	}
	if picks[normalGlobalUpdateLimit].CourseID != 151 {
		t.Fatalf("first public-priority course=%d, want tier-0 course 151 before older tier-1 rows", picks[normalGlobalUpdateLimit].CourseID)
	}
}

func TestPickUpdateURLsInactiveRolloutPreservesLegacyGlobalHundred(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query = string(body)
		rows := make([]map[string]any, 0, 100)
		for id := 1; id <= 100; id++ {
			rows = append(rows, map[string]any{
				"course_id": id, "locale": "ko", "source_url": fmt.Sprintf("https://example.test/course/%d", id),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicationV2Enabled = false
	svc.Cfg.PublicUpdatePriority = false
	picks, err := svc.pickUpdateURLs(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(picks) != 100 || strings.Contains(query, "LIMIT 100") {
		t.Fatalf("picks=%d query=%s, want post-filter global 100 without a pre-filter SQL limit", len(picks), query)
	}
	for _, forbidden := range []string{"lecture_publication.", "v_inflearn_r_lecture_catalog", "v_inflearn_language_lecture_catalog"} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("inactive rollout queried unavailable publication object %q: %s", forbidden, query)
		}
	}
}

func TestPickUpdateURLsRuntimeCheckHonorsExactLimitTen(t *testing.T) {
	queries := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		queries = append(queries, query)
		rows := make([]map[string]any, 0, 20)
		for id := 1; id <= 20; id++ {
			rows = append(rows, map[string]any{
				"course_id":  id,
				"locale":     "ko",
				"source_url": fmt.Sprintf("https://example.test/course/%d", id),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicUpdatePriority = true
	svc.Cfg.PublicationV2Enabled = false
	picks, err := svc.pickUpdateURLs(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(picks) != 10 {
		t.Fatalf("picked=%d, want exact runtime-check cap 10", len(picks))
	}
	if len(queries) != 1 || strings.Contains(queries[0], "LIMIT 10") {
		t.Fatalf("runtime-check must cap after locale validation: %v", queries)
	}
	if strings.Contains(queries[0], "v_inflearn_public_update_priority_keys") {
		t.Fatalf("runtime-check must not add public-priority work: %s", queries[0])
	}
}

func TestPickGlobalUpdateURLsSkipsLocaleMismatchesBeforeTakingLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := make([]map[string]any, 0, 260)
		for id := 1; id <= 160; id++ {
			rows = append(rows, map[string]any{
				"course_id": id, "locale": "ko",
				"source_url": fmt.Sprintf("https://www.inflearn.com/ja/course/%d?cid=%d", id, id),
			})
		}
		for id := 161; id <= 260; id++ {
			rows = append(rows, map[string]any{
				"course_id": id, "locale": "ko",
				"source_url": fmt.Sprintf("https://www.inflearn.com/course/%d?cid=%d", id, id),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicationV2Enabled = false
	picks, err := svc.pickUpdateURLs(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(picks) != 100 || picks[0].CourseID != 161 || picks[99].CourseID != 260 {
		t.Fatalf("picks=%d first=%+v last=%+v, want 100 locale-consistent rows behind mismatches", len(picks), picks[0], picks[len(picks)-1])
	}
}

func TestUpdatePickRejectsStoredLocaleDifferentFromURLLocale(t *testing.T) {
	row := map[string]any{
		"course_id": 333786, "locale": "ko",
		"source_url": "https://www.inflearn.com/ja/course/example?cid=333786",
	}
	if pick, ok := updatePickFromRow(row, "global_oldest"); ok {
		t.Fatalf("mismatched legacy row admitted: %+v", pick)
	}
	row["locale"] = "ja"
	if pick, ok := updatePickFromRow(row, "global_oldest"); !ok || pick.Locale != "ja" {
		t.Fatalf("matching row rejected: %+v ok=%v", pick, ok)
	}
}

func TestRunUpdateExistingReturnsMachineDeferredOnTemporaryCheckpointRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		if strings.Contains(query, "SELECT 1 AS ok") {
			_, _ = io.WriteString(w, `{"data":[{"ok":1}]}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "Code: 202. DB::Exception: Too many simultaneous queries")
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicUpdatePriority = true
	svc.Cfg.PublicationV2Enabled = false
	svc.Cfg.IngestMode = "clickhouse"
	svc.Cfg.CHDirectOutboxFallback = false
	svc.Cfg.CHPreflightRetryBudget = time.Second
	svc.Cfg.CHPreflightRetryBackoff = time.Millisecond
	err := svc.RunUpdateExisting(context.Background())
	assertUpdateStateError(t, err, "deferred", "checkpoint_read", "too_many_simultaneous_queries")
}

func TestRunUpdateExistingReturnsMachineDeferredOnTemporarySelectionRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		switch {
		case strings.Contains(query, "SELECT 1 AS ok"):
			_, _ = io.WriteString(w, `{"data":[{"ok":1}]}`)
		case strings.Contains(query, "inflearn_crawl_checkpoint"):
			_, _ = io.WriteString(w, `{"data":[{"sitemap_index":0,"url_index":0}]}`)
		case strings.Contains(query, "public_exact AS"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "Code: 159. DB::Exception: Timeout exceeded")
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicUpdatePriority = true
	svc.Cfg.PublicationV2Enabled = false
	svc.Cfg.IngestMode = "clickhouse"
	svc.Cfg.CHDirectOutboxFallback = false
	err := svc.RunUpdateExisting(context.Background())
	assertUpdateStateError(t, err, "deferred", "update_selection_read", "timeout")
}

func TestRunUpdateExistingReturnsMachineDegradedOnTemporaryCheckpointWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := r.URL.Query().Get("query")
		if query == "" {
			query = string(body)
		}
		switch {
		case strings.Contains(query, "SELECT 1 AS ok"):
			_, _ = io.WriteString(w, `{"data":[{"ok":1}]}`)
		case strings.Contains(query, "inflearn_crawl_checkpoint") && strings.Contains(query, "argMax"):
			_, _ = io.WriteString(w, `{"data":[{"sitemap_index":0,"url_index":0}]}`)
		case strings.Contains(query, "public_exact AS"):
			_, _ = io.WriteString(w, `{"data":[]}`)
		case strings.Contains(query, "INSERT INTO") && strings.Contains(query, "inflearn_crawl_checkpoint"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "Code: 159. DB::Exception: Timeout exceeded")
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.StateBackend = "clickhouse"
	svc.Cfg.PublicUpdatePriority = true
	svc.Cfg.PublicationV2Enabled = false
	svc.Cfg.IngestMode = "clickhouse"
	svc.Cfg.CHDirectReplicaFallback = false
	svc.Cfg.CHDirectOutboxFallback = false
	svc.Cfg.CHInsertTimeout = time.Second
	err := svc.RunUpdateExisting(context.Background())
	assertUpdateStateError(t, err, "degraded", "checkpoint_write", "timeout")
}

func TestCourseBatchWriteTemporaryFailureIsMachineDegraded(t *testing.T) {
	err := courseBatchWriteError(errString("Code: 159. DB::Exception: Timeout exceeded after insert dispatch"))
	assertUpdateStateError(t, err, "degraded", "course_batch_write", "timeout")
}

func TestWriteUpdateCourseBatchInsertTimeoutIsMachineDegraded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "Code: 159. DB::Exception: Timeout exceeded after insert dispatch")
	}))
	defer server.Close()

	svc := testClickHouseService(t, server)
	svc.Cfg.IngestMode = "clickhouse"
	svc.Cfg.CHDirectReplicaFallback = false
	svc.Cfg.CHDirectOutboxFallback = false
	err := svc.writeUpdateCourseBatch(context.Background(), CourseRows{
		SnapshotRaw: []map[string]any{{"course_id": 1, "payload": map[string]any{}}},
	})
	assertUpdateStateError(t, err, "degraded", "course_batch_write", "timeout")
}

func TestCourseBatchWriteContractFailureRemainsExact(t *testing.T) {
	want := errString("Code: 60. DB::Exception: Table does not exist")
	if got := courseBatchWriteError(want); got != want {
		t.Fatalf("error=%v, want original contract failure", got)
	}
}

func assertUpdateStateError(t *testing.T, err error, status, phase, category string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected update state error")
	}
	var payload map[string]any
	if jsonErr := json.Unmarshal([]byte(err.Error()), &payload); jsonErr != nil {
		t.Fatalf("error is not machine-readable JSON: %v (%v)", err, jsonErr)
	}
	for key, want := range map[string]string{
		"schema":   "statground.inflearn.update_state.v1",
		"status":   status,
		"phase":    phase,
		"category": category,
	} {
		if got := asString(payload[key]); got != want {
			t.Fatalf("%s=%q, want %q; payload=%v", key, got, want, payload)
		}
	}
}
