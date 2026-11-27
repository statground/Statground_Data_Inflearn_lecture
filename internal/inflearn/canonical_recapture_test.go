package inflearn

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCanonicalKoreanSourceScope(t *testing.T) {
	const canonical = "https://www.inflearn.com/course/%EC%A4%91%EA%B5%AD%EC%96%B4?cid=42"
	sources, err := parseCanonicalKoreanSources([]string{canonical})
	if err != nil || len(sources) != 1 || sources[0].courseID != 42 || sources[0].url != canonical {
		t.Fatalf("canonical URL did not retain its exact identity: %v, %v", sources, err)
	}
	for _, urls := range [][]string{
		nil,
		{canonical, canonical},
		{"https://www.inflearn.com/ja/course/test?cid=42"},
		{"https://www.inflearn.com/course/test?cid=42&locale=ja"},
		{"https://www.inflearn.com/course/test"},
		{"https://example.invalid/course/test?cid=42"},
	} {
		if _, err := parseCanonicalKoreanSources(urls); err == nil {
			t.Errorf("invalid URL scope admitted: %v", urls)
		}
	}
}

func TestCanonicalKoreanExistingSourceStopsBeforeAnyWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.HasPrefix(string(body), "SELECT 1 AS ok"):
			writeCHRows(w, []map[string]any{{"ok": 1}})
		case strings.Contains(string(body), "latest_is_mismatch") && strings.Contains(string(body), "locale = 'ko'"):
			writeCHRows(w, []map[string]any{{"course_id": 42, "latest_is_mismatch": 0}})
		default:
			t.Errorf("unexpected request during reconciliation hold: %s", body)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	service := testClickHouseService(t, server)
	service.Cfg.IngestMode = "clickhouse"
	service.Cfg.StateBackend = "clickhouse"
	service.Cfg.CHInsertDistributedSync = true
	service.Cfg.CHDirectOutboxFallback = false
	if err := service.RunCanonicalKoreanRecapture(context.Background(), []string{"https://www.inflearn.com/course/test?cid=42"}); err == nil || !strings.Contains(err.Error(), "no longer has the latest legacy URL mismatch") {
		t.Fatalf("expected existing-source hold, got %v", err)
	}
}
