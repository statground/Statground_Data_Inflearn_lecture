package inflearn

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigClickHouseIngestDoesNotRequireKafka(t *testing.T) {
	t.Setenv("INGEST_MODE", "clickhouse")
	t.Setenv("CH_HOST", "clickhouse.example")
	t.Setenv("CH_USER", "statground_ch_app")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("CH_INSERT_CHUNK_SIZE", "")
	t.Setenv("CH_INSERT_TIMEOUT_SECONDS", "")
	t.Setenv("CH_PREFLIGHT_RETRY_BUDGET_SECONDS", "")
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", "")
	t.Setenv("CH_PREFLIGHT_RETRY_BACKOFF_SECONDS", "")
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS", "")
	t.Setenv("CH_INSERT_DISTRIBUTED_SYNC", "")
	t.Setenv("CH_DIRECT_REPLICA_FALLBACK", "")
	t.Setenv("CH_DIRECT_OUTBOX_FALLBACK", "")
	t.Setenv("CH_OUTBOX_DATABASE", "")
	t.Setenv("CH_OUTBOX_TABLE", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.IngestMode != "clickhouse" {
		t.Fatalf("IngestMode = %q, want clickhouse", cfg.IngestMode)
	}
	if cfg.CHInsertChunkSize != 100 {
		t.Fatalf("CHInsertChunkSize = %d, want 100", cfg.CHInsertChunkSize)
	}
	if cfg.CHInsertTimeout != 5*time.Minute {
		t.Fatalf("CHInsertTimeout = %s, want 5m", cfg.CHInsertTimeout)
	}
	if cfg.CHPreflightRetryBudget != 90*time.Second || cfg.CHPreflightRetryBackoff != 5*time.Second {
		t.Fatalf("preflight retry config = %s/%s, want 90s/5s", cfg.CHPreflightRetryBudget, cfg.CHPreflightRetryBackoff)
	}
	if cfg.CHInsertDistributedSync {
		t.Fatal("CHInsertDistributedSync should default to false")
	}
	if !cfg.CHDirectReplicaFallback {
		t.Fatal("CHDirectReplicaFallback should default to true")
	}
	if !cfg.CHDirectOutboxFallback {
		t.Fatal("CHDirectOutboxFallback should default to true")
	}
	if cfg.CHOutboxDatabase != "Data_Lecture_Inflearn_Log" {
		t.Fatalf("CHOutboxDatabase = %q, want Data_Lecture_Inflearn_Log", cfg.CHOutboxDatabase)
	}
	if cfg.CHOutboxTable != "inflearn_direct_insert_outbox" {
		t.Fatalf("CHOutboxTable = %q, want inflearn_direct_insert_outbox", cfg.CHOutboxTable)
	}
}

func TestLoadConfigKafkaIngestStillRequiresBrokers(t *testing.T) {
	t.Setenv("INGEST_MODE", "kafka")
	t.Setenv("CH_HOST", "clickhouse.example")
	t.Setenv("CH_USER", "statground_ch_app")
	t.Setenv("KAFKA_BROKERS", "")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected missing KAFKA_BROKERS error")
	}
}

func TestClickHouseSnapshotRowsConvertPayloadToPayloadJSON(t *testing.T) {
	rows := clickHouseSnapshotRows([]map[string]any{{
		"uuid":         "019f1d52-0000-7000-8000-000000000001",
		"payload":      map[string]any{"statusCode": "OK"},
		"payload_hash": uint64(42),
	}})
	if len(rows) != 1 {
		t.Fatalf("row count = %d, want 1", len(rows))
	}
	if _, ok := rows[0]["payload"]; ok {
		t.Fatal("payload key should not be sent to ClickHouse direct insert")
	}
	if got := rows[0]["payload_json"]; got != `{"statusCode":"OK"}` {
		t.Fatalf("payload_json = %#v", got)
	}
}

func TestBoolToInt(t *testing.T) {
	if got := boolToInt(false); got != 0 {
		t.Fatalf("boolToInt(false) = %d, want 0", got)
	}
	if got := boolToInt(true); got != 1 {
		t.Fatalf("boolToInt(true) = %d, want 1", got)
	}
}

func TestClickHouseLocalTableName(t *testing.T) {
	if got := clickHouseLocalTableName("inflearn_course_snapshot_raw"); got != "inflearn_course_snapshot_raw_local" {
		t.Fatalf("local table = %q", got)
	}
	if got := clickHouseLocalTableName("inflearn_course_snapshot_raw_local"); got != "inflearn_course_snapshot_raw_local" {
		t.Fatalf("local table should not double suffix, got %q", got)
	}
}

func TestClickHouseResponseCategoryAndTransientBoundary(t *testing.T) {
	cases := []struct {
		name      string
		message   string
		category  string
		temporary bool
	}{
		{"not initialized", "Code: 667. DB::Exception: Table secret_local is not initialized yet. (NOT_INITIALIZED)", "not_initialized", true},
		{"cancelled code", "Code: 394. DB::Exception: query cancelled by the workload scheduler", "query_cancelled", true},
		{"cancelled name", "DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)", "query_cancelled", true},
		{"simultaneous query overload", "Code: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES)", "too_many_simultaneous_queries", true},
		{"pending query overload", "DB::Exception: Too many pending queries. (TOO_MANY_PENDING_QUERIES)", "too_many_pending_queries", true},
		{"memory overload", "Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)", "memory_limit_exceeded", true},
		{"part overload", "Code: 252. DB::Exception: Too many parts. (TOO_MANY_PARTS)", "too_many_parts", true},
		{"temporary server", "upstream service temporarily unavailable; try again later", "temporary_unavailable", true},
		{"authentication", "Code: 516. DB::Exception: Authentication failed: password is incorrect. (AUTHENTICATION_FAILED)", "authentication", false},
		{"permission", "Code: 497. DB::Exception: Not enough privileges. (ACCESS_DENIED)", "permission", false},
		{"schema", "Code: 60. DB::Exception: Table secret_table does not exist. (UNKNOWN_TABLE)", "schema", false},
		{"parse beats echoed transient text", "Code: 62. DB::Exception: Syntax error near string 'QUERY_WAS_CANCELLED'. (SYNTAX_ERROR)", "parse", false},
		{"unknown 500 body", "Code: 1001. DB::Exception: internal failure", "request_failed", false},
		{"code prefix is not code 394", "Code: 3940. DB::Exception: internal failure", "request_failed", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			category := clickHouseResponseCategory(tc.message)
			if category != tc.category {
				t.Fatalf("category = %q, want %q", category, tc.category)
			}
			err := fmt.Errorf("clickhouse http status=500 category=%s", category)
			if got := isTemporaryClickHouseWriteError(err); got != tc.temporary {
				t.Fatalf("isTemporaryClickHouseWriteError(%q) = %v, want %v", err, got, tc.temporary)
			}
		})
	}
}

func TestInsertClickHouseRowsChunkUsesOutboxOnlyForTransientHTTPCategory(t *testing.T) {
	cases := []struct {
		name            string
		response        string
		wantErr         bool
		wantRequests    int
		wantOutboxWrite int
	}{
		{
			name:            "query cancelled falls back",
			response:        "Code: 394. DB::Exception: secret response from http://clickhouse.internal:8123/ (QUERY_WAS_CANCELLED)",
			wantRequests:    3,
			wantOutboxWrite: 1,
		},
		{
			name:         "parse failure stays fail fast",
			response:     "Code: 62. DB::Exception: secret response from http://clickhouse.internal:8123/ (SYNTAX_ERROR)",
			wantErr:      true,
			wantRequests: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			outboxWrites := 0
			outboxPayload := ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				body, _ := io.ReadAll(r.Body)
				sql := r.URL.Query().Get("query")
				if sql == "" {
					sql = string(body)
				}
				switch {
				case strings.Contains(sql, "SELECT count() AS c"):
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"data":[{"c":0}]}`))
				case strings.Contains(sql, "inflearn_direct_insert_outbox"):
					outboxWrites++
					outboxPayload = string(body)
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(tc.response))
				}
			}))
			defer server.Close()

			svc := testClickHouseService(t, server)
			err := svc.insertClickHouseRowsChunk(
				context.Background(),
				"Data_Lecture_Inflearn_Service",
				"inflearn_course_dim",
				[]string{"course_id"},
				[]map[string]any{{"course_id": 42}},
			)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tc.wantErr)
			}
			if err != nil {
				for _, leaked := range []string{"secret response", "clickhouse.internal", server.URL} {
					if strings.Contains(err.Error(), leaked) {
						t.Fatalf("error leaked %q: %v", leaked, err)
					}
				}
			}
			if requests != tc.wantRequests {
				t.Fatalf("requests = %d, want %d", requests, tc.wantRequests)
			}
			if outboxWrites != tc.wantOutboxWrite {
				t.Fatalf("outbox writes = %d, want %d", outboxWrites, tc.wantOutboxWrite)
			}
			for _, leaked := range []string{"secret response", "clickhouse.internal", server.URL} {
				if strings.Contains(outboxPayload, leaked) {
					t.Fatalf("outbox payload leaked %q: %s", leaked, outboxPayload)
				}
			}
		})
	}
}

func TestCHPostDoesNotExposeTransportEndpoint(t *testing.T) {
	svc := &Service{
		Cfg: Config{
			CHHost:        "clickhouse-secret.internal",
			CHPort:        8123,
			CHRawDatabase: "Data_Lecture_Inflearn_Raw",
			UserAgent:     "test",
		},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("dial failed for %s: connection refused", req.URL.String())
		})},
	}
	_, err := svc.chPost(context.Background(), "SELECT 1", nil, "text/plain")
	if err == nil {
		t.Fatal("expected transport error")
	}
	if got := err.Error(); got != "clickhouse transport category=temporary_network" {
		t.Fatalf("error = %q, want sanitized temporary_network category", got)
	}
	for _, leaked := range []string{"clickhouse-secret.internal", "http://", "SELECT 1"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("transport error leaked %q: %v", leaked, err)
		}
	}
}

func TestSanitizeClickHouseErrorRedactsEndpoint(t *testing.T) {
	svc := &Service{Cfg: Config{CHHost: "clickhouse-secret.internal"}}
	got := svc.sanitizeClickHouseError(errString(`Post "http://clickhouse-secret.internal:8123/?database=secret": context deadline exceeded`))
	if strings.Contains(got, "clickhouse-secret.internal") || strings.Contains(got, "http://") {
		t.Fatalf("sanitized error leaked endpoint: %q", got)
	}
	if !isTemporaryClickHouseWriteError(errString(got)) {
		t.Fatalf("sanitized timeout lost transient classification: %q", got)
	}
}

func TestIsTemporaryClickHouseWriteError(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"Code: 242. DB::Exception: Table is in readonly mode (TABLE_IS_READ_ONLY)", true},
		{"Code: 667. DB::Exception: Table is not initialized yet. (NOT_INITIALIZED)", true},
		{"KEEPER_EXCEPTION Coordination error: Connection loss", true},
		{"Code: 999. DB::Exception: ClickHouse Keeper: session expired", true},
		{"Code: 210. DB::NetException: Connection refused", true},
		{"Post http://clickhouse:8123/: context deadline exceeded", true},
		{"Code: 60. DB::Exception: Table does not exist", false},
		{"Code: 497. DB::Exception: Not enough privileges", false},
	}
	for _, tc := range cases {
		if got := isTemporaryClickHouseWriteError(errString(tc.text)); got != tc.want {
			t.Fatalf("isTemporaryClickHouseWriteError(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestRetryClickHousePreflightRecoversWithinBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	calls := 0
	attempts, err := retryClickHousePreflight(ctx, time.Millisecond, func(context.Context) error {
		calls++
		if calls < 3 {
			return errString("connection refused")
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d error=%v, want three attempts and success", attempts, err)
	}
}

func TestRetryClickHousePreflightStopsAtBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	attempts, err := retryClickHousePreflight(ctx, 5*time.Millisecond, func(context.Context) error {
		return errString("connection refused")
	})
	if err == nil || attempts < 2 || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("attempts=%d elapsed=%s error=%v, want bounded transient retries", attempts, time.Since(started), err)
	}
}

func TestRetryClickHousePreflightFailsContractImmediately(t *testing.T) {
	calls := 0
	attempts, err := retryClickHousePreflight(context.Background(), time.Millisecond, func(context.Context) error {
		calls++
		return errString("Code: 60. DB::Exception: Table does not exist")
	})
	if err == nil || attempts != 1 || calls != 1 {
		t.Fatalf("attempts=%d calls=%d error=%v, want immediate contract failure", attempts, calls, err)
	}
}

func TestValidateClickHousePreflightHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &Service{Cfg: Config{CHHost: "clickhouse.invalid", CHPort: 8123, CHUser: "test"}}
	if err := svc.ValidateClickHouseIngest(ctx); err != context.Canceled {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
}

func TestInflearnWorkflowPinsBoundedPreflightRetry(t *testing.T) {
	source, err := os.ReadFile("../../.github/workflows/inflearn_collect_all.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(source)
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS: "90"`); got != 3 {
		t.Fatalf("preflight retry budget count=%d, want 3", got)
	}
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS: "5"`); got != 3 {
		t.Fatalf("preflight retry backoff count=%d, want 3", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testClickHouseService(t *testing.T, server *httptest.Server) *Service {
	t.Helper()
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("parse test ClickHouse endpoint: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test ClickHouse port: %v", err)
	}
	return &Service{
		Cfg: Config{
			CHHost:                  host,
			CHPort:                  port,
			CHUser:                  "test",
			CHRawDatabase:           "Data_Lecture_Inflearn_Raw",
			CHOutboxDatabase:        "Data_Lecture_Inflearn_Log",
			CHOutboxTable:           "inflearn_direct_insert_outbox",
			CHInsertTimeout:         time.Second,
			CHDirectReplicaFallback: false,
			CHDirectOutboxFallback:  true,
			UserAgent:               "test",
		},
		HTTPClient: server.Client(),
	}
}
