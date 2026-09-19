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
	t.Setenv("CH_OUTBOX_REPLAY_LIMIT", "")
	t.Setenv("WORKERS", "")
	t.Setenv("INFLEARN_PUBLIC_UPDATE_PRIORITY_ENABLED", "")
	t.Setenv("INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED", "")
	t.Setenv("INFLEARN_LECTURE_PUBLISHER_WRITER_ID", "")

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
	if !cfg.CHInsertDistributedSync {
		t.Fatal("CHInsertDistributedSync should default to true")
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
	if cfg.CHOutboxReplayLimit != 0 {
		t.Fatalf("CHOutboxReplayLimit = %d, want scheduled-safe default 0", cfg.CHOutboxReplayLimit)
	}
	if cfg.Workers != 4 {
		t.Fatalf("Workers = %d, want 4", cfg.Workers)
	}
	if cfg.PublicationV2Enabled {
		t.Fatal("publication v2 must default inactive for rollout safety")
	}
	if cfg.ReaderRefreshRequired || cfg.PublicationReaderConfig != "" {
		t.Fatalf("reader refresh rollout must default inactive: required=%v config=%q", cfg.ReaderRefreshRequired, cfg.PublicationReaderConfig)
	}
	if cfg.PublicUpdatePriority {
		t.Fatal("public update priority must default inactive until Phase A exists")
	}
	if cfg.PublicationWriterID != "" {
		t.Fatalf("publication writer ID=%q, want empty while rollout is inactive", cfg.PublicationWriterID)
	}
	t.Setenv("INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED", "true")
	t.Setenv("INFLEARN_LECTURE_READER_REFRESH_CONFIG_FILE", " /run/secrets/lecture-readers.json ")
	cfg, err = LoadConfig()
	if err != nil || !cfg.ReaderRefreshRequired || cfg.PublicationReaderConfig != "/run/secrets/lecture-readers.json" {
		t.Fatalf("reader refresh rollout config=%q required=%v err=%v", cfg.PublicationReaderConfig, cfg.ReaderRefreshRequired, err)
	}
	t.Setenv("INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED", "")
	t.Setenv("INFLEARN_LECTURE_READER_REFRESH_CONFIG_FILE", "")
	t.Setenv("INFLEARN_LECTURE_PUBLISHER_WRITER_ID", " gha:test:1:2 ")
	cfg, err = LoadConfig()
	if err != nil || cfg.PublicationWriterID != "gha:test:1:2" {
		t.Fatalf("writer identity=%q err=%v", cfg.PublicationWriterID, err)
	}
	t.Setenv("CH_OUTBOX_REPLAY_LIMIT", "500")
	cfg, err = LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig with replay override returned error: %v", err)
	}
	if cfg.CHOutboxReplayLimit != 50 {
		t.Fatalf("CHOutboxReplayLimit = %d, want hard cap 50", cfg.CHOutboxReplayLimit)
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
	if got := strings.Count(workflow, `CH_OUTBOX_REPLAY_LIMIT: "0"`); got != 2 {
		t.Fatalf("scheduled-safe replay limit count=%d, want update and translation", got)
	}
	conditionalReplicaFallback := `CH_DIRECT_REPLICA_FALLBACK: ${{ vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true' && 'false' || 'true' }}`
	conditionalOutboxFallback := `CH_DIRECT_OUTBOX_FALLBACK: ${{ vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true' && 'false' || 'true' }}`
	if got := strings.Count(workflow, conditionalReplicaFallback); got != 3 {
		t.Fatalf("rollout-safe replica fallback count=%d, want collect/update/translation", got)
	}
	if got := strings.Count(workflow, conditionalOutboxFallback); got != 3 {
		t.Fatalf("rollout-safe outbox fallback count=%d, want collect/update/translation", got)
	}
	if got := strings.Count(workflow, `CH_DIRECT_REPLICA_FALLBACK: "false"`); got != 2 {
		t.Fatalf("hard-disabled replica fallback count=%d, want publisher and verifier", got)
	}
	if got := strings.Count(workflow, `CH_DIRECT_OUTBOX_FALLBACK: "false"`); got != 2 {
		t.Fatalf("hard-disabled outbox fallback count=%d, want publisher and verifier", got)
	}
	if !strings.Contains(workflow, "cancel-in-progress: false") || strings.Contains(workflow, "cancel-in-progress: true") {
		t.Fatal("scheduled writer concurrency must queue rather than cancel an active write/refresh")
	}
	if !strings.Contains(workflow, `github.event_name == 'workflow_dispatch' && github.event.inputs.outbox_replay_limit || '0'`) {
		t.Fatal("collect-new must allow only explicit manual outbox replay")
	}
	if !strings.Contains(workflow, `UPDATE_BATCH_SIZE: ${{ github.event_name == 'push' && startsWith(github.ref, 'refs/tags/inflearn-runtime-check-') && '10' || '100' }}`) {
		t.Fatal("update workflow must preserve the 100-row normal sweep and exact 10-row runtime-check cap")
	}
	if !strings.Contains(workflow, `WORKERS: "4"`) || strings.Contains(workflow, `WORKERS: "8"`) {
		t.Fatal("update workflow must use four workers")
	}
	for _, want := range []string{
		"./cmd/inflearn-refresh-public-views",
		"./cmd/inflearn-verify-public-freshness",
		"id: refresh_public_lecture_views",
		"INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED: ${{ vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED || 'false' }}",
		"INFLEARN_PUBLIC_UPDATE_PRIORITY_ENABLED: ${{ vars.INFLEARN_PUBLIC_UPDATE_PRIORITY_ENABLED || 'false' }}",
		"INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED must be exactly true or false",
		"INFLEARN_PUBLIC_UPDATE_PRIORITY_ENABLED must be exactly true or false",
		"PUBLIC_REFRESH_RUN_UUID: ${{ steps.refresh_public_lecture_views.outputs.publication_run_uuid || 'missing' }}",
		"secrets.INFLEARN_LECTURE_PUBLISHER_CH_USER",
		"secrets.INFLEARN_LECTURE_PUBLISHER_CH_PASSWORD",
		"secrets.INFLEARN_LECTURE_READER_REFRESH_CONFIG_JSON",
		"INFLEARN_LECTURE_READER_REFRESH_CONFIG_FILE",
		"replica:lecture_publication.inflearn_public_catalog_reader_refresh_ack_local",
		"replica:lecture_publication.inflearn_public_catalog_reader_transition_ack_v2_local",
		"INFLEARN_LECTURE_PUBLISHER_WRITER_ID: gha:${{ github.repository_id }}:${{ github.run_id }}:${{ github.run_attempt }}",
		"vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true'",
		`"status":"inactive","publication_claim":false`,
		"replica:webr_lecture.inflearn_r_lecture_catalog_local",
		"replica:mirtype_lecture.inflearn_language_lecture_catalog_local",
		"Verify public lecture freshness and refresh health",
		"if: always() && vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true'",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("workflow missing public freshness contract %q", want)
		}
	}
	generalValidationStart := strings.Index(workflow, "- name: Validate required ClickHouse repository secrets")
	publisherValidationStart := strings.Index(workflow, "- name: Validate publication publisher secrets")
	moduleStart := strings.Index(workflow, "- name: Resolve Go modules and checksums")
	if generalValidationStart < 0 || publisherValidationStart <= generalValidationStart || moduleStart <= publisherValidationStart {
		t.Fatal("cannot isolate secret validation steps")
	}
	if strings.Contains(workflow[generalValidationStart:publisherValidationStart], "INFLEARN_LECTURE_PUBLISHER_CH_") {
		t.Fatal("inactive rollout must not resolve publisher secrets in the always-run validation step")
	}
	publisherValidation := workflow[publisherValidationStart:moduleStart]
	if !strings.Contains(publisherValidation, "if: vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true'") ||
		!strings.Contains(publisherValidation, "secrets.INFLEARN_LECTURE_PUBLISHER_CH_USER") ||
		!strings.Contains(publisherValidation, "secrets.INFLEARN_LECTURE_PUBLISHER_CH_PASSWORD") ||
		!strings.Contains(publisherValidation, "secrets.INFLEARN_LECTURE_READER_REFRESH_CONFIG_JSON") {
		t.Fatal("publisher secrets must be resolved only by the enabled conditional validation step")
	}
	for _, stepName := range []string{
		"- name: Collect new Inflearn courses into ClickHouse",
		"- name: Update existing Inflearn courses in ClickHouse",
	} {
		start := strings.Index(workflow, stepName)
		if start < 0 {
			t.Fatalf("missing collector step %q", stepName)
		}
		end := strings.Index(workflow[start+len(stepName):], "\n      - name:")
		if end < 0 {
			t.Fatalf("cannot isolate collector step %q", stepName)
		}
		block := workflow[start : start+len(stepName)+end]
		for _, secret := range []string{
			"secrets.INFLEARN_LECTURE_PUBLISHER_CH_USER",
			"secrets.INFLEARN_LECTURE_PUBLISHER_CH_PASSWORD",
		} {
			conditional := "vars.INFLEARN_LECTURE_GENERATION_PUBLICATION_ENABLED == 'true' && " + secret + " || ''"
			if !strings.Contains(block, conditional) {
				t.Fatalf("inactive collector must short-circuit publisher secret %q", secret)
			}
		}
	}
	if strings.Contains(workflow, "PUBLIC_REFRESH_MIN_SUCCESS_EPOCH") || strings.Contains(workflow, "refresh_started_epoch") {
		t.Fatal("workflow must use the UUID publication receipt rather than second-precision timestamps")
	}
	refreshStart := strings.Index(workflow, "- name: Refresh exact public lecture views serially")
	verifyStart := strings.Index(workflow, "- name: Verify public lecture freshness and refresh health")
	inactiveStart := strings.Index(workflow, "- name: Report inactive publication v2")
	if refreshStart < 0 || verifyStart <= refreshStart || inactiveStart <= verifyStart {
		t.Fatal("cannot isolate publication workflow steps")
	}
	for name, block := range map[string]string{
		"publisher": workflow[refreshStart:verifyStart],
		"verifier":  workflow[verifyStart:inactiveStart],
	} {
		if !strings.Contains(block, `CH_USER: ${{ secrets.INFLEARN_LECTURE_PUBLISHER_CH_USER }}`) ||
			!strings.Contains(block, `CH_PASSWORD: ${{ secrets.INFLEARN_LECTURE_PUBLISHER_CH_PASSWORD }}`) ||
			strings.Contains(block, `CH_USER: ${{ secrets.CH_USER`) || strings.Contains(block, `CH_PASSWORD: ${{ secrets.CH_PASSWORD`) {
			t.Fatalf("%s must use only the dedicated publisher identity", name)
		}
	}
	publisherBlock := workflow[refreshStart:verifyStart]
	for _, want := range []string{
		`READER_REFRESH_CONFIG_JSON: ${{ secrets.INFLEARN_LECTURE_READER_REFRESH_CONFIG_JSON }}`,
		`READER_REFRESH_CONFIG_FILE="$(mktemp)"`,
		`trap 'rm -f "$READER_REFRESH_CONFIG_FILE"' EXIT`,
		`umask 077`,
		`unset READER_REFRESH_CONFIG_JSON`,
		`export INFLEARN_LECTURE_READER_REFRESH_CONFIG_FILE="$READER_REFRESH_CONFIG_FILE"`,
	} {
		if !strings.Contains(publisherBlock, want) {
			t.Fatalf("publisher reader refresh config handling missing %q", want)
		}
	}
	writeSecret := strings.Index(publisherBlock, `printf '%s' "$READER_REFRESH_CONFIG_JSON"`)
	clearSecret := strings.Index(publisherBlock, "unset READER_REFRESH_CONFIG_JSON")
	runPublisher := strings.Index(publisherBlock, "go run -mod=mod ./cmd/inflearn-refresh-public-views")
	if writeSecret < 0 || clearSecret <= writeSecret || runPublisher <= clearSecret {
		t.Fatal("reader refresh secret must be removed from the publisher environment immediately after the private file is written")
	}
	for _, want := range []string{
		`"status":"deferred","phase":"provider_practice_refresh","category":"temporary_clickhouse"}'` + "\n              exit 1",
		`"status":"deferred","phase":"provider_practice_verify","category":"temporary_clickhouse"}'` + "\n              exit 1",
		`"status":"deferred","phase":"display_translation","category":"temporary_clickhouse"}'` + "\n              exit 1",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("workflow temporary failure must be machine-readable and nonzero: missing %q", want)
		}
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
