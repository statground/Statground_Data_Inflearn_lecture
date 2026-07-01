package inflearn

import (
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

type errString string

func (e errString) Error() string { return string(e) }
