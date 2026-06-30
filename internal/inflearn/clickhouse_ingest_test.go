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
