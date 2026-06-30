package inflearn

import "testing"

func TestLoadConfigClickHouseIngestDoesNotRequireKafka(t *testing.T) {
	t.Setenv("INGEST_MODE", "clickhouse")
	t.Setenv("CH_HOST", "clickhouse.example")
	t.Setenv("CH_USER", "statground_ch_app")
	t.Setenv("KAFKA_BROKERS", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.IngestMode != "clickhouse" {
		t.Fatalf("IngestMode = %q, want clickhouse", cfg.IngestMode)
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
