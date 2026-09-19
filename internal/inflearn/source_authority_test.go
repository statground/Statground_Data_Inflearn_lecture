package inflearn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSourceAuthorityEventsRejectNilAndConflictingSameTime(t *testing.T) {
	if _, err := sourceAuthorityEventsFromCourseRows([]map[string]any{nil}); err == nil || !strings.Contains(err.Error(), "nil_event") {
		t.Fatalf("nil row error=%v", err)
	}
	fetched := "2026-09-20 12:00:00.000"
	_, err := sourceAuthorityEventsFromCourseRows([]map[string]any{
		{"course_id": 10, "locale": "ko", "status": "PUBLISH", "fetched_at": fetched},
		{"course_id": 10, "locale": "ko", "status": "HIDE", "fetched_at": fetched},
	})
	if err == nil || !strings.Contains(err.Error(), "conflicting_same_time_event") {
		t.Fatalf("conflicting event error=%v", err)
	}
}

func TestSourceAuthorityDisabledPerformsNoQuery(t *testing.T) {
	svc := &Service{Cfg: Config{PublicationV2Enabled: false, PublicUpdatePriority: true}}
	if err := svc.publishSourceAuthorityEvents(context.Background(), []map[string]any{nil}); err != nil {
		t.Fatalf("disabled publication must perform no validation or SQL: %v", err)
	}
}

func TestSourceAuthorityRejectsStaleWriter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCHRows(w, []map[string]any{{
			"course_id": 10, "locale": "ko", "authority_revision": 9,
			"operation_uuid": "018f0000-0000-7000-8000-000000000090",
			"event_uuid":     "018f0000-0000-7000-8000-000000000091",
			"status":         "HIDE", "source_fetched_ms": int64(2000),
		}})
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	events := []sourceAuthorityEvent{{CourseID: 10, Locale: "ko", Status: "PUBLISH", SourceFetched: time.UnixMilli(1000).In(KST)}}
	_, err := svc.rejectOrRemoveStaleSourceAuthorityEvents(context.Background(), events)
	if err == nil || !strings.Contains(err.Error(), "stale_writer_rejected") {
		t.Fatalf("stale writer error=%v", err)
	}
}

func TestSourceAuthorityFenceAmbiguousTimeoutRetriesStableOperation(t *testing.T) {
	insertQueries := []string{}
	insertBodies := []string{}
	readbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, body := clickHouseTestRequest(r)
		switch {
		case strings.HasPrefix(query, "INSERT INTO "+sourceAuthorityFenceTable):
			insertQueries = append(insertQueries, query)
			insertBodies = append(insertBodies, body)
			if len(insertQueries) == 1 {
				w.WriteHeader(http.StatusGatewayTimeout)
				_, _ = w.Write([]byte("timeout"))
				return
			}
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "inflearn_public_catalog_source_authority_fence_local"):
			readbacks++
			rows := sourceAuthorityFenceReadbackRows(4)
			if readbacks == 1 {
				rows = rows[:3]
			}
			writeCHRows(w, rows)
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	svc.Cfg.PublicationWriterID = "authority-test"
	err := svc.insertAndReadbackSourceAuthorityFence(
		context.Background(), publicationTopology(), 2,
		"018f0000-0000-7000-8000-000000000020", time.UnixMilli(2000).In(KST),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(insertQueries) != 2 || insertQueries[0] != insertQueries[1] || insertBodies[0] != insertBodies[1] {
		t.Fatalf("ambiguous retry changed identity: queries=%d", len(insertQueries))
	}
}

func TestSourceAuthorityFenceIncompleteReadbackRetriesStableOperation(t *testing.T) {
	insertQueries := []string{}
	insertBodies := []string{}
	readbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, body := clickHouseTestRequest(r)
		switch {
		case strings.HasPrefix(query, "INSERT INTO "+sourceAuthorityFenceTable):
			insertQueries = append(insertQueries, query)
			insertBodies = append(insertBodies, body)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "inflearn_public_catalog_source_authority_fence_local"):
			readbacks++
			rows := sourceAuthorityFenceReadbackRows(4)
			if readbacks == 1 {
				rows = rows[:3]
			}
			writeCHRows(w, rows)
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	svc.Cfg.PublicationWriterID = "authority-test"
	err := svc.insertAndReadbackSourceAuthorityFence(
		context.Background(), publicationTopology(), 2,
		"018f0000-0000-7000-8000-000000000020", time.UnixMilli(2000).In(KST),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(insertQueries) != 2 || insertQueries[0] != insertQueries[1] || insertBodies[0] != insertBodies[1] {
		t.Fatalf("incomplete readback retry changed identity: queries=%d", len(insertQueries))
	}
}

func TestSourceAuthorityEventIncompleteReadbackRetriesStableOperation(t *testing.T) {
	insertQueries := []string{}
	insertBodies := []string{}
	readbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, body := clickHouseTestRequest(r)
		switch {
		case strings.HasPrefix(query, "INSERT INTO "+sourceAuthorityEventTable):
			insertQueries = append(insertQueries, query)
			insertBodies = append(insertBodies, body)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "inflearn_public_catalog_source_authority_local"):
			readbacks++
			rows := sourceAuthorityEventReadbackRows(1)
			if readbacks == 1 {
				rows = rows[:3]
			}
			writeCHRows(w, rows)
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	events := []sourceAuthorityEvent{{
		CourseID: 10, Locale: "ko", Status: "HIDE",
		SourceFetched: time.UnixMilli(1000).In(KST), AuthorityRev: 2,
		OperationUUID: "018f0000-0000-7000-8000-000000000020",
		EventUUID:     "018f0000-0000-7000-8000-000000000021",
		RecordedAt:    time.UnixMilli(2000).In(KST),
	}}
	if err := svc.insertAndReadbackSourceAuthorityEvents(context.Background(), publicationTopology(), events); err != nil {
		t.Fatal(err)
	}
	if len(insertQueries) != 2 || insertQueries[0] != insertQueries[1] || insertBodies[0] != insertBodies[1] {
		t.Fatalf("event readback retry changed identity: queries=%d", len(insertQueries))
	}
}

func TestSourceAuthorityRevisionRequiresFourEndpoints(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCHRows(w, sourceAuthorityRevisionRows(1, "01994602-0000-7000-8000-000000000001")[:3])
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	_, _, err := svc.readSourceAuthorityRevision(context.Background(), publicationTopology(), "authority_test")
	if err == nil || !strings.Contains(err.Error(), "authority_fence_endpoint_count") {
		t.Fatalf("endpoint loss error=%v", err)
	}
}

func TestRestrictiveSourceAuthorityRejectsEndpointAdmissionLeak(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := sourceAuthorityAdmissionRows(0)
		rows[2]["admitted_rows"] = int64(1)
		writeCHRows(w, rows)
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	events := []sourceAuthorityEvent{{CourseID: 10, Locale: "ko", Status: "HIDE"}}
	err := svc.requireSourceAuthorityAdmissionClosed(context.Background(), publicationTopology(), events)
	if err == nil || !strings.Contains(err.Error(), "withdrawal_still_admitted") {
		t.Fatalf("admission leak error=%v", err)
	}
}

func TestRestrictiveSourceAuthorityDoesNotAckUndrainedRead(t *testing.T) {
	oldTimeout := sourceAuthorityDrainTimeout
	sourceAuthorityDrainTimeout = time.Nanosecond
	defer func() { sourceAuthorityDrainTimeout = oldTimeout }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCHRows(w, sourceAuthorityDrainRows(1))
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	err := svc.waitForSourceAuthorityReadDrain(context.Background(), publicationTopology(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "inflight_reads_not_drained") {
		t.Fatalf("undrained read error=%v", err)
	}
}

func TestRestrictiveSourceAuthorityClosesAdmissionAndDrainsBeforeAck(t *testing.T) {
	fetchedAt := "2026-09-20 12:00:00.000"
	oldPoll, oldTimeout := sourceAuthorityDrainPoll, sourceAuthorityDrainTimeout
	sourceAuthorityDrainPoll, sourceAuthorityDrainTimeout = time.Millisecond, 100*time.Millisecond
	defer func() { sourceAuthorityDrainPoll, sourceAuthorityDrainTimeout = oldPoll, oldTimeout }()

	type capturedEvent struct {
		AuthorityRevision uint64 `json:"authority_revision"`
		OperationUUID     string `json:"operation_uuid"`
		EventUUID         string `json:"event_uuid"`
		CourseID          int    `json:"course_id"`
		Locale            string `json:"locale"`
		Status            string `json:"status"`
		SourceFetchedAt   string `json:"source_fetched_at"`
	}
	fenceInserted := false
	eventInserted := false
	operationUUID := ""
	event := capturedEvent{}
	latestReads, admissionReads, drainReads := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, body := clickHouseTestRequest(r)
		switch {
		case strings.Contains(query, "FROM system.clusters"):
			writeCHRows(w, publicationTopologyRows())
		case strings.Contains(query, "v_inflearn_public_catalog_source_authority_latest_local"):
			latestReads++
			if !eventInserted {
				writeCHRows(w, nil)
				return
			}
			writeCHRows(w, []map[string]any{{
				"course_id": event.CourseID, "locale": event.Locale, "authority_revision": event.AuthorityRevision,
				"operation_uuid": event.OperationUUID, "event_uuid": event.EventUUID,
				"status": event.Status, "source_fetched_ms": mustParseTestTime(t, event.SourceFetchedAt).UnixMilli(),
			}})
		case strings.Contains(query, "v_inflearn_public_catalog_source_authority_fence_current_local"):
			if fenceInserted {
				writeCHRows(w, sourceAuthorityRevisionRows(2, operationUUID))
			} else {
				writeCHRows(w, sourceAuthorityRevisionRows(1, "01994602-0000-7000-8000-000000000001"))
			}
		case strings.Contains(query, "SELECT toUnixTimestamp64Milli(now64"):
			writeCHRows(w, []map[string]any{{"server_time_ms": mustParseTestTime(t, fetchedAt).Add(time.Second).UnixMilli()}})
		case strings.HasPrefix(query, "INSERT INTO "+sourceAuthorityFenceTable):
			var row map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &row); err != nil {
				t.Fatal(err)
			}
			operationUUID = asString(row["operation_uuid"])
			fenceInserted = true
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "inflearn_public_catalog_source_authority_fence_local"):
			writeCHRows(w, sourceAuthorityFenceReadbackRows(4))
		case strings.HasPrefix(query, "INSERT INTO "+sourceAuthorityEventTable):
			if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &event); err != nil {
				t.Fatal(err)
			}
			eventInserted = true
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "inflearn_public_catalog_source_authority_local"):
			writeCHRows(w, sourceAuthorityEventReadbackRows(1))
		case strings.Contains(query, "FROM clusterAllReplicas") && strings.Contains(query, "system.one"):
			admissionReads++
			writeCHRows(w, sourceAuthorityAdmissionRows(0))
		case strings.Contains(query, "system.processes"):
			drainReads++
			active := int64(0)
			if drainReads == 1 {
				active = 1
			}
			writeCHRows(w, sourceAuthorityDrainRows(active))
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()
	svc := testClickHouseService(t, server)
	svc.Cfg.PublicationV2Enabled = true
	svc.Cfg.PublicationWriterID = "gha-source:test"
	svc.Cfg.PublicationCHUser = "publisher"
	svc.Cfg.PublicationCHPassword = "publisher-password"
	err := svc.publishSourceAuthorityEvents(context.Background(), []map[string]any{{
		"course_id": 10, "locale": "ko", "status": "HIDE", "fetched_at": fetchedAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if latestReads < 4 || admissionReads != 2 || drainReads != 2 || !eventInserted {
		t.Fatalf("latest=%d admission=%d drain=%d event=%t", latestReads, admissionReads, drainReads, eventInserted)
	}
}

func clickHouseTestRequest(r *http.Request) (string, string) {
	body, _ := io.ReadAll(r.Body)
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if query == "" {
		query = strings.TrimSpace(string(body))
	}
	return query, string(body)
}

func sourceAuthorityFenceReadbackRows(count int) []map[string]any {
	rows := make([]map[string]any, 0, count)
	for _, host := range publicationHosts()[:count] {
		rows = append(rows, map[string]any{"hostname": host, "rows": 1, "versions": 1, "matching_operations": 1})
	}
	return rows
}

func sourceAuthorityEventReadbackRows(count int) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "rows": count, "versions": count})
	}
	return rows
}

func sourceAuthorityAdmissionRows(admitted int64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "admitted_rows": admitted})
	}
	return rows
}

func sourceAuthorityDrainRows(active int64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "active_reads": active})
	}
	return rows
}

func mustParseTestTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, ok := ParseDT64(value)
	if !ok {
		t.Fatalf("invalid test time %q", value)
	}
	return parsed
}
