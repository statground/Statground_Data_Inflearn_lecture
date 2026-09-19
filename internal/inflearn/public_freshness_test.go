package inflearn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const freshnessRunUUID = "018f0000-0000-7000-8000-000000000010"
const freshnessActivationUUID = "018f0000-0000-7000-8000-000000000011"

type freshnessFixture struct {
	MirtypeMaxAge, StatgroundMaxAge, WebRMaxAge                   int
	MirtypeGenerationMS, StatgroundGenerationMS, WebRGenerationMS int64
	PointerRunUUID, RefreshStatus                                 string
	RefreshException, RefreshReceiptDrift                         bool
	GlobalOver60D                                                 int
	WebRPublicRows, WebRFingerprintSum                            uint64
	DropWebRConsumerEndpoint                                      bool
}

func healthyFreshnessFixture() freshnessFixture {
	return freshnessFixture{
		MirtypeMaxAge: 1200, StatgroundMaxAge: 1100, WebRMaxAge: 1000,
		MirtypeGenerationMS: 300500, StatgroundGenerationMS: 400500, WebRGenerationMS: 200500,
		PointerRunUUID: freshnessRunUUID, RefreshStatus: "Scheduled", GlobalOver60D: 900,
		WebRPublicRows: 30, WebRFingerprintSum: 3000,
	}
}

func TestVerifyPublicLectureFreshnessUsesActivatedGenerationAndWarnsForGlobalAge(t *testing.T) {
	fixture := healthyFreshnessFixture()
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	if err := svc.VerifyPublicLectureFreshness(context.Background()); err != nil {
		t.Fatalf("global 60-day ratio must remain warning-only: %v", err)
	}
}

func TestVerifyPublicLectureFreshnessGatesEachSurfaceIndependently(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.WebRMaxAge = publicFreshnessMaxAgeSeconds + 1
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	if err == nil || !strings.Contains(err.Error(), "public_max_age_over_36h:webr") || strings.Contains(err.Error(), "public_max_age_over_36h:mirtype") {
		t.Fatalf("error=%v, want only Web-R max-age failure", err)
	}
}

func TestVerifyPublicLectureFreshnessRejectsGenerationOutsidePointer(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.WebRGenerationMS++
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, "public_generation_mismatch:webr")
}

func TestVerifyPublicLectureFreshnessRejectsPartialPublicSurface(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.WebRPublicRows = 21
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, `"phase":"public_consumer_parity"`, `"category":"public_signature_mismatch"`)
}

func TestVerifyPublicLectureFreshnessRejectsEndpointLoss(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.DropWebRConsumerEndpoint = true
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, `"phase":"public_consumer_parity"`, `"category":"public_endpoint_count"`)
}

func TestVerifyPublicLectureFreshnessRejectsNormalizedFingerprintDrift(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.WebRFingerprintSum++
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, `"phase":"public_serving_signature"`, `"category":"normalized_signature_mismatch"`)
}

func TestVerifyPublicLectureFreshnessRequiresCurrentRunUUID(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.PointerRunUUID = "018f0000-0000-7000-8000-000000000099"
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, `"category":"current_run_not_activated"`)
}

func TestVerifyPublicLectureFreshnessRejectsInvalidReceiptBeforeQuery(t *testing.T) {
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", "missing")
	err := (&Service{}).VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, `"category":"invalid_refresh_receipt"`)
}

func TestVerifyPublicLectureFreshnessFailsRefreshExceptionAndReceiptDrift(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.RefreshException, fixture.RefreshReceiptDrift = true, true
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err,
		"refresh_exception:webr_lecture.mv_inflearn_r_lecture_catalog_refresh",
		"refresh_receipt_mismatch:webr_lecture.mv_inflearn_r_lecture_catalog_refresh")
}

func TestVerifyPublicLectureFreshnessFailsClosedWhileRefreshIsRunning(t *testing.T) {
	fixture := healthyFreshnessFixture()
	fixture.RefreshStatus = "RunningOnAnotherReplica"
	svc, closeServer := publicFreshnessTestService(t, fixture)
	defer closeServer()
	t.Setenv("PUBLIC_REFRESH_RUN_UUID", freshnessRunUUID)
	err := svc.VerifyPublicLectureFreshness(context.Background())
	assertErrorContains(t, err, "refresh_busy:webr_lecture.mv_inflearn_r_lecture_catalog_refresh")
}

func assertErrorContains(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error=nil, want fragments %v", fragments)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error=%v, missing %q", err, fragment)
		}
	}
}

func publicFreshnessTestService(t *testing.T, fixture freshnessFixture) (*Service, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		var rows []map[string]any
		switch {
		case strings.Contains(query, "FROM system.clusters"):
			rows = publicationTopologyRows()
		case strings.Contains(query, "v_inflearn_public_catalog_source_authority_fence_current_local"):
			rows = sourceAuthorityRevisionRows(1, "01994602-0000-7000-8000-000000000001")
		case strings.Contains(query, "public_exact AS"):
			for _, want := range []string{"GLOBAL INNER JOIN", "statground_lecture.v_inflearn_workbench_catalog", freshnessRunUUID} {
				if !strings.Contains(query, want) {
					t.Errorf("freshness query missing %q", want)
				}
			}
			rows = []map[string]any{
				freshnessMetricRow("mirtype", 20, fixture.MirtypeMaxAge, fixture.MirtypeGenerationMS),
				freshnessMetricRow("statground", 40, fixture.StatgroundMaxAge, fixture.StatgroundGenerationMS),
				freshnessMetricRow("webr", 30, fixture.WebRMaxAge, fixture.WebRGenerationMS),
			}
		case strings.Contains(query, "public_serving_counts AS"):
			rows = consumerEndpointRows(fixture)
		case strings.Contains(query, "'v_inflearn_public_catalog_admitted_serving_local'"):
			rows = normalizedEndpointRows(fixture)
		case strings.Contains(query, "v_inflearn_public_catalog_generation_latest") && strings.Contains(query, "ORDER BY surface"):
			rows = freshnessPointerRows(fixture.PointerRunUUID)
		case strings.Contains(query, "global_over_60d_rows"):
			rows = []map[string]any{{"global_rows": 1000, "global_over_60d_rows": fixture.GlobalOver60D, "global_over_60d_ratio": float64(fixture.GlobalOver60D) / 1000}}
		case strings.Contains(query, "FROM system.view_refreshes"):
			webSuccess, webEnd := int64(200000), int64(201000)
			if fixture.RefreshReceiptDrift {
				webSuccess, webEnd = 202000, 203000
			}
			exceptionFlag := 0
			if fixture.RefreshException {
				exceptionFlag = 1
			}
			rows = []map[string]any{
				{"database": "mirtype_lecture", "view": "mv_inflearn_language_lecture_catalog_refresh", "status": fixture.RefreshStatus, "last_success_ms": 300000, "last_refresh_ms": 301000, "exception_present": 0, "now_ms": 302000},
				{"database": "webr_lecture", "view": "mv_inflearn_r_lecture_catalog_refresh", "status": fixture.RefreshStatus, "last_success_ms": webSuccess, "last_refresh_ms": webEnd, "exception_present": exceptionFlag, "now_ms": 204000},
			}
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	svc := testClickHouseService(t, server)
	svc.Cfg.CHCluster = "statground_cluster"
	return svc, server.Close
}

func freshnessMetricRow(surface string, count, maxAge int, generation int64) map[string]any {
	return map[string]any{"surface": surface, "public_exact_rows": count, "p95_age_seconds": maxAge,
		"max_age_seconds": maxAge, "over_36h_rows": 0, "oldest_generation_ms": generation, "latest_generation_ms": generation}
}

func normalizedEndpointRows(f freshnessFixture) []map[string]any {
	var rows []map[string]any
	for _, host := range []string{"s1r1", "s1r2", "s2r1", "s2r2"} {
		webFingerprint := uint64(3000)
		if host == "s2r2" {
			webFingerprint = f.WebRFingerprintSum
		}
		rows = append(rows,
			signatureEndpointRow(host, "mirtype", 300500, 20, 2000, 21),
			signatureEndpointRow(host, "statground", 400500, 40, 4000, 41),
			signatureEndpointRow(host, "webr", 200500, 30, webFingerprint, 31))
	}
	return rows
}

func signatureEndpointRow(host, surface string, generation int64, rows, sum, xor uint64) map[string]any {
	return map[string]any{"hostname": host, "surface": surface, "generation_ms": generation,
		"row_count": rows, "logical_key_count": rows, "fingerprint_sum": sum, "fingerprint_xor": xor}
}

func consumerEndpointRows(f freshnessFixture) []map[string]any {
	var rows []map[string]any
	for i, host := range []string{"s1r1", "s1r2", "s2r1", "s2r2"} {
		rows = append(rows,
			consumerEndpointRow(host, "mirtype", 300500, 20),
			consumerEndpointRow(host, "statground", 400500, 40))
		if !(f.DropWebRConsumerEndpoint && i == 3) {
			rows = append(rows, consumerEndpointRow(host, "webr", 200500, f.WebRPublicRows))
		}
	}
	return rows
}

func consumerEndpointRow(host, surface string, generation int64, rows uint64) map[string]any {
	return map[string]any{"hostname": host, "surface": surface, "row_count": rows,
		"logical_key_count": rows, "generation_count": 1, "generation_ms": generation}
}

func freshnessPointerRows(runUUID string) []map[string]any {
	return []map[string]any{
		freshnessPointerRow("mirtype", 300500, "018f0000-0000-7000-8000-000000000012", runUUID, 20, 2000, 21, 300000, 301000),
		freshnessPointerRow("statground", 400500, "018f0000-0000-7000-8000-000000000014", runUUID, 40, 4000, 41, 400000, 401000),
		freshnessPointerRow("webr", 200500, "018f0000-0000-7000-8000-000000000013", runUUID, 30, 3000, 31, 200000, 201000),
	}
}

func freshnessPointerRow(surface string, generation int64, marker, run string, rows, sum, xor uint64, success, end int64) map[string]any {
	return map[string]any{
		"surface": surface, "generation_ms": generation, "marker_uuid": marker, "run_uuid": run,
		"target_row_count": rows, "target_logical_key_count": rows, "target_fingerprint_sum": sum, "target_fingerprint_xor": xor,
		"row_count": rows, "logical_key_count": rows, "fingerprint_sum": sum, "fingerprint_xor": xor,
		"source_fetched_max_ms": end, "refresh_success_ms": success, "refresh_end_ms": end,
		"activation_revision": 7, "activation_uuid": freshnessActivationUUID, "activation_kind": "publish",
	}
}
