package inflearn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var markerReadbackPattern = regexp.MustCompile(`(?s)marker_uuid = toUUID\('([^']+)'\).*run_uuid = toUUID\('([^']+)'\)`)
var activationReadbackPattern = regexp.MustCompile(`(?s)activation_uuid = toUUID\('([^']+)'\).*run_uuid = toUUID\('([^']+)'\)`)
var leaseInsertPattern = regexp.MustCompile(`(?s)SELECT toUInt64\(([0-9]+)\), toUUID\('([^']+)'\), '([^']+)', fromUnixTimestamp64Milli\(([0-9]+)\), fromUnixTimestamp64Milli\(([0-9]+)\)`)
var markerSurfacePattern = regexp.MustCompile(`(?s)SELECT '([^']+)', fromUnixTimestamp64Milli`)

func TestRefreshPublicLectureViewsPublishesThreeEndpointLocalSurfacesThenOneActivation(t *testing.T) {
	h := &publicationHappyHarness{t: t, markers: map[string]string{}}
	server := httptest.NewServer(http.HandlerFunc(h.serveHTTP))
	defer server.Close()
	svc := publicRefreshTestService(t, server)
	receipt, err := svc.RefreshPublicLectureViews(context.Background())
	if err != nil {
		t.Fatalf("%v (preflight_reads=%d time_reads=%d snapshots=%v)", err, h.preflightReads, h.timeReads, h.snapshotSurfaces)
	}
	if receipt.RunUUID != h.runUUID || receipt.ActivationUUID != h.activationUUID || receipt.ActivationRevision != 7 {
		t.Fatalf("receipt=%+v harness run=%s activation=%s", receipt, h.runUUID, h.activationUUID)
	}
	if h.rawRevisionReads != 2 || h.preflightReads != 2 || h.authorityRevisionReads != 3 {
		t.Fatalf("raw revision reads=%d preflight reads=%d authority reads=%d, want 2/2/3", h.rawRevisionReads, h.preflightReads, h.authorityRevisionReads)
	}
	if strings.Join(h.snapshotSurfaces, ",") != "webr,mirtype,statground" || h.markerReadbacks != 3 {
		t.Fatalf("snapshots=%v marker readbacks=%d", h.snapshotSurfaces, h.markerReadbacks)
	}
	wantCommands := []string{
		"SYSTEM REFRESH VIEW webr_lecture.mv_inflearn_r_lecture_catalog_refresh",
		"SYSTEM WAIT VIEW webr_lecture.mv_inflearn_r_lecture_catalog_refresh",
		"SYSTEM REFRESH VIEW mirtype_lecture.mv_inflearn_language_lecture_catalog_refresh",
		"SYSTEM WAIT VIEW mirtype_lecture.mv_inflearn_language_lecture_catalog_refresh",
	}
	if strings.Join(h.systemCommands, "\n") != strings.Join(wantCommands, "\n") {
		t.Fatalf("commands=%q, want=%q", h.systemCommands, wantCommands)
	}
}

func TestRefreshPublicLectureViewsRejectsSourceAuthorityRaceBeforeActivation(t *testing.T) {
	h := &publicationHappyHarness{t: t, markers: map[string]string{}, authorityRevisions: []uint64{1, 2}}
	server := httptest.NewServer(http.HandlerFunc(h.serveHTTP))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).RefreshPublicLectureViews(context.Background())
	assertUpdateStateError(t, err, "degraded", "public_activation_source_authority", "source_authority_changed_before_activation")
	if h.activationUUID != "" {
		t.Fatalf("activation was written despite source authority race: %s", h.activationUUID)
	}
}

type publicationHappyHarness struct {
	t                                                                                                  *testing.T
	lease                                                                                              publicationLease
	leaseSet                                                                                           bool
	pointerReads, rawRevisionReads, timeReads, markerReadbacks, preflightReads, authorityRevisionReads int
	markers                                                                                            map[string]string
	runUUID, activationUUID                                                                            string
	currentMarkerSurface                                                                               string
	systemCommands, snapshotSurfaces                                                                   []string
	authorityRevisions                                                                                 []uint64
}

func (h *publicationHappyHarness) serveHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	query := strings.TrimSpace(string(body))
	switch {
	case strings.Contains(query, "replayed_at IS NULL"):
		writeCHRows(w, []map[string]any{{"pending": 0}})
	case strings.Contains(query, "FROM system.clusters"):
		writeCHRows(w, publicationTopologyRows())
	case strings.Contains(query, "FROM "+lectureCurrentLeaseView) && !strings.Contains(query, "candidate AS"):
		if !h.leaseSet {
			writeCHRows(w, nil)
		} else {
			writeCHRows(w, []map[string]any{leaseCurrentRow(h.lease)})
		}
	case strings.Contains(query, "maxOrDefault(fence_epoch)"):
		writeCHRows(w, rawLeaseEpochRows(0))
	case strings.HasPrefix(query, "INSERT INTO "+lectureLeaseTable):
		matches := leaseInsertPattern.FindStringSubmatch(query)
		if len(matches) != 6 {
			h.t.Fatalf("cannot parse lease insert: %s", query)
		}
		epoch, _ := strconv.ParseUint(matches[1], 10, 64)
		acquired, _ := strconv.ParseInt(matches[4], 10, 64)
		expires, _ := strconv.ParseInt(matches[5], 10, 64)
		h.lease = publicationLease{FenceEpoch: epoch, LeaseUUID: matches[2], WriterID: matches[3], AcquiredMS: acquired, ExpiresMS: expires}
		h.leaseSet = true
		w.WriteHeader(http.StatusOK)
	case strings.Contains(query, lectureLeaseLocalTable) && strings.Contains(query, "WHERE fence_epoch"):
		writeCHRows(w, leaseReplicaRows(h.lease, 4))
	case strings.Contains(query, "v_inflearn_public_catalog_generation_latest") && strings.Contains(query, "ORDER BY surface") && !strings.Contains(query, "candidate AS"):
		h.pointerReads++
		if h.pointerReads == 1 {
			writeCHRows(w, nil)
		} else {
			writeCHRows(w, publicationPointerRows(h.runUUID, h.activationUUID, h.markers, h.lease))
		}
	case strings.Contains(query, "maxOrDefault(activation_revision)"):
		h.rawRevisionReads++
		writeCHRows(w, rawActivationRevisionRows(6))
	case strings.Contains(query, "v_inflearn_public_catalog_source_authority_fence_current_local"):
		revision := uint64(1)
		if h.authorityRevisionReads < len(h.authorityRevisions) {
			revision = h.authorityRevisions[h.authorityRevisionReads]
		}
		h.authorityRevisionReads++
		writeCHRows(w, sourceAuthorityRevisionRows(revision, "01994602-0000-7000-8000-000000000001"))
	case strings.Contains(query, "FROM system.view_refreshes"):
		switch {
		case strings.Contains(query, "database = 'webr_lecture'"):
			writeCHRows(w, []map[string]any{refreshStateRow("webr", 200000, 201000, 20, 12)})
		case strings.Contains(query, "database = 'mirtype_lecture'"):
			writeCHRows(w, []map[string]any{refreshStateRow("mirtype", 300000, 301000, 20, 12)})
		default:
			writeCHRows(w, []map[string]any{refreshStateRow("mirtype", 100000, 101000, 10, 10), refreshStateRow("webr", 100000, 101000, 10, 10)})
		}
	case strings.HasPrefix(query, "SYSTEM "):
		h.systemCommands = append(h.systemCommands, query)
		w.WriteHeader(http.StatusOK)
	case strings.Contains(query, "candidate AS"):
		h.preflightReads++
		writeCHRows(w, preflightRows())
	case strings.Contains(query, lectureCandidateLocalView):
		writeCHRows(w, candidateReplicaRows(surfaceGeneration(querySurface(query))))
	case strings.Contains(query, "FROM "+lectureCandidateView):
		writeCHRows(w, []map[string]any{candidateStatsRow(12, 12, 120, 30, surfaceGeneration(querySurface(query)))})
	case strings.Contains(query, "FROM "+lectureProjectionCandidateView):
		writeCHRows(w, []map[string]any{candidateStatsRow(12, 12, 120, 30, surfaceGeneration(querySurface(query)))})
	case strings.Contains(query, "FROM "+lectureStatgroundProjectionView):
		writeCHRows(w, []map[string]any{{"rows": 12, "unique_keys": 12, "fingerprint_sum": 120, "fingerprint_xor": 30, "source_fetched_max_ms": 400000, "observed_ms": 400500}})
	case strings.Contains(query, "SELECT toUnixTimestamp64Milli(now64"):
		times := []int64{100000, 400500, 401000, 402000, 403000}
		if h.timeReads >= len(times) {
			h.t.Fatalf("too many server-time reads: %s", query)
		}
		writeCHRows(w, []map[string]any{{"server_time_ms": times[h.timeReads]}})
		h.timeReads++
	case strings.HasPrefix(query, "INSERT INTO webr_lecture.inflearn_r_lecture_catalog_serving"):
		h.snapshotSurfaces = append(h.snapshotSurfaces, "webr")
		w.WriteHeader(http.StatusOK)
	case strings.HasPrefix(query, "INSERT INTO mirtype_lecture.inflearn_language_lecture_catalog_serving"):
		h.snapshotSurfaces = append(h.snapshotSurfaces, "mirtype")
		w.WriteHeader(http.StatusOK)
	case strings.HasPrefix(query, "INSERT INTO statground_lecture.inflearn_workbench_catalog_serving"):
		h.snapshotSurfaces = append(h.snapshotSurfaces, "statground")
		w.WriteHeader(http.StatusOK)
	case strings.Contains(query, lectureProjectionSnapshotLocalView):
		surface := querySurface(query)
		writeCHRows(w, snapshotReplicaRows(surface, surfaceGeneration(surface)))
	case strings.HasPrefix(query, "INSERT INTO "+lectureMarkerTable):
		if !strings.Contains(query, "insert_quorum = 4") || !strings.Contains(query, "insert_deduplication_token") {
			h.t.Errorf("unsafe marker insert: %s", query)
		}
		if match := markerSurfacePattern.FindStringSubmatch(query); len(match) == 2 {
			h.currentMarkerSurface = match[1]
		}
		w.WriteHeader(http.StatusOK)
	case strings.Contains(query, lectureMarkerLocalTable) && strings.Contains(query, "WHERE marker_uuid"):
		matches := markerReadbackPattern.FindStringSubmatch(query)
		if len(matches) != 3 {
			h.t.Fatalf("cannot parse marker readback: %s", query)
		}
		surface := h.currentMarkerSurface
		h.markerReadbacks++
		h.markers[surface], h.runUUID = matches[1], matches[2]
		writeCHRows(w, markerReadbackRows(publicationHosts(), surface, surfaceGeneration(surface), matches[1], matches[2], 402000, h.lease))
	case strings.HasPrefix(query, "INSERT INTO "+lectureActivationTable):
		if h.markerReadbacks != 3 {
			h.t.Errorf("activation inserted before three marker readbacks")
		}
		w.WriteHeader(http.StatusOK)
	case strings.Contains(query, lectureActivationLocal) && strings.Contains(query, "WHERE activation_uuid"):
		matches := activationReadbackPattern.FindStringSubmatch(query)
		if len(matches) != 3 {
			h.t.Fatalf("cannot parse activation readback: %s", query)
		}
		h.activationUUID, h.runUUID = matches[1], matches[2]
		writeCHRows(w, activationReadbackRows(publicationHosts(), h.activationUUID, h.runUUID, h.markers, h.lease))
	default:
		h.t.Errorf("unexpected query: %s", query)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func TestValidateCandidateGenerationRejectsPartialReplicaSet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeCHRows(w, candidateReplicaRows(200500)[:3]) }))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).validateCandidateGeneration(context.Background(), publicationTopology(), "webr", refreshState("webr", 200000, 201000, 20, 12), publicationPointer{})
	assertUpdateStateError(t, err, "degraded", "public_candidate_webr", "partial_replica_set")
}

func TestValidateCandidateGenerationRejectsSameShardReplicaDrift(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := candidateReplicaRows(200500)
		rows[1]["fingerprint_sum"] = "51"
		writeCHRows(w, rows)
	}))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).validateCandidateGeneration(context.Background(), publicationTopology(), "webr", refreshState("webr", 200000, 201000, 20, 12), publicationPointer{})
	assertUpdateStateError(t, err, "degraded", "public_candidate_webr", "same_shard_replica_drift")
}

func TestValidateCandidateGenerationRejectsPriorGenerationWithoutStrictAdvancement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		if strings.Contains(query, lectureCandidateLocalView) {
			writeCHRows(w, candidateReplicaRows(200500))
			return
		}
		writeCHRows(w, []map[string]any{candidateStatsRow(12, 12, 120, 30, 200500)})
	}))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).validateCandidateGeneration(context.Background(), publicationTopology(), "webr",
		refreshState("webr", 200000, 201000, 20, 12), publicationPointer{Surface: "webr", GenerationMS: 200500, RefreshEndMS: 199000, SourceFetchedMaxMS: 199000})
	assertUpdateStateError(t, err, "degraded", "public_candidate_webr", "candidate_not_strictly_advanced")
}

func TestValidateAdvancedPublicRefreshStateRejectsPriorSameSecondReceipt(t *testing.T) {
	pre := refreshState("webr", 200000, 201000, 20, 12)
	err := validateAdvancedPublicRefreshState(pre, pre, "public_refresh_webr_receipt")
	assertUpdateStateError(t, err, "degraded", "public_refresh_webr_receipt", "refresh_timestamp_not_advanced")
}

func TestAcquirePublicationLeaseRetriesSameOperationAfterAmbiguousTimeout(t *testing.T) {
	var lease publicationLease
	insertQueries := []string{}
	readbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := strings.TrimSpace(string(body))
		switch {
		case strings.Contains(query, "FROM "+lectureCurrentLeaseView):
			if lease.LeaseUUID == "" {
				writeCHRows(w, nil)
			} else {
				writeCHRows(w, []map[string]any{leaseCurrentRow(lease)})
			}
		case strings.Contains(query, "maxOrDefault(fence_epoch)"):
			writeCHRows(w, rawLeaseEpochRows(8))
		case strings.Contains(query, "SELECT toUnixTimestamp64Milli(now64"):
			writeCHRows(w, []map[string]any{{"server_time_ms": 100000}})
		case strings.HasPrefix(query, "INSERT INTO "+lectureLeaseTable):
			insertQueries = append(insertQueries, query)
			m := leaseInsertPattern.FindStringSubmatch(query)
			epoch, _ := strconv.ParseUint(m[1], 10, 64)
			acquired, _ := strconv.ParseInt(m[4], 10, 64)
			expires, _ := strconv.ParseInt(m[5], 10, 64)
			lease = publicationLease{FenceEpoch: epoch, LeaseUUID: m[2], WriterID: m[3], AcquiredMS: acquired, ExpiresMS: expires}
			if len(insertQueries) == 1 {
				w.WriteHeader(http.StatusGatewayTimeout)
				_, _ = w.Write([]byte("timeout"))
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case strings.Contains(query, lectureLeaseLocalTable) && strings.Contains(query, "WHERE fence_epoch"):
			readbacks++
			count := 4
			if readbacks == 1 {
				count = 3
			}
			writeCHRows(w, leaseReplicaRows(lease, count))
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()
	svc := publicRefreshTestService(t, server)
	got, err := svc.acquirePublicationLease(context.Background(), publicationTopology())
	if err != nil {
		t.Fatal(err)
	}
	if got.FenceEpoch != 9 || len(insertQueries) != 2 || insertQueries[0] != insertQueries[1] {
		t.Fatalf("lease=%+v inserts=%d same=%v", got, len(insertQueries), len(insertQueries) == 2 && insertQueries[0] == insertQueries[1])
	}
}

func TestRequireCurrentPublicationLeaseRejectsStaleWriter(t *testing.T) {
	want := publicationLease{FenceEpoch: 5, LeaseUUID: "018f0000-0000-7000-8000-000000000020", WriterID: "writer-a", AcquiredMS: 1000, ExpiresMS: 2000}
	other := want
	other.FenceEpoch = 6
	other.LeaseUUID = "018f0000-0000-7000-8000-000000000021"
	other.WriterID = "writer-b"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeCHRows(w, []map[string]any{leaseCurrentRow(other)}) }))
	defer server.Close()
	err := publicRefreshTestService(t, server).requireCurrentPublicationLease(context.Background(), want, "lease_test")
	assertUpdateStateError(t, err, "deferred", "lease_test", "publication_lease_lost")
}

func TestReadbackGenerationRejectsPartialGlobalReplication(t *testing.T) {
	lease := testPublicationLease()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCHRows(w, markerReadbackRows(publicationHosts()[:3], "webr", 200500, "018f0000-0000-7000-8000-000000000001", "018f0000-0000-7000-8000-000000000002", 202000, lease))
	}))
	defer server.Close()
	err := publicRefreshTestService(t, server).readbackGeneration(context.Background(), publicationTopology(), generationEvidence{
		Surface: "webr", GenerationMS: 200500, MarkerUUID: "018f0000-0000-7000-8000-000000000001", RunUUID: "018f0000-0000-7000-8000-000000000002",
		FenceEpoch: lease.FenceEpoch, LeaseUUID: lease.LeaseUUID, TargetRows: 12, TargetUnique: 12, TargetFingerprintSum: 120, TargetFingerprintXOR: 30,
		Rows: 12, Unique: 12, FingerprintSum: 120, FingerprintXOR: 30, SourceFetchedMaxMS: 200000,
		RefreshReadRows: 20, RefreshWriteRows: 12, RefreshSuccessMS: 200000, RefreshEndMS: 201000, CompletedMS: 202000,
	})
	assertUpdateStateError(t, err, "degraded", "public_marker_webr_readback", "marker_replica_count")
}

func TestReadRawActivationRevisionRejectsReplicaDrift(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := rawActivationRevisionRows(6)
		rows[3]["raw_revision"] = 7
		writeCHRows(w, rows)
	}))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).readRawActivationRevision(context.Background(), publicationTopology())
	assertUpdateStateError(t, err, "degraded", "public_activation_revision", "activation_revision_replica_drift")
}

func TestRefreshPublicLectureViewsRejectsEnabledFallbackBeforeQuery(t *testing.T) {
	svc := &Service{Cfg: Config{PublicationV2Enabled: true, PublicationWriterID: "test-writer", CHDirectReplicaFallback: true}}
	_, err := svc.RefreshPublicLectureViews(context.Background())
	assertUpdateStateError(t, err, "degraded", "public_refresh_write_safety", "unsafe_write_fallback_enabled")
}

func TestRefreshPublicLectureViewsInactiveModeMakesNoSQLRequest(t *testing.T) {
	svc := &Service{Cfg: Config{PublicationV2Enabled: false, CHHost: "127.0.0.1", CHPort: 1}}
	_, err := svc.RefreshPublicLectureViews(context.Background())
	assertUpdateStateError(t, err, "degraded", "public_refresh_activation", "publication_v2_inactive")
}

func TestRefreshPublicLectureViewsRequiresReaderRefreshConfigForLoadedRollout(t *testing.T) {
	svc := &Service{Cfg: Config{PublicationV2Enabled: true, PublicationWriterID: "test-writer", ReaderRefreshRequired: true}}
	_, err := svc.RefreshPublicLectureViews(context.Background())
	assertUpdateStateError(t, err, "degraded", "public_reader_config", "missing_reader_refresh_config")
}

func TestRefreshPublicLectureViewsDefersForPendingOutbox(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeCHRows(w, []map[string]any{{"pending": 1}}) }))
	defer server.Close()
	_, err := publicRefreshTestService(t, server).RefreshPublicLectureViews(context.Background())
	assertUpdateStateError(t, err, "deferred", "public_refresh_outbox_preflight", "pending_direct_outbox")
}

func TestPublicationPreflightRendererRequiresThreeGenerationsAndFence(t *testing.T) {
	sql := renderPublicationCandidatePreflightSQL(map[string]int64{"webr": 200500, "mirtype": 300500, "statground": 400500}, 9, "018f0000-0000-7000-8000-000000000020")
	for _, fragment := range []string{"fromUnixTimestamp64Milli(200500)", "fromUnixTimestamp64Milli(300500)", "fromUnixTimestamp64Milli(400500)", "fence_epoch=toUInt64(9)", "dateDiff('second',p.source_fetched_max,now64(3,'Asia/Seoul'))BETWEEN0AND129600"} {
		if !strings.Contains(strings.ReplaceAll(sql, " ", ""), strings.ReplaceAll(fragment, " ", "")) {
			t.Fatalf("preflight missing %q", fragment)
		}
	}
}

func TestClickHouseDateTimeMSAcceptsJSONNumber(t *testing.T) {
	got, err := clickHouseDateTimeMS(json.Number("300500"))
	if err != nil || got != 300500 {
		t.Fatalf("got=%d err=%v", got, err)
	}
}

func publicRefreshTestService(t *testing.T, server *httptest.Server) *Service {
	t.Helper()
	svc := testClickHouseService(t, server)
	svc.Cfg.CHCluster = "statground_cluster"
	svc.Cfg.PublicationV2Enabled = true
	svc.Cfg.PublicationWriterID = "test-writer"
	svc.Cfg.CHDirectReplicaFallback = false
	svc.Cfg.CHDirectOutboxFallback = false
	return svc
}

func publicationHosts() []string { return []string{"s1r1", "s1r2", "s2r1", "s2r2"} }
func publicationTopology() map[string]int {
	return map[string]int{"s1r1": 1, "s1r2": 1, "s2r1": 2, "s2r2": 2}
}
func publicationTopologyRows() []map[string]any {
	return []map[string]any{{"shard_num": 1, "replica_num": 1, "host_name": "s1r1"}, {"shard_num": 1, "replica_num": 2, "host_name": "s1r2"}, {"shard_num": 2, "replica_num": 1, "host_name": "s2r1"}, {"shard_num": 2, "replica_num": 2, "host_name": "s2r2"}}
}

func rawLeaseEpochRows(epoch uint64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "raw_fence_epoch": strconv.FormatUint(epoch, 10)})
	}
	return rows
}
func leaseCurrentRow(lease publicationLease) map[string]any {
	return map[string]any{"fence_epoch": lease.FenceEpoch, "lease_uuid": lease.LeaseUUID, "writer_id": lease.WriterID, "acquired_ms": lease.AcquiredMS, "expires_ms": lease.ExpiresMS}
}
func leaseReplicaRows(lease publicationLease, count int) []map[string]any {
	rows := make([]map[string]any, 0, count)
	for _, host := range publicationHosts()[:count] {
		row := leaseCurrentRow(lease)
		row["hostname"] = host
		rows = append(rows, row)
	}
	return rows
}
func testPublicationLease() publicationLease {
	return publicationLease{FenceEpoch: 9, LeaseUUID: "018f0000-0000-7000-8000-000000000020", WriterID: "test-writer", AcquiredMS: 100000, ExpiresMS: 6700000}
}

func rawActivationRevisionRows(revision uint64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "raw_revision": revision})
	}
	return rows
}

func querySurface(query string) string {
	if strings.Contains(query, "'statground'") {
		return "statground"
	}
	if strings.Contains(query, "'mirtype'") {
		return "mirtype"
	}
	return "webr"
}
func surfaceGeneration(surface string) int64 {
	if surface == "mirtype" {
		return 300500
	}
	if surface == "statground" {
		return 400500
	}
	return 200500
}
func surfaceSourceMax(surface string) int64 { return surfaceGeneration(surface) - 500 }

func refreshState(surface string, success, end int64, read, written uint64) publicRefreshState {
	view := publicLectureRefreshViews[0]
	if surface == "mirtype" {
		view = publicLectureRefreshViews[1]
	}
	return publicRefreshState{Database: view.Database, View: view.View, Status: "Scheduled", LastSuccessMS: success, LastRefreshMS: end, ReadRows: read, WrittenRows: written, ObservedMS: end + 1000}
}
func refreshStateRow(surface string, success, end int64, read, written uint64) map[string]any {
	state := refreshState(surface, success, end, read, written)
	return map[string]any{"database": state.Database, "view": state.View, "status": state.Status, "last_success_ms": success, "last_refresh_ms": end, "read_rows": read, "written_rows": written, "exception_present": 0, "observed_ms": state.ObservedMS}
}

func candidateStatsRow(rows, unique, sum, xor uint64, generation int64) map[string]any {
	return map[string]any{"rows": rows, "unique_keys": unique, "fingerprint_sum": sum, "fingerprint_xor": xor, "generation_count": 1, "min_generation_ms": generation, "max_generation_ms": generation, "source_fetched_max_ms": generation - 500}
}
func candidateReplicaRows(generation int64) []map[string]any {
	out := make([]map[string]any, 0, 4)
	for _, host := range []string{"s1r1", "s1r2"} {
		row := candidateStatsRow(5, 5, 50, 10, generation)
		row["hostname"] = host
		out = append(out, row)
	}
	for _, host := range []string{"s2r1", "s2r2"} {
		row := candidateStatsRow(7, 7, 70, 20, generation)
		row["hostname"] = host
		out = append(out, row)
	}
	return out
}
func snapshotReplicaRows(surface string, generation int64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		row := candidateStatsRow(12, 12, 120, 30, generation)
		row["hostname"], row["surface"] = host, surface
		rows = append(rows, row)
	}
	return rows
}

func preflightRows() []map[string]any {
	rows := make([]map[string]any, 0, 3)
	for _, surface := range []string{"mirtype", "statground", "webr"} {
		rows = append(rows, map[string]any{"surface": surface, "generation": surfaceGeneration(surface), "target_row_count": 12, "target_logical_key_count": 12, "target_fingerprint_sum": 120, "target_fingerprint_xor": 30, "row_count": 12, "logical_key_count": 12, "fingerprint_sum": 120, "fingerprint_xor": 30, "source_fetched_max": surfaceSourceMax(surface), "authority_revision": 1})
	}
	return rows
}

func sourceAuthorityRevisionRows(revision uint64, operationUUID string) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for _, host := range publicationHosts() {
		rows = append(rows, map[string]any{"hostname": host, "authority_revision": revision, "operation_uuid": operationUUID})
	}
	return rows
}

func markerReadbackRows(hosts []string, surface string, generation int64, markerUUID, runUUID string, completed int64, lease publicationLease) []map[string]any {
	rows := make([]map[string]any, 0, len(hosts))
	success, end, read := int64(200000), int64(201000), uint64(20)
	if surface == "mirtype" {
		success, end = 300000, 301000
	}
	if surface == "statground" {
		success, end, read = 400500, 401000, 12
	}
	for _, host := range hosts {
		rows = append(rows, map[string]any{"hostname": host, "surface": surface, "generation_ms": generation, "marker_uuid": markerUUID, "run_uuid": runUUID, "fence_epoch": lease.FenceEpoch, "lease_uuid": lease.LeaseUUID, "target_row_count": 12, "target_logical_key_count": 12, "target_fingerprint_sum": 120, "target_fingerprint_xor": 30, "row_count": 12, "logical_key_count": 12, "fingerprint_sum": 120, "fingerprint_xor": 30, "source_fetched_max_ms": surfaceSourceMax(surface), "refresh_read_rows": read, "refresh_written_rows": 12, "refresh_success_ms": success, "refresh_end_ms": end, "completed_ms": completed})
	}
	return rows
}

func activationReadbackRows(hosts []string, activationUUID, runUUID string, markers map[string]string, lease publicationLease) []map[string]any {
	rows := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		rows = append(rows, map[string]any{"hostname": host, "activation_revision": 7, "activation_uuid": activationUUID, "run_uuid": runUUID, "fence_epoch": lease.FenceEpoch, "lease_uuid": lease.LeaseUUID, "webr_generation_ms": 200500, "webr_marker_uuid": markers["webr"], "mirtype_generation_ms": 300500, "mirtype_marker_uuid": markers["mirtype"], "statground_generation_ms": 400500, "statground_marker_uuid": markers["statground"], "activation_kind": "publish", "activated_ms": 403000})
	}
	return rows
}

func publicationPointerRows(runUUID, activationUUID string, markers map[string]string, lease publicationLease) []map[string]any {
	rows := make([]map[string]any, 0, 3)
	for _, surface := range []string{"mirtype", "statground", "webr"} {
		success, end := int64(200000), int64(201000)
		if surface == "mirtype" {
			success, end = 300000, 301000
		}
		if surface == "statground" {
			success, end = 400500, 401000
		}
		rows = append(rows, map[string]any{"surface": surface, "generation_ms": surfaceGeneration(surface), "marker_uuid": markers[surface], "run_uuid": runUUID, "target_row_count": 12, "target_logical_key_count": 12, "target_fingerprint_sum": 120, "target_fingerprint_xor": 30, "row_count": 12, "logical_key_count": 12, "fingerprint_sum": 120, "fingerprint_xor": 30, "source_fetched_max_ms": surfaceSourceMax(surface), "refresh_success_ms": success, "refresh_end_ms": end, "activation_revision": 7, "activation_uuid": activationUUID, "activation_kind": "publish"})
	}
	return rows
}

func writeCHRows(w http.ResponseWriter, rows []map[string]any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
}
