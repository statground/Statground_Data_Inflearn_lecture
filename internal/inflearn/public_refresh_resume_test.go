package inflearn

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResumeFirstPublicationRejectsLeaseAndServingDrift(t *testing.T) {
	lease := testPublicationLease()
	cp := FirstPublicationCheckpoint{
		LeaseUUID: lease.LeaseUUID, FenceEpoch: lease.FenceEpoch, AcquiredMS: lease.AcquiredMS, ExpiresMS: lease.ExpiresMS,
		InventoryRevision: 3, InventoryUUID: testInventoryUUID, SourceAuthorityRevision: 1,
		WebRGenerationMS: 200500, WebRSuccessMS: 200000, WebRRefreshMS: 201000, WebRRows: 12,
	}
	for _, tc := range []struct {
		name, status, phase, category string
	}{
		{"other_writer", "deferred", firstPublicationResumePhase, "publication_lease_lost"},
		{"missing_lease_replica", "degraded", "public_refresh_lease_readback", "lease_replica_count"},
		{"prior_activation", "degraded", firstPublicationResumePhase, "first_activation_already_started"},
		{"prior_snapshot", "degraded", firstPublicationResumePhase, "serving_history_already_started"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				query := strings.TrimSpace(string(body))
				if !strings.HasPrefix(query, "SELECT ") {
					t.Errorf("unexpected write: %s", query)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				switch {
				case strings.Contains(query, "replayed_at IS NULL"):
					writeCHRows(w, []map[string]any{{"pending": 0}})
				case strings.Contains(query, "FROM system.clusters"):
					writeCHRows(w, publicationTopologyRows())
				case strings.Contains(query, lectureCurrentLeaseView):
					current := lease
					if tc.name == "other_writer" {
						current.WriterID = "other-writer"
					}
					writeCHRows(w, []map[string]any{leaseCurrentRow(current)})
				case strings.Contains(query, "maxOrDefault(fence_epoch)"):
					writeCHRows(w, rawLeaseEpochRows(lease.FenceEpoch))
				case strings.Contains(query, "WHERE lease.fence_epoch"):
					count := 4
					if tc.name == "missing_lease_replica" {
						count = 3
					}
					writeCHRows(w, leaseReplicaRows(lease, count))
				case strings.Contains(query, "maxOrDefault(activation_revision)"):
					var revision uint64
					if tc.name == "prior_activation" {
						revision = 1
					}
					writeCHRows(w, rawActivationRevisionRows(revision))
				case strings.Contains(query, lectureMarkerLocalTable), strings.Contains(query, lectureTransitionReleaseLocalTable):
					writeCHRows(w, nil)
				case strings.Contains(query, lectureProjectionSnapshotLocalView):
					writeCHRows(w, []map[string]any{{"generation": "2026-11-28 04:10:12.743"}})
				default:
					t.Errorf("unexpected query: %s", query)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			svc := publicRefreshTestService(t, server)
			svc.Cfg.PublicationReaderConfig = "unneeded-before-rejection"
			_, err := svc.ResumeFirstPublicLectureViews(context.Background(), cp)
			assertUpdateStateError(t, err, tc.status, tc.phase, tc.category)
		})
	}
}

func TestResumeSkipsWebRRefreshAndKeepsOriginalLease(t *testing.T) {
	lease := testPublicationLease()
	h := &publicationHappyHarness{t: t, lease: lease, leaseSet: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := strings.TrimSpace(string(body))
		switch {
		case strings.Contains(query, "maxOrDefault(activation_revision)"):
			writeCHRows(w, rawActivationRevisionRows(0))
		case strings.Contains(query, "FROM system.view_refreshes"):
			writeCHRows(w, []map[string]any{refreshStateRow("mirtype", 100000, 101000, 10, 10), refreshStateRow("webr", 200000, 201000, 20, 12)})
		case strings.HasPrefix(query, "INSERT "), strings.HasPrefix(query, "SYSTEM ") && strings.Contains(query, "webr_lecture"):
			t.Errorf("unexpected write before MirType refresh: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		default:
			r.Body = io.NopCloser(strings.NewReader(query))
			h.serveHTTP(w, r)
		}
	}))
	defer server.Close()
	svc := publicRefreshTestService(t, server)
	resume := firstPublicationResume{
		lease: lease, authorityRevision: 1,
		candidates: map[string]generationEvidence{
			"webr": {RefreshSuccessMS: 200000, RefreshEndMS: 201000, RefreshReadRows: 20, RefreshWriteRows: 12},
		},
	}
	_, err := svc.refreshPublicLectureViews(context.Background(), &resume)
	assertUpdateStateError(t, err, "degraded", "public_refresh_mirtype_receipt", "refresh_coordinator_unavailable")
	want := "SYSTEM REFRESH VIEW mirtype_lecture.mv_inflearn_language_lecture_catalog_refresh\nSYSTEM WAIT VIEW mirtype_lecture.mv_inflearn_language_lecture_catalog_refresh"
	if strings.Join(h.systemCommands, "\n") != want {
		t.Fatalf("commands=%q, want=%q", h.systemCommands, want)
	}
}
