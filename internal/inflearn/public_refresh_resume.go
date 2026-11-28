package inflearn

import (
	"context"
	"fmt"
	"strings"
)

const firstPublicationResumePhase = "public_first_publication_resume"

// FirstPublicationCheckpoint identifies the initial lease and completed work
// from a first publication that stopped before creating any generation marker.
type FirstPublicationCheckpoint struct {
	LeaseUUID               string                        `json:"lease_uuid"`
	FenceEpoch              uint64                        `json:"fence_epoch"`
	AcquiredMS              int64                         `json:"acquired_ms"`
	ExpiresMS               int64                         `json:"expires_ms"`
	InventoryRevision       uint64                        `json:"inventory_revision"`
	InventoryUUID           string                        `json:"inventory_uuid"`
	SourceAuthorityRevision uint64                        `json:"source_authority_revision"`
	WebRGenerationMS        int64                         `json:"webr_generation_ms"`
	WebRSuccessMS           int64                         `json:"webr_success_ms"`
	WebRRefreshMS           int64                         `json:"webr_refresh_ms"`
	WebRRows                uint64                        `json:"webr_rows"`
	WebRFingerprintSum      uint64                        `json:"webr_fingerprint_sum"`
	WebRFingerprintXOR      uint64                        `json:"webr_fingerprint_xor"`
	WebRSnapshot            bool                          `json:"webr_snapshot,omitempty"`
	MirType                 *LectureRefreshCheckpoint     `json:"mirtype,omitempty"`
	StatgroundSnapshot      *StatgroundSnapshotCheckpoint `json:"statground_snapshot,omitempty"`
	SupersededSnapshots     map[string]int64              `json:"superseded_snapshots,omitempty"`
	Markers                 *LectureMarkerCheckpoint      `json:"markers,omitempty"`
}

type StatgroundSnapshotCheckpoint struct {
	GenerationMS int64 `json:"generation_ms"`
	CompletedMS  int64 `json:"completed_ms"`
}

type LectureRefreshCheckpoint struct {
	GenerationMS   int64  `json:"generation_ms"`
	SuccessMS      int64  `json:"success_ms"`
	RefreshMS      int64  `json:"refresh_ms"`
	Rows           uint64 `json:"rows"`
	FingerprintSum uint64 `json:"fingerprint_sum"`
	FingerprintXOR uint64 `json:"fingerprint_xor"`
}

type LectureMarkerCheckpoint struct {
	RunUUID     string            `json:"run_uuid"`
	CompletedMS int64             `json:"completed_ms"`
	MarkerUUIDs map[string]string `json:"marker_uuids"`
}

type firstPublicationResume struct {
	lease             publicationLease
	candidates        map[string]generationEvidence
	snapshots         map[string]generationEvidence
	inventoryRevision uint64
	inventoryUUID     string
	authorityRevision uint64
	markers           *LectureMarkerCheckpoint
}

func (s *Service) CheckFirstPublicationResume(ctx context.Context, checkpoint FirstPublicationCheckpoint) (FirstPublicationCheckpoint, error) {
	if _, err := s.validateFirstPublicationResume(ctx, checkpoint); err != nil {
		return FirstPublicationCheckpoint{}, err
	}
	return checkpoint, nil
}

func (s *Service) ResumeFirstPublicLectureViews(ctx context.Context, checkpoint FirstPublicationCheckpoint) (PublicRefreshReceipt, error) {
	resume, err := s.validateFirstPublicationResume(ctx, checkpoint)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	return s.refreshPublicLectureViews(ctx, &resume)
}

func (s *Service) validateFirstPublicationResume(ctx context.Context, cp FirstPublicationCheckpoint) (firstPublicationResume, error) {
	if !s.Cfg.PublicationV2Enabled || s.Cfg.PublicationReaderConfig == "" || s.Cfg.PublicationWriterID == "" ||
		!isCanonicalUUID(cp.LeaseUUID) || !isCanonicalUUID(cp.InventoryUUID) || cp.FenceEpoch == 0 ||
		cp.InventoryRevision == 0 || cp.SourceAuthorityRevision == 0 || cp.AcquiredMS <= 0 {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_checkpoint")
	}
	web := LectureRefreshCheckpoint{
		GenerationMS: cp.WebRGenerationMS, SuccessMS: cp.WebRSuccessMS, RefreshMS: cp.WebRRefreshMS,
		Rows: cp.WebRRows, FingerprintSum: cp.WebRFingerprintSum, FingerprintXOR: cp.WebRFingerprintXOR,
	}
	if !web.within(cp.AcquiredMS, cp.ExpiresMS) || (cp.WebRSnapshot && cp.MirType == nil) ||
		(cp.MirType != nil && !cp.MirType.within(cp.AcquiredMS, cp.ExpiresMS)) {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_checkpoint")
	}
	if cp.StatgroundSnapshot != nil {
		if !cp.WebRSnapshot || cp.StatgroundSnapshot.GenerationMS < cp.MirType.RefreshMS ||
			cp.StatgroundSnapshot.CompletedMS < cp.StatgroundSnapshot.GenerationMS ||
			cp.StatgroundSnapshot.CompletedMS >= cp.ExpiresMS ||
			cp.StatgroundSnapshot.CompletedMS-cp.StatgroundSnapshot.GenerationMS > maxPublicRefreshMillis {
			return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_snapshot_checkpoint")
		}
	}
	if len(cp.SupersededSnapshots) != 0 {
		if cp.StatgroundSnapshot == nil || len(cp.SupersededSnapshots) != len(expectedPublicationSurfaces) {
			return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_superseded_snapshot_set")
		}
		for _, surface := range expectedPublicationSurfaces {
			if generation := cp.SupersededSnapshots[surface]; generation <= 0 || generation >= cp.AcquiredMS {
				return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_superseded_snapshot_set")
			}
		}
	}
	markerFilter := ""
	if cp.Markers != nil {
		if cp.StatgroundSnapshot == nil || !isCanonicalUUID(cp.Markers.RunUUID) ||
			cp.Markers.CompletedMS < cp.StatgroundSnapshot.CompletedMS || cp.Markers.CompletedMS >= cp.ExpiresMS ||
			len(cp.Markers.MarkerUUIDs) != len(expectedPublicationSurfaces) {
			return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_marker_checkpoint")
		}
		markers := make([]string, 0, len(expectedPublicationSurfaces))
		seen := map[string]bool{}
		for _, surface := range expectedPublicationSurfaces {
			uuid := cp.Markers.MarkerUUIDs[surface]
			if !isCanonicalUUID(uuid) || seen[uuid] {
				return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_marker_checkpoint")
			}
			seen[uuid] = true
			markers = append(markers, fmt.Sprintf("(surface=%s AND marker_uuid=toUUID(%s))", QuoteSQLString(surface), QuoteSQLString(uuid)))
		}
		markerFilter = fmt.Sprintf("WHERE NOT (run_uuid=toUUID(%s) AND (%s))", QuoteSQLString(cp.Markers.RunUUID), strings.Join(markers, " OR "))
	}
	if err := s.validatePublicRefreshWriteSafety(ctx); err != nil {
		return firstPublicationResume{}, err
	}
	topology, err := s.readPublicationTopology(ctx)
	if err != nil {
		return firstPublicationResume{}, err
	}
	lease := publicationLease{FenceEpoch: cp.FenceEpoch, LeaseUUID: cp.LeaseUUID,
		WriterID: s.Cfg.PublicationWriterID, AcquiredMS: cp.AcquiredMS, ExpiresMS: cp.ExpiresMS}
	if err := s.requireCurrentPublicationLease(ctx, lease, firstPublicationResumePhase); err != nil {
		return firstPublicationResume{}, err
	}
	epoch, err := s.readRawLeaseEpoch(ctx, topology)
	if err != nil {
		return firstPublicationResume{}, err
	}
	if epoch != lease.FenceEpoch {
		return firstPublicationResume{}, stateError("deferred", firstPublicationResumePhase, "publication_lease_lost")
	}
	if err := s.readbackPublicationLease(ctx, topology, lease); err != nil {
		return firstPublicationResume{}, err
	}
	revision, err := s.readRawActivationRevision(ctx, topology)
	if err != nil {
		return firstPublicationResume{}, err
	}
	if revision != 0 {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "first_activation_already_started")
	}
	snapshotFilter := ""
	if cp.WebRSnapshot {
		snapshotFilter = fmt.Sprintf("WHERE surface != 'webr' OR generation != fromUnixTimestamp64Milli(%d)", cp.WebRGenerationMS)
	}
	if cp.StatgroundSnapshot != nil {
		matches := []string{
			fmt.Sprintf("(surface = 'webr' AND generation = fromUnixTimestamp64Milli(%d))", cp.WebRGenerationMS),
			fmt.Sprintf("(surface = 'mirtype' AND generation = fromUnixTimestamp64Milli(%d))", cp.MirType.GenerationMS),
			fmt.Sprintf("(surface = 'statground' AND generation = fromUnixTimestamp64Milli(%d))", cp.StatgroundSnapshot.GenerationMS),
		}
		for _, surface := range expectedPublicationSurfaces {
			if generation := cp.SupersededSnapshots[surface]; generation != 0 {
				matches = append(matches, fmt.Sprintf("(surface = %s AND generation = fromUnixTimestamp64Milli(%d))",
					QuoteSQLString(surface), generation))
			}
		}
		snapshotFilter = "WHERE NOT (" + strings.Join(matches, " OR ") + ")"
	}
	for _, source := range []struct{ table, column, filter string }{
		{lectureMarkerLocalTable, "marker_uuid", markerFilter},
		{lectureTransitionReleaseLocalTable, "activation_uuid", ""},
		{lectureProjectionSnapshotLocalView, "generation", snapshotFilter},
	} {
		rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT %s FROM clusterAllReplicas(%s, %s) %s LIMIT 1
			SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 15`, source.column, QuoteSQLString(s.Cfg.CHCluster), source.table, source.filter))
		if err != nil {
			return firstPublicationResume{}, newUpdateReadStateError(firstPublicationResumePhase, err)
		}
		if len(rows) != 0 {
			return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "serving_history_already_started")
		}
	}
	authority, _, err := s.readSourceAuthorityRevision(ctx, topology, firstPublicationResumePhase)
	if err != nil {
		return firstPublicationResume{}, err
	}
	if authority != cp.SourceAuthorityRevision {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "source_authority_changed")
	}
	readers, err := s.CheckPublicationReaders(ctx)
	if err != nil {
		return firstPublicationResume{}, err
	}
	if readers.InventoryRevision != cp.InventoryRevision || readers.InventoryUUID != cp.InventoryUUID {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "reader_inventory_changed")
	}
	for _, reader := range readers.Readers {
		if reader.Phase != "steady" || !reader.AdmissionOpen {
			return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "reader_admission_unsettled")
		}
	}
	candidates := make(map[string]generationEvidence, 2)
	for _, view := range publicLectureRefreshViews {
		reused := &web
		if view.Surface == "mirtype" {
			reused = cp.MirType
		}
		if reused == nil {
			continue
		}
		evidence, err := s.verifyResumeCandidate(ctx, topology, view, *reused)
		if err != nil {
			return firstPublicationResume{}, err
		}
		candidates[view.Surface] = evidence
	}
	snapshots := make(map[string]generationEvidence, 3)
	if cp.WebRSnapshot {
		snapshots["webr"] = candidates["webr"]
	}
	if cp.StatgroundSnapshot != nil {
		statground, err := s.validateStatgroundProjectionCandidate(ctx, cp.StatgroundSnapshot.GenerationMS, publicationPointer{})
		if err != nil {
			return firstPublicationResume{}, err
		}
		statground.RefreshEndMS = cp.StatgroundSnapshot.CompletedMS
		statground.RefreshWriteRows = statground.Rows
		snapshots["statground"], snapshots["mirtype"] = statground, candidates["mirtype"]
	}
	for _, surface := range expectedPublicationSurfaces {
		if evidence, ok := snapshots[surface]; ok {
			if err := s.readbackSnapshot(ctx, topology, evidence); err != nil {
				return firstPublicationResume{}, err
			}
		}
	}
	return firstPublicationResume{lease: lease, candidates: candidates, snapshots: snapshots, inventoryRevision: cp.InventoryRevision,
		inventoryUUID: cp.InventoryUUID, authorityRevision: authority, markers: cp.Markers}, nil
}

func (cp LectureRefreshCheckpoint) within(acquiredMS, expiresMS int64) bool {
	return cp.SuccessMS >= acquiredMS && cp.RefreshMS >= cp.SuccessMS && cp.RefreshMS < expiresMS &&
		cp.GenerationMS >= cp.SuccessMS && cp.GenerationMS-cp.RefreshMS < 1000 && cp.Rows > 0
}

func (s *Service) verifyResumeCandidate(ctx context.Context, topology map[string]int, view publicLectureRefreshView, cp LectureRefreshCheckpoint) (generationEvidence, error) {
	refreshes, err := s.readPublicRefreshStates(ctx, &view)
	if err != nil {
		return generationEvidence{}, newUpdateReadStateError(firstPublicationResumePhase, err)
	}
	if len(refreshes) != 1 || refreshes[0].LastSuccessMS != cp.SuccessMS || refreshes[0].LastRefreshMS != cp.RefreshMS {
		return generationEvidence{}, stateError("degraded", firstPublicationResumePhase, "refresh_checkpoint_changed")
	}
	if err := validateIdlePublicRefreshState(refreshes[0], firstPublicationResumePhase); err != nil {
		return generationEvidence{}, err
	}
	evidence, err := s.validateCandidateGeneration(ctx, topology, view.Surface, refreshes[0], publicationPointer{})
	if err != nil {
		return generationEvidence{}, err
	}
	if evidence.GenerationMS != cp.GenerationMS || evidence.TargetRows != cp.Rows ||
		evidence.TargetFingerprintSum != cp.FingerprintSum || evidence.TargetFingerprintXOR != cp.FingerprintXOR {
		return generationEvidence{}, stateError("degraded", firstPublicationResumePhase, "candidate_checkpoint_changed")
	}
	return evidence, nil
}

func matchesResumeRefresh(state publicRefreshState, evidence generationEvidence) bool {
	return state.LastSuccessMS == evidence.RefreshSuccessMS && state.LastRefreshMS == evidence.RefreshEndMS &&
		state.ReadRows == evidence.RefreshReadRows && state.WrittenRows == evidence.RefreshWriteRows
}
