package inflearn

import (
	"context"
	"fmt"
)

const firstPublicationResumePhase = "public_first_publication_resume"

// FirstPublicationCheckpoint identifies the existing lease and Web-R candidate
// from a first publication that stopped before writing serving snapshots.
type FirstPublicationCheckpoint struct {
	LeaseUUID               string `json:"lease_uuid"`
	FenceEpoch              uint64 `json:"fence_epoch"`
	AcquiredMS              int64  `json:"acquired_ms"`
	ExpiresMS               int64  `json:"expires_ms"`
	InventoryRevision       uint64 `json:"inventory_revision"`
	InventoryUUID           string `json:"inventory_uuid"`
	SourceAuthorityRevision uint64 `json:"source_authority_revision"`
	WebRGenerationMS        int64  `json:"webr_generation_ms"`
	WebRSuccessMS           int64  `json:"webr_success_ms"`
	WebRRefreshMS           int64  `json:"webr_refresh_ms"`
	WebRRows                uint64 `json:"webr_rows"`
	WebRFingerprintSum      uint64 `json:"webr_fingerprint_sum"`
	WebRFingerprintXOR      uint64 `json:"webr_fingerprint_xor"`
}

type firstPublicationResume struct {
	lease             publicationLease
	webr              generationEvidence
	inventoryRevision uint64
	inventoryUUID     string
	authorityRevision uint64
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
		cp.InventoryRevision == 0 || cp.SourceAuthorityRevision == 0 || cp.AcquiredMS <= 0 ||
		cp.WebRSuccessMS < cp.AcquiredMS || cp.WebRRefreshMS < cp.WebRSuccessMS || cp.WebRRefreshMS >= cp.ExpiresMS ||
		cp.WebRGenerationMS < cp.WebRSuccessMS || cp.WebRGenerationMS-cp.WebRRefreshMS >= 1000 || cp.WebRRows == 0 {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "invalid_checkpoint")
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
	// A marker or a serving snapshot means the interrupted attempt advanced
	// beyond this checkpoint and needs its own reconciliation.
	for _, source := range []struct{ table, column string }{
		{lectureMarkerLocalTable, "marker_uuid"},
		{lectureTransitionReleaseLocalTable, "activation_uuid"},
		{lectureProjectionSnapshotLocalView, "generation"},
	} {
		rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT %s FROM clusterAllReplicas(%s, %s) LIMIT 1
			SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 15`, source.column, QuoteSQLString(s.Cfg.CHCluster), source.table))
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
	view := publicLectureRefreshViews[0]
	refreshes, err := s.readPublicRefreshStates(ctx, &view)
	if err != nil {
		return firstPublicationResume{}, newUpdateReadStateError(firstPublicationResumePhase, err)
	}
	if len(refreshes) != 1 || refreshes[0].LastSuccessMS != cp.WebRSuccessMS || refreshes[0].LastRefreshMS != cp.WebRRefreshMS {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "refresh_checkpoint_changed")
	}
	if err := validateIdlePublicRefreshState(refreshes[0], firstPublicationResumePhase); err != nil {
		return firstPublicationResume{}, err
	}
	evidence, err := s.validateCandidateGeneration(ctx, topology, view.Surface, refreshes[0], publicationPointer{})
	if err != nil {
		return firstPublicationResume{}, err
	}
	if evidence.GenerationMS != cp.WebRGenerationMS || evidence.TargetRows != cp.WebRRows ||
		evidence.TargetFingerprintSum != cp.WebRFingerprintSum || evidence.TargetFingerprintXOR != cp.WebRFingerprintXOR {
		return firstPublicationResume{}, stateError("degraded", firstPublicationResumePhase, "candidate_checkpoint_changed")
	}
	return firstPublicationResume{lease: lease, webr: evidence, inventoryRevision: cp.InventoryRevision,
		inventoryUUID: cp.InventoryUUID, authorityRevision: authority}, nil
}

func matchesResumeRefresh(state publicRefreshState, evidence generationEvidence) bool {
	return state.LastSuccessMS == evidence.RefreshSuccessMS && state.LastRefreshMS == evidence.RefreshEndMS &&
		state.ReadRows == evidence.RefreshReadRows && state.WrittenRows == evidence.RefreshWriteRows
}
