package inflearn

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	lectureCandidateLocalView            = "lecture_publication.v_inflearn_public_catalog_candidate_local"
	lectureCandidateView                 = "lecture_publication.v_inflearn_public_catalog_candidate"
	lectureProjectionCandidateView       = "lecture_publication.v_inflearn_public_catalog_projection_candidate"
	lectureStatgroundSourceLocalView     = "lecture_publication.v_statground_inflearn_workbench_source_candidate_local"
	lectureStatgroundSourceView          = "lecture_publication.v_statground_inflearn_workbench_source_candidate"
	lectureStatgroundProjectionView      = "lecture_publication.v_statground_inflearn_workbench_projection_candidate"
	lectureProjectionSnapshotLocalView   = "lecture_publication.v_inflearn_public_catalog_projection_snapshot_local"
	lectureServingView                   = "lecture_publication.v_inflearn_public_catalog_serving"
	lectureLeaseLocalTable               = "lecture_publication.inflearn_public_catalog_publish_lease_local"
	lectureLeaseTable                    = "lecture_publication.inflearn_public_catalog_publish_lease"
	lectureCurrentLeaseView              = "lecture_publication.v_inflearn_public_catalog_publish_lease_current"
	lectureMarkerLocalTable              = "lecture_publication.inflearn_public_catalog_generation_local"
	lectureMarkerTable                   = "lecture_publication.inflearn_public_catalog_generation"
	lectureActivationLocal               = "lecture_publication.inflearn_public_catalog_activation_local"
	lectureActivationTable               = "lecture_publication.inflearn_public_catalog_activation"
	lecturePointerView                   = "lecture_publication.v_inflearn_public_catalog_generation_latest"
	maxPublicRefreshMillis               = int64(90 * time.Minute / time.Millisecond)
	publicationLeaseDurationMillis       = int64(110 * time.Minute / time.Millisecond)
	publicationSourceFreshnessMillis     = int64(36 * time.Hour / time.Millisecond)
	publicationMutationReconcileAttempts = 2
)

var expectedPublicationSurfaces = []string{"mirtype", "statground", "webr"}

type publicLectureRefreshView struct {
	Database string
	View     string
	Surface  string
}

func (v publicLectureRefreshView) qualifiedName() string { return v.Database + "." + v.View }

var publicLectureRefreshViews = []publicLectureRefreshView{
	{Database: "webr_lecture", View: "mv_inflearn_r_lecture_catalog_refresh", Surface: "webr"},
	{Database: "mirtype_lecture", View: "mv_inflearn_language_lecture_catalog_refresh", Surface: "mirtype"},
}

type PublicRefreshReceipt struct {
	RunUUID            string `json:"run_uuid"`
	ActivationUUID     string `json:"activation_uuid"`
	ActivationRevision uint64 `json:"activation_revision"`
}

type publicRefreshState struct {
	Database         string
	View             string
	Status           string
	LastSuccessMS    int64
	LastRefreshMS    int64
	ReadRows         uint64
	WrittenRows      uint64
	ExceptionPresent bool
	ObservedMS       int64
}

type publicationLease struct {
	FenceEpoch uint64
	LeaseUUID  string
	WriterID   string
	AcquiredMS int64
	ExpiresMS  int64
}

type publicationPointer struct {
	Surface              string
	GenerationMS         int64
	MarkerUUID           string
	RunUUID              string
	TargetRows           uint64
	TargetUnique         uint64
	TargetFingerprintSum uint64
	TargetFingerprintXOR uint64
	Rows                 uint64
	Unique               uint64
	FingerprintSum       uint64
	FingerprintXOR       uint64
	SourceFetchedMaxMS   int64
	RefreshSuccessMS     int64
	RefreshEndMS         int64
	ActivationRevision   uint64
	ActivationUUID       string
	ActivationKind       string
}

type candidateStats struct {
	Rows            uint64
	Unique          uint64
	FingerprintSum  uint64
	FingerprintXOR  uint64
	GenerationCount uint64
	GenerationMS    int64
}

type candidateReplicaStats struct {
	Host  string
	Shard int
	candidateStats
}

type generationEvidence struct {
	Surface                 string
	GenerationMS            int64
	MarkerUUID              string
	RunUUID                 string
	FenceEpoch              uint64
	LeaseUUID               string
	TargetRows              uint64
	TargetUnique            uint64
	TargetFingerprintSum    uint64
	TargetFingerprintXOR    uint64
	Rows                    uint64
	Unique                  uint64
	FingerprintSum          uint64
	FingerprintXOR          uint64
	SourceFetchedMaxMS      int64
	RefreshReadRows         uint64
	RefreshWriteRows        uint64
	RefreshSuccessMS        int64
	RefreshEndMS            int64
	CompletedMS             int64
	SourceAuthorityRevision uint64
}

type activationEvidence struct {
	Revision               uint64
	ActivationUUID         string
	RunUUID                string
	FenceEpoch             uint64
	LeaseUUID              string
	WebRGenerationMS       int64
	WebRMarkerUUID         string
	MirGenerationMS        int64
	MirMarkerUUID          string
	StatgroundGenerationMS int64
	StatgroundMarkerUUID   string
	Kind                   string
	ActivatedMS            int64
}

func (s *Service) RefreshPublicLectureViews(ctx context.Context) (PublicRefreshReceipt, error) {
	if !s.Cfg.PublicationV2Enabled {
		return PublicRefreshReceipt{}, stateError("degraded", "public_refresh_activation", "publication_v2_inactive")
	}
	if strings.TrimSpace(s.Cfg.PublicationWriterID) == "" {
		return PublicRefreshReceipt{}, stateError("degraded", "public_refresh_lease", "missing_writer_identity")
	}
	var readerConfig lectureReaderConfigFile
	readerRefreshEnabled := strings.TrimSpace(s.Cfg.PublicationReaderConfig) != ""
	if s.Cfg.ReaderRefreshRequired && !readerRefreshEnabled {
		return PublicRefreshReceipt{}, stateError("degraded", "public_reader_config", "missing_reader_refresh_config")
	}
	if readerRefreshEnabled {
		var configErr error
		readerConfig, configErr = loadLectureReaderConfig(s.Cfg.PublicationReaderConfig)
		if configErr != nil {
			return PublicRefreshReceipt{}, configErr
		}
	}
	if err := s.validatePublicRefreshWriteSafety(ctx); err != nil {
		return PublicRefreshReceipt{}, err
	}
	topology, err := s.readPublicationTopology(ctx)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	lease, err := s.acquirePublicationLease(ctx, topology)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	if readerRefreshEnabled {
		if err := s.reconcilePublicationTransitionReaders(ctx, topology, readerConfig, lease); err != nil {
			return PublicRefreshReceipt{}, err
		}
	}
	priorPointers, _, err := s.readPublicationPointer(ctx, "public_refresh_pointer_preflight")
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	initialRevision, err := s.readRawActivationRevision(ctx, topology)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	initialAuthorityRevision, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_refresh_source_authority_preflight")
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	preStates, err := s.readPublicRefreshStates(ctx, nil)
	if err != nil {
		return PublicRefreshReceipt{}, newUpdateReadStateError("public_refresh_preflight", err)
	}
	preByName, err := exactRefreshStates(preStates, "public_refresh_preflight")
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	for _, view := range publicLectureRefreshViews {
		if err := validateIdlePublicRefreshState(preByName[view.qualifiedName()], "public_refresh_preflight"); err != nil {
			return PublicRefreshReceipt{}, err
		}
	}

	identityTime := time.Now()
	runUUID := UUIDv7String(identityTime)
	markerUUIDs := map[string]string{
		"webr":       UUIDv7String(identityTime.Add(time.Millisecond)),
		"mirtype":    UUIDv7String(identityTime.Add(2 * time.Millisecond)),
		"statground": UUIDv7String(identityTime.Add(3 * time.Millisecond)),
	}
	activationUUID := UUIDv7String(identityTime.Add(4 * time.Millisecond))
	evidenceBySurface := make(map[string]generationEvidence, 3)

	for _, view := range publicLectureRefreshViews {
		phase := "public_refresh_" + view.Surface
		if err := s.requireCurrentPublicationLease(ctx, lease, phase+"_lease"); err != nil {
			return PublicRefreshReceipt{}, err
		}
		if err := s.execPublicRefreshCommand(ctx, "SYSTEM REFRESH VIEW "+view.qualifiedName(), 2*time.Minute); err != nil {
			return PublicRefreshReceipt{}, newUpdateReadStateError(phase, err)
		}
		if err := s.execPublicRefreshCommand(ctx, "SYSTEM WAIT VIEW "+view.qualifiedName(), 30*time.Minute); err != nil {
			return PublicRefreshReceipt{}, newUpdateReadStateError(phase, err)
		}
		postRows, err := s.readPublicRefreshStates(ctx, &view)
		if err != nil {
			return PublicRefreshReceipt{}, newUpdateReadStateError(phase+"_receipt", err)
		}
		if len(postRows) != 1 {
			return PublicRefreshReceipt{}, stateError("degraded", phase+"_receipt", "refresh_coordinator_unavailable")
		}
		post := postRows[0]
		if err := validateAdvancedPublicRefreshState(preByName[view.qualifiedName()], post, phase+"_receipt"); err != nil {
			return PublicRefreshReceipt{}, err
		}
		evidence, err := s.validateCandidateGeneration(ctx, topology, view.Surface, post, priorPointers[view.Surface])
		if err != nil {
			return PublicRefreshReceipt{}, err
		}
		evidenceBySurface[view.Surface] = evidence
	}

	statgroundStartMS, err := s.readServerTimeMS(ctx)
	if err != nil {
		return PublicRefreshReceipt{}, newUpdateReadStateError("public_candidate_statground_time", err)
	}
	statground, err := s.validateStatgroundProjectionCandidate(ctx, statgroundStartMS, priorPointers["statground"])
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	evidenceBySurface["statground"] = statground

	for _, surface := range []string{"webr", "mirtype", "statground"} {
		if err := s.requireCurrentPublicationLease(ctx, lease, "public_snapshot_"+surface+"_lease"); err != nil {
			return PublicRefreshReceipt{}, err
		}
		if err := s.insertAndReadbackSnapshot(ctx, topology, evidenceBySurface[surface], runUUID); err != nil {
			return PublicRefreshReceipt{}, err
		}
	}
	statgroundEndMS, err := s.readServerTimeMS(ctx)
	if err != nil {
		return PublicRefreshReceipt{}, newUpdateReadStateError("public_candidate_statground_end_time", err)
	}
	if statgroundEndMS < statgroundStartMS || statgroundEndMS-statgroundStartMS > maxPublicRefreshMillis {
		return PublicRefreshReceipt{}, stateError("degraded", "public_candidate_statground", "invalid_snapshot_interval")
	}
	statground = evidenceBySurface["statground"]
	statground.RefreshEndMS = statgroundEndMS
	statground.RefreshWriteRows = statground.Rows
	evidenceBySurface["statground"] = statground

	generations := map[string]int64{
		"webr":       evidenceBySurface["webr"].GenerationMS,
		"mirtype":    evidenceBySurface["mirtype"].GenerationMS,
		"statground": evidenceBySurface["statground"].GenerationMS,
	}
	preflightEvidence, err := s.runPublicationCandidatePreflight(ctx, generations, lease)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	for _, surface := range []string{"webr", "mirtype", "statground"} {
		verified := preflightEvidence[surface]
		if verified.SourceAuthorityRevision != initialAuthorityRevision {
			return PublicRefreshReceipt{}, stateError("degraded", "public_candidate_preflight", "source_authority_changed_during_run")
		}
		current := evidenceBySurface[surface]
		if current.GenerationMS != verified.GenerationMS || current.Rows != verified.Rows ||
			current.Unique != verified.Unique || current.FingerprintSum != verified.FingerprintSum ||
			current.FingerprintXOR != verified.FingerprintXOR || current.SourceFetchedMaxMS != verified.SourceFetchedMaxMS {
			return PublicRefreshReceipt{}, stateError("degraded", "public_candidate_preflight", "candidate_changed_during_preflight")
		}
		current.TargetRows = verified.TargetRows
		current.TargetUnique = verified.TargetUnique
		current.TargetFingerprintSum = verified.TargetFingerprintSum
		current.TargetFingerprintXOR = verified.TargetFingerprintXOR
		if surface == "statground" {
			// Statground has no Refreshable MV receipt. Its read count is the
			// exact verified three-component raw target, available only after
			// the shared 002a preflight has completed.
			current.RefreshReadRows = verified.TargetRows
		}
		current.MarkerUUID = markerUUIDs[surface]
		current.RunUUID = runUUID
		current.FenceEpoch = lease.FenceEpoch
		current.LeaseUUID = lease.LeaseUUID
		evidenceBySurface[surface] = current
	}

	completedMS, err := s.readServerTimeMS(ctx)
	if err != nil {
		return PublicRefreshReceipt{}, newUpdateReadStateError("public_marker_time", err)
	}
	if completedMS >= lease.ExpiresMS {
		return PublicRefreshReceipt{}, stateError("deferred", "public_marker_time", "publication_lease_expired")
	}
	if err := s.requireCurrentPublicationLease(ctx, lease, "public_marker_lease"); err != nil {
		return PublicRefreshReceipt{}, err
	}
	for _, surface := range []string{"webr", "mirtype", "statground"} {
		evidence := evidenceBySurface[surface]
		if completedMS < evidence.RefreshEndMS || completedMS > evidence.SourceFetchedMaxMS+publicationSourceFreshnessMillis {
			return PublicRefreshReceipt{}, stateError("degraded", "public_marker_"+surface, "invalid_completion_time")
		}
		evidence.CompletedMS = completedMS
		evidenceBySurface[surface] = evidence
		if err := s.insertAndReadbackGeneration(ctx, topology, evidence); err != nil {
			return PublicRefreshReceipt{}, err
		}
		printMachineJSON(map[string]any{
			"schema": "statground.inflearn.public_generation.v2", "status": "marker_verified",
			"surface": evidence.Surface, "generation_ms": evidence.GenerationMS,
			"target_row_count": evidence.TargetRows, "row_count": evidence.Rows,
			"run_uuid": evidence.RunUUID, "marker_uuid": evidence.MarkerUUID,
			"fence_epoch": evidence.FenceEpoch,
		})
	}

	if err := s.requireCurrentPublicationLease(ctx, lease, "public_activation_lease"); err != nil {
		return PublicRefreshReceipt{}, err
	}
	activationPreflight, err := s.runPublicationCandidatePreflight(ctx, generations, lease)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	for _, surface := range expectedPublicationSurfaces {
		before, after := evidenceBySurface[surface], activationPreflight[surface]
		if after.SourceAuthorityRevision != initialAuthorityRevision || before.GenerationMS != after.GenerationMS ||
			before.Rows != after.Rows || before.Unique != after.Unique || before.FingerprintSum != after.FingerprintSum ||
			before.FingerprintXOR != after.FingerprintXOR || before.SourceFetchedMaxMS != after.SourceFetchedMaxMS {
			return PublicRefreshReceipt{}, stateError("degraded", "public_activation_source_authority", "source_or_candidate_changed_before_activation")
		}
	}
	activationAuthorityRevision, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_activation_source_authority")
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	if activationAuthorityRevision != initialAuthorityRevision {
		return PublicRefreshReceipt{}, stateError("degraded", "public_activation_source_authority", "source_authority_changed_before_activation")
	}
	rawRevision, err := s.readRawActivationRevision(ctx, topology)
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	if rawRevision != initialRevision {
		return PublicRefreshReceipt{}, stateError("degraded", "public_activation_revision", "activation_revision_changed_during_run")
	}
	if rawRevision == ^uint64(0) {
		return PublicRefreshReceipt{}, stateError("degraded", "public_activation_revision", "activation_revision_exhausted")
	}
	activation := activationEvidence{
		Revision: rawRevision + 1, ActivationUUID: activationUUID, RunUUID: runUUID,
		FenceEpoch: lease.FenceEpoch, LeaseUUID: lease.LeaseUUID,
		WebRGenerationMS:       evidenceBySurface["webr"].GenerationMS,
		WebRMarkerUUID:         evidenceBySurface["webr"].MarkerUUID,
		MirGenerationMS:        evidenceBySurface["mirtype"].GenerationMS,
		MirMarkerUUID:          evidenceBySurface["mirtype"].MarkerUUID,
		StatgroundGenerationMS: evidenceBySurface["statground"].GenerationMS,
		StatgroundMarkerUUID:   evidenceBySurface["statground"].MarkerUUID,
		Kind:                   "publish",
	}
	var preparedTransition preparedPublicationTransition
	if readerRefreshEnabled {
		preparedTransition, err = s.preparePublicationTransitionReaders(
			ctx, topology, readerConfig, priorPointers, activation, initialAuthorityRevision,
		)
		if err != nil {
			return PublicRefreshReceipt{}, err
		}
	}
	failPrepared := func(cause error) (PublicRefreshReceipt, error) {
		if !readerRefreshEnabled {
			return PublicRefreshReceipt{}, cause
		}
		if abortErr := abortPreparedPublicationTransition(ctx, preparedTransition); abortErr != nil {
			return PublicRefreshReceipt{}, stateError("degraded", "public_reader_transition_abort", "preactivation_abort_failed")
		}
		return PublicRefreshReceipt{}, cause
	}
	if readerRefreshEnabled {
		if err := s.requireCurrentPublicationLease(ctx, lease, "public_activation_post_prepare_lease"); err != nil {
			return failPrepared(err)
		}
		postPreparePreflight, err := s.runPublicationCandidatePreflight(ctx, generations, lease)
		if err != nil {
			return failPrepared(err)
		}
		for _, surface := range expectedPublicationSurfaces {
			before, after := evidenceBySurface[surface], postPreparePreflight[surface]
			if after.SourceAuthorityRevision != initialAuthorityRevision || before.GenerationMS != after.GenerationMS ||
				before.Rows != after.Rows || before.Unique != after.Unique || before.FingerprintSum != after.FingerprintSum ||
				before.FingerprintXOR != after.FingerprintXOR || before.SourceFetchedMaxMS != after.SourceFetchedMaxMS {
				return failPrepared(stateError("degraded", "public_activation_post_prepare", "source_or_candidate_changed_during_prepare"))
			}
		}
		postPrepareAuthorityRevision, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_activation_post_prepare_authority")
		if err != nil {
			return failPrepared(err)
		}
		if postPrepareAuthorityRevision != initialAuthorityRevision {
			return failPrepared(stateError("degraded", "public_activation_post_prepare_authority", "source_authority_changed_during_prepare"))
		}
		postPrepareRawRevision, err := s.readRawActivationRevision(ctx, topology)
		if err != nil {
			return failPrepared(err)
		}
		if postPrepareRawRevision != rawRevision {
			return failPrepared(stateError("degraded", "public_activation_post_prepare_revision", "activation_revision_changed_during_prepare"))
		}
	}
	activatedMS, err := s.readServerTimeMS(ctx)
	if err != nil {
		return failPrepared(newUpdateReadStateError("public_activation_time", err))
	}
	if activatedMS < completedMS || activatedMS >= lease.ExpiresMS {
		return failPrepared(stateError("degraded", "public_activation_time", "activation_outside_lease"))
	}
	activation.ActivatedMS = activatedMS
	if err := s.insertAndReadbackActivation(ctx, topology, activation); err != nil {
		// An ambiguous activation write may already be visible. Never issue an
		// unsafe ABORT after that point; leave every reader closed for the next
		// reconciliation run.
		return PublicRefreshReceipt{}, err
	}
	postActivationAuthorityRevision, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_activation_source_authority_readback")
	if err != nil {
		return PublicRefreshReceipt{}, err
	}
	if postActivationAuthorityRevision != initialAuthorityRevision {
		return PublicRefreshReceipt{}, stateError("degraded", "public_activation_source_authority_readback", "source_authority_changed_during_activation")
	}
	if err := s.verifyActivatedPointer(ctx, activation); err != nil {
		return PublicRefreshReceipt{}, err
	}
	if readerRefreshEnabled {
		committedReaders, commitErr := s.commitPublicationTransitionReaders(ctx, preparedTransition, activation, initialAuthorityRevision)
		if commitErr != nil {
			return PublicRefreshReceipt{}, commitErr
		}
		if err := s.proveCommittedPublicationTransitionReaders(ctx, preparedTransition, committedReaders); err != nil {
			return PublicRefreshReceipt{}, err
		}
		finalInventory, err := s.proveLectureReaderInventory(ctx, topology, readerConfig)
		if err != nil || !sameLectureReaderInventory(finalInventory, preparedTransition.Inventory) {
			return PublicRefreshReceipt{}, stateError("degraded", "public_reader_transition_final_inventory", "reader_inventory_changed_after_commit")
		}
		finalAuthority, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_reader_transition_final_authority")
		if err != nil {
			return PublicRefreshReceipt{}, err
		}
		if finalAuthority != initialAuthorityRevision {
			return PublicRefreshReceipt{}, stateError("degraded", "public_reader_transition_final_authority", "source_authority_changed_after_commit")
		}
		if err := s.verifyActivatedPointer(ctx, activation); err != nil {
			return PublicRefreshReceipt{}, err
		}
		finalPreflight, err := s.runPublicationCandidatePreflight(ctx, generations, lease)
		if err != nil {
			return PublicRefreshReceipt{}, err
		}
		for _, surface := range expectedPublicationSurfaces {
			before, after := evidenceBySurface[surface], finalPreflight[surface]
			if after.SourceAuthorityRevision != initialAuthorityRevision || before.GenerationMS != after.GenerationMS ||
				before.Rows != after.Rows || before.Unique != after.Unique || before.FingerprintSum != after.FingerprintSum ||
				before.FingerprintXOR != after.FingerprintXOR || before.SourceFetchedMaxMS != after.SourceFetchedMaxMS {
				return PublicRefreshReceipt{}, stateError("degraded", "public_reader_transition_final_proof", "source_or_candidate_changed_after_commit")
			}
		}
		release, err := s.appendAndReadbackPublicationTransitionRelease(
			ctx, topology, preparedTransition, committedReaders, activation, initialAuthorityRevision,
		)
		if err != nil {
			return PublicRefreshReceipt{}, err
		}
		if err := s.finalizePublicationTransitionReaders(ctx, committedReaders, release); err != nil {
			return PublicRefreshReceipt{}, err
		}
	}
	receipt := PublicRefreshReceipt{RunUUID: runUUID, ActivationUUID: activationUUID, ActivationRevision: activation.Revision}
	printMachineJSON(map[string]any{
		"schema": "statground.inflearn.public_activation.v2", "status": "activated",
		"run_uuid": receipt.RunUUID, "activation_uuid": receipt.ActivationUUID,
		"activation_revision": receipt.ActivationRevision, "fence_epoch": lease.FenceEpoch,
	})
	return receipt, nil
}

func stateError(status, phase, category string) error {
	return &updateRunStateError{Status: status, Phase: phase, Category: category}
}

func (s *Service) validatePublicRefreshWriteSafety(ctx context.Context) error {
	if s.Cfg.CHDirectReplicaFallback || s.Cfg.CHDirectOutboxFallback {
		return stateError("degraded", "public_refresh_write_safety", "unsafe_write_fallback_enabled")
	}
	database := strings.TrimSpace(s.Cfg.CHOutboxDatabase)
	if database == "" {
		database = "Data_Lecture_Inflearn_Log"
	}
	table := strings.TrimSpace(s.Cfg.CHOutboxTable)
	if table == "" {
		table = "inflearn_direct_insert_outbox"
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT count() AS pending FROM %s.%s
		WHERE replayed_at IS NULL SETTINGS max_threads = 1, max_execution_time = 15`, chIdent(database), chIdent(table)))
	if err != nil {
		return newUpdateReadStateError("public_refresh_outbox_preflight", err)
	}
	if len(rows) != 1 {
		return stateError("degraded", "public_refresh_outbox_preflight", "unexpected_result")
	}
	if asInt(rows[0]["pending"]) > 0 {
		return stateError("deferred", "public_refresh_outbox_preflight", "pending_direct_outbox")
	}
	return nil
}

func (s *Service) readPublicationTopology(ctx context.Context) (map[string]int, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT shard_num, replica_num, host_name
		FROM system.clusters WHERE cluster = %s ORDER BY shard_num, replica_num
		SETTINGS max_threads = 1, max_execution_time = 15`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return nil, newUpdateReadStateError("public_refresh_topology", err)
	}
	topology := make(map[string]int, len(rows))
	shards := map[int]int{}
	replicasByShard := map[int]map[int]bool{}
	for _, row := range rows {
		host := strings.TrimSpace(asString(row["host_name"]))
		shard := asInt(row["shard_num"])
		replica := asInt(row["replica_num"])
		if replicasByShard[shard] == nil {
			replicasByShard[shard] = map[int]bool{}
		}
		if host == "" || shard <= 0 || replica <= 0 || topology[host] != 0 || replicasByShard[shard][replica] {
			return nil, stateError("degraded", "public_refresh_topology", "invalid_cluster_topology")
		}
		topology[host] = shard
		replicasByShard[shard][replica] = true
		shards[shard]++
	}
	if len(topology) != 4 || len(shards) != 2 {
		return nil, stateError("degraded", "public_refresh_topology", "unexpected_endpoint_count")
	}
	for _, replicas := range shards {
		if replicas != 2 {
			return nil, stateError("degraded", "public_refresh_topology", "unexpected_replica_count")
		}
	}
	return topology, nil
}

func (s *Service) acquirePublicationLease(ctx context.Context, topology map[string]int) (publicationLease, error) {
	current, err := s.readCurrentPublicationLease(ctx)
	if err != nil {
		return publicationLease{}, err
	}
	if current.LeaseUUID != "" {
		return publicationLease{}, stateError("deferred", "public_refresh_lease", "publication_lease_already_active")
	}
	maxEpoch, err := s.readRawLeaseEpoch(ctx, topology)
	if err != nil {
		return publicationLease{}, err
	}
	if maxEpoch == ^uint64(0) {
		return publicationLease{}, stateError("degraded", "public_refresh_lease", "lease_epoch_exhausted")
	}
	acquiredMS, err := s.readServerTimeMS(ctx)
	if err != nil {
		return publicationLease{}, newUpdateReadStateError("public_refresh_lease_time", err)
	}
	lease := publicationLease{
		FenceEpoch: maxEpoch + 1,
		LeaseUUID:  UUIDv7String(time.Now()),
		WriterID:   strings.TrimSpace(s.Cfg.PublicationWriterID),
		AcquiredMS: acquiredMS,
		ExpiresMS:  acquiredMS + publicationLeaseDurationMillis,
	}
	token := fmt.Sprintf("inflearn-public-lease-v2-%016x", H64(lease.LeaseUUID+"\x1f"+strconv.FormatUint(lease.FenceEpoch, 10)))
	sql := fmt.Sprintf(`INSERT INTO %s
		(fence_epoch, lease_uuid, writer_id, acquired_at, expires_at)
		SELECT toUInt64(%d), toUUID(%s), %s, fromUnixTimestamp64Milli(%d), fromUnixTimestamp64Milli(%d)
		SETTINGS distributed_foreground_insert = 1, insert_distributed_sync = 1, async_insert = 0,
		 insert_quorum = 4, insert_quorum_parallel = 0, insert_quorum_timeout = 60000,
		 insert_deduplicate = 1, insert_deduplication_token = %s`,
		lectureLeaseTable, lease.FenceEpoch, QuoteSQLString(lease.LeaseUUID), QuoteSQLString(lease.WriterID),
		lease.AcquiredMS, lease.ExpiresMS, QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt < publicationMutationReconcileAttempts; attempt++ {
		// Reissue the exact fenced operation after either an insert timeout or an
		// incomplete readback. The stable deduplication token makes this retry
		// idempotent while allowing a partially delivered Distributed insert to
		// converge on all four replicas.
		insertErr = s.execPublicRefreshCommand(ctx, sql, 2*time.Minute)
		if err := s.readbackPublicationLease(ctx, topology, lease); err == nil {
			if err := s.requireCurrentPublicationLease(ctx, lease, "public_refresh_lease_current"); err != nil {
				return publicationLease{}, err
			}
			return lease, nil
		} else if attempt+1 == publicationMutationReconcileAttempts {
			if insertErr != nil {
				return publicationLease{}, newUpdateReadStateError("public_refresh_lease_insert", insertErr)
			}
			return publicationLease{}, err
		}
	}
	return publicationLease{}, stateError("degraded", "public_refresh_lease", "lease_reconcile_failed")
}

func (s *Service) readRawLeaseEpoch(ctx context.Context, topology map[string]int) (uint64, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(maxOrDefault(fence_epoch)) AS raw_fence_epoch
		FROM clusterAllReplicas(%s, lecture_publication.inflearn_public_catalog_publish_lease_local)
		GROUP BY hostname ORDER BY hostname
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 30`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return 0, newUpdateReadStateError("public_refresh_lease_epoch", err)
	}
	if len(rows) != 4 {
		return 0, stateError("degraded", "public_refresh_lease_epoch", "lease_epoch_replica_count")
	}
	seen := map[string]bool{}
	var epoch uint64
	for i, row := range rows {
		host := asString(row["hostname"])
		value, parseErr := exactUint64(row["raw_fence_epoch"])
		if parseErr != nil || topology[host] == 0 || seen[host] {
			return 0, stateError("degraded", "public_refresh_lease_epoch", "lease_epoch_evidence_mismatch")
		}
		if i > 0 && value != epoch {
			return 0, stateError("degraded", "public_refresh_lease_epoch", "lease_epoch_replica_drift")
		}
		epoch = value
		seen[host] = true
	}
	return epoch, nil
}

func (s *Service) readbackPublicationLease(ctx context.Context, topology map[string]int, lease publicationLease) error {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(fence_epoch) AS fence_epoch, toString(lease_uuid) AS lease_uuid, writer_id,
		toUnixTimestamp64Milli(acquired_at) AS acquired_ms,
		toUnixTimestamp64Milli(expires_at) AS expires_ms
		FROM clusterAllReplicas(%s, lecture_publication.inflearn_public_catalog_publish_lease_local)
		WHERE fence_epoch = toUInt64(%d)
		ORDER BY hostname
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 30`,
		QuoteSQLString(s.Cfg.CHCluster), lease.FenceEpoch))
	if err != nil {
		return newUpdateReadStateError("public_refresh_lease_readback", err)
	}
	if len(rows) != 4 {
		return stateError("degraded", "public_refresh_lease_readback", "lease_replica_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		epoch, parseErr := exactUint64(row["fence_epoch"])
		if parseErr != nil || topology[host] == 0 || seen[host] || epoch != lease.FenceEpoch ||
			!strings.EqualFold(asString(row["lease_uuid"]), lease.LeaseUUID) || asString(row["writer_id"]) != lease.WriterID ||
			asInt64(row["acquired_ms"]) != lease.AcquiredMS || asInt64(row["expires_ms"]) != lease.ExpiresMS {
			return stateError("degraded", "public_refresh_lease_readback", "lease_evidence_mismatch")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) readCurrentPublicationLease(ctx context.Context) (publicationLease, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT toString(fence_epoch) AS fence_epoch,
		toString(lease_uuid) AS lease_uuid, writer_id,
		toUnixTimestamp64Milli(acquired_at) AS acquired_ms,
		toUnixTimestamp64Milli(expires_at) AS expires_ms
		FROM %s SETTINGS max_threads = 1, max_execution_time = 15`, lectureCurrentLeaseView))
	if err != nil {
		return publicationLease{}, newUpdateReadStateError("public_refresh_lease_current", err)
	}
	if len(rows) == 0 {
		return publicationLease{}, nil
	}
	if len(rows) != 1 {
		return publicationLease{}, stateError("degraded", "public_refresh_lease_current", "conflicting_current_lease")
	}
	epoch, parseErr := exactUint64(rows[0]["fence_epoch"])
	lease := publicationLease{
		FenceEpoch: epoch, LeaseUUID: strings.ToLower(asString(rows[0]["lease_uuid"])),
		WriterID: asString(rows[0]["writer_id"]), AcquiredMS: asInt64(rows[0]["acquired_ms"]),
		ExpiresMS: asInt64(rows[0]["expires_ms"]),
	}
	if parseErr != nil || !isCanonicalUUID(lease.LeaseUUID) || lease.WriterID == "" ||
		lease.AcquiredMS <= 0 || lease.ExpiresMS <= lease.AcquiredMS {
		return publicationLease{}, stateError("degraded", "public_refresh_lease_current", "invalid_current_lease")
	}
	return lease, nil
}

func (s *Service) requireCurrentPublicationLease(ctx context.Context, expected publicationLease, phase string) error {
	current, err := s.readCurrentPublicationLease(ctx)
	if err != nil {
		return err
	}
	if current.FenceEpoch != expected.FenceEpoch || !strings.EqualFold(current.LeaseUUID, expected.LeaseUUID) ||
		current.WriterID != expected.WriterID || current.AcquiredMS != expected.AcquiredMS || current.ExpiresMS != expected.ExpiresMS {
		return stateError("deferred", phase, "publication_lease_lost")
	}
	return nil
}

func exactRefreshStates(states []publicRefreshState, phase string) (map[string]publicRefreshState, error) {
	if len(states) != len(publicLectureRefreshViews) {
		return nil, stateError("degraded", phase, "refresh_coordinator_unavailable")
	}
	out := make(map[string]publicRefreshState, len(states))
	for _, state := range states {
		key := state.Database + "." + state.View
		if _, exists := out[key]; exists {
			return nil, stateError("degraded", phase, "duplicate_refresh_state")
		}
		out[key] = state
	}
	for _, view := range publicLectureRefreshViews {
		if _, ok := out[view.qualifiedName()]; !ok {
			return nil, stateError("degraded", phase, "refresh_coordinator_unavailable")
		}
	}
	return out, nil
}

func validateIdlePublicRefreshState(state publicRefreshState, phase string) error {
	if state.ExceptionPresent {
		return stateError("degraded", phase, "refresh_exception")
	}
	switch strings.TrimSpace(state.Status) {
	case "Scheduled":
		return nil
	case "Running", "RunningOnAnotherReplica", "Scheduling", "WaitingForDependencies":
		return stateError("deferred", phase, "refresh_already_running")
	case "", "Disabled":
		return stateError("degraded", phase, "refresh_schedule_inactive")
	default:
		return stateError("degraded", phase, "unexpected_refresh_status")
	}
}

func validateAdvancedPublicRefreshState(pre, post publicRefreshState, phase string) error {
	if err := validateIdlePublicRefreshState(post, phase); err != nil {
		return err
	}
	if post.LastSuccessMS <= pre.LastSuccessMS || post.LastRefreshMS <= pre.LastRefreshMS {
		return stateError("degraded", phase, "refresh_timestamp_not_advanced")
	}
	if post.LastSuccessMS <= 0 || post.LastRefreshMS < post.LastSuccessMS {
		return stateError("degraded", phase, "invalid_refresh_interval")
	}
	if post.LastRefreshMS-post.LastSuccessMS > maxPublicRefreshMillis {
		return stateError("degraded", phase, "refresh_duration_over_90m")
	}
	return nil
}

func (s *Service) readPublicRefreshStates(ctx context.Context, exact *publicLectureRefreshView) ([]publicRefreshState, error) {
	filter := `(database, view) IN (('webr_lecture', 'mv_inflearn_r_lecture_catalog_refresh'),
		('mirtype_lecture', 'mv_inflearn_language_lecture_catalog_refresh'))`
	if exact != nil {
		filter = fmt.Sprintf("database = %s AND view = %s", QuoteSQLString(exact.Database), QuoteSQLString(exact.View))
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT database, view, status,
		toInt64(ifNull(toUnixTimestamp(last_success_time), 0)) * 1000 AS last_success_ms,
		toInt64(ifNull(toUnixTimestamp(last_refresh_time), 0)) * 1000 AS last_refresh_ms,
		toString(read_rows) AS read_rows, toString(written_rows) AS written_rows,
		toUInt8(notEmpty(ifNull(exception, ''))) AS exception_present,
		toUnixTimestamp64Milli(now64(3, 'Asia/Seoul')) AS observed_ms
		FROM system.view_refreshes WHERE %s ORDER BY database, view`, filter))
	if err != nil {
		return nil, err
	}
	states := make([]publicRefreshState, 0, len(rows))
	for _, row := range rows {
		readRows, readErr := exactUint64(row["read_rows"])
		writtenRows, writeErr := exactUint64(row["written_rows"])
		if readErr != nil || writeErr != nil {
			return nil, fmt.Errorf("refresh row counter encoding")
		}
		states = append(states, publicRefreshState{
			Database: asString(row["database"]), View: asString(row["view"]), Status: asString(row["status"]),
			LastSuccessMS: asInt64(row["last_success_ms"]), LastRefreshMS: asInt64(row["last_refresh_ms"]),
			ReadRows: readRows, WrittenRows: writtenRows, ExceptionPresent: asInt(row["exception_present"]) != 0,
			ObservedMS: asInt64(row["observed_ms"]),
		})
	}
	return states, nil
}

func (s *Service) readPublicationPointer(ctx context.Context, phase string) (map[string]publicationPointer, uint64, error) {
	rows, err := s.CHQueryRows(ctx, `SELECT surface,
		toUnixTimestamp64Milli(generation) AS generation_ms, toString(marker_uuid) AS marker_uuid,
		toString(run_uuid) AS run_uuid, toString(target_row_count) AS target_row_count,
		toString(target_logical_key_count) AS target_logical_key_count,
		toString(target_fingerprint_sum) AS target_fingerprint_sum,
		toString(target_fingerprint_xor) AS target_fingerprint_xor,
		toString(row_count) AS row_count, toString(logical_key_count) AS logical_key_count,
		toString(fingerprint_sum) AS fingerprint_sum, toString(fingerprint_xor) AS fingerprint_xor,
		toUnixTimestamp64Milli(source_fetched_max) AS source_fetched_max_ms,
		toUnixTimestamp64Milli(refresh_success_time) AS refresh_success_ms,
		toUnixTimestamp64Milli(refresh_end_time) AS refresh_end_ms,
		toString(activation_revision) AS activation_revision, toString(activation_uuid) AS activation_uuid,
		toString(activation_kind) AS activation_kind
		FROM lecture_publication.v_inflearn_public_catalog_generation_latest ORDER BY surface`)
	if err != nil {
		return nil, 0, newUpdateReadStateError(phase, err)
	}
	if len(rows) == 0 {
		return map[string]publicationPointer{}, 0, nil
	}
	if len(rows) != 3 {
		return nil, 0, stateError("degraded", phase, "incomplete_activation_pointer")
	}
	out := make(map[string]publicationPointer, 3)
	var revision uint64
	var activationUUID, runUUID, activationKind string
	for _, row := range rows {
		integerKeys := []string{"activation_revision", "target_row_count", "target_logical_key_count", "target_fingerprint_sum", "target_fingerprint_xor", "row_count", "logical_key_count", "fingerprint_sum", "fingerprint_xor"}
		values := make(map[string]uint64, len(integerKeys))
		for _, key := range integerKeys {
			value, parseErr := exactUint64(row[key])
			if parseErr != nil {
				return nil, 0, stateError("degraded", phase, "pointer_integer_encoding")
			}
			values[key] = value
		}
		item := publicationPointer{
			Surface: asString(row["surface"]), GenerationMS: asInt64(row["generation_ms"]),
			MarkerUUID: strings.ToLower(asString(row["marker_uuid"])), RunUUID: strings.ToLower(asString(row["run_uuid"])),
			TargetRows: values["target_row_count"], TargetUnique: values["target_logical_key_count"],
			TargetFingerprintSum: values["target_fingerprint_sum"], TargetFingerprintXOR: values["target_fingerprint_xor"],
			Rows: values["row_count"], Unique: values["logical_key_count"], FingerprintSum: values["fingerprint_sum"], FingerprintXOR: values["fingerprint_xor"],
			SourceFetchedMaxMS: asInt64(row["source_fetched_max_ms"]), RefreshSuccessMS: asInt64(row["refresh_success_ms"]),
			RefreshEndMS: asInt64(row["refresh_end_ms"]), ActivationRevision: values["activation_revision"],
			ActivationUUID: strings.ToLower(asString(row["activation_uuid"])), ActivationKind: asString(row["activation_kind"]),
		}
		if _, exists := out[item.Surface]; exists || !containsString(expectedPublicationSurfaces, item.Surface) ||
			item.GenerationMS <= 0 || !isCanonicalUUID(item.MarkerUUID) || !isCanonicalUUID(item.RunUUID) ||
			item.TargetRows == 0 || item.TargetRows != item.TargetUnique || item.Rows == 0 || item.Rows != item.Unique ||
			item.SourceFetchedMaxMS <= 0 || item.RefreshSuccessMS <= 0 || item.RefreshEndMS < item.RefreshSuccessMS ||
			!isCanonicalUUID(item.ActivationUUID) || (item.ActivationKind != "publish" && item.ActivationKind != "rollback") {
			return nil, 0, stateError("degraded", phase, "conflicting_activation_pointer")
		}
		if len(out) == 0 {
			revision, activationUUID, runUUID, activationKind = item.ActivationRevision, item.ActivationUUID, item.RunUUID, item.ActivationKind
		} else if item.ActivationRevision != revision || item.ActivationUUID != activationUUID || item.RunUUID != runUUID || item.ActivationKind != activationKind {
			return nil, 0, stateError("degraded", phase, "conflicting_activation_pointer")
		}
		out[item.Surface] = item
	}
	if len(out) != 3 {
		return nil, 0, stateError("degraded", phase, "incomplete_activation_pointer")
	}
	return out, revision, nil
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func (s *Service) readRawActivationRevision(ctx context.Context, topology map[string]int) (uint64, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(maxOrDefault(activation_revision)) AS raw_revision
		FROM clusterAllReplicas(%s, lecture_publication.inflearn_public_catalog_activation_local)
		GROUP BY hostname ORDER BY hostname
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 30`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return 0, newUpdateReadStateError("public_activation_revision", err)
	}
	if len(rows) != 4 {
		return 0, stateError("degraded", "public_activation_revision", "activation_revision_replica_count")
	}
	seen := map[string]bool{}
	var revision uint64
	for i, row := range rows {
		host := asString(row["hostname"])
		value, parseErr := exactUint64(row["raw_revision"])
		if parseErr != nil || topology[host] == 0 || seen[host] {
			return 0, stateError("degraded", "public_activation_revision", "activation_revision_evidence_mismatch")
		}
		if i > 0 && value != revision {
			return 0, stateError("degraded", "public_activation_revision", "activation_revision_replica_drift")
		}
		revision = value
		seen[host] = true
	}
	return revision, nil
}

func (s *Service) validateCandidateGeneration(ctx context.Context, topology map[string]int, surface string, refresh publicRefreshState, prior publicationPointer) (generationEvidence, error) {
	phase := "public_candidate_" + surface
	where := fmt.Sprintf("surface = %s AND generation >= fromUnixTimestamp64Milli(%d) AND generation < fromUnixTimestamp64Milli(%d)",
		QuoteSQLString(surface), refresh.LastSuccessMS, refresh.LastRefreshMS+1000)
	replicaRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(count()) AS rows, toString(uniqExact(logical_key)) AS unique_keys,
		toString(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toString(groupBitXor(row_fingerprint)) AS fingerprint_xor,
		toString(uniqExact(generation)) AS generation_count,
		toUnixTimestamp64Milli(min(generation)) AS min_generation_ms,
		toUnixTimestamp64Milli(max(generation)) AS max_generation_ms
		FROM clusterAllReplicas(%s, lecture_publication.v_inflearn_public_catalog_candidate_local)
		WHERE %s GROUP BY hostname ORDER BY hostname
		SETTINGS skip_unavailable_shards = 0, max_threads = 2, max_execution_time = 120`, QuoteSQLString(s.Cfg.CHCluster), where))
	if err != nil {
		return generationEvidence{}, newUpdateReadStateError(phase+"_replicas", err)
	}
	if len(replicaRows) != 4 {
		return generationEvidence{}, stateError("degraded", phase, "partial_replica_set")
	}
	byShard := map[int][]candidateReplicaStats{}
	seenHosts := map[string]bool{}
	for _, row := range replicaRows {
		host := asString(row["hostname"])
		shard := topology[host]
		if shard == 0 || seenHosts[host] {
			return generationEvidence{}, stateError("degraded", phase, "candidate_endpoint_mismatch")
		}
		seenHosts[host] = true
		stats, parseErr := candidateStatsFromRow(row)
		if parseErr != nil {
			return generationEvidence{}, stateError("degraded", phase, "candidate_integer_encoding")
		}
		byShard[shard] = append(byShard[shard], candidateReplicaStats{Host: host, Shard: shard, candidateStats: stats})
	}
	var representative candidateStats
	firstShard := true
	for _, replicas := range byShard {
		if len(replicas) != 2 || !sameCandidateStats(replicas[0].candidateStats, replicas[1].candidateStats) {
			return generationEvidence{}, stateError("degraded", phase, "same_shard_replica_drift")
		}
		stats := replicas[0].candidateStats
		if stats.Rows == 0 || stats.Rows != stats.Unique || stats.GenerationCount != 1 {
			return generationEvidence{}, stateError("degraded", phase, "invalid_candidate_shard")
		}
		if firstShard {
			representative.GenerationMS, representative.GenerationCount, firstShard = stats.GenerationMS, 1, false
		} else if stats.GenerationMS != representative.GenerationMS {
			return generationEvidence{}, stateError("degraded", phase, "multiple_candidate_generations")
		}
		representative.Rows += stats.Rows
		representative.Unique += stats.Unique
		representative.FingerprintSum += stats.FingerprintSum
		representative.FingerprintXOR ^= stats.FingerprintXOR
	}
	if len(byShard) != 2 {
		return generationEvidence{}, stateError("degraded", phase, "partial_shard_set")
	}
	globalRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT toString(count()) AS rows,
		toString(uniqExact(logical_key)) AS unique_keys,
		toString(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toString(groupBitXor(row_fingerprint)) AS fingerprint_xor,
		toString(uniqExact(generation)) AS generation_count,
		toUnixTimestamp64Milli(min(generation)) AS min_generation_ms,
		toUnixTimestamp64Milli(max(generation)) AS max_generation_ms
		FROM %s WHERE %s SETTINGS skip_unavailable_shards = 0, max_threads = 2, max_execution_time = 120`, lectureCandidateView, where))
	if err != nil {
		return generationEvidence{}, newUpdateReadStateError(phase+"_global", err)
	}
	if len(globalRows) != 1 {
		return generationEvidence{}, stateError("degraded", phase, "unexpected_global_result")
	}
	global, parseErr := candidateStatsFromRow(globalRows[0])
	if parseErr != nil || global.Rows == 0 || global.Rows != global.Unique || global.GenerationCount != 1 || !sameCandidateStats(representative, global) {
		return generationEvidence{}, stateError("degraded", phase, "representative_global_parity")
	}
	if global.GenerationMS < refresh.LastSuccessMS || global.GenerationMS >= refresh.LastRefreshMS+1000 {
		return generationEvidence{}, stateError("degraded", phase, "candidate_outside_refresh_interval")
	}
	projection, sourceMaxMS, err := s.readProjectionCandidate(ctx, surface, global.GenerationMS)
	if err != nil {
		return generationEvidence{}, err
	}
	if projection.Rows == 0 || projection.Rows != projection.Unique || projection.GenerationCount != 1 || projection.GenerationMS != global.GenerationMS {
		return generationEvidence{}, stateError("degraded", phase, "invalid_projection_candidate")
	}
	if sourceMaxMS <= 0 || refresh.ObservedMS < sourceMaxMS || refresh.ObservedMS-sourceMaxMS > publicationSourceFreshnessMillis {
		return generationEvidence{}, stateError("degraded", phase, "stale_provider_authority")
	}
	if prior.Surface != "" && (global.GenerationMS <= prior.GenerationMS || refresh.LastRefreshMS <= prior.RefreshEndMS || sourceMaxMS <= prior.SourceFetchedMaxMS) {
		return generationEvidence{}, stateError("degraded", phase, "candidate_not_strictly_advanced")
	}
	return generationEvidence{
		Surface: surface, GenerationMS: global.GenerationMS,
		TargetRows: global.Rows, TargetUnique: global.Unique,
		TargetFingerprintSum: global.FingerprintSum, TargetFingerprintXOR: global.FingerprintXOR,
		Rows: projection.Rows, Unique: projection.Unique,
		FingerprintSum: projection.FingerprintSum, FingerprintXOR: projection.FingerprintXOR,
		SourceFetchedMaxMS: sourceMaxMS, RefreshReadRows: refresh.ReadRows,
		RefreshWriteRows: refresh.WrittenRows, RefreshSuccessMS: refresh.LastSuccessMS,
		RefreshEndMS: refresh.LastRefreshMS,
	}, nil
}

func (s *Service) readProjectionCandidate(ctx context.Context, surface string, generationMS int64) (candidateStats, int64, error) {
	phase := "public_projection_" + surface
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT toString(count()) AS rows,
		toString(uniqExact(logical_key)) AS unique_keys,
		toString(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toString(groupBitXor(row_fingerprint)) AS fingerprint_xor,
		toString(uniqExact(generation)) AS generation_count,
		toUnixTimestamp64Milli(min(generation)) AS min_generation_ms,
		toUnixTimestamp64Milli(max(generation)) AS max_generation_ms,
		toUnixTimestamp64Milli(max(source_fetched_at)) AS source_fetched_max_ms
		FROM %s WHERE surface = %s AND generation = fromUnixTimestamp64Milli(%d)
		SETTINGS skip_unavailable_shards = 0, max_threads = 2, max_execution_time = 120`,
		lectureProjectionCandidateView, QuoteSQLString(surface), generationMS))
	if err != nil {
		return candidateStats{}, 0, newUpdateReadStateError(phase, err)
	}
	if len(rows) != 1 {
		return candidateStats{}, 0, stateError("degraded", phase, "unexpected_result")
	}
	stats, parseErr := candidateStatsFromRow(rows[0])
	if parseErr != nil {
		return candidateStats{}, 0, stateError("degraded", phase, "candidate_integer_encoding")
	}
	return stats, asInt64(rows[0]["source_fetched_max_ms"]), nil
}

func (s *Service) validateStatgroundProjectionCandidate(ctx context.Context, generationMS int64, prior publicationPointer) (generationEvidence, error) {
	phase := "public_candidate_statground"
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT toString(count()) AS rows,
		toString(uniqExact(logical_key)) AS unique_keys,
		toString(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toString(groupBitXor(row_fingerprint)) AS fingerprint_xor,
		toUnixTimestamp64Milli(max(source_fetched_at)) AS source_fetched_max_ms,
		toUnixTimestamp64Milli(now64(3, 'Asia/Seoul')) AS observed_ms
		FROM %s SETTINGS skip_unavailable_shards = 0, max_threads = 2, max_execution_time = 120`, lectureStatgroundProjectionView))
	if err != nil {
		return generationEvidence{}, newUpdateReadStateError(phase, err)
	}
	if len(rows) != 1 {
		return generationEvidence{}, stateError("degraded", phase, "unexpected_result")
	}
	values := []string{"rows", "unique_keys", "fingerprint_sum", "fingerprint_xor"}
	parsed := map[string]uint64{}
	for _, key := range values {
		value, parseErr := exactUint64(rows[0][key])
		if parseErr != nil {
			return generationEvidence{}, stateError("degraded", phase, "candidate_integer_encoding")
		}
		parsed[key] = value
	}
	sourceMaxMS, observedMS := asInt64(rows[0]["source_fetched_max_ms"]), asInt64(rows[0]["observed_ms"])
	if parsed["rows"] == 0 || parsed["rows"] != parsed["unique_keys"] || sourceMaxMS <= 0 || observedMS < sourceMaxMS || observedMS-sourceMaxMS > publicationSourceFreshnessMillis {
		return generationEvidence{}, stateError("degraded", phase, "stale_or_incomplete_projection")
	}
	if prior.Surface != "" && (generationMS <= prior.GenerationMS || sourceMaxMS <= prior.SourceFetchedMaxMS) {
		return generationEvidence{}, stateError("degraded", phase, "candidate_not_strictly_advanced")
	}
	return generationEvidence{
		Surface: "statground", GenerationMS: generationMS,
		Rows: parsed["rows"], Unique: parsed["unique_keys"],
		FingerprintSum: parsed["fingerprint_sum"], FingerprintXOR: parsed["fingerprint_xor"],
		SourceFetchedMaxMS: sourceMaxMS, RefreshSuccessMS: generationMS,
	}, nil
}

func (s *Service) insertAndReadbackSnapshot(ctx context.Context, topology map[string]int, evidence generationEvidence, runUUID string) error {
	phase := "public_snapshot_" + evidence.Surface
	var sql string
	switch evidence.Surface {
	case "webr":
		sql = fmt.Sprintf(`INSERT INTO webr_lecture.inflearn_r_lecture_catalog_serving
			SELECT * FROM webr_lecture.v_inflearn_r_lecture_catalog_projection
			WHERE refresh_batch = fromUnixTimestamp64Milli(%d)`, evidence.GenerationMS)
	case "mirtype":
		sql = fmt.Sprintf(`INSERT INTO mirtype_lecture.inflearn_language_lecture_catalog_serving
			SELECT * FROM mirtype_lecture.v_inflearn_language_lecture_catalog_projection
			WHERE refresh_batch = fromUnixTimestamp64Milli(%d)`, evidence.GenerationMS)
	case "statground":
		sql = fmt.Sprintf(`INSERT INTO statground_lecture.inflearn_workbench_catalog_serving
			SELECT course_id,locale,display_language,language_rank,slug,en_slug,status,title,
			description,thumbnail_url,category_main_slug,category_main_title,category_sub_slug,
			category_sub_title,level_code,is_new,is_best,student_count,like_count,review_count,
			average_star,lecture_unit_count,preview_unit_count,runtime_sec,provides_certificate,
			provides_instructor_answer,provides_inquiry,published_at,last_updated_at,fetched_at,
			latest_activity_at,keywords,krw_regular_price,krw_pay_price,discount_rate,discount_title,
			price_known,fromUnixTimestamp64Milli(%d) AS refresh_batch
			FROM statground_lecture.v_inflearn_workbench_catalog_projection`, evidence.GenerationMS)
	default:
		return stateError("degraded", phase, "unknown_surface")
	}
	token := fmt.Sprintf("inflearn-public-snapshot-v2-%016x", H64(runUUID+"\x1f"+evidence.Surface+"\x1f"+strconv.FormatInt(evidence.GenerationMS, 10)))
	sql += fmt.Sprintf(` SETTINGS distributed_foreground_insert = 1, insert_distributed_sync = 1, async_insert = 0,
		insert_quorum = 4, insert_quorum_parallel = 0, insert_quorum_timeout = 60000,
		insert_deduplicate = 1, insert_deduplication_token = %s`, QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt < publicationMutationReconcileAttempts; attempt++ {
		insertErr = s.execPublicRefreshCommand(ctx, sql, 5*time.Minute)
		if err := s.readbackSnapshot(ctx, topology, evidence); err == nil {
			return nil
		} else if attempt+1 == publicationMutationReconcileAttempts {
			if insertErr != nil {
				return newUpdateReadStateError(phase+"_insert", insertErr)
			}
			return err
		}
	}
	return stateError("degraded", phase, "snapshot_reconcile_failed")
}

func (s *Service) readbackSnapshot(ctx context.Context, topology map[string]int, evidence generationEvidence) error {
	phase := "public_snapshot_" + evidence.Surface + "_readback"
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname, surface,
		toString(count()) AS rows, toString(uniqExact(logical_key)) AS unique_keys,
		toString(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toString(groupBitXor(row_fingerprint)) AS fingerprint_xor,
		toString(uniqExact(generation)) AS generation_count,
		toUnixTimestamp64Milli(min(generation)) AS min_generation_ms,
		toUnixTimestamp64Milli(max(generation)) AS max_generation_ms
		FROM clusterAllReplicas(%s, lecture_publication.v_inflearn_public_catalog_projection_snapshot_local)
		WHERE surface = %s AND generation = fromUnixTimestamp64Milli(%d)
		GROUP BY hostname, surface ORDER BY hostname
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 120`,
		QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(evidence.Surface), evidence.GenerationMS))
	if err != nil {
		return newUpdateReadStateError(phase, err)
	}
	if len(rows) != 4 {
		return stateError("degraded", phase, "snapshot_replica_count")
	}
	seen := map[string]bool{}
	want := candidateStats{Rows: evidence.Rows, Unique: evidence.Unique, FingerprintSum: evidence.FingerprintSum,
		FingerprintXOR: evidence.FingerprintXOR, GenerationCount: 1, GenerationMS: evidence.GenerationMS}
	for _, row := range rows {
		host := asString(row["hostname"])
		stats, parseErr := candidateStatsFromRow(row)
		if parseErr != nil || topology[host] == 0 || seen[host] || asString(row["surface"]) != evidence.Surface || !sameCandidateStats(stats, want) {
			return stateError("degraded", phase, "snapshot_evidence_mismatch")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) runPublicationCandidatePreflight(ctx context.Context, generations map[string]int64, lease publicationLease) (map[string]generationEvidence, error) {
	if err := s.requireCurrentPublicationLease(ctx, lease, "public_candidate_preflight_lease"); err != nil {
		return nil, err
	}
	rows, err := s.CHQueryRows(ctx, renderPublicationCandidatePreflightSQL(generations, lease.FenceEpoch, lease.LeaseUUID))
	if err != nil {
		return nil, newUpdateReadStateError("public_candidate_preflight", err)
	}
	if len(rows) != 3 {
		return nil, stateError("degraded", "public_candidate_preflight", "surface_count")
	}
	out := make(map[string]generationEvidence, 3)
	for _, row := range rows {
		surface := asString(row["surface"])
		if !containsString(expectedPublicationSurfaces, surface) || out[surface].Surface != "" {
			return nil, stateError("degraded", "public_candidate_preflight", "surface_identity")
		}
		integerKeys := []string{"target_row_count", "target_logical_key_count", "target_fingerprint_sum", "target_fingerprint_xor", "row_count", "logical_key_count", "fingerprint_sum", "fingerprint_xor"}
		values := map[string]uint64{}
		for _, key := range integerKeys {
			value, parseErr := exactUint64(row[key])
			if parseErr != nil {
				return nil, stateError("degraded", "public_candidate_preflight", "integer_encoding")
			}
			values[key] = value
		}
		authorityRevision, authorityErr := exactUint64(row["authority_revision"])
		if authorityErr != nil || authorityRevision == 0 {
			return nil, stateError("degraded", "public_candidate_preflight", "source_authority_revision_encoding")
		}
		generationMS, timeErr := clickHouseDateTimeMS(row["generation"])
		sourceMaxMS, sourceErr := clickHouseDateTimeMS(row["source_fetched_max"])
		if timeErr != nil {
			return nil, stateError("degraded", "public_candidate_preflight", "generation_timestamp_encoding")
		}
		if sourceErr != nil {
			return nil, stateError("degraded", "public_candidate_preflight", "source_timestamp_encoding")
		}
		if generationMS != generations[surface] {
			return nil, stateError("degraded", "public_candidate_preflight", "generation_mismatch")
		}
		if values["target_row_count"] == 0 || values["target_row_count"] != values["target_logical_key_count"] ||
			values["row_count"] == 0 || values["row_count"] != values["logical_key_count"] {
			return nil, stateError("degraded", "public_candidate_preflight", "evidence_mismatch")
		}
		out[surface] = generationEvidence{
			Surface: surface, GenerationMS: generationMS,
			TargetRows: values["target_row_count"], TargetUnique: values["target_logical_key_count"],
			TargetFingerprintSum: values["target_fingerprint_sum"], TargetFingerprintXOR: values["target_fingerprint_xor"],
			Rows: values["row_count"], Unique: values["logical_key_count"],
			FingerprintSum: values["fingerprint_sum"], FingerprintXOR: values["fingerprint_xor"],
			SourceFetchedMaxMS: sourceMaxMS, SourceAuthorityRevision: authorityRevision,
		}
	}
	return out, nil
}

func clickHouseDateTimeMS(value any) (int64, error) {
	if raw := strings.TrimSpace(asString(value)); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms > 0 {
			return ms, nil
		}
		if parsed, ok := ParseDT64(raw); ok {
			return parsed.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("invalid DateTime64")
}

func candidateStatsFromRow(row map[string]any) (candidateStats, error) {
	rows, e1 := exactUint64(row["rows"])
	unique, e2 := exactUint64(row["unique_keys"])
	sum, e3 := exactUint64(row["fingerprint_sum"])
	xor, e4 := exactUint64(row["fingerprint_xor"])
	generations, e5 := exactUint64(row["generation_count"])
	minGeneration, maxGeneration := asInt64(row["min_generation_ms"]), asInt64(row["max_generation_ms"])
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || minGeneration <= 0 || minGeneration != maxGeneration {
		return candidateStats{}, fmt.Errorf("candidate stats encoding")
	}
	return candidateStats{Rows: rows, Unique: unique, FingerprintSum: sum, FingerprintXOR: xor, GenerationCount: generations, GenerationMS: minGeneration}, nil
}

func sameCandidateStats(a, b candidateStats) bool { return a == b }

func exactUint64(value any) (uint64, error) {
	raw := strings.TrimSpace(asString(value))
	if raw == "" {
		return 0, fmt.Errorf("missing uint64")
	}
	return strconv.ParseUint(raw, 10, 64)
}

func (s *Service) readServerTimeMS(ctx context.Context) (int64, error) {
	rows, err := s.CHQueryRows(ctx, `SELECT toUnixTimestamp64Milli(now64(3, 'Asia/Seoul')) AS server_time_ms`)
	if err != nil || len(rows) != 1 || asInt64(rows[0]["server_time_ms"]) <= 0 {
		if err == nil {
			err = fmt.Errorf("invalid server time")
		}
		return 0, err
	}
	return asInt64(rows[0]["server_time_ms"]), nil
}

func (s *Service) insertAndReadbackGeneration(ctx context.Context, topology map[string]int, evidence generationEvidence) error {
	phase := "public_marker_" + evidence.Surface
	token := fmt.Sprintf("inflearn-public-marker-v2-%016x", H64(evidence.RunUUID+"\x1f"+evidence.Surface+"\x1f"+evidence.MarkerUUID))
	sql := fmt.Sprintf(`INSERT INTO %s
		(surface, generation, marker_uuid, run_uuid, fence_epoch, lease_uuid,
		 target_row_count, target_logical_key_count, target_fingerprint_sum, target_fingerprint_xor,
		 row_count, logical_key_count, fingerprint_sum, fingerprint_xor, source_fetched_max,
		 refresh_read_rows, refresh_written_rows, refresh_success_time, refresh_end_time, completed_at)
		SELECT %s, fromUnixTimestamp64Milli(%d), toUUID(%s), toUUID(%s), toUInt64(%d), toUUID(%s),
		 toUInt64(%d), toUInt64(%d), toUInt64(%d), toUInt64(%d),
		 toUInt64(%d), toUInt64(%d), toUInt64(%d), toUInt64(%d), fromUnixTimestamp64Milli(%d),
		 toUInt64(%d), toUInt64(%d), fromUnixTimestamp64Milli(%d), fromUnixTimestamp64Milli(%d), fromUnixTimestamp64Milli(%d)
		SETTINGS distributed_foreground_insert = 1, insert_distributed_sync = 1, async_insert = 0,
		 insert_quorum = 4, insert_quorum_parallel = 0, insert_quorum_timeout = 60000,
		 insert_deduplicate = 1, insert_deduplication_token = %s`, lectureMarkerTable,
		QuoteSQLString(evidence.Surface), evidence.GenerationMS, QuoteSQLString(evidence.MarkerUUID), QuoteSQLString(evidence.RunUUID),
		evidence.FenceEpoch, QuoteSQLString(evidence.LeaseUUID), evidence.TargetRows, evidence.TargetUnique,
		evidence.TargetFingerprintSum, evidence.TargetFingerprintXOR, evidence.Rows, evidence.Unique,
		evidence.FingerprintSum, evidence.FingerprintXOR, evidence.SourceFetchedMaxMS,
		evidence.RefreshReadRows, evidence.RefreshWriteRows, evidence.RefreshSuccessMS, evidence.RefreshEndMS, evidence.CompletedMS,
		QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt < publicationMutationReconcileAttempts; attempt++ {
		insertErr = s.execPublicRefreshCommand(ctx, sql, 2*time.Minute)
		if err := s.readbackGeneration(ctx, topology, evidence); err == nil {
			return nil
		} else if attempt+1 == publicationMutationReconcileAttempts {
			if insertErr != nil {
				return newUpdateReadStateError(phase+"_insert", insertErr)
			}
			return err
		}
	}
	return stateError("degraded", phase, "marker_reconcile_failed")
}

func (s *Service) readbackGeneration(ctx context.Context, topology map[string]int, evidence generationEvidence) error {
	phase := "public_marker_" + evidence.Surface + "_readback"
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname, surface,
		toUnixTimestamp64Milli(generation) AS generation_ms, toString(marker_uuid) AS marker_uuid,
		toString(run_uuid) AS run_uuid, toString(fence_epoch) AS fence_epoch, toString(lease_uuid) AS lease_uuid,
		toString(target_row_count) AS target_row_count, toString(target_logical_key_count) AS target_logical_key_count,
		toString(target_fingerprint_sum) AS target_fingerprint_sum, toString(target_fingerprint_xor) AS target_fingerprint_xor,
		toString(row_count) AS row_count, toString(logical_key_count) AS logical_key_count,
		toString(fingerprint_sum) AS fingerprint_sum, toString(fingerprint_xor) AS fingerprint_xor,
		toUnixTimestamp64Milli(source_fetched_max) AS source_fetched_max_ms,
		toString(refresh_read_rows) AS refresh_read_rows, toString(refresh_written_rows) AS refresh_written_rows,
		toUnixTimestamp64Milli(refresh_success_time) AS refresh_success_ms,
		toUnixTimestamp64Milli(refresh_end_time) AS refresh_end_ms,
		toUnixTimestamp64Milli(completed_at) AS completed_ms
		FROM clusterAllReplicas(%s, lecture_publication.inflearn_public_catalog_generation_local)
		WHERE marker_uuid = toUUID(%s) AND run_uuid = toUUID(%s)
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 30`,
		QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(evidence.MarkerUUID), QuoteSQLString(evidence.RunUUID)))
	if err != nil {
		return newUpdateReadStateError(phase, err)
	}
	if len(rows) != 4 {
		return stateError("degraded", phase, "marker_replica_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		values := []struct {
			key  string
			want uint64
		}{
			{"fence_epoch", evidence.FenceEpoch}, {"target_row_count", evidence.TargetRows},
			{"target_logical_key_count", evidence.TargetUnique}, {"target_fingerprint_sum", evidence.TargetFingerprintSum},
			{"target_fingerprint_xor", evidence.TargetFingerprintXOR}, {"row_count", evidence.Rows},
			{"logical_key_count", evidence.Unique}, {"fingerprint_sum", evidence.FingerprintSum},
			{"fingerprint_xor", evidence.FingerprintXOR}, {"refresh_read_rows", evidence.RefreshReadRows},
			{"refresh_written_rows", evidence.RefreshWriteRows},
		}
		if topology[host] == 0 || seen[host] || asString(row["surface"]) != evidence.Surface ||
			asInt64(row["generation_ms"]) != evidence.GenerationMS || !strings.EqualFold(asString(row["marker_uuid"]), evidence.MarkerUUID) ||
			!strings.EqualFold(asString(row["run_uuid"]), evidence.RunUUID) || !strings.EqualFold(asString(row["lease_uuid"]), evidence.LeaseUUID) ||
			asInt64(row["source_fetched_max_ms"]) != evidence.SourceFetchedMaxMS ||
			asInt64(row["refresh_success_ms"]) != evidence.RefreshSuccessMS || asInt64(row["refresh_end_ms"]) != evidence.RefreshEndMS ||
			asInt64(row["completed_ms"]) != evidence.CompletedMS {
			return stateError("degraded", phase, "marker_evidence_mismatch")
		}
		seen[host] = true
		for _, value := range values {
			got, parseErr := exactUint64(row[value.key])
			if parseErr != nil || got != value.want {
				return stateError("degraded", phase, "marker_evidence_mismatch")
			}
		}
	}
	return nil
}

func (s *Service) insertAndReadbackActivation(ctx context.Context, topology map[string]int, activation activationEvidence) error {
	token := fmt.Sprintf("inflearn-public-activation-v2-%016x", H64(activation.RunUUID+"\x1f"+activation.ActivationUUID))
	sql := fmt.Sprintf(`INSERT INTO %s
		(activation_revision, activation_uuid, run_uuid, fence_epoch, lease_uuid,
		 webr_generation, webr_marker_uuid, mirtype_generation, mirtype_marker_uuid,
		 statground_generation, statground_marker_uuid, activation_kind, activated_at)
		SELECT toUInt64(%d), toUUID(%s), toUUID(%s), toUInt64(%d), toUUID(%s),
		 fromUnixTimestamp64Milli(%d), toUUID(%s), fromUnixTimestamp64Milli(%d), toUUID(%s),
		 fromUnixTimestamp64Milli(%d), toUUID(%s), CAST(%s AS Enum8('publish'=1,'rollback'=2)), fromUnixTimestamp64Milli(%d)
		SETTINGS distributed_foreground_insert = 1, insert_distributed_sync = 1, async_insert = 0,
		 insert_quorum = 4, insert_quorum_parallel = 0, insert_quorum_timeout = 60000,
		 insert_deduplicate = 1, insert_deduplication_token = %s`, lectureActivationTable,
		activation.Revision, QuoteSQLString(activation.ActivationUUID), QuoteSQLString(activation.RunUUID),
		activation.FenceEpoch, QuoteSQLString(activation.LeaseUUID), activation.WebRGenerationMS,
		QuoteSQLString(activation.WebRMarkerUUID), activation.MirGenerationMS, QuoteSQLString(activation.MirMarkerUUID),
		activation.StatgroundGenerationMS, QuoteSQLString(activation.StatgroundMarkerUUID),
		QuoteSQLString(activation.Kind), activation.ActivatedMS, QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt < publicationMutationReconcileAttempts; attempt++ {
		insertErr = s.execPublicRefreshCommand(ctx, sql, 2*time.Minute)
		if err := s.readbackActivation(ctx, topology, activation); err == nil {
			return nil
		} else if attempt+1 == publicationMutationReconcileAttempts {
			if insertErr != nil {
				return newUpdateReadStateError("public_activation_insert", insertErr)
			}
			return err
		}
	}
	return stateError("degraded", "public_activation_insert", "activation_reconcile_failed")
}

func (s *Service) readbackActivation(ctx context.Context, topology map[string]int, activation activationEvidence) error {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(activation_revision) AS activation_revision, toString(activation_uuid) AS activation_uuid,
		toString(run_uuid) AS run_uuid, toString(fence_epoch) AS fence_epoch, toString(lease_uuid) AS lease_uuid,
		toUnixTimestamp64Milli(webr_generation) AS webr_generation_ms, toString(webr_marker_uuid) AS webr_marker_uuid,
		toUnixTimestamp64Milli(mirtype_generation) AS mirtype_generation_ms, toString(mirtype_marker_uuid) AS mirtype_marker_uuid,
		toUnixTimestamp64Milli(statground_generation) AS statground_generation_ms, toString(statground_marker_uuid) AS statground_marker_uuid,
		toString(activation_kind) AS activation_kind, toUnixTimestamp64Milli(activated_at) AS activated_ms
		FROM clusterAllReplicas(%s, lecture_publication.inflearn_public_catalog_activation_local)
		WHERE activation_uuid = toUUID(%s) AND run_uuid = toUUID(%s)
		SETTINGS skip_unavailable_shards = 0, max_threads = 1, max_execution_time = 30`,
		QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(activation.ActivationUUID), QuoteSQLString(activation.RunUUID)))
	if err != nil {
		return newUpdateReadStateError("public_activation_readback", err)
	}
	if len(rows) != 4 {
		return stateError("degraded", "public_activation_readback", "activation_replica_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		revision, revErr := exactUint64(row["activation_revision"])
		fence, fenceErr := exactUint64(row["fence_epoch"])
		host := asString(row["hostname"])
		if revErr != nil || fenceErr != nil || topology[host] == 0 || seen[host] || revision != activation.Revision || fence != activation.FenceEpoch ||
			!strings.EqualFold(asString(row["activation_uuid"]), activation.ActivationUUID) || !strings.EqualFold(asString(row["run_uuid"]), activation.RunUUID) ||
			!strings.EqualFold(asString(row["lease_uuid"]), activation.LeaseUUID) ||
			asInt64(row["webr_generation_ms"]) != activation.WebRGenerationMS || !strings.EqualFold(asString(row["webr_marker_uuid"]), activation.WebRMarkerUUID) ||
			asInt64(row["mirtype_generation_ms"]) != activation.MirGenerationMS || !strings.EqualFold(asString(row["mirtype_marker_uuid"]), activation.MirMarkerUUID) ||
			asInt64(row["statground_generation_ms"]) != activation.StatgroundGenerationMS || !strings.EqualFold(asString(row["statground_marker_uuid"]), activation.StatgroundMarkerUUID) ||
			asString(row["activation_kind"]) != activation.Kind || asInt64(row["activated_ms"]) != activation.ActivatedMS {
			return stateError("degraded", "public_activation_readback", "activation_evidence_mismatch")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) verifyActivatedPointer(ctx context.Context, activation activationEvidence) error {
	pointers, revision, err := s.readPublicationPointer(ctx, "public_activation_pointer")
	if err != nil {
		return err
	}
	if revision != activation.Revision || len(pointers) != 3 {
		return stateError("degraded", "public_activation_pointer", "activation_not_selected")
	}
	wanted := map[string]struct {
		generation int64
		marker     string
	}{
		"webr":       {activation.WebRGenerationMS, activation.WebRMarkerUUID},
		"mirtype":    {activation.MirGenerationMS, activation.MirMarkerUUID},
		"statground": {activation.StatgroundGenerationMS, activation.StatgroundMarkerUUID},
	}
	for surface, want := range wanted {
		got, ok := pointers[surface]
		if !ok || !strings.EqualFold(got.RunUUID, activation.RunUUID) || !strings.EqualFold(got.ActivationUUID, activation.ActivationUUID) ||
			got.GenerationMS != want.generation || !strings.EqualFold(got.MarkerUUID, want.marker) || got.ActivationKind != activation.Kind {
			return stateError("degraded", "public_activation_pointer", "activation_pointer_mismatch")
		}
	}
	return nil
}

func (s *Service) execPublicRefreshCommand(ctx context.Context, command string, timeout time.Duration) error {
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := s.chPost(commandCtx, command, nil, "text/plain; charset=utf-8")
	return err
}
