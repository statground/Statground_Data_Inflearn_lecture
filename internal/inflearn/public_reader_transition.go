package inflearn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	publicationTransitionDomain = "lecture"
	publicationTransitionRoot   = "/internal/lecture-publication/transition"

	publicationTransitionInventoryFormat = "statground.publication-reader-transition-inventory.v2"
	publicationTransitionPrepareRequest  = "statground.publication-reader-transition-prepare-request.v2"
	publicationTransitionPrepareReceipt  = "statground.publication-reader-transition-prepare-receipt.v2"
	publicationTransitionCommitRequest   = "statground.publication-reader-transition-commit-request.v2"
	publicationTransitionCommitReceipt   = "statground.publication-reader-transition-commit-receipt.v2"
	publicationTransitionFinalizeRequest = "statground.publication-reader-transition-finalize-request.v2"
	publicationTransitionFinalizeReceipt = "statground.publication-reader-transition-finalize-receipt.v2"
	publicationTransitionAbortRequest    = "statground.publication-reader-transition-abort-request.v2"
	publicationTransitionAbortReceipt    = "statground.publication-reader-transition-abort-receipt.v2"

	publicationTransitionTimeoutSeconds = uint64(60)

	lectureTransitionReleaseLocalTable = "lecture_publication.inflearn_public_catalog_reader_transition_ack_v2_local"
	lectureTransitionReleaseTable      = "lecture_publication.inflearn_public_catalog_reader_transition_ack_v2"
)

var publicationTransitionInventoryKeys = stringSet(
	"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
	"phase", "admission_open", "transition_id", "serving_authority_sha256", "old_authority_sha256",
	"candidate_authority_sha256", "prepare_receipt_sha256", "commit_receipt_sha256",
	"old_state_proof_sha256", "last_finalized_transition_id", "last_finalize_receipt_sha256",
	"inflight", "observed_at",
)

var publicationTransitionPrepareReceiptKeys = stringSet(
	"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
	"transition_id", "prepare_nonce", "phase", "old_authority_sha256", "candidate_authority_sha256",
	"inflight_before", "inflight_after", "old_state_proof_sha256", "prepared_at", "receipt_sha256",
)

var publicationTransitionCommitReceiptKeys = stringSet(
	"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
	"transition_id", "commit_nonce", "phase", "candidate_authority_sha256", "prepare_receipt_sha256",
	"cache_entries_removed", "list_count", "detail_course_id", "homepage_count", "sitemap_entry_count",
	"sitemap_sha256", "refresh_started_at", "refreshed_at", "receipt_sha256",
)

var publicationTransitionFinalizeReceiptKeys = stringSet(
	"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
	"transition_id", "finalize_nonce", "phase", "candidate_authority_sha256", "commit_receipt_sha256",
	"ack_uuid", "ack_revision", "ack_reader_count", "reader_inventory_sha256", "reader_receipts_sha256",
	"final_proof_sha256", "finalized_at", "receipt_sha256",
)

var publicationTransitionAbortReceiptKeys = stringSet(
	"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
	"transition_id", "abort_nonce", "phase", "old_authority_sha256", "old_state_proof_sha256",
	"aborted_at", "receipt_sha256",
)

type publicationTransitionAuthority struct {
	ActivationRevision      uint64
	ActivationUUID          string
	RunUUID                 string
	SourceAuthorityRevision uint64
	Surface                 string
	GenerationMS            int64
}

func (authority publicationTransitionAuthority) jsonValue() map[string]any {
	return map[string]any{
		"activation_revision":       authority.ActivationRevision,
		"activation_uuid":           authority.ActivationUUID,
		"run_uuid":                  authority.RunUUID,
		"source_authority_revision": authority.SourceAuthorityRevision,
		"surface":                   authority.Surface,
		"generation_ms":             authority.GenerationMS,
	}
}

func (authority publicationTransitionAuthority) validCandidate() bool {
	return authority.ActivationRevision > 0 && isCanonicalUUID(authority.ActivationUUID) &&
		isCanonicalUUID(authority.RunUUID) && authority.SourceAuthorityRevision > 0 &&
		containsString(expectedPublicationSurfaces, authority.Surface) && authority.GenerationMS > 0
}

func (authority publicationTransitionAuthority) validOld() bool {
	if authority.ActivationRevision == 0 {
		return authority.ActivationUUID == zeroUUID && authority.RunUUID == zeroUUID &&
			authority.SourceAuthorityRevision == 0 && authority.GenerationMS == 0 &&
			containsString(expectedPublicationSurfaces, authority.Surface)
	}
	return authority.validCandidate()
}

type publicationTransitionInventory struct {
	Reader                    preparedLectureReader
	InventoryNonce            string
	Phase                     string
	AdmissionOpen             bool
	TransitionID              string
	ServingAuthoritySHA256    string
	OldAuthoritySHA256        string
	CandidateAuthoritySHA256  string
	PrepareReceiptSHA256      string
	CommitReceiptSHA256       string
	OldStateProofSHA256       string
	LastFinalizedTransitionID string
	LastFinalizeReceiptSHA256 string
	Inflight                  uint64
}

type publicationTransitionPrepareReceiptValue struct {
	Inventory                publicationTransitionInventory
	TransitionID             string
	PrepareNonce             string
	OldAuthoritySHA256       string
	CandidateAuthoritySHA256 string
	OldStateProofSHA256      string
	ReceiptSHA256            string
	InflightBefore           uint64
	PreparedAt               time.Time
}

type publicationTransitionCommitReceiptValue struct {
	Prepared            publicationTransitionPrepareReceiptValue
	CommitNonce         string
	CandidateSHA256     string
	ReceiptSHA256       string
	CacheEntriesRemoved uint64
	ListCount           uint64
	DetailCourseID      uint64
	HomepageCount       uint64
	SitemapEntryCount   uint64
	SitemapSHA256       string
	RefreshStartedAt    time.Time
	RefreshedAt         time.Time
}

type publicationTransitionRelease struct {
	TransitionID             string
	CandidateAuthoritySHA256 string
	CommitReceiptSHA256      string
	ACKUUID                  string
	ACKRevision              uint64
	ACKReaderCount           uint64
	ReaderInventorySHA256    string
	ReaderReceiptsSHA256     string
	FinalProofSHA256         string
}

type preparedPublicationTransition struct {
	TransitionID string
	Inventory    lectureReaderInventory
	Readers      []publicationTransitionPrepareReceiptValue
}

func abortPreparedPublicationTransition(ctx context.Context, prepared preparedPublicationTransition) error {
	var firstErr error
	for _, receipt := range prepared.Readers {
		pending := receipt.Inventory
		pending.Phase = "prepared"
		pending.AdmissionOpen = false
		pending.TransitionID = receipt.TransitionID
		pending.OldAuthoritySHA256 = receipt.OldAuthoritySHA256
		pending.CandidateAuthoritySHA256 = receipt.CandidateAuthoritySHA256
		pending.PrepareReceiptSHA256 = receipt.ReceiptSHA256
		pending.OldStateProofSHA256 = receipt.OldStateProofSHA256
		if _, err := abortPublicationTransitionReader(ctx, pending); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

const zeroUUID = "00000000-0000-0000-0000-000000000000"

func canonicalJSONSHA256(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalReceiptSHA256(value map[string]any) (string, error) {
	copyValue := make(map[string]any, len(value)-1)
	for key, item := range value {
		if key != "receipt_sha256" {
			copyValue[key] = item
		}
	}
	return canonicalJSONSHA256(copyValue)
}

func validLowerSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func stablePublicationTransitionUUID(label string, values ...string) string {
	seed := strings.Join(append([]string{"statground-publication-transition-v2", label}, values...), "\x1f")
	digest := sha256.Sum256([]byte(seed))
	b := digest[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])
}

func publicationTransitionEndpoint(reader lectureReaderConfig, suffix string) (string, error) {
	base, err := url.Parse(reader.InventoryEndpoint)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid transition reader origin")
	}
	base.Path = publicationTransitionRoot + suffix
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

func publicationTransitionHTTPClient(reader lectureReaderConfig) (*http.Client, error) {
	return readerHTTPClient(reader)
}

func requestPublicationTransition(ctx context.Context, reader lectureReaderConfig, endpoint string, body []byte) (map[string]any, error) {
	client, err := publicationTransitionHTTPClient(reader)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 1; attempt <= lectureReaderRefreshAttempts; attempt++ {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if requestErr != nil {
			return nil, requestErr
		}
		request.Header.Set("Authorization", "Bearer "+reader.BearerToken)
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Cache-Control", "no-store")
		response, requestErr := client.Do(request)
		if requestErr == nil {
			value, readErr := readBoundedJSONResponse(response)
			if readErr == nil {
				return value, nil
			}
			requestErr = readErr
		}
		lastErr = requestErr
		if attempt == lectureReaderRefreshAttempts || !retryableLectureReaderRefreshError(requestErr) {
			break
		}
		timer := time.NewTimer(lectureReaderRefreshBackoff * time.Duration(1<<(attempt-1)))
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func discoverPublicationTransitionReader(ctx context.Context, reader lectureReaderConfig, inventoryNonce string) (publicationTransitionInventory, error) {
	endpoint, err := publicationTransitionEndpoint(reader, "")
	if err != nil {
		return publicationTransitionInventory{}, err
	}
	u, _ := url.Parse(endpoint)
	query := u.Query()
	query.Set("inventory_nonce", inventoryNonce)
	u.RawQuery = query.Encode()
	client, err := publicationTransitionHTTPClient(reader)
	if err != nil {
		return publicationTransitionInventory{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return publicationTransitionInventory{}, err
	}
	request.Header.Set("Authorization", "Bearer "+reader.BearerToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	response, err := client.Do(request)
	if err != nil {
		return publicationTransitionInventory{}, err
	}
	value, err := readBoundedJSONResponse(response)
	if err != nil || !exactJSONKeys(value, publicationTransitionInventoryKeys) {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory contract")
	}
	epoch := strings.ToLower(strings.TrimSpace(asString(value["reader_epoch_uuid"])))
	if asString(value["format"]) != publicationTransitionInventoryFormat ||
		asString(value["app_service"]) != reader.AppService || asString(value["domain"]) != publicationTransitionDomain ||
		asString(value["reader_instance"]) != reader.ReaderInstance || asString(value["inventory_nonce"]) != inventoryNonce ||
		!isCanonicalUUID(epoch) {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory identity")
	}
	phase := asString(value["phase"])
	if phase != "steady" && phase != "prepared" && phase != "committed" {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory phase")
	}
	admissionOpen, ok := value["admission_open"].(bool)
	if !ok || admissionOpen != (phase == "steady") {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory admission")
	}
	inflight, err := strictJSONUint(value["inflight"])
	if err != nil {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory inflight")
	}
	if _, err := time.Parse(time.RFC3339Nano, asString(value["observed_at"])); err != nil {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition inventory time")
	}
	result := publicationTransitionInventory{
		Reader: preparedLectureReader{Config: reader, EpochUUID: epoch}, InventoryNonce: inventoryNonce,
		Phase: phase, AdmissionOpen: admissionOpen, TransitionID: strings.ToLower(strings.TrimSpace(asString(value["transition_id"]))),
		ServingAuthoritySHA256:    strings.TrimSpace(asString(value["serving_authority_sha256"])),
		OldAuthoritySHA256:        strings.TrimSpace(asString(value["old_authority_sha256"])),
		CandidateAuthoritySHA256:  strings.TrimSpace(asString(value["candidate_authority_sha256"])),
		PrepareReceiptSHA256:      strings.TrimSpace(asString(value["prepare_receipt_sha256"])),
		CommitReceiptSHA256:       strings.TrimSpace(asString(value["commit_receipt_sha256"])),
		OldStateProofSHA256:       strings.TrimSpace(asString(value["old_state_proof_sha256"])),
		LastFinalizedTransitionID: strings.ToLower(strings.TrimSpace(asString(value["last_finalized_transition_id"]))),
		LastFinalizeReceiptSHA256: strings.TrimSpace(asString(value["last_finalize_receipt_sha256"])), Inflight: inflight,
	}
	if phase == "steady" {
		if result.TransitionID != "" || result.OldAuthoritySHA256 != "" || result.CandidateAuthoritySHA256 != "" ||
			result.PrepareReceiptSHA256 != "" || result.CommitReceiptSHA256 != "" || result.OldStateProofSHA256 != "" {
			return publicationTransitionInventory{}, fmt.Errorf("publication transition steady residue")
		}
		if result.ServingAuthoritySHA256 != "" && !validLowerSHA256(result.ServingAuthoritySHA256) {
			return publicationTransitionInventory{}, fmt.Errorf("publication transition serving digest")
		}
		if result.LastFinalizedTransitionID != "" && (!isCanonicalUUID(result.LastFinalizedTransitionID) || !validLowerSHA256(result.LastFinalizeReceiptSHA256)) {
			return publicationTransitionInventory{}, fmt.Errorf("publication transition finalized identity")
		}
		return result, nil
	}
	if !isCanonicalUUID(result.TransitionID) || !validLowerSHA256(result.OldAuthoritySHA256) ||
		!validLowerSHA256(result.CandidateAuthoritySHA256) || !validLowerSHA256(result.PrepareReceiptSHA256) ||
		!validLowerSHA256(result.OldStateProofSHA256) || (phase == "committed" && !validLowerSHA256(result.CommitReceiptSHA256)) {
		return publicationTransitionInventory{}, fmt.Errorf("publication transition pending identity")
	}
	return result, nil
}

func preparePublicationTransitionReader(ctx context.Context, inventory publicationTransitionInventory, transitionID string, oldAuthority, candidateAuthority publicationTransitionAuthority) (publicationTransitionPrepareReceiptValue, error) {
	if !oldAuthority.validOld() || !candidateAuthority.validCandidate() || !isCanonicalUUID(transitionID) {
		return publicationTransitionPrepareReceiptValue{}, fmt.Errorf("invalid publication transition authority")
	}
	oldDigest, _ := canonicalJSONSHA256(oldAuthority.jsonValue())
	candidateDigest, _ := canonicalJSONSHA256(candidateAuthority.jsonValue())
	prepareNonce := stablePublicationTransitionUUID("prepare", transitionID, inventory.Reader.Config.AppService, inventory.Reader.Config.ReaderInstance, inventory.Reader.EpochUUID)
	requestValue := map[string]any{
		"format": publicationTransitionPrepareRequest, "app_service": inventory.Reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": inventory.Reader.Config.ReaderInstance,
		"reader_epoch_uuid": inventory.Reader.EpochUUID, "inventory_nonce": inventory.InventoryNonce,
		"transition_id": transitionID, "prepare_nonce": prepareNonce,
		"timeout_seconds": publicationTransitionTimeoutSeconds,
		"old_authority":   oldAuthority.jsonValue(), "candidate_authority": candidateAuthority.jsonValue(),
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return publicationTransitionPrepareReceiptValue{}, err
	}
	endpoint, err := publicationTransitionEndpoint(inventory.Reader.Config, "/prepare")
	if err != nil {
		return publicationTransitionPrepareReceiptValue{}, err
	}
	value, err := requestPublicationTransition(ctx, inventory.Reader.Config, endpoint, body)
	if err != nil || !exactJSONKeys(value, publicationTransitionPrepareReceiptKeys) {
		return publicationTransitionPrepareReceiptValue{}, fmt.Errorf("publication transition prepare contract")
	}
	expectedStrings := map[string]string{
		"format": publicationTransitionPrepareReceipt, "app_service": inventory.Reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": inventory.Reader.Config.ReaderInstance,
		"reader_epoch_uuid": inventory.Reader.EpochUUID, "inventory_nonce": inventory.InventoryNonce,
		"transition_id": transitionID, "prepare_nonce": prepareNonce, "phase": "prepared",
		"old_authority_sha256": oldDigest, "candidate_authority_sha256": candidateDigest,
	}
	for key, expected := range expectedStrings {
		if asString(value[key]) != expected {
			return publicationTransitionPrepareReceiptValue{}, fmt.Errorf("publication transition prepare identity")
		}
	}
	before, beforeErr := strictJSONUint(value["inflight_before"])
	after, afterErr := strictJSONUint(value["inflight_after"])
	oldProof := strings.TrimSpace(asString(value["old_state_proof_sha256"]))
	receiptDigest := strings.TrimSpace(asString(value["receipt_sha256"]))
	calculated, hashErr := canonicalReceiptSHA256(value)
	preparedAt, timeErr := time.Parse(time.RFC3339Nano, asString(value["prepared_at"]))
	if beforeErr != nil || afterErr != nil || after != 0 || !validLowerSHA256(oldProof) ||
		!validLowerSHA256(receiptDigest) || hashErr != nil || receiptDigest != calculated || timeErr != nil {
		return publicationTransitionPrepareReceiptValue{}, fmt.Errorf("publication transition prepare proof")
	}
	return publicationTransitionPrepareReceiptValue{
		Inventory: inventory, TransitionID: transitionID, PrepareNonce: prepareNonce,
		OldAuthoritySHA256: oldDigest, CandidateAuthoritySHA256: candidateDigest,
		OldStateProofSHA256: oldProof, ReceiptSHA256: receiptDigest,
		InflightBefore: before, PreparedAt: preparedAt.UTC(),
	}, nil
}

func commitPublicationTransitionReader(ctx context.Context, prepared publicationTransitionPrepareReceiptValue, candidateAuthority publicationTransitionAuthority) (publicationTransitionCommitReceiptValue, error) {
	if !candidateAuthority.validCandidate() {
		return publicationTransitionCommitReceiptValue{}, fmt.Errorf("invalid publication transition candidate")
	}
	commitNonce := stablePublicationTransitionUUID("commit", prepared.TransitionID, prepared.Inventory.Reader.Config.AppService, prepared.Inventory.Reader.Config.ReaderInstance, prepared.Inventory.Reader.EpochUUID)
	requestValue := map[string]any{
		"format": publicationTransitionCommitRequest, "app_service": prepared.Inventory.Reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": prepared.Inventory.Reader.Config.ReaderInstance,
		"reader_epoch_uuid": prepared.Inventory.Reader.EpochUUID, "inventory_nonce": prepared.Inventory.InventoryNonce,
		"transition_id": prepared.TransitionID, "prepare_nonce": prepared.PrepareNonce,
		"prepare_receipt_sha256": prepared.ReceiptSHA256, "commit_nonce": commitNonce,
		"timeout_seconds": publicationTransitionTimeoutSeconds, "candidate_authority": candidateAuthority.jsonValue(),
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return publicationTransitionCommitReceiptValue{}, err
	}
	endpoint, err := publicationTransitionEndpoint(prepared.Inventory.Reader.Config, "/commit")
	if err != nil {
		return publicationTransitionCommitReceiptValue{}, err
	}
	value, err := requestPublicationTransition(ctx, prepared.Inventory.Reader.Config, endpoint, body)
	if err != nil || !exactJSONKeys(value, publicationTransitionCommitReceiptKeys) {
		return publicationTransitionCommitReceiptValue{}, fmt.Errorf("publication transition commit contract")
	}
	expectedStrings := map[string]string{
		"format": publicationTransitionCommitReceipt, "app_service": prepared.Inventory.Reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": prepared.Inventory.Reader.Config.ReaderInstance,
		"reader_epoch_uuid": prepared.Inventory.Reader.EpochUUID, "inventory_nonce": prepared.Inventory.InventoryNonce,
		"transition_id": prepared.TransitionID, "commit_nonce": commitNonce, "phase": "committed",
		"candidate_authority_sha256": prepared.CandidateAuthoritySHA256, "prepare_receipt_sha256": prepared.ReceiptSHA256,
	}
	for key, expected := range expectedStrings {
		if asString(value[key]) != expected {
			return publicationTransitionCommitReceiptValue{}, fmt.Errorf("publication transition commit identity")
		}
	}
	numericKeys := []string{"cache_entries_removed", "list_count", "detail_course_id", "homepage_count", "sitemap_entry_count"}
	numbers := make(map[string]uint64, len(numericKeys))
	for _, key := range numericKeys {
		parsed, parseErr := strictJSONUint(value[key])
		if parseErr != nil {
			return publicationTransitionCommitReceiptValue{}, fmt.Errorf("publication transition commit evidence")
		}
		numbers[key] = parsed
	}
	if numbers["list_count"] == 0 || numbers["detail_course_id"] == 0 || numbers["homepage_count"] == 0 || numbers["sitemap_entry_count"] == 0 {
		return publicationTransitionCommitReceiptValue{}, fmt.Errorf("publication transition empty public surface")
	}
	sitemapDigest := strings.TrimSpace(asString(value["sitemap_sha256"]))
	receiptDigest := strings.TrimSpace(asString(value["receipt_sha256"]))
	calculated, hashErr := canonicalReceiptSHA256(value)
	started, startErr := time.Parse(time.RFC3339Nano, asString(value["refresh_started_at"]))
	refreshed, refreshErr := time.Parse(time.RFC3339Nano, asString(value["refreshed_at"]))
	if !validLowerSHA256(sitemapDigest) || !validLowerSHA256(receiptDigest) || hashErr != nil || receiptDigest != calculated ||
		startErr != nil || refreshErr != nil || refreshed.Before(started) || refreshed.Sub(started) > time.Duration(publicationTransitionTimeoutSeconds)*time.Second {
		return publicationTransitionCommitReceiptValue{}, fmt.Errorf("publication transition commit proof")
	}
	return publicationTransitionCommitReceiptValue{
		Prepared: prepared, CommitNonce: commitNonce, CandidateSHA256: prepared.CandidateAuthoritySHA256,
		ReceiptSHA256: receiptDigest, CacheEntriesRemoved: numbers["cache_entries_removed"],
		ListCount: numbers["list_count"], DetailCourseID: numbers["detail_course_id"], HomepageCount: numbers["homepage_count"],
		SitemapEntryCount: numbers["sitemap_entry_count"], SitemapSHA256: sitemapDigest,
		RefreshStartedAt: started.UTC(), RefreshedAt: refreshed.UTC(),
	}, nil
}

func finalizePublicationTransitionReader(ctx context.Context, committed publicationTransitionCommitReceiptValue, release publicationTransitionRelease) (string, error) {
	reader := committed.Prepared.Inventory.Reader
	if !isCanonicalUUID(release.TransitionID) || release.TransitionID != committed.Prepared.TransitionID ||
		!isCanonicalUUID(release.ACKUUID) || release.ACKRevision == 0 || release.ACKReaderCount == 0 ||
		!validLowerSHA256(release.CandidateAuthoritySHA256) || release.CandidateAuthoritySHA256 != committed.CandidateSHA256 ||
		!validLowerSHA256(release.CommitReceiptSHA256) || release.CommitReceiptSHA256 != committed.ReceiptSHA256 ||
		!validLowerSHA256(release.ReaderInventorySHA256) || !validLowerSHA256(release.ReaderReceiptsSHA256) ||
		!validLowerSHA256(release.FinalProofSHA256) {
		return "", fmt.Errorf("invalid publication transition release")
	}
	finalizeNonce := stablePublicationTransitionUUID("finalize", release.TransitionID, reader.Config.AppService, reader.Config.ReaderInstance, reader.EpochUUID)
	requestValue := map[string]any{
		"format": publicationTransitionFinalizeRequest, "app_service": reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": reader.Config.ReaderInstance,
		"reader_epoch_uuid": reader.EpochUUID, "inventory_nonce": committed.Prepared.Inventory.InventoryNonce,
		"transition_id": release.TransitionID, "finalize_nonce": finalizeNonce,
		"timeout_seconds":            publicationTransitionTimeoutSeconds,
		"candidate_authority_sha256": release.CandidateAuthoritySHA256,
		"commit_receipt_sha256":      release.CommitReceiptSHA256,
		"ack_uuid":                   release.ACKUUID,
		"ack_revision":               release.ACKRevision,
		"ack_reader_count":           release.ACKReaderCount,
		"reader_inventory_sha256":    release.ReaderInventorySHA256,
		"reader_receipts_sha256":     release.ReaderReceiptsSHA256,
		"final_proof_sha256":         release.FinalProofSHA256,
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return "", err
	}
	endpoint, err := publicationTransitionEndpoint(reader.Config, "/finalize")
	if err != nil {
		return "", err
	}
	value, err := requestPublicationTransition(ctx, reader.Config, endpoint, body)
	if err != nil || !exactJSONKeys(value, publicationTransitionFinalizeReceiptKeys) {
		return "", fmt.Errorf("publication transition finalize contract")
	}
	expectedStrings := map[string]string{
		"format": publicationTransitionFinalizeReceipt, "app_service": reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": reader.Config.ReaderInstance,
		"reader_epoch_uuid": reader.EpochUUID, "inventory_nonce": committed.Prepared.Inventory.InventoryNonce,
		"transition_id": release.TransitionID, "finalize_nonce": finalizeNonce, "phase": "steady",
		"candidate_authority_sha256": release.CandidateAuthoritySHA256,
		"commit_receipt_sha256":      release.CommitReceiptSHA256, "ack_uuid": release.ACKUUID,
		"reader_inventory_sha256": release.ReaderInventorySHA256,
		"reader_receipts_sha256":  release.ReaderReceiptsSHA256, "final_proof_sha256": release.FinalProofSHA256,
	}
	for key, expected := range expectedStrings {
		if asString(value[key]) != expected {
			return "", fmt.Errorf("publication transition finalize identity")
		}
	}
	ackRevision, revisionErr := strictJSONUint(value["ack_revision"])
	ackReaderCount, countErr := strictJSONUint(value["ack_reader_count"])
	receiptDigest := strings.TrimSpace(asString(value["receipt_sha256"]))
	calculated, hashErr := canonicalReceiptSHA256(value)
	_, timeErr := time.Parse(time.RFC3339Nano, asString(value["finalized_at"]))
	if revisionErr != nil || countErr != nil || ackRevision != release.ACKRevision || ackReaderCount != release.ACKReaderCount ||
		!validLowerSHA256(receiptDigest) || hashErr != nil || receiptDigest != calculated || timeErr != nil {
		return "", fmt.Errorf("publication transition finalize proof")
	}
	return receiptDigest, nil
}

func abortPublicationTransitionReader(ctx context.Context, inventory publicationTransitionInventory) (string, error) {
	if inventory.Phase != "prepared" || !isCanonicalUUID(inventory.TransitionID) ||
		!validLowerSHA256(inventory.OldAuthoritySHA256) || !validLowerSHA256(inventory.OldStateProofSHA256) {
		return "", fmt.Errorf("publication transition cannot abort")
	}
	reader := inventory.Reader
	abortNonce := stablePublicationTransitionUUID("abort", inventory.TransitionID, reader.Config.AppService, reader.Config.ReaderInstance, reader.EpochUUID)
	requestValue := map[string]any{
		"format": publicationTransitionAbortRequest, "app_service": reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": reader.Config.ReaderInstance,
		"reader_epoch_uuid": reader.EpochUUID, "inventory_nonce": inventory.InventoryNonce,
		"transition_id": inventory.TransitionID, "abort_nonce": abortNonce,
		"timeout_seconds":      publicationTransitionTimeoutSeconds,
		"old_authority_sha256": inventory.OldAuthoritySHA256, "old_state_proof_sha256": inventory.OldStateProofSHA256,
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return "", err
	}
	endpoint, err := publicationTransitionEndpoint(reader.Config, "/abort")
	if err != nil {
		return "", err
	}
	value, err := requestPublicationTransition(ctx, reader.Config, endpoint, body)
	if err != nil || !exactJSONKeys(value, publicationTransitionAbortReceiptKeys) {
		return "", fmt.Errorf("publication transition abort contract")
	}
	expectedStrings := map[string]string{
		"format": publicationTransitionAbortReceipt, "app_service": reader.Config.AppService,
		"domain": publicationTransitionDomain, "reader_instance": reader.Config.ReaderInstance,
		"reader_epoch_uuid": reader.EpochUUID, "inventory_nonce": inventory.InventoryNonce,
		"transition_id": inventory.TransitionID, "abort_nonce": abortNonce, "phase": "steady",
		"old_authority_sha256": inventory.OldAuthoritySHA256, "old_state_proof_sha256": inventory.OldStateProofSHA256,
	}
	for key, expected := range expectedStrings {
		if asString(value[key]) != expected {
			return "", fmt.Errorf("publication transition abort identity")
		}
	}
	receiptDigest := strings.TrimSpace(asString(value["receipt_sha256"]))
	calculated, hashErr := canonicalReceiptSHA256(value)
	_, timeErr := time.Parse(time.RFC3339Nano, asString(value["aborted_at"]))
	if !validLowerSHA256(receiptDigest) || hashErr != nil || receiptDigest != calculated || timeErr != nil {
		return "", fmt.Errorf("publication transition abort proof")
	}
	return receiptDigest, nil
}

func publicationTransitionReaderInventorySHA256(inventory lectureReaderInventory) (string, error) {
	readers := make([]map[string]any, 0, len(inventory.Readers))
	for _, reader := range inventory.Readers {
		readers = append(readers, map[string]any{
			"app_service": reader.AppService, "reader_instance": reader.ReaderInstance,
			"inventory_endpoint_sha256": reader.inventoryEndpointSHA,
		})
	}
	sort.Slice(readers, func(i, j int) bool {
		return fmt.Sprint(readers[i]["app_service"], "\x00", readers[i]["reader_instance"]) <
			fmt.Sprint(readers[j]["app_service"], "\x00", readers[j]["reader_instance"])
	})
	return canonicalJSONSHA256(map[string]any{
		"reader_inventory_revision": inventory.Revision,
		"reader_inventory_uuid":     inventory.UUID,
		"readers":                   readers,
	})
}

func publicationTransitionReceiptSetSHA256(receipts []publicationTransitionCommitReceiptValue) (string, error) {
	values := make([]string, 0, len(receipts))
	for _, receipt := range receipts {
		values = append(values, strings.Join([]string{
			receipt.Prepared.Inventory.Reader.Config.AppService,
			receipt.Prepared.Inventory.Reader.Config.ReaderInstance,
			receipt.Prepared.Inventory.Reader.EpochUUID,
			receipt.ReceiptSHA256,
		}, "\x1f"))
	}
	sort.Strings(values)
	return canonicalJSONSHA256(values)
}

func publicationTransitionFinalProofSHA256(transitionID string, activation activationEvidence, sourceAuthorityRevision uint64, readerInventorySHA256, receiptSetSHA256 string, receipts []publicationTransitionCommitReceiptValue) (string, error) {
	readerProofs := make([]string, 0, len(receipts))
	for _, receipt := range receipts {
		readerProofs = append(readerProofs, strings.Join([]string{
			receipt.Prepared.Inventory.Reader.Config.AppService,
			receipt.Prepared.Inventory.Reader.Config.ReaderInstance,
			receipt.CandidateSHA256,
			receipt.ReceiptSHA256,
		}, "\x1f"))
	}
	sort.Strings(readerProofs)
	return canonicalJSONSHA256(map[string]any{
		"transition_id": transitionID, "activation_revision": activation.Revision,
		"activation_uuid": activation.ActivationUUID, "run_uuid": activation.RunUUID,
		"source_authority_revision": sourceAuthorityRevision,
		"reader_inventory_sha256":   readerInventorySHA256,
		"reader_receipts_sha256":    receiptSetSHA256, "reader_proofs": readerProofs,
	})
}

func stablePublicationTransitionACKUUID(transitionID, readerInventorySHA256, receiptSetSHA256 string) string {
	return stablePublicationTransitionUUID("ack", transitionID, readerInventorySHA256, receiptSetSHA256)
}

func transitionAuthorityForReader(activation activationEvidence, sourceAuthorityRevision uint64, service string) publicationTransitionAuthority {
	return publicationTransitionAuthority{
		ActivationRevision: activation.Revision, ActivationUUID: activation.ActivationUUID,
		RunUUID: activation.RunUUID, SourceAuthorityRevision: sourceAuthorityRevision,
		Surface: lectureSurfaceForService(service), GenerationMS: generationForReader(activation, service),
	}
}

func oldTransitionAuthorityForReader(pointer publicationPointer, sourceAuthorityRevision uint64, service string) publicationTransitionAuthority {
	if pointer.ActivationRevision == 0 {
		return publicationTransitionAuthority{ActivationUUID: zeroUUID, RunUUID: zeroUUID, Surface: lectureSurfaceForService(service)}
	}
	return publicationTransitionAuthority{
		ActivationRevision: pointer.ActivationRevision, ActivationUUID: pointer.ActivationUUID,
		RunUUID: pointer.RunUUID, SourceAuthorityRevision: sourceAuthorityRevision,
		Surface: lectureSurfaceForService(service), GenerationMS: pointer.GenerationMS,
	}
}

func (s *Service) readServingSourceAuthorityRevision(ctx context.Context, activationRevision uint64) (uint64, error) {
	if activationRevision == 0 {
		return 0, nil
	}
	queries := []string{
		fmt.Sprintf(`SELECT DISTINCT toString(source_authority_revision) AS source_authority_revision
			FROM lecture_publication.inflearn_public_catalog_reader_transition_ack_v2_local
			WHERE ack_revision=%d ORDER BY source_authority_revision LIMIT 2
			SETTINGS max_threads=1,max_execution_time=30`, activationRevision),
		fmt.Sprintf(`SELECT DISTINCT toString(source_authority_revision) AS source_authority_revision
			FROM lecture_publication.inflearn_public_catalog_reader_refresh_ack_local
			WHERE activation_revision=%d ORDER BY source_authority_revision LIMIT 2
			SETTINGS max_threads=1,max_execution_time=30`, activationRevision),
	}
	for _, query := range queries {
		rows, err := s.CHQueryRows(ctx, query)
		if err != nil {
			return 0, newUpdateReadStateError("public_reader_transition_serving_authority", err)
		}
		if len(rows) == 0 {
			continue
		}
		if len(rows) != 1 {
			return 0, stateError("degraded", "public_reader_transition_serving_authority", "serving_authority_revision_divergence")
		}
		revision, parseErr := exactUint64(rows[0]["source_authority_revision"])
		if parseErr != nil || revision == 0 {
			return 0, stateError("degraded", "public_reader_transition_serving_authority", "serving_authority_revision_invalid")
		}
		return revision, nil
	}
	return 0, stateError("degraded", "public_reader_transition_serving_authority", "serving_authority_revision_missing")
}

func (s *Service) resolveTransitionCandidateSourceAuthority(ctx context.Context, activation activationEvidence, states []publicationTransitionInventory) (uint64, error) {
	rows, err := s.CHQueryRows(ctx, `SELECT toString(authority_revision) AS authority_revision
		FROM lecture_publication.v_inflearn_public_catalog_source_authority_fence_valid_local
		ORDER BY authority_revision DESC LIMIT 10000
		SETTINGS max_threads=1,max_execution_time=30`)
	if err != nil {
		return 0, newUpdateReadStateError("public_reader_transition_candidate_authority", err)
	}
	matches := make([]uint64, 0, 1)
	seen := map[uint64]bool{}
	for _, row := range rows {
		revision, parseErr := exactUint64(row["authority_revision"])
		if parseErr != nil || revision == 0 || seen[revision] {
			continue
		}
		seen[revision] = true
		matched := true
		for _, state := range states {
			if state.Phase == "steady" {
				continue
			}
			candidate := transitionAuthorityForReader(activation, revision, state.Reader.Config.AppService)
			digest, hashErr := canonicalJSONSHA256(candidate.jsonValue())
			if hashErr != nil || digest != state.CandidateAuthoritySHA256 {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, revision)
		}
	}
	if len(matches) != 1 {
		return 0, stateError("degraded", "public_reader_transition_candidate_authority", "candidate_authority_revision_unresolved")
	}
	return matches[0], nil
}

func (s *Service) resolveTransitionServingSourceAuthority(ctx context.Context, activation activationEvidence, states []publicationTransitionInventory) (uint64, error) {
	rows, err := s.CHQueryRows(ctx, `SELECT toString(authority_revision) AS authority_revision
		FROM lecture_publication.v_inflearn_public_catalog_source_authority_fence_valid_local
		ORDER BY authority_revision DESC LIMIT 10000
		SETTINGS max_threads=1,max_execution_time=30`)
	if err != nil {
		return 0, newUpdateReadStateError("public_reader_transition_serving_authority", err)
	}
	matches := make([]uint64, 0, 1)
	seen := map[uint64]bool{}
	for _, row := range rows {
		revision, parseErr := exactUint64(row["authority_revision"])
		if parseErr != nil || revision == 0 || seen[revision] {
			continue
		}
		seen[revision] = true
		matched := true
		for _, state := range states {
			authority := transitionAuthorityForReader(activation, revision, state.Reader.Config.AppService)
			digest, hashErr := canonicalJSONSHA256(authority.jsonValue())
			if hashErr != nil || digest != state.ServingAuthoritySHA256 {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, revision)
		}
	}
	if len(matches) != 1 {
		return 0, stateError("degraded", "public_reader_transition_serving_authority", "serving_authority_revision_unresolved")
	}
	return matches[0], nil
}

func publicationTransitionID(activation activationEvidence) string {
	return stablePublicationTransitionUUID("transition", strconv.FormatUint(activation.Revision, 10), activation.ActivationUUID, activation.RunUUID)
}

func (s *Service) preparePublicationTransitionReaders(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile, priorPointers map[string]publicationPointer, activation activationEvidence, sourceAuthorityRevision uint64) (preparedPublicationTransition, error) {
	inventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return preparedPublicationTransition{}, err
	}
	transitionID := publicationTransitionID(activation)
	prepared := preparedPublicationTransition{TransitionID: transitionID, Inventory: inventory}
	discoveredReaders := make([]publicationTransitionInventory, 0, len(inventory.Readers))
	for _, reader := range inventory.Readers {
		inventoryNonce := stablePublicationTransitionUUID("inventory", transitionID, reader.AppService, reader.ReaderInstance)
		discovered, err := discoverPublicationTransitionReader(ctx, reader, inventoryNonce)
		if err != nil {
			return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_inventory", "reader_inventory_failed")
		}
		if discovered.Phase != "steady" || !discovered.AdmissionOpen || discovered.Inflight != 0 {
			return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_inventory", "pending_transition_requires_reconcile")
		}
		discoveredReaders = append(discoveredReaders, discovered)
	}
	var priorSourceAuthorityRevision uint64
	if len(priorPointers) != 0 {
		priorActivation, activationErr := activationFromCurrentPointers(priorPointers)
		if activationErr != nil {
			return preparedPublicationTransition{}, activationErr
		}
		priorSourceAuthorityRevision, err = s.resolveTransitionServingSourceAuthority(ctx, priorActivation, discoveredReaders)
		if err != nil {
			return preparedPublicationTransition{}, err
		}
	}
	for _, discovered := range discoveredReaders {
		reader := discovered.Reader.Config
		surface := lectureSurfaceForService(reader.AppService)
		pointer := priorPointers[surface]
		oldSourceRevision := priorSourceAuthorityRevision
		oldAuthority := oldTransitionAuthorityForReader(pointer, oldSourceRevision, reader.AppService)
		candidateAuthority := transitionAuthorityForReader(activation, sourceAuthorityRevision, reader.AppService)
		oldDigest, _ := canonicalJSONSHA256(oldAuthority.jsonValue())
		if discovered.ServingAuthoritySHA256 != oldDigest {
			return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_inventory", "reader_serving_authority_mismatch")
		}
		receipt, err := preparePublicationTransitionReader(ctx, discovered, transitionID, oldAuthority, candidateAuthority)
		if err != nil {
			_ = abortPreparedPublicationTransition(ctx, prepared)
			return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_prepare", "reader_prepare_failed")
		}
		prepared.Readers = append(prepared.Readers, receipt)
	}
	if len(prepared.Readers) != len(inventory.Readers) {
		return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_prepare", "reader_prepare_set_incomplete")
	}
	postInventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil || !sameLectureReaderInventory(postInventory, inventory) {
		return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_prepare", "reader_inventory_changed")
	}
	currentAuthority, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_reader_transition_prepare_authority")
	if err != nil || currentAuthority != sourceAuthorityRevision {
		return preparedPublicationTransition{}, stateError("degraded", "public_reader_transition_prepare", "source_authority_changed")
	}
	return prepared, nil
}

func (s *Service) commitPublicationTransitionReaders(ctx context.Context, prepared preparedPublicationTransition, activation activationEvidence, sourceAuthorityRevision uint64) ([]publicationTransitionCommitReceiptValue, error) {
	receipts := make([]publicationTransitionCommitReceiptValue, 0, len(prepared.Readers))
	for _, reader := range prepared.Readers {
		candidate := transitionAuthorityForReader(activation, sourceAuthorityRevision, reader.Inventory.Reader.Config.AppService)
		receipt, err := commitPublicationTransitionReader(ctx, reader, candidate)
		if err != nil {
			return nil, stateError("degraded", "public_reader_transition_commit", "reader_commit_failed")
		}
		receipts = append(receipts, receipt)
	}
	if len(receipts) != len(prepared.Inventory.Readers) {
		return nil, stateError("degraded", "public_reader_transition_commit", "reader_commit_set_incomplete")
	}
	return receipts, nil
}

func (s *Service) proveCommittedPublicationTransitionReaders(ctx context.Context, prepared preparedPublicationTransition, receipts []publicationTransitionCommitReceiptValue) error {
	if len(receipts) != len(prepared.Readers) || len(receipts) == 0 {
		return stateError("degraded", "public_reader_transition_commit_proof", "reader_commit_set_incomplete")
	}
	for _, receipt := range receipts {
		inventory, err := discoverPublicationTransitionReader(ctx, receipt.Prepared.Inventory.Reader.Config, receipt.Prepared.Inventory.InventoryNonce)
		if err != nil || inventory.Phase != "committed" || inventory.AdmissionOpen || inventory.TransitionID != prepared.TransitionID ||
			inventory.Reader.EpochUUID != receipt.Prepared.Inventory.Reader.EpochUUID ||
			inventory.CandidateAuthoritySHA256 != receipt.CandidateSHA256 || inventory.CommitReceiptSHA256 != receipt.ReceiptSHA256 || inventory.Inflight != 0 {
			return stateError("degraded", "public_reader_transition_commit_proof", "reader_commit_state_mismatch")
		}
	}
	return nil
}

func (s *Service) appendAndReadbackPublicationTransitionRelease(ctx context.Context, topology map[string]int, prepared preparedPublicationTransition, receipts []publicationTransitionCommitReceiptValue, activation activationEvidence, sourceAuthorityRevision uint64) (publicationTransitionRelease, error) {
	readerInventorySHA, err := publicationTransitionReaderInventorySHA256(prepared.Inventory)
	if err != nil {
		return publicationTransitionRelease{}, stateError("degraded", "public_reader_transition_ack", "reader_inventory_digest_failed")
	}
	receiptSetSHA, err := publicationTransitionReceiptSetSHA256(receipts)
	if err != nil {
		return publicationTransitionRelease{}, stateError("degraded", "public_reader_transition_ack", "reader_receipt_digest_failed")
	}
	finalProofSHA, err := publicationTransitionFinalProofSHA256(prepared.TransitionID, activation, sourceAuthorityRevision, readerInventorySHA, receiptSetSHA, receipts)
	if err != nil {
		return publicationTransitionRelease{}, stateError("degraded", "public_reader_transition_ack", "final_proof_digest_failed")
	}
	ackUUID := stablePublicationTransitionACKUUID(prepared.TransitionID, readerInventorySHA, receiptSetSHA)
	readerCount := uint64(len(receipts))
	columns := []string{
		"ack_uuid", "ack_revision", "transition_id", "activation_uuid", "run_uuid", "source_authority_revision",
		"reader_inventory_revision", "reader_inventory_uuid", "reader_inventory_sha256", "reader_receipts_sha256",
		"final_proof_sha256", "expected_reader_count", "received_reader_count", "reader_epoch_uuid",
		"app_service", "reader_instance", "surface", "candidate_authority_sha256", "prepare_receipt_sha256",
		"commit_receipt_sha256", "generation", "list_count", "detail_course_id", "homepage_count",
		"sitemap_entry_count", "sitemap_sha256", "complete", "recorded_at",
	}
	recordedAt := time.Now().UTC()
	rows := make([]map[string]any, 0, len(receipts))
	for _, receipt := range receipts {
		rows = append(rows, map[string]any{
			"ack_uuid": ackUUID, "ack_revision": activation.Revision, "transition_id": prepared.TransitionID,
			"activation_uuid": activation.ActivationUUID, "run_uuid": activation.RunUUID,
			"source_authority_revision": sourceAuthorityRevision,
			"reader_inventory_revision": prepared.Inventory.Revision, "reader_inventory_uuid": prepared.Inventory.UUID,
			"reader_inventory_sha256": readerInventorySHA, "reader_receipts_sha256": receiptSetSHA,
			"final_proof_sha256": finalProofSHA, "expected_reader_count": readerCount, "received_reader_count": readerCount,
			"reader_epoch_uuid":          receipt.Prepared.Inventory.Reader.EpochUUID,
			"app_service":                receipt.Prepared.Inventory.Reader.Config.AppService,
			"reader_instance":            receipt.Prepared.Inventory.Reader.Config.ReaderInstance,
			"surface":                    lectureSurfaceForService(receipt.Prepared.Inventory.Reader.Config.AppService),
			"candidate_authority_sha256": receipt.CandidateSHA256,
			"prepare_receipt_sha256":     receipt.Prepared.ReceiptSHA256, "commit_receipt_sha256": receipt.ReceiptSHA256,
			"generation": time.UnixMilli(generationForReader(activation, receipt.Prepared.Inventory.Reader.Config.AppService)).In(KST).Format("2006-01-02 15:04:05.000"),
			"list_count": receipt.ListCount, "detail_course_id": receipt.DetailCourseID,
			"homepage_count": receipt.HomepageCount, "sitemap_entry_count": receipt.SitemapEntryCount,
			"sitemap_sha256": receipt.SitemapSHA256, "complete": 1, "recorded_at": utcClickHouseTime(recordedAt),
		})
	}
	payload, err := encodeClickHouseJSONEachRow(columns, rows)
	if err != nil {
		return publicationTransitionRelease{}, stateError("degraded", "public_reader_transition_ack", "reader_ack_encoding_failed")
	}
	token := "lecture-reader-transition-v2-" + ackUUID
	sql := fmt.Sprintf(`INSERT INTO %s (%s)
		SETTINGS insert_distributed_sync=1,insert_quorum=4,insert_quorum_parallel=0,
		insert_deduplicate=1,insert_deduplication_token=%s FORMAT JSONEachRow`,
		lectureTransitionReleaseTable, clickHouseColumnList(columns), QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt <= publicationMutationReconcileAttempts; attempt++ {
		_, insertErr = s.chPost(ctx, sql, payload, "application/x-ndjson")
		if readbackErr := s.readbackPublicationTransitionRelease(ctx, topology, prepared, receipts, activation, sourceAuthorityRevision, ackUUID, readerInventorySHA, receiptSetSHA, finalProofSHA); readbackErr == nil {
			return publicationTransitionRelease{
				TransitionID: prepared.TransitionID, ACKUUID: ackUUID, ACKRevision: activation.Revision,
				ACKReaderCount: readerCount, ReaderInventorySHA256: readerInventorySHA,
				ReaderReceiptsSHA256: receiptSetSHA, FinalProofSHA256: finalProofSHA,
			}, nil
		} else if insertErr != nil && !isTemporaryClickHouseWriteError(insertErr) {
			return publicationTransitionRelease{}, newUpdateReadStateError("public_reader_transition_ack_insert", insertErr)
		} else if attempt == publicationMutationReconcileAttempts {
			return publicationTransitionRelease{}, readbackErr
		}
	}
	return publicationTransitionRelease{}, insertErr
}

func (s *Service) readbackPublicationTransitionRelease(ctx context.Context, topology map[string]int, prepared preparedPublicationTransition, receipts []publicationTransitionCommitReceiptValue, activation activationEvidence, sourceAuthorityRevision uint64, ackUUID, inventorySHA, receiptSetSHA, finalProofSHA string) error {
	if err := s.execPublicRefreshCommand(ctx, "SYSTEM SYNC REPLICA "+lectureTransitionReleaseLocalTable+" STRICT", 2*time.Minute); err != nil {
		return newUpdateReadStateError("public_reader_transition_ack_sync", err)
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,toString(ack_uuid) AS ack_uuid,
		toString(ack_revision) AS ack_revision,toString(transition_id) AS transition_id,
		toString(activation_uuid) AS activation_uuid,toString(run_uuid) AS run_uuid,
		toString(source_authority_revision) AS source_authority_revision,
		toString(reader_inventory_revision) AS reader_inventory_revision,toString(reader_inventory_uuid) AS reader_inventory_uuid,
		toString(reader_inventory_sha256) AS reader_inventory_sha256,toString(reader_receipts_sha256) AS reader_receipts_sha256,
		toString(final_proof_sha256) AS final_proof_sha256,toString(expected_reader_count) AS expected_reader_count,
		toString(received_reader_count) AS received_reader_count,toString(reader_epoch_uuid) AS reader_epoch_uuid,
		toString(app_service) AS app_service,reader_instance,toString(surface) AS surface,
		toString(candidate_authority_sha256) AS candidate_authority_sha256,
		toString(prepare_receipt_sha256) AS prepare_receipt_sha256,toString(commit_receipt_sha256) AS commit_receipt_sha256,
		toUnixTimestamp64Milli(generation) AS generation_ms,toString(list_count) AS list_count,
		toString(detail_course_id) AS detail_course_id,toString(homepage_count) AS homepage_count,
		toString(sitemap_entry_count) AS sitemap_entry_count,toString(sitemap_sha256) AS sitemap_sha256,toString(complete) AS complete
		FROM clusterAllReplicas(%s,'lecture_publication','inflearn_public_catalog_reader_transition_ack_v2_local')
		WHERE transition_id=toUUID(%s) AND ack_uuid=toUUID(%s)
		ORDER BY hostname,app_service,reader_instance
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(prepared.TransitionID), QuoteSQLString(ackUUID)))
	if err != nil {
		return newUpdateReadStateError("public_reader_transition_ack_readback", err)
	}
	expected := make(map[string]publicationTransitionCommitReceiptValue, len(receipts))
	for _, receipt := range receipts {
		expected[receipt.Prepared.Inventory.Reader.Config.AppService+"\x00"+receipt.Prepared.Inventory.Reader.Config.ReaderInstance] = receipt
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		identity := asString(row["app_service"]) + "\x00" + asString(row["reader_instance"])
		receipt, ok := expected[identity]
		key := host + "\x00" + identity
		integerExpected := map[string]uint64{
			"ack_revision": activation.Revision, "source_authority_revision": sourceAuthorityRevision,
			"reader_inventory_revision": prepared.Inventory.Revision,
			"expected_reader_count":     uint64(len(receipts)), "received_reader_count": uint64(len(receipts)),
			"list_count": receipt.ListCount, "detail_course_id": receipt.DetailCourseID,
			"homepage_count": receipt.HomepageCount, "sitemap_entry_count": receipt.SitemapEntryCount, "complete": 1,
		}
		mismatch := topology[host] == 0 || seen[key] || !ok || asString(row["ack_uuid"]) != ackUUID ||
			asString(row["transition_id"]) != prepared.TransitionID || asString(row["activation_uuid"]) != activation.ActivationUUID ||
			asString(row["run_uuid"]) != activation.RunUUID || asString(row["reader_inventory_uuid"]) != prepared.Inventory.UUID ||
			asString(row["reader_inventory_sha256"]) != inventorySHA || asString(row["reader_receipts_sha256"]) != receiptSetSHA ||
			asString(row["final_proof_sha256"]) != finalProofSHA || asString(row["reader_epoch_uuid"]) != receipt.Prepared.Inventory.Reader.EpochUUID ||
			asString(row["surface"]) != lectureSurfaceForService(receipt.Prepared.Inventory.Reader.Config.AppService) ||
			asString(row["candidate_authority_sha256"]) != receipt.CandidateSHA256 ||
			asString(row["prepare_receipt_sha256"]) != receipt.Prepared.ReceiptSHA256 || asString(row["commit_receipt_sha256"]) != receipt.ReceiptSHA256 ||
			asInt64(row["generation_ms"]) != generationForReader(activation, receipt.Prepared.Inventory.Reader.Config.AppService) ||
			asString(row["sitemap_sha256"]) != receipt.SitemapSHA256
		for column, want := range integerExpected {
			got, parseErr := exactUint64(row[column])
			mismatch = mismatch || parseErr != nil || got != want
		}
		if mismatch {
			return stateError("degraded", "public_reader_transition_ack_readback", "reader_ack_evidence_mismatch")
		}
		seen[key] = true
	}
	if len(seen) != len(topology)*len(expected) {
		return stateError("degraded", "public_reader_transition_ack_readback", "reader_ack_endpoint_set_incomplete")
	}
	return nil
}

func (s *Service) finalizePublicationTransitionReaders(ctx context.Context, receipts []publicationTransitionCommitReceiptValue, baseRelease publicationTransitionRelease) error {
	for _, receipt := range receipts {
		release := baseRelease
		release.CandidateAuthoritySHA256 = receipt.CandidateSHA256
		release.CommitReceiptSHA256 = receipt.ReceiptSHA256
		if _, err := finalizePublicationTransitionReader(ctx, receipt, release); err != nil {
			return stateError("degraded", "public_reader_transition_finalize", "reader_finalize_failed")
		}
	}
	return nil
}

func (s *Service) readPublicationTransitionReleaseRows(ctx context.Context, transitionID string, expectedReaders int) (map[string]publicationTransitionRelease, bool, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT app_service,domain,toString(transition_id) AS transition_id,reader_instance,
		toString(candidate_authority_sha256) AS candidate_authority_sha256,
		toString(commit_receipt_sha256) AS commit_receipt_sha256,toString(ack_uuid) AS ack_uuid,
		toString(ack_revision) AS ack_revision,toString(expected_reader_count) AS expected_reader_count,
		toString(received_reader_count) AS received_reader_count,
		toString(reader_inventory_sha256) AS reader_inventory_sha256,
		toString(reader_receipts_sha256) AS reader_receipts_sha256,
		toString(final_proof_sha256) AS final_proof_sha256,toString(complete) AS complete
		FROM Clickhouse_Statground.publication_transition_release_current
		WHERE domain='lecture' AND transition_id=toUUID(%s)
		ORDER BY app_service,reader_instance LIMIT %d
		SETTINGS max_threads=1,max_execution_time=30`, QuoteSQLString(transitionID), expectedReaders+1))
	if err != nil {
		return nil, false, newUpdateReadStateError("public_reader_transition_release", err)
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	if len(rows) != expectedReaders {
		return nil, false, stateError("degraded", "public_reader_transition_release", "release_reader_set_incomplete")
	}
	result := make(map[string]publicationTransitionRelease, len(rows))
	var common publicationTransitionRelease
	for index, row := range rows {
		ackRevision, revisionErr := exactUint64(row["ack_revision"])
		expectedCount, expectedErr := exactUint64(row["expected_reader_count"])
		receivedCount, receivedErr := exactUint64(row["received_reader_count"])
		complete, completeErr := exactUint64(row["complete"])
		release := publicationTransitionRelease{
			TransitionID: asString(row["transition_id"]), CandidateAuthoritySHA256: asString(row["candidate_authority_sha256"]),
			CommitReceiptSHA256: asString(row["commit_receipt_sha256"]), ACKUUID: asString(row["ack_uuid"]),
			ACKRevision: ackRevision, ACKReaderCount: expectedCount,
			ReaderInventorySHA256: asString(row["reader_inventory_sha256"]),
			ReaderReceiptsSHA256:  asString(row["reader_receipts_sha256"]), FinalProofSHA256: asString(row["final_proof_sha256"]),
		}
		identity := asString(row["app_service"]) + "\x00" + asString(row["reader_instance"])
		if asString(row["domain"]) != publicationTransitionDomain || release.TransitionID != transitionID ||
			revisionErr != nil || expectedErr != nil || receivedErr != nil || completeErr != nil || complete != 1 ||
			expectedCount != uint64(expectedReaders) || receivedCount != expectedCount ||
			!isCanonicalUUID(release.ACKUUID) || release.ACKRevision == 0 ||
			!validLowerSHA256(release.CandidateAuthoritySHA256) || !validLowerSHA256(release.CommitReceiptSHA256) ||
			!validLowerSHA256(release.ReaderInventorySHA256) || !validLowerSHA256(release.ReaderReceiptsSHA256) ||
			!validLowerSHA256(release.FinalProofSHA256) || identity == "\x00" || result[identity].TransitionID != "" {
			return nil, false, stateError("degraded", "public_reader_transition_release", "release_evidence_mismatch")
		}
		if index == 0 {
			common = release
		} else if release.ACKUUID != common.ACKUUID || release.ACKRevision != common.ACKRevision ||
			release.ACKReaderCount != common.ACKReaderCount || release.ReaderInventorySHA256 != common.ReaderInventorySHA256 ||
			release.ReaderReceiptsSHA256 != common.ReaderReceiptsSHA256 || release.FinalProofSHA256 != common.FinalProofSHA256 {
			return nil, false, stateError("degraded", "public_reader_transition_release", "release_set_divergence")
		}
		result[identity] = release
	}
	return result, true, nil
}

func (s *Service) readPublicationPointerRevision(ctx context.Context, revision uint64) (map[string]publicationPointer, error) {
	if revision == 0 {
		return map[string]publicationPointer{}, nil
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT surface,
		toUnixTimestamp64Milli(generation) AS generation_ms,toString(marker_uuid) AS marker_uuid,
		toString(run_uuid) AS run_uuid,toString(activation_revision) AS activation_revision,
		toString(activation_uuid) AS activation_uuid,toString(activation_kind) AS activation_kind
		FROM lecture_publication.v_inflearn_public_catalog_activation_valid
		WHERE activation_revision=%d ORDER BY surface SETTINGS max_threads=1,max_execution_time=30`, revision))
	if err != nil {
		return nil, newUpdateReadStateError("public_reader_transition_old_pointer", err)
	}
	if len(rows) != 3 {
		return nil, stateError("degraded", "public_reader_transition_old_pointer", "old_pointer_set_incomplete")
	}
	result := make(map[string]publicationPointer, 3)
	var activationUUID, runUUID, kind string
	for _, row := range rows {
		rowRevision, revisionErr := exactUint64(row["activation_revision"])
		pointer := publicationPointer{
			Surface: asString(row["surface"]), GenerationMS: asInt64(row["generation_ms"]),
			MarkerUUID: strings.ToLower(asString(row["marker_uuid"])), RunUUID: strings.ToLower(asString(row["run_uuid"])),
			ActivationRevision: rowRevision, ActivationUUID: strings.ToLower(asString(row["activation_uuid"])),
			ActivationKind: asString(row["activation_kind"]),
		}
		if revisionErr != nil || rowRevision != revision || !containsString(expectedPublicationSurfaces, pointer.Surface) ||
			pointer.GenerationMS <= 0 || !isCanonicalUUID(pointer.MarkerUUID) || !isCanonicalUUID(pointer.RunUUID) ||
			!isCanonicalUUID(pointer.ActivationUUID) || (pointer.ActivationKind != "publish" && pointer.ActivationKind != "rollback") ||
			result[pointer.Surface].Surface != "" {
			return nil, stateError("degraded", "public_reader_transition_old_pointer", "old_pointer_evidence_mismatch")
		}
		if len(result) == 0 {
			activationUUID, runUUID, kind = pointer.ActivationUUID, pointer.RunUUID, pointer.ActivationKind
		} else if pointer.ActivationUUID != activationUUID || pointer.RunUUID != runUUID || pointer.ActivationKind != kind {
			return nil, stateError("degraded", "public_reader_transition_old_pointer", "old_pointer_set_divergence")
		}
		result[pointer.Surface] = pointer
	}
	return result, nil
}

func activationFromCurrentPointers(pointers map[string]publicationPointer) (activationEvidence, error) {
	activation, present, err := activationFromPublicationPointers(pointers)
	if err != nil || !present {
		return activationEvidence{}, stateError("degraded", "public_reader_transition_reconcile", "current_activation_missing")
	}
	activation.WebRMarkerUUID = pointers["webr"].MarkerUUID
	activation.MirMarkerUUID = pointers["mirtype"].MarkerUUID
	activation.StatgroundMarkerUUID = pointers["statground"].MarkerUUID
	return activation, nil
}

func (s *Service) reconcilePublicationTransitionReaders(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile, lease publicationLease) error {
	inventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return err
	}
	probed := make([]publicationTransitionInventory, 0, len(inventory.Readers))
	transitionIDs := map[string]bool{}
	pending := 0
	for _, reader := range inventory.Readers {
		probeNonce := UUIDv7String(time.Now())
		state, err := discoverPublicationTransitionReader(ctx, reader, probeNonce)
		if err != nil {
			return stateError("degraded", "public_reader_transition_reconcile", "reader_inventory_failed")
		}
		probed = append(probed, state)
		if state.Phase != "steady" {
			pending++
			transitionIDs[state.TransitionID] = true
		}
	}
	if pending == 0 {
		return nil
	}
	if len(transitionIDs) != 1 {
		return stateError("degraded", "public_reader_transition_reconcile", "pending_transition_set_divergence")
	}
	var transitionID string
	for value := range transitionIDs {
		transitionID = value
	}
	states := make([]publicationTransitionInventory, 0, len(probed))
	for _, probe := range probed {
		stableNonce := stablePublicationTransitionUUID("inventory", transitionID, probe.Reader.Config.AppService, probe.Reader.Config.ReaderInstance)
		state, err := discoverPublicationTransitionReader(ctx, probe.Reader.Config, stableNonce)
		if err != nil {
			return stateError("degraded", "public_reader_transition_reconcile", "stable_reader_inventory_failed")
		}
		states = append(states, state)
	}
	releaseRows, released, err := s.readPublicationTransitionReleaseRows(ctx, transitionID, len(inventory.Readers))
	if err != nil {
		return err
	}
	if released {
		for _, state := range states {
			identity := state.Reader.Config.AppService + "\x00" + state.Reader.Config.ReaderInstance
			release, ok := releaseRows[identity]
			if !ok {
				return stateError("degraded", "public_reader_transition_reconcile", "release_reader_missing")
			}
			if state.Phase == "steady" {
				if state.LastFinalizedTransitionID != transitionID {
					return stateError("degraded", "public_reader_transition_reconcile", "unexpected_steady_reader")
				}
				continue
			}
			if state.Phase != "committed" || state.CommitReceiptSHA256 != release.CommitReceiptSHA256 ||
				state.CandidateAuthoritySHA256 != release.CandidateAuthoritySHA256 {
				return stateError("degraded", "public_reader_transition_reconcile", "released_reader_state_mismatch")
			}
			prepared := publicationTransitionPrepareReceiptValue{
				Inventory: state, TransitionID: transitionID,
				PrepareNonce:             stablePublicationTransitionUUID("prepare", transitionID, state.Reader.Config.AppService, state.Reader.Config.ReaderInstance, state.Reader.EpochUUID),
				CandidateAuthoritySHA256: state.CandidateAuthoritySHA256, ReceiptSHA256: state.PrepareReceiptSHA256,
			}
			committed := publicationTransitionCommitReceiptValue{Prepared: prepared, CandidateSHA256: state.CandidateAuthoritySHA256, ReceiptSHA256: state.CommitReceiptSHA256}
			if _, err := finalizePublicationTransitionReader(ctx, committed, release); err != nil {
				return stateError("degraded", "public_reader_transition_reconcile", "reader_finalize_resume_failed")
			}
		}
		return nil
	}
	currentPointers, _, err := s.readPublicationPointer(ctx, "public_reader_transition_reconcile_pointer")
	if err != nil {
		return err
	}
	currentActivation, activationErr := activationFromCurrentPointers(currentPointers)
	currentIsCandidate := activationErr == nil && publicationTransitionID(currentActivation) == transitionID
	if !currentIsCandidate {
		var servingSourceRevision uint64
		if len(currentPointers) != 0 {
			servingActivation, activationErr := activationFromCurrentPointers(currentPointers)
			if activationErr != nil {
				return activationErr
			}
			servingSourceRevision, err = s.resolveTransitionServingSourceAuthority(ctx, servingActivation, states)
			if err != nil {
				return err
			}
		}
		for _, state := range states {
			if state.Phase == "steady" {
				continue
			}
			if state.Phase != "prepared" {
				return stateError("degraded", "public_reader_transition_reconcile", "committed_transition_not_current")
			}
			surface := lectureSurfaceForService(state.Reader.Config.AppService)
			pointer := currentPointers[surface]
			oldSourceRevision := servingSourceRevision
			oldAuthority := oldTransitionAuthorityForReader(pointer, oldSourceRevision, state.Reader.Config.AppService)
			oldDigest, _ := canonicalJSONSHA256(oldAuthority.jsonValue())
			if state.OldAuthoritySHA256 != oldDigest || state.ServingAuthoritySHA256 != oldDigest {
				return stateError("degraded", "public_reader_transition_reconcile", "old_authority_changed_before_abort")
			}
			if _, err := abortPublicationTransitionReader(ctx, state); err != nil {
				return stateError("degraded", "public_reader_transition_reconcile", "reader_abort_resume_failed")
			}
		}
		return nil
	}
	candidateSourceAuthorityRevision, err := s.resolveTransitionCandidateSourceAuthority(ctx, currentActivation, states)
	if err != nil {
		return err
	}
	for _, state := range states {
		if state.Phase == "steady" {
			return stateError("degraded", "public_reader_transition_reconcile", "unreleased_steady_reader")
		}
		candidate := transitionAuthorityForReader(currentActivation, candidateSourceAuthorityRevision, state.Reader.Config.AppService)
		candidateDigest, _ := canonicalJSONSHA256(candidate.jsonValue())
		if candidateDigest != state.CandidateAuthoritySHA256 {
			return stateError("degraded", "public_reader_transition_reconcile", "candidate_authority_changed")
		}
	}
	oldPointers, err := s.readPublicationPointerRevision(ctx, currentActivation.Revision-1)
	if err != nil {
		return err
	}
	var oldSourceAuthorityRevision uint64
	if currentActivation.Revision > 1 {
		oldActivation, activationErr := activationFromCurrentPointers(oldPointers)
		if activationErr != nil {
			return activationErr
		}
		oldStates := make([]publicationTransitionInventory, len(states))
		copy(oldStates, states)
		for index := range oldStates {
			oldStates[index].ServingAuthoritySHA256 = oldStates[index].OldAuthoritySHA256
		}
		oldSourceAuthorityRevision, err = s.resolveTransitionServingSourceAuthority(ctx, oldActivation, oldStates)
		if err != nil {
			return err
		}
	}
	prepared := preparedPublicationTransition{TransitionID: transitionID, Inventory: inventory}
	for _, state := range states {
		surface := lectureSurfaceForService(state.Reader.Config.AppService)
		oldAuthority := oldTransitionAuthorityForReader(oldPointers[surface], oldSourceAuthorityRevision, state.Reader.Config.AppService)
		candidate := transitionAuthorityForReader(currentActivation, candidateSourceAuthorityRevision, state.Reader.Config.AppService)
		receipt, err := preparePublicationTransitionReader(ctx, state, transitionID, oldAuthority, candidate)
		if err != nil {
			return stateError("degraded", "public_reader_transition_reconcile", "reader_prepare_resume_failed")
		}
		prepared.Readers = append(prepared.Readers, receipt)
	}
	committed, err := s.commitPublicationTransitionReaders(ctx, prepared, currentActivation, candidateSourceAuthorityRevision)
	if err != nil {
		return err
	}
	if err := s.proveCommittedPublicationTransitionReaders(ctx, prepared, committed); err != nil {
		return err
	}
	generations := map[string]int64{
		"webr": currentActivation.WebRGenerationMS, "mirtype": currentActivation.MirGenerationMS,
		"statground": currentActivation.StatgroundGenerationMS,
	}
	proof, err := s.runPublicationCandidatePreflight(ctx, generations, lease)
	if err != nil {
		return err
	}
	for _, surface := range expectedPublicationSurfaces {
		if proof[surface].GenerationMS != generations[surface] || proof[surface].SourceAuthorityRevision != candidateSourceAuthorityRevision {
			return stateError("degraded", "public_reader_transition_reconcile", "candidate_reproof_failed")
		}
	}
	if err := s.verifyActivatedPointer(ctx, currentActivation); err != nil {
		return err
	}
	release, err := s.appendAndReadbackPublicationTransitionRelease(ctx, topology, prepared, committed, currentActivation, candidateSourceAuthorityRevision)
	if err != nil {
		return err
	}
	return s.finalizePublicationTransitionReaders(ctx, committed, release)
}
