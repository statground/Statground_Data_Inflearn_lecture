package inflearn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func hashedTransitionReceipt(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	digest, err := canonicalReceiptSHA256(value)
	if err != nil {
		t.Fatal(err)
	}
	value["receipt_sha256"] = digest
	return value
}

func decodeTransitionRequest(t *testing.T, request *http.Request) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(request.Body)
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestPublicationTransitionV2ContractLiteralsAndDeterministicIDs(t *testing.T) {
	if publicationTransitionRoot != "/internal/lecture-publication/transition" ||
		publicationTransitionInventoryFormat != "statground.publication-reader-transition-inventory.v2" ||
		publicationTransitionPrepareRequest != "statground.publication-reader-transition-prepare-request.v2" ||
		publicationTransitionPrepareReceipt != "statground.publication-reader-transition-prepare-receipt.v2" ||
		publicationTransitionCommitRequest != "statground.publication-reader-transition-commit-request.v2" ||
		publicationTransitionCommitReceipt != "statground.publication-reader-transition-commit-receipt.v2" ||
		publicationTransitionFinalizeRequest != "statground.publication-reader-transition-finalize-request.v2" ||
		publicationTransitionFinalizeReceipt != "statground.publication-reader-transition-finalize-receipt.v2" ||
		publicationTransitionAbortRequest != "statground.publication-reader-transition-abort-request.v2" ||
		publicationTransitionAbortReceipt != "statground.publication-reader-transition-abort-receipt.v2" {
		t.Fatal("publication transition v2 literals drifted")
	}
	first := stablePublicationTransitionUUID("prepare", testActivationUUID, "web-r", "web-r-test-1", testReaderEpoch)
	second := stablePublicationTransitionUUID("prepare", testActivationUUID, "web-r", "web-r-test-1", testReaderEpoch)
	different := stablePublicationTransitionUUID("commit", testActivationUUID, "web-r", "web-r-test-1", testReaderEpoch)
	if first != second || first == different || !isCanonicalUUID(first) {
		t.Fatalf("deterministic IDs=%q/%q/%q", first, second, different)
	}
	wantInventory := stringSet(
		"format", "app_service", "domain", "reader_instance", "reader_epoch_uuid", "inventory_nonce",
		"phase", "admission_open", "transition_id", "serving_authority_sha256", "old_authority_sha256",
		"candidate_authority_sha256", "prepare_receipt_sha256", "commit_receipt_sha256",
		"old_state_proof_sha256", "last_finalized_transition_id", "last_finalize_receipt_sha256",
		"inflight", "observed_at",
	)
	if !reflect.DeepEqual(publicationTransitionInventoryKeys, wantInventory) {
		t.Fatalf("inventory keys=%v", publicationTransitionInventoryKeys)
	}
}

func TestPublicationTransitionPrepareCommitFinalizeExactFlow(t *testing.T) {
	oldAuthority := publicationTransitionAuthority{
		ActivationRevision: 7, ActivationUUID: testActivationUUID, RunUUID: testActivationRun,
		SourceAuthorityRevision: 9, Surface: "webr", GenerationMS: 1700000000000,
	}
	candidate := publicationTransitionAuthority{
		ActivationRevision: 8, ActivationUUID: "01994602-0000-7000-8000-000000000121",
		RunUUID: "01994602-0000-7000-8000-000000000122", SourceAuthorityRevision: 9,
		Surface: "webr", GenerationMS: 1700000000123,
	}
	oldDigest, _ := canonicalJSONSHA256(oldAuthority.jsonValue())
	candidateDigest, _ := canonicalJSONSHA256(candidate.jsonValue())
	transitionID := stablePublicationTransitionUUID("transition", "8", candidate.ActivationUUID, candidate.RunUUID)
	inventoryNonce := stablePublicationTransitionUUID("inventory", transitionID, "web-r", "web-r-test-1")
	oldStateProof := strings.Repeat("b", 64)
	sitemapDigest := strings.Repeat("c", 64)
	var prepareReceiptSHA, commitReceiptSHA string

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+testReaderBearer || request.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("headers=%v", request.Header)
		}
		writer.Header().Set("Content-Type", "application/json")
		now := time.Now().UTC()
		switch request.URL.Path {
		case publicationTransitionRoot:
			if request.Method != http.MethodGet || request.URL.Query().Get("inventory_nonce") != inventoryNonce {
				t.Errorf("inventory request=%s %v", request.Method, request.URL)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"format": publicationTransitionInventoryFormat, "app_service": "web-r", "domain": "lecture",
				"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
				"inventory_nonce": inventoryNonce, "phase": "steady", "admission_open": true,
				"transition_id": "", "serving_authority_sha256": oldDigest, "old_authority_sha256": "",
				"candidate_authority_sha256": "", "prepare_receipt_sha256": "", "commit_receipt_sha256": "",
				"old_state_proof_sha256": "", "last_finalized_transition_id": "", "last_finalize_receipt_sha256": "",
				"inflight": 0, "observed_at": now.Format(time.RFC3339Nano),
			})
		case publicationTransitionRoot + "/prepare":
			value := decodeTransitionRequest(t, request)
			if len(value) != 11 || asString(value["format"]) != publicationTransitionPrepareRequest ||
				asString(value["transition_id"]) != transitionID || asString(value["reader_epoch_uuid"]) != testReaderEpoch {
				t.Errorf("prepare request=%v", value)
			}
			prepareNonce := asString(value["prepare_nonce"])
			receipt := hashedTransitionReceipt(t, map[string]any{
				"format": publicationTransitionPrepareReceipt, "app_service": "web-r", "domain": "lecture",
				"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
				"inventory_nonce": inventoryNonce, "transition_id": transitionID, "prepare_nonce": prepareNonce,
				"phase": "prepared", "old_authority_sha256": oldDigest, "candidate_authority_sha256": candidateDigest,
				"inflight_before": 3, "inflight_after": 0, "old_state_proof_sha256": oldStateProof,
				"prepared_at": now.Format(time.RFC3339Nano),
			})
			prepareReceiptSHA = asString(receipt["receipt_sha256"])
			_ = json.NewEncoder(writer).Encode(receipt)
		case publicationTransitionRoot + "/commit":
			value := decodeTransitionRequest(t, request)
			if len(value) != 12 || asString(value["format"]) != publicationTransitionCommitRequest ||
				asString(value["prepare_receipt_sha256"]) != prepareReceiptSHA {
				t.Errorf("commit request=%v", value)
			}
			commitNonce := asString(value["commit_nonce"])
			receipt := hashedTransitionReceipt(t, map[string]any{
				"format": publicationTransitionCommitReceipt, "app_service": "web-r", "domain": "lecture",
				"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
				"inventory_nonce": inventoryNonce, "transition_id": transitionID, "commit_nonce": commitNonce,
				"phase": "committed", "candidate_authority_sha256": candidateDigest,
				"prepare_receipt_sha256": prepareReceiptSHA, "cache_entries_removed": 4,
				"list_count": 40, "detail_course_id": 123, "homepage_count": 8,
				"sitemap_entry_count": 40, "sitemap_sha256": sitemapDigest,
				"refresh_started_at": now.Format(time.RFC3339Nano), "refreshed_at": now.Add(time.Second).Format(time.RFC3339Nano),
			})
			commitReceiptSHA = asString(receipt["receipt_sha256"])
			_ = json.NewEncoder(writer).Encode(receipt)
		case publicationTransitionRoot + "/finalize":
			value := decodeTransitionRequest(t, request)
			if len(value) != 17 || asString(value["format"]) != publicationTransitionFinalizeRequest ||
				asString(value["commit_receipt_sha256"]) != commitReceiptSHA {
				t.Errorf("finalize request=%v", value)
			}
			receipt := hashedTransitionReceipt(t, map[string]any{
				"format": publicationTransitionFinalizeReceipt, "app_service": "web-r", "domain": "lecture",
				"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
				"inventory_nonce": inventoryNonce, "transition_id": transitionID,
				"finalize_nonce": asString(value["finalize_nonce"]), "phase": "steady",
				"candidate_authority_sha256": candidateDigest, "commit_receipt_sha256": commitReceiptSHA,
				"ack_uuid": asString(value["ack_uuid"]), "ack_revision": 8, "ack_reader_count": 3,
				"reader_inventory_sha256": asString(value["reader_inventory_sha256"]),
				"reader_receipts_sha256":  asString(value["reader_receipts_sha256"]),
				"final_proof_sha256":      asString(value["final_proof_sha256"]),
				"finalized_at":            now.Format(time.RFC3339Nano),
			})
			_ = json.NewEncoder(writer).Encode(receipt)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	reader := lectureReaderConfig{
		AppService: "web-r", ReaderInstance: "web-r-test-1",
		InventoryEndpoint: server.URL + "/internal/book-publication/drain",
		RefreshEndpoint:   server.URL + lectureReaderRefreshPath, BearerToken: testReaderBearer,
	}
	inventory, err := discoverPublicationTransitionReader(t.Context(), reader, inventoryNonce)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparePublicationTransitionReader(t.Context(), inventory, transitionID, oldAuthority, candidate)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := commitPublicationTransitionReader(t.Context(), prepared, candidate)
	if err != nil {
		t.Fatal(err)
	}
	release := publicationTransitionRelease{
		TransitionID: transitionID, CandidateAuthoritySHA256: candidateDigest,
		CommitReceiptSHA256: committed.ReceiptSHA256,
		ACKUUID:             stablePublicationTransitionUUID("ack", transitionID), ACKRevision: 8, ACKReaderCount: 3,
		ReaderInventorySHA256: strings.Repeat("d", 64), ReaderReceiptsSHA256: strings.Repeat("e", 64),
		FinalProofSHA256: strings.Repeat("f", 64),
	}
	if _, err := finalizePublicationTransitionReader(t.Context(), committed, release); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationTransitionAbortRequiresPreparedProof(t *testing.T) {
	transitionID := stablePublicationTransitionUUID("transition", testActivationUUID)
	oldDigest := strings.Repeat("a", 64)
	oldProof := strings.Repeat("b", 64)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		value := decodeTransitionRequest(t, request)
		now := time.Now().UTC()
		receipt := hashedTransitionReceipt(t, map[string]any{
			"format": publicationTransitionAbortReceipt, "app_service": "web-r", "domain": "lecture",
			"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
			"inventory_nonce": testRefreshNonce, "transition_id": transitionID,
			"abort_nonce": asString(value["abort_nonce"]), "phase": "steady",
			"old_authority_sha256": oldDigest, "old_state_proof_sha256": oldProof,
			"aborted_at": now.Format(time.RFC3339Nano),
		})
		_ = json.NewEncoder(writer).Encode(receipt)
	}))
	defer server.Close()
	inventory := publicationTransitionInventory{
		Reader: preparedLectureReader{Config: lectureReaderConfig{
			AppService: "web-r", ReaderInstance: "web-r-test-1",
			InventoryEndpoint: server.URL + "/internal/book-publication/drain", BearerToken: testReaderBearer,
		}, EpochUUID: testReaderEpoch},
		InventoryNonce: testRefreshNonce, Phase: "prepared", TransitionID: transitionID,
		OldAuthoritySHA256: oldDigest, OldStateProofSHA256: oldProof,
	}
	if _, err := abortPublicationTransitionReader(t.Context(), inventory); err != nil {
		t.Fatal(err)
	}
	inventory.Phase = "committed"
	if _, err := abortPublicationTransitionReader(t.Context(), inventory); err == nil {
		t.Fatal("post-COMMIT abort was accepted")
	}
}
