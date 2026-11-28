package inflearn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
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

func TestPublicationTransitionInventoryAcceptsDurableRecoveryPhases(t *testing.T) {
	oldDigest := strings.Repeat("a", 64)
	candidateDigest := strings.Repeat("b", 64)
	prepareDigest := strings.Repeat("c", 64)
	proofDigest := strings.Repeat("d", 64)
	commitDigest := strings.Repeat("e", 64)
	transitionID := stablePublicationTransitionUUID("transition", testActivationUUID)
	inventoryNonce := stablePublicationTransitionUUID("inventory", transitionID, "web-r", "web-r-test-1")
	phase := "steady"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		value := map[string]any{
			"format": publicationTransitionInventoryFormat, "app_service": "web-r", "domain": "lecture",
			"reader_instance": "web-r-test-1", "reader_epoch_uuid": testReaderEpoch,
			"inventory_nonce": inventoryNonce, "phase": phase, "admission_open": phase == "steady",
			"transition_id": transitionID, "serving_authority_sha256": oldDigest,
			"old_authority_sha256": oldDigest, "candidate_authority_sha256": candidateDigest,
			"prepare_receipt_sha256": "", "commit_receipt_sha256": "", "old_state_proof_sha256": "",
			"last_finalized_transition_id": "", "last_finalize_receipt_sha256": "",
			"inflight": 0, "observed_at": time.Now().UTC().Format(time.RFC3339Nano),
		}
		switch phase {
		case "steady":
			value["transition_id"], value["old_authority_sha256"], value["candidate_authority_sha256"] = "", "", ""
		case "prepared":
			value["prepare_receipt_sha256"], value["old_state_proof_sha256"] = prepareDigest, proofDigest
		case "committing":
			value["serving_authority_sha256"] = candidateDigest
			value["prepare_receipt_sha256"], value["old_state_proof_sha256"] = prepareDigest, proofDigest
		case "committed":
			value["serving_authority_sha256"] = candidateDigest
			value["prepare_receipt_sha256"], value["old_state_proof_sha256"] = prepareDigest, proofDigest
			value["commit_receipt_sha256"] = commitDigest
		case "recovery_hold":
			value["transition_id"], value["serving_authority_sha256"] = "", ""
			value["old_authority_sha256"], value["candidate_authority_sha256"] = "", ""
		}
		_ = json.NewEncoder(writer).Encode(value)
	}))
	defer server.Close()
	reader := lectureReaderConfig{
		AppService: "web-r", ReaderInstance: "web-r-test-1",
		InventoryEndpoint: server.URL, BearerToken: testReaderBearer,
	}
	for _, want := range []string{"steady", "preparing", "prepared", "committing", "committed", "recovery_hold"} {
		phase = want
		got, err := discoverPublicationTransitionReader(context.Background(), reader, inventoryNonce)
		if err != nil {
			t.Fatalf("phase %s: %v", want, err)
		}
		if got.Phase != want || got.AdmissionOpen != (want == "steady") {
			t.Fatalf("phase %s inventory=%+v", want, got)
		}
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
	inventory, err := discoverPublicationTransitionReader(context.Background(), reader, inventoryNonce)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparePublicationTransitionReader(context.Background(), inventory, transitionID, oldAuthority, candidate)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := commitPublicationTransitionReader(context.Background(), prepared, candidate)
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
	if _, err := finalizePublicationTransitionReader(context.Background(), committed, release); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationTransitionAbortRequiresPreparedProof(t *testing.T) {
	transitionID := stablePublicationTransitionUUID("transition", testActivationUUID)
	oldDigest := strings.Repeat("a", 64)
	oldProof := strings.Repeat("b", 64)
	expectedRequestProof := oldProof
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		value := decodeTransitionRequest(t, request)
		if got := asString(value["old_state_proof_sha256"]); got != expectedRequestProof {
			t.Errorf("abort request proof=%q, want %q", got, expectedRequestProof)
		}
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
	if _, err := abortPublicationTransitionReader(context.Background(), inventory); err != nil {
		t.Fatal(err)
	}
	inventory.Phase = "preparing"
	inventory.OldStateProofSHA256 = ""
	expectedRequestProof = ""
	if _, err := abortPublicationTransitionReader(context.Background(), inventory); err != nil {
		t.Fatalf("preparing recovery abort: %v", err)
	}
	inventory.Phase = "committed"
	if _, err := abortPublicationTransitionReader(context.Background(), inventory); err == nil {
		t.Fatal("post-COMMIT abort was accepted")
	}
}

func TestPublicationTransitionInventoryDriftAbortsEveryPreparedReader(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "inventory_drift"
		if interrupted {
			name = "canceled_proof_and_first_abort_rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var preparedCount, abortAttempts, abortSuccesses atomic.Int32
			cfg := lectureReaderConfigFile{ReaderInventoryRevision: 2}
			for _, service := range []string{"web-r", "mirtype", "statground"} {
				reader := lectureReaderConfig{AppService: service, ReaderInstance: service + "-1", BearerToken: testReaderBearer}
				old := oldTransitionAuthorityForReader(publicationPointer{}, 0, service)
				oldDigest, _ := canonicalJSONSHA256(old.jsonValue())
				oldProof := strings.Repeat("b", 64)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					now := time.Now().UTC().Format(time.RFC3339Nano)
					response := map[string]any{
						"app_service": service, "domain": "lecture", "reader_instance": reader.ReaderInstance,
						"reader_epoch_uuid": testReaderEpoch,
					}
					if r.Method == http.MethodGet && r.URL.Path == publicationTransitionRoot {
						response["format"], response["inventory_nonce"] = publicationTransitionInventoryFormat, r.URL.Query().Get("inventory_nonce")
						response["phase"], response["admission_open"], response["inflight"], response["observed_at"] = "steady", true, 1, now
						response["serving_authority_sha256"] = oldDigest
						for _, key := range []string{"transition_id", "old_authority_sha256", "candidate_authority_sha256",
							"prepare_receipt_sha256", "commit_receipt_sha256", "old_state_proof_sha256",
							"last_finalized_transition_id", "last_finalize_receipt_sha256"} {
							response[key] = ""
						}
						_ = json.NewEncoder(w).Encode(response)
						return
					}
					request := decodeTransitionRequest(t, r)
					response["inventory_nonce"], response["transition_id"] = request["inventory_nonce"], request["transition_id"]
					response["old_authority_sha256"], response["old_state_proof_sha256"] = oldDigest, oldProof
					switch r.URL.Path {
					case publicationTransitionRoot + "/prepare":
						preparedCount.Add(1)
						candidateDigest, _ := canonicalJSONSHA256(request["candidate_authority"])
						response["format"], response["phase"] = publicationTransitionPrepareReceipt, "prepared"
						response["prepare_nonce"], response["candidate_authority_sha256"] = request["prepare_nonce"], candidateDigest
						response["inflight_before"], response["inflight_after"], response["prepared_at"] = 1, 0, now
					case publicationTransitionRoot + "/abort":
						abortAttempts.Add(1)
						if interrupted && service == "web-r" {
							http.Error(w, "old authority no longer matches", http.StatusConflict)
							return
						}
						abortSuccesses.Add(1)
						response["format"], response["phase"] = publicationTransitionAbortReceipt, "steady"
						response["abort_nonce"], response["aborted_at"] = request["abort_nonce"], now
					default:
						t.Errorf("unexpected reader action %s", r.URL.Path)
						http.Error(w, "unexpected reader action", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(hashedTransitionReceipt(t, response))
				}))
				t.Cleanup(server.Close)
				reader.InventoryEndpoint = server.URL + "/internal/book-publication/drain"
				reader.inventoryEndpointSHA, _ = canonicalJSONSHA256(reader.InventoryEndpoint)
				cfg.Readers = append(cfg.Readers, reader)
			}
			topology := make(map[string]int)
			var authorityRows, inventoryRows []map[string]any
			for i, host := range publicationHosts() {
				topology[host] = i + 1
				authorityRows = append(authorityRows, map[string]any{
					"hostname": host, "inventory_revision": "2", "inventory_uuid": testInventoryUUID,
					"reader_count": "3", "complete": "1",
				})
				for _, reader := range cfg.Readers {
					inventoryRows = append(inventoryRows, map[string]any{
						"hostname": host, "inventory_revision": "2", "inventory_uuid": testInventoryUUID,
						"app_service": reader.AppService, "reader_instance": reader.ReaderInstance,
						"endpoint_sha256": reader.inventoryEndpointSHA,
					})
				}
			}
			var inventoryReads atomic.Int32
			db := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				query := strings.TrimSpace(string(body))
				switch {
				case query == "SYSTEM SYNC REPLICA "+readerInventoryLocalTable+" STRICT",
					query == "SYSTEM SYNC REPLICA "+readerInventoryMarkerTable+" STRICT":
					w.WriteHeader(http.StatusOK)
				case strings.Contains(query, "'v_book_publication_reader_inventory_authority'"):
					if inventoryReads.Add(1) > 1 {
						if interrupted {
							cancel()
						}
						writeCHRows(w, nil)
						return
					}
					writeCHRows(w, authorityRows)
				case strings.Contains(query, "'v_book_publication_reader_inventory_current'"):
					writeCHRows(w, inventoryRows)
				default:
					t.Errorf("unexpected database statement: %s", query)
					http.Error(w, "unexpected SQL", http.StatusBadRequest)
				}
			}))
			t.Cleanup(db.Close)
			activation := activationEvidence{
				Revision: 1, ActivationUUID: testActivationUUID, RunUUID: testActivationRun,
				WebRGenerationMS: 1700000000123, MirGenerationMS: 1700000000123, StatgroundGenerationMS: 1700000000123,
			}
			_, err := publicRefreshTestService(t, db).preparePublicationTransitionReaders(ctx, topology, cfg, nil, activation, 1)
			if interrupted {
				assertUpdateStateError(t, err, "degraded", "public_reader_transition_abort", "preactivation_abort_failed")
			} else {
				assertUpdateStateError(t, err, "degraded", "public_reader_transition_prepare", "reader_inventory_changed")
			}
			wantSuccesses := int32(3)
			if interrupted {
				wantSuccesses = 2
			}
			if preparedCount.Load() != 3 || abortAttempts.Load() != 3 || abortSuccesses.Load() != wantSuccesses {
				t.Fatalf("prepared=%d aborts=%d successful aborts=%d", preparedCount.Load(), abortAttempts.Load(), abortSuccesses.Load())
			}
		})
	}
}

func TestCheckPublicationReadersUsesOnlySelectsAndAuthenticatedInventoryGETs(t *testing.T) {
	const laterInventory = "01994602-0000-7000-8000-000000000116"
	for _, changed := range []bool{false, true} {
		name := "four_registered_readers"
		if changed {
			name = "inventory_changed_during_check"
		}
		t.Run(name, func(t *testing.T) {
			var gets, authorityReads atomic.Int32
			cfg := lectureReaderConfigFile{ReaderInventoryRevision: 2}
			services := map[string]string{"web-r-1": "web-r", "web-r-2": "web-r", "mirtype-1": "mirtype", "statground-1": "statground"}
			for instance, service := range services {
				reader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nonce := r.URL.Query().Get("inventory_nonce")
					if r.Method != http.MethodGet || r.URL.Path != publicationTransitionRoot ||
						r.Header.Get("Authorization") != "Bearer "+testReaderBearer || !isCanonicalUUID(nonce) {
						t.Error("unexpected reader inventory request")
						http.Error(w, "unexpected request", http.StatusBadRequest)
						return
					}
					gets.Add(1)
					phase := "steady"
					if service == "mirtype" {
						phase = "recovery_hold"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"format": publicationTransitionInventoryFormat, "app_service": service, "domain": "lecture",
						"reader_instance": instance, "reader_epoch_uuid": testReaderEpoch, "inventory_nonce": nonce,
						"phase": phase, "admission_open": phase == "steady", "inflight": 0,
						"observed_at": time.Now().UTC().Format(time.RFC3339Nano), "transition_id": "",
						"serving_authority_sha256": "", "old_authority_sha256": "", "candidate_authority_sha256": "",
						"prepare_receipt_sha256": "", "commit_receipt_sha256": "", "old_state_proof_sha256": "",
						"last_finalized_transition_id": "", "last_finalize_receipt_sha256": "",
					})
				}))
				t.Cleanup(reader.Close)
				cfg.Readers = append(cfg.Readers, lectureReaderConfig{
					AppService: service, ReaderInstance: instance, BearerToken: testReaderBearer,
					InventoryEndpoint: reader.URL + "/internal/book-publication/drain", RefreshEndpoint: reader.URL + lectureReaderRefreshPath,
				})
			}
			path := writeReaderConfig(t, 0o600, cfg)
			configured, err := loadLectureReaderConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			db := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				query := strings.TrimSpace(string(body))
				if !strings.HasPrefix(query, "SELECT ") {
					t.Errorf("read-only check issued non-SELECT SQL: %s", query)
					http.Error(w, "unexpected SQL", http.StatusBadRequest)
					return
				}
				var rows []map[string]any
				switch {
				case strings.Contains(query, "FROM system.clusters"):
					rows = publicationTopologyRows()
				case strings.Contains(query, "'v_book_publication_reader_inventory_authority'"):
					revisionUUID := testInventoryUUID
					if authorityReads.Add(1) == 2 && changed {
						revisionUUID = laterInventory
					}
					for _, host := range publicationHosts() {
						rows = append(rows, map[string]any{"hostname": host, "inventory_revision": "2",
							"inventory_uuid": revisionUUID, "reader_count": "4", "complete": "1"})
					}
				case strings.Contains(query, "'v_book_publication_reader_inventory_current'"):
					if !strings.Contains(query, "'v_book_publication_reader_inventory_current') AS registry") ||
						!strings.Contains(query, "WHERE registry.inventory_revision=2 AND registry.inventory_uuid=toUUID(") {
						t.Errorf("inventory filters must use the typed source columns: %s", query)
					}
					revisionUUID := testInventoryUUID
					if authorityReads.Load() == 2 && changed {
						revisionUUID = laterInventory
					}
					for _, host := range publicationHosts() {
						for _, reader := range configured.Readers {
							rows = append(rows, map[string]any{"hostname": host, "inventory_revision": "2",
								"inventory_uuid": revisionUUID, "app_service": reader.AppService, "reader_instance": reader.ReaderInstance,
								"endpoint_sha256": reader.inventoryEndpointSHA})
						}
					}
				default:
					t.Errorf("unexpected SQL: %s", query)
					http.Error(w, "unexpected SQL", http.StatusBadRequest)
					return
				}
				writeCHRows(w, rows)
			}))
			t.Cleanup(db.Close)
			svc := publicRefreshTestService(t, db)
			svc.Cfg.PublicationReaderConfig = path
			got, err := svc.CheckPublicationReaders(context.Background())
			if gets.Load() != 4 || authorityReads.Load() != 2 {
				t.Fatalf("observed %d readers and %d inventory readbacks", gets.Load(), authorityReads.Load())
			}
			if changed {
				assertUpdateStateError(t, err, "degraded", "public_reader_transition_inventory", "reader_inventory_changed")
				return
			}
			if err != nil || got.InventoryRevision != 2 || got.InventoryUUID != testInventoryUUID || len(got.Readers) != 4 {
				t.Fatalf("preflight=%+v, err=%v", got, err)
			}
			for _, row := range got.Readers {
				admitted := row.AppService != "mirtype"
				if row.EpochUUID != testReaderEpoch || row.AdmissionOpen != admitted ||
					(admitted && row.Phase != "steady") || (!admitted && row.Phase != "recovery_hold") {
					t.Fatalf("unexpected observation %+v", row)
				}
			}
			output, marshalErr := json.Marshal(got)
			if marshalErr != nil || strings.Contains(string(output), testReaderBearer) || strings.Contains(string(output), "endpoint") {
				t.Fatal("reader preflight must emit process evidence without credentials or endpoint URLs")
			}
		})
	}
}
