package inflearn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testReaderEpoch    = "01994602-0000-7000-8000-000000000111"
	testRefreshNonce   = "01994602-0000-7000-8000-000000000112"
	testActivationUUID = "01994602-0000-7000-8000-000000000113"
	testActivationRun  = "01994602-0000-7000-8000-000000000114"
	testInventoryUUID  = "01994602-0000-7000-8000-000000000115"
	testReaderBearer   = "0123456789abcdef0123456789abcdef"
)

func writeReaderConfig(t *testing.T, mode os.FileMode, value any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "reader-refresh.json")
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func readerConfigValue() map[string]any {
	origins := []string{
		"http://127.0.0.1:18080",
		"http://127.0.0.2:18080",
		"http://127.0.0.3:18080",
	}
	readers := make([]map[string]any, 0, 3)
	for i, service := range []string{"web-r", "mirtype", "statground"} {
		readers = append(readers, map[string]any{
			"app_service": service, "reader_instance": service + "-test-1",
			"inventory_endpoint": origins[i] + "/internal/book-publication/drain",
			"refresh_endpoint":   origins[i] + lectureReaderRefreshPath,
			"bearer_token":       testReaderBearer, "ca_file": nil,
		})
	}
	return map[string]any{"reader_inventory_revision": 7, "readers": readers}
}

func TestLoadLectureReaderConfigRequiresPrivateExactInventory(t *testing.T) {
	path := writeReaderConfig(t, 0o600, readerConfigValue())
	cfg, err := loadLectureReaderConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReaderInventoryRevision != 7 || len(cfg.Readers) != 3 {
		t.Fatalf("config=%+v", cfg)
	}
	for _, reader := range cfg.Readers {
		if len(reader.inventoryEndpointSHA) != 64 || lectureSurfaceForService(reader.AppService) == "" {
			t.Fatalf("reader=%+v", reader)
		}
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLectureReaderConfig(path); err == nil || !strings.Contains(err.Error(), "reader_refresh_config_not_private_regular_file") {
		t.Fatalf("permission error=%v", err)
	}
}

func TestLoadLectureReaderConfigRejectsUnknownAndCrossOriginEndpoints(t *testing.T) {
	value := readerConfigValue()
	value["unknown"] = true
	if _, err := loadLectureReaderConfig(writeReaderConfig(t, 0o600, value)); err == nil || !strings.Contains(err.Error(), "invalid_json") {
		t.Fatalf("unknown field error=%v", err)
	}
	value = readerConfigValue()
	value["readers"].([]map[string]any)[0]["refresh_endpoint"] = "http://127.0.0.9:18080" + lectureReaderRefreshPath
	if _, err := loadLectureReaderConfig(writeReaderConfig(t, 0o600, value)); err == nil || !strings.Contains(err.Error(), "invalid_refresh_endpoint") {
		t.Fatalf("cross-origin error=%v", err)
	}
	value = readerConfigValue()
	value["readers"].([]map[string]any)[0]["refresh_endpoint"] = "http://127.0.0.1:18080/internal/another-refresh"
	if _, err := loadLectureReaderConfig(writeReaderConfig(t, 0o600, value)); err == nil || !strings.Contains(err.Error(), "invalid_refresh_endpoint") {
		t.Fatalf("same-origin path substitution error=%v", err)
	}
}

func TestLectureReaderContractLiteralsAndExactKeySets(t *testing.T) {
	if lectureReaderInventoryFormat != "statground.lecture-publication-reader-inventory.v1" ||
		lectureReaderRefreshFormat != "statground.lecture-publication-reader-refresh.v1" ||
		lectureReaderRefreshPath != "/internal/lecture-publication/refresh" {
		t.Fatalf("contract literals changed: inventory=%q refresh=%q path=%q", lectureReaderInventoryFormat, lectureReaderRefreshFormat, lectureReaderRefreshPath)
	}
	wantDiscovery := stringSet("format", "app_service", "reader_instance", "reader_epoch_uuid", "refresh_nonce", "observed_at")
	wantReceipt := stringSet(
		"format", "app_service", "reader_instance", "reader_epoch_uuid",
		"activation_revision", "activation_uuid", "run_uuid", "source_authority_revision",
		"refresh_nonce", "surface", "generation_ms", "list_count", "detail_course_id",
		"homepage_count", "sitemap_entry_count", "sitemap_sha256", "refresh_started_at", "refreshed_at",
	)
	if !reflect.DeepEqual(lectureReaderDiscoveryKeys, wantDiscovery) || !reflect.DeepEqual(lectureReaderReceiptKeys, wantReceipt) {
		t.Fatalf("contract keys changed: discovery=%v receipt=%v", lectureReaderDiscoveryKeys, lectureReaderReceiptKeys)
	}
}

func TestLectureReaderDiscoveryAndRefreshUseExactEpochBoundContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testReaderBearer || r.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("headers=%v", r.Header)
		}
		if r.URL.Path != lectureReaderRefreshPath {
			t.Fatalf("path=%q, want %q", r.URL.Path, lectureReaderRefreshPath)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("refresh_nonce") != testRefreshNonce || len(r.URL.Query()) != 1 {
				t.Fatalf("query=%v", r.URL.Query())
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"format": lectureReaderInventoryFormat, "app_service": "web-r", "reader_instance": "web-r-test-1",
				"reader_epoch_uuid": testReaderEpoch, "refresh_nonce": testRefreshNonce,
				"observed_at": time.Now().UTC().Format(time.RFC3339Nano),
			})
		case http.MethodPost:
			dec := json.NewDecoder(r.Body)
			dec.UseNumber()
			var request map[string]any
			if err := dec.Decode(&request); err != nil || len(request) != 9 || request["reader_epoch_uuid"] != testReaderEpoch {
				t.Fatalf("request=%v err=%v", request, err)
			}
			now := time.Now().UTC()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"format": lectureReaderRefreshFormat, "app_service": "web-r", "reader_instance": "web-r-test-1",
				"reader_epoch_uuid": testReaderEpoch, "activation_revision": 8, "activation_uuid": testActivationUUID,
				"run_uuid": testActivationRun, "source_authority_revision": 9, "refresh_nonce": testRefreshNonce,
				"surface": "webr", "generation_ms": 1700000000123, "list_count": 40, "detail_course_id": 123,
				"homepage_count": 8, "sitemap_entry_count": 40, "sitemap_sha256": strings.Repeat("a", 64),
				"refresh_started_at": now.Format(time.RFC3339Nano), "refreshed_at": now.Add(time.Second).Format(time.RFC3339Nano),
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	reader := lectureReaderConfig{AppService: "web-r", ReaderInstance: "web-r-test-1", RefreshEndpoint: server.URL + lectureReaderRefreshPath, BearerToken: testReaderBearer}
	epoch, err := discoverLectureReader(context.Background(), reader, testRefreshNonce)
	if err != nil || epoch != testReaderEpoch {
		t.Fatalf("epoch=%q err=%v", epoch, err)
	}
	activation := activationEvidence{Revision: 8, ActivationUUID: testActivationUUID, RunUUID: testActivationRun, WebRGenerationMS: 1700000000123}
	receipt, err := postLectureReaderRefresh(context.Background(), preparedLectureReader{Config: reader, EpochUUID: epoch}, testRefreshNonce, activation, 9, activation.WebRGenerationMS)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ListCount != 40 || receipt.DetailCourseID != 123 || receipt.SitemapSHA256 != strings.Repeat("a", 64) {
		t.Fatalf("receipt=%+v", receipt)
	}
}

func TestLectureReaderDiscoveryRejectsReceiptFormatForInventory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"format": lectureReaderRefreshFormat, "app_service": "web-r", "reader_instance": "web-r-test-1",
			"reader_epoch_uuid": testReaderEpoch, "refresh_nonce": testRefreshNonce,
			"observed_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}))
	defer server.Close()
	reader := lectureReaderConfig{
		AppService: "web-r", ReaderInstance: "web-r-test-1",
		RefreshEndpoint: server.URL + lectureReaderRefreshPath, BearerToken: testReaderBearer,
	}
	if epoch, err := discoverLectureReader(context.Background(), reader, testRefreshNonce); err == nil || epoch != "" {
		t.Fatalf("receipt format was accepted as inventory: epoch=%q err=%v", epoch, err)
	}
}

func TestVerifyLectureReaderEpochSetDetectsRestartAfterReceipts(t *testing.T) {
	epoch := testReaderEpoch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"format": lectureReaderInventoryFormat, "app_service": "web-r", "reader_instance": "web-r-test-1",
			"reader_epoch_uuid": epoch, "refresh_nonce": testRefreshNonce,
			"observed_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}))
	defer server.Close()
	prepared := preparedLectureReaderRefresh{
		Nonce: testRefreshNonce,
		Readers: []preparedLectureReader{{
			Config:    lectureReaderConfig{AppService: "web-r", ReaderInstance: "web-r-test-1", RefreshEndpoint: server.URL + lectureReaderRefreshPath, BearerToken: testReaderBearer},
			EpochUUID: testReaderEpoch,
		}},
	}
	if err := verifyLectureReaderEpochSet(context.Background(), prepared, "epoch_test"); err != nil {
		t.Fatal(err)
	}
	epoch = "01994602-0000-7000-8000-000000000119"
	err := verifyLectureReaderEpochSet(context.Background(), prepared, "epoch_test")
	assertUpdateStateError(t, err, "degraded", "epoch_test", "reader_restarted_or_unavailable")
}

func TestStableReceiptUUIDBindsRefreshAttempt(t *testing.T) {
	reader := preparedLectureReader{Config: lectureReaderConfig{AppService: "web-r", ReaderInstance: "web-r-test-1"}, EpochUUID: testReaderEpoch}
	receipt := lectureReaderReceipt{Reader: reader, ActivationRevision: 8, ActivationUUID: testActivationUUID, RunUUID: testActivationRun, SourceAuthorityRevision: 9, RefreshNonce: testRefreshNonce}
	inventory := lectureReaderInventory{Revision: 7, UUID: testInventoryUUID}
	first := stableReceiptUUID(receipt, inventory)
	receipt.RefreshNonce = "01994602-0000-7000-8000-000000000116"
	second := stableReceiptUUID(receipt, inventory)
	if first == second || !isCanonicalUUID(first) || !isCanonicalUUID(second) {
		t.Fatalf("stable UUIDs=%q/%q", first, second)
	}
}

func TestActivationFromPublicationPointersRequiresOneExactActivation(t *testing.T) {
	pointers := map[string]publicationPointer{}
	for _, surface := range expectedPublicationSurfaces {
		pointers[surface] = publicationPointer{Surface: surface, GenerationMS: 100, ActivationRevision: 8, ActivationUUID: testActivationUUID, RunUUID: testActivationRun, ActivationKind: "publish"}
	}
	activation, present, err := activationFromPublicationPointers(pointers)
	if err != nil || !present || activation.Revision != 8 {
		t.Fatalf("activation=%+v present=%v err=%v", activation, present, err)
	}
	pointer := pointers["mirtype"]
	pointer.ActivationRevision++
	pointers["mirtype"] = pointer
	if _, _, err := activationFromPublicationPointers(pointers); err == nil {
		t.Fatal("conflicting activation was accepted")
	}
}
