package inflearn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	lectureReaderInventoryFormat = "statground.lecture-publication-reader-inventory.v1"
	lectureReaderRefreshFormat   = "statground.lecture-publication-reader-refresh.v1"
	lectureReaderRefreshPath     = "/internal/lecture-publication/refresh"
	lectureReaderACKLocalTable   = "lecture_publication.inflearn_public_catalog_reader_refresh_ack_local"
	lectureReaderACKTable        = "lecture_publication.inflearn_public_catalog_reader_refresh_ack"
	readerInventoryLocalTable    = "Data_Book_Service.book_publication_reader_inventory_local"
	readerInventoryMarkerTable   = "Data_Book_Service.book_publication_reader_inventory_revision_local"
)

var lectureReaderDiscoveryKeys = stringSet(
	"format", "app_service", "reader_instance", "reader_epoch_uuid",
	"refresh_nonce", "observed_at",
)

var lectureReaderReceiptKeys = stringSet(
	"format", "app_service", "reader_instance", "reader_epoch_uuid",
	"activation_revision", "activation_uuid", "run_uuid",
	"source_authority_revision", "refresh_nonce", "surface", "generation_ms",
	"list_count", "detail_course_id", "homepage_count", "sitemap_entry_count",
	"sitemap_sha256", "refresh_started_at", "refreshed_at",
)

type lectureReaderConfigFile struct {
	ReaderInventoryRevision uint64                `json:"reader_inventory_revision"`
	Readers                 []lectureReaderConfig `json:"readers"`
}

type lectureReaderConfig struct {
	AppService        string  `json:"app_service"`
	ReaderInstance    string  `json:"reader_instance"`
	InventoryEndpoint string  `json:"inventory_endpoint"`
	RefreshEndpoint   string  `json:"refresh_endpoint"`
	BearerToken       string  `json:"bearer_token"`
	CAFile            *string `json:"ca_file"`

	inventoryEndpointSHA string
}

type lectureReaderInventory struct {
	Revision uint64
	UUID     string
	Readers  []lectureReaderConfig
}

type preparedLectureReader struct {
	Config    lectureReaderConfig
	EpochUUID string
}

type preparedLectureReaderRefresh struct {
	Inventory lectureReaderInventory
	Nonce     string
	Readers   []preparedLectureReader
}

type lectureReaderReceipt struct {
	Reader                  preparedLectureReader
	ActivationRevision      uint64
	ActivationUUID          string
	RunUUID                 string
	SourceAuthorityRevision uint64
	RefreshNonce            string
	Surface                 string
	GenerationMS            int64
	ListCount               uint64
	DetailCourseID          uint64
	HomepageCount           uint64
	SitemapEntryCount       uint64
	SitemapSHA256           string
	RefreshStartedAt        time.Time
	RefreshedAt             time.Time
}

func loadLectureReaderConfig(path string) (lectureReaderConfigFile, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "missing_reader_refresh_config")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_unreadable")
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > 1<<20 {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_not_private_regular_file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_unreadable")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg lectureReaderConfigFile
	if err := dec.Decode(&cfg); err != nil {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_json")
	}
	if err := requireJSONEOF(dec); err != nil {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_trailing_json")
	}
	if cfg.ReaderInventoryRevision == 0 || len(cfg.Readers) < 3 || len(cfg.Readers) > 128 {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_inventory")
	}
	identities := map[string]bool{}
	inventoryEndpoints := map[string]bool{}
	refreshEndpoints := map[string]bool{}
	services := map[string]bool{}
	for i := range cfg.Readers {
		reader := &cfg.Readers[i]
		reader.AppService = strings.TrimSpace(reader.AppService)
		reader.ReaderInstance = strings.TrimSpace(reader.ReaderInstance)
		reader.InventoryEndpoint = strings.TrimSpace(reader.InventoryEndpoint)
		reader.RefreshEndpoint = strings.TrimSpace(reader.RefreshEndpoint)
		if reader.AppService != "web-r" && reader.AppService != "mirtype" && reader.AppService != "statground" {
			return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_service")
		}
		if !validReaderInstance(reader.ReaderInstance) || len(reader.BearerToken) < 32 || len(reader.BearerToken) > 4096 {
			return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_identity")
		}
		inventoryURL, err := canonicalReaderEndpoint(reader.InventoryEndpoint)
		if err != nil {
			return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_inventory_endpoint")
		}
		refreshURL, err := canonicalReaderEndpoint(reader.RefreshEndpoint)
		if err != nil || refreshURL.String() != lectureRefreshEndpointForInventory(inventoryURL) {
			return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_refresh_endpoint")
		}
		// The DB-authoritative inventory binds the reader identity and origin. Never
		// let the secret-side config redirect a lecture refresh to another path on
		// that origin.
		reader.RefreshEndpoint = lectureRefreshEndpointForInventory(inventoryURL)
		if reader.CAFile != nil {
			caPath := strings.TrimSpace(*reader.CAFile)
			if caPath == "" || !filepath.IsAbs(caPath) {
				return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_invalid_ca_file")
			}
			reader.CAFile = &caPath
		}
		identity := reader.AppService + "\x00" + reader.ReaderInstance
		if identities[identity] || inventoryEndpoints[reader.InventoryEndpoint] || refreshEndpoints[reader.RefreshEndpoint] {
			return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_duplicate_reader")
		}
		identities[identity] = true
		inventoryEndpoints[reader.InventoryEndpoint] = true
		refreshEndpoints[reader.RefreshEndpoint] = true
		services[reader.AppService] = true
		digest := sha256.Sum256([]byte(reader.InventoryEndpoint))
		reader.inventoryEndpointSHA = hex.EncodeToString(digest[:])
	}
	if len(services) != 3 || !services["web-r"] || !services["mirtype"] || !services["statground"] {
		return lectureReaderConfigFile{}, stateError("degraded", "public_reader_config", "reader_refresh_config_incomplete_services")
	}
	sort.Slice(cfg.Readers, func(i, j int) bool {
		if cfg.Readers[i].AppService != cfg.Readers[j].AppService {
			return cfg.Readers[i].AppService < cfg.Readers[j].AppService
		}
		return cfg.Readers[i].ReaderInstance < cfg.Readers[j].ReaderInstance
	})
	return cfg, nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("extra JSON value")
}

func validReaderInstance(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == ':' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func canonicalReaderEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		u.Path == "" || u.Path == "/" || !strings.HasPrefix(u.Path, "/") || u.String() != raw {
		return nil, fmt.Errorf("non-canonical reader endpoint")
	}
	if u.Scheme == "http" {
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("plain HTTP is restricted to loopback")
		}
	}
	return u, nil
}

func lectureRefreshEndpointForInventory(inventory *url.URL) string {
	if inventory == nil {
		return ""
	}
	return (&url.URL{Scheme: inventory.Scheme, Host: inventory.Host, Path: lectureReaderRefreshPath}).String()
}

func stringSet(values ...string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func exactJSONKeys(value map[string]any, expected map[string]bool) bool {
	if len(value) != len(expected) {
		return false
	}
	for key := range value {
		if !expected[key] {
			return false
		}
	}
	return true
}

func strictJSONUint(value any) (uint64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("not a JSON number")
	}
	raw := number.String()
	if raw == "" || strings.HasPrefix(raw, "+") || strings.ContainsAny(raw, ".eE-") {
		return 0, fmt.Errorf("not an unsigned integer")
	}
	return strconv.ParseUint(raw, 10, 64)
}

func (s *Service) proveLectureReaderInventory(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile) (lectureReaderInventory, error) {
	for _, table := range []string{readerInventoryLocalTable, readerInventoryMarkerTable} {
		if err := s.execPublicRefreshCommand(ctx, "SYSTEM SYNC REPLICA "+table+" STRICT", 2*time.Minute); err != nil {
			return lectureReaderInventory{}, newUpdateReadStateError("public_reader_inventory_sync", err)
		}
	}
	authorityRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(inventory_revision) AS inventory_revision,toString(inventory_uuid) AS inventory_uuid,
		toString(reader_count) AS reader_count,toString(complete) AS complete
		FROM clusterAllReplicas(%s,'Data_Book_Service','v_book_publication_reader_inventory_authority')
		ORDER BY hostname SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return lectureReaderInventory{}, newUpdateReadStateError("public_reader_inventory_authority", err)
	}
	if len(authorityRows) != 4 {
		return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_authority", "reader_inventory_endpoint_count")
	}
	seenHosts := map[string]bool{}
	var inventoryUUID string
	for _, row := range authorityRows {
		host := strings.TrimSpace(asString(row["hostname"]))
		revision, revisionErr := exactUint64(row["inventory_revision"])
		readerCount, countErr := exactUint64(row["reader_count"])
		complete, completeErr := exactUint64(row["complete"])
		uuid := strings.ToLower(strings.TrimSpace(asString(row["inventory_uuid"])))
		if topology[host] == 0 || seenHosts[host] || revisionErr != nil || countErr != nil || completeErr != nil ||
			revision != cfg.ReaderInventoryRevision || readerCount != uint64(len(cfg.Readers)) || complete != 1 || !isCanonicalUUID(uuid) {
			return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_authority", "reader_inventory_authority_mismatch")
		}
		if inventoryUUID == "" {
			inventoryUUID = uuid
		} else if inventoryUUID != uuid {
			return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_authority", "reader_inventory_authority_divergence")
		}
		seenHosts[host] = true
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		toString(inventory_revision) AS inventory_revision,toString(inventory_uuid) AS inventory_uuid,
		toString(app_service) AS app_service,reader_instance,toString(endpoint_sha256) AS endpoint_sha256
		FROM clusterAllReplicas(%s,'Data_Book_Service','v_book_publication_reader_inventory_current')
		WHERE inventory_revision=%d AND inventory_uuid=toUUID(%s)
		ORDER BY hostname,app_service,reader_instance
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(s.Cfg.CHCluster), cfg.ReaderInventoryRevision, QuoteSQLString(inventoryUUID)))
	if err != nil {
		return lectureReaderInventory{}, newUpdateReadStateError("public_reader_inventory_rows", err)
	}
	expected := make(map[string]bool, len(cfg.Readers))
	for _, reader := range cfg.Readers {
		expected[reader.AppService+"\x00"+reader.ReaderInstance+"\x00"+reader.inventoryEndpointSHA] = true
	}
	byHost := make(map[string]map[string]bool, 4)
	for _, row := range rows {
		host := strings.TrimSpace(asString(row["hostname"]))
		revision, revisionErr := exactUint64(row["inventory_revision"])
		uuid := strings.ToLower(strings.TrimSpace(asString(row["inventory_uuid"])))
		identity := asString(row["app_service"]) + "\x00" + asString(row["reader_instance"]) + "\x00" + asString(row["endpoint_sha256"])
		if topology[host] == 0 || revisionErr != nil || revision != cfg.ReaderInventoryRevision || uuid != inventoryUUID || !expected[identity] {
			return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_rows", "reader_inventory_row_mismatch")
		}
		if byHost[host] == nil {
			byHost[host] = map[string]bool{}
		}
		if byHost[host][identity] {
			return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_rows", "reader_inventory_row_duplicate")
		}
		byHost[host][identity] = true
	}
	if len(byHost) != 4 {
		return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_rows", "reader_inventory_endpoint_count")
	}
	for host, values := range byHost {
		if topology[host] == 0 || len(values) != len(expected) {
			return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_rows", "reader_inventory_set_mismatch")
		}
		for identity := range expected {
			if !values[identity] {
				return lectureReaderInventory{}, stateError("degraded", "public_reader_inventory_rows", "reader_inventory_set_mismatch")
			}
		}
	}
	return lectureReaderInventory{Revision: cfg.ReaderInventoryRevision, UUID: inventoryUUID, Readers: cfg.Readers}, nil
}

func lectureSurfaceForService(service string) string {
	switch service {
	case "web-r":
		return "webr"
	case "mirtype":
		return "mirtype"
	case "statground":
		return "statground"
	default:
		return ""
	}
}

func readerHTTPClient(reader lectureReaderConfig) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	transport.MaxIdleConns = 8
	transport.MaxIdleConnsPerHost = 2
	transport.IdleConnTimeout = 30 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	if reader.CAFile != nil {
		pem, err := os.ReadFile(*reader.CAFile)
		if err != nil {
			return nil, err
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file has no certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Transport: transport,
		Timeout:   70 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("reader redirects are forbidden")
		},
	}, nil
}

func readBoundedJSONResponse(response *http.Response) (map[string]any, error) {
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("reader status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(raw) == 0 || len(raw) > 65536 {
		return nil, fmt.Errorf("reader response size")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value map[string]any
	if err := dec.Decode(&value); err != nil || value == nil {
		return nil, fmt.Errorf("reader response JSON")
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, fmt.Errorf("reader response trailing JSON")
	}
	return value, nil
}

func discoverLectureReader(ctx context.Context, reader lectureReaderConfig, nonce string) (string, error) {
	client, err := readerHTTPClient(reader)
	if err != nil {
		return "", err
	}
	u, _ := url.Parse(reader.RefreshEndpoint)
	query := u.Query()
	query.Set("refresh_nonce", nonce)
	u.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+reader.BearerToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	value, err := readBoundedJSONResponse(response)
	if err != nil || !exactJSONKeys(value, lectureReaderDiscoveryKeys) {
		return "", fmt.Errorf("reader discovery contract")
	}
	epoch := strings.ToLower(strings.TrimSpace(asString(value["reader_epoch_uuid"])))
	if asString(value["format"]) != lectureReaderInventoryFormat || asString(value["app_service"]) != reader.AppService ||
		asString(value["reader_instance"]) != reader.ReaderInstance || asString(value["refresh_nonce"]) != nonce ||
		!isCanonicalUUID(epoch) {
		return "", fmt.Errorf("reader discovery identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, asString(value["observed_at"])); err != nil {
		return "", fmt.Errorf("reader discovery time")
	}
	return epoch, nil
}

func (s *Service) prepareLectureReaderRefresh(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile) (preparedLectureReaderRefresh, error) {
	inventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return preparedLectureReaderRefresh{}, err
	}
	nonce := UUIDv7String(time.Now())
	prepared := preparedLectureReaderRefresh{Inventory: inventory, Nonce: nonce, Readers: make([]preparedLectureReader, 0, len(inventory.Readers))}
	for _, reader := range inventory.Readers {
		epoch, err := discoverLectureReader(ctx, reader, nonce)
		if err != nil {
			return preparedLectureReaderRefresh{}, stateError("degraded", "public_reader_discovery", "reader_discovery_failed")
		}
		prepared.Readers = append(prepared.Readers, preparedLectureReader{Config: reader, EpochUUID: epoch})
	}
	return prepared, nil
}

func postLectureReaderRefresh(ctx context.Context, reader preparedLectureReader, nonce string, activation activationEvidence, sourceAuthorityRevision uint64, generationMS int64) (lectureReaderReceipt, error) {
	bodyValue := map[string]any{
		"format":                    lectureReaderRefreshFormat,
		"reader_epoch_uuid":         reader.EpochUUID,
		"activation_revision":       activation.Revision,
		"activation_uuid":           activation.ActivationUUID,
		"run_uuid":                  activation.RunUUID,
		"source_authority_revision": sourceAuthorityRevision,
		"refresh_nonce":             nonce,
		"surface":                   lectureSurfaceForService(reader.Config.AppService),
		"generation_ms":             generationMS,
	}
	body, err := json.Marshal(bodyValue)
	if err != nil {
		return lectureReaderReceipt{}, err
	}
	client, err := readerHTTPClient(reader.Config)
	if err != nil {
		return lectureReaderReceipt{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, reader.Config.RefreshEndpoint, bytes.NewReader(body))
	if err != nil {
		return lectureReaderReceipt{}, err
	}
	request.Header.Set("Authorization", "Bearer "+reader.Config.BearerToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	response, err := client.Do(request)
	if err != nil {
		return lectureReaderReceipt{}, err
	}
	value, err := readBoundedJSONResponse(response)
	if err != nil || !exactJSONKeys(value, lectureReaderReceiptKeys) {
		return lectureReaderReceipt{}, fmt.Errorf("reader refresh contract")
	}
	exactStrings := map[string]string{
		"format":            lectureReaderRefreshFormat,
		"app_service":       reader.Config.AppService,
		"reader_instance":   reader.Config.ReaderInstance,
		"reader_epoch_uuid": reader.EpochUUID,
		"activation_uuid":   activation.ActivationUUID,
		"run_uuid":          activation.RunUUID,
		"refresh_nonce":     nonce,
		"surface":           lectureSurfaceForService(reader.Config.AppService),
	}
	for key, expected := range exactStrings {
		if asString(value[key]) != expected {
			return lectureReaderReceipt{}, fmt.Errorf("reader refresh identity")
		}
	}
	activationRevision, activationErr := strictJSONUint(value["activation_revision"])
	authorityRevision, authorityErr := strictJSONUint(value["source_authority_revision"])
	generation, generationErr := strictJSONUint(value["generation_ms"])
	listCount, listErr := strictJSONUint(value["list_count"])
	detailID, detailErr := strictJSONUint(value["detail_course_id"])
	homepageCount, homepageErr := strictJSONUint(value["homepage_count"])
	sitemapCount, sitemapErr := strictJSONUint(value["sitemap_entry_count"])
	if activationErr != nil || authorityErr != nil || generationErr != nil || listErr != nil || detailErr != nil || homepageErr != nil || sitemapErr != nil ||
		activationRevision != activation.Revision || authorityRevision != sourceAuthorityRevision || generation > uint64(^uint64(0)>>1) || int64(generation) != generationMS ||
		listCount == 0 || detailID == 0 || homepageCount == 0 || sitemapCount == 0 {
		return lectureReaderReceipt{}, fmt.Errorf("reader refresh evidence")
	}
	digest := strings.TrimSpace(asString(value["sitemap_sha256"]))
	if len(digest) != 64 || strings.ToLower(digest) != digest {
		return lectureReaderReceipt{}, fmt.Errorf("reader sitemap digest")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return lectureReaderReceipt{}, fmt.Errorf("reader sitemap digest")
	}
	started, startErr := time.Parse(time.RFC3339Nano, asString(value["refresh_started_at"]))
	refreshed, refreshedErr := time.Parse(time.RFC3339Nano, asString(value["refreshed_at"]))
	if startErr != nil || refreshedErr != nil || refreshed.Before(started) || refreshed.Sub(started) > 60*time.Second {
		return lectureReaderReceipt{}, fmt.Errorf("reader refresh interval")
	}
	return lectureReaderReceipt{
		Reader: reader, ActivationRevision: activation.Revision, ActivationUUID: activation.ActivationUUID,
		RunUUID: activation.RunUUID, SourceAuthorityRevision: sourceAuthorityRevision, RefreshNonce: nonce,
		Surface: lectureSurfaceForService(reader.Config.AppService), GenerationMS: generationMS,
		ListCount: listCount, DetailCourseID: detailID, HomepageCount: homepageCount,
		SitemapEntryCount: sitemapCount, SitemapSHA256: digest,
		RefreshStartedAt: started.UTC(), RefreshedAt: refreshed.UTC(),
	}, nil
}

func generationForReader(activation activationEvidence, service string) int64 {
	switch service {
	case "web-r":
		return activation.WebRGenerationMS
	case "mirtype":
		return activation.MirGenerationMS
	case "statground":
		return activation.StatgroundGenerationMS
	default:
		return 0
	}
}

func verifyLectureReaderEpochSet(ctx context.Context, prepared preparedLectureReaderRefresh, phase string) error {
	if len(prepared.Readers) == 0 || !isCanonicalUUID(prepared.Nonce) {
		return stateError("degraded", phase, "reader_epoch_set_invalid")
	}
	for _, reader := range prepared.Readers {
		epoch, err := discoverLectureReader(ctx, reader.Config, prepared.Nonce)
		if err != nil || epoch != reader.EpochUUID {
			return stateError("degraded", phase, "reader_restarted_or_unavailable")
		}
	}
	return nil
}

func sameLectureReaderInventory(left, right lectureReaderInventory) bool {
	return left.Revision == right.Revision && left.UUID == right.UUID &&
		len(left.Readers) == len(right.Readers)
}

func (s *Service) executePreparedLectureReaderRefresh(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile, prepared preparedLectureReaderRefresh, activation activationEvidence, sourceAuthorityRevision uint64) error {
	if sourceAuthorityRevision == 0 || activation.Revision == 0 || !isCanonicalUUID(activation.ActivationUUID) || !isCanonicalUUID(activation.RunUUID) || len(prepared.Readers) != len(cfg.Readers) {
		return stateError("degraded", "public_reader_refresh", "invalid_reader_refresh_identity")
	}
	inventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return err
	}
	if inventory.Revision != prepared.Inventory.Revision || inventory.UUID != prepared.Inventory.UUID {
		return stateError("degraded", "public_reader_refresh", "reader_inventory_changed_before_refresh")
	}
	receipts := make([]lectureReaderReceipt, 0, len(prepared.Readers))
	for _, reader := range prepared.Readers {
		generation := generationForReader(activation, reader.Config.AppService)
		if generation <= 0 {
			return stateError("degraded", "public_reader_refresh", "reader_generation_missing")
		}
		receipt, err := postLectureReaderRefresh(ctx, reader, prepared.Nonce, activation, sourceAuthorityRevision, generation)
		if err != nil {
			return stateError("degraded", "public_reader_refresh", "reader_refresh_failed")
		}
		receipts = append(receipts, receipt)
	}
	// A reader refreshed early in the sequence can restart while later readers
	// are still working. Validate the complete process set only after every
	// receipt has arrived, immediately before the durable ACK fence.
	if err := verifyLectureReaderEpochSet(ctx, prepared, "public_reader_refresh_epoch_pre_ack"); err != nil {
		return err
	}
	postInventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return err
	}
	if !sameLectureReaderInventory(postInventory, inventory) {
		return stateError("degraded", "public_reader_refresh", "reader_inventory_changed_during_refresh")
	}
	currentAuthority, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_reader_refresh_source_authority")
	if err != nil {
		return err
	}
	if currentAuthority != sourceAuthorityRevision {
		return stateError("degraded", "public_reader_refresh_source_authority", "source_authority_changed_during_reader_refresh")
	}
	if err := s.appendAndReadbackLectureReaderACKs(ctx, topology, inventory, receipts); err != nil {
		return err
	}
	// The ACK write/readback is itself a bounded race window. Do not report
	// success unless the same process set, DB inventory, and source authority
	// still hold after the quorum receipt is durable.
	if err := verifyLectureReaderEpochSet(ctx, prepared, "public_reader_refresh_epoch_post_ack"); err != nil {
		return err
	}
	finalInventory, err := s.proveLectureReaderInventory(ctx, topology, cfg)
	if err != nil {
		return err
	}
	if !sameLectureReaderInventory(finalInventory, inventory) {
		return stateError("degraded", "public_reader_refresh_final_inventory", "reader_inventory_changed_after_ack")
	}
	finalAuthority, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_reader_refresh_final_source_authority")
	if err != nil {
		return err
	}
	if finalAuthority != sourceAuthorityRevision {
		return stateError("degraded", "public_reader_refresh_final_source_authority", "source_authority_changed_after_ack")
	}
	return nil
}

func stableReceiptUUID(receipt lectureReaderReceipt, inventory lectureReaderInventory) string {
	seed := strings.Join([]string{
		"inflearn-reader-refresh-ack-v1",
		strconv.FormatUint(receipt.ActivationRevision, 10), receipt.ActivationUUID, receipt.RunUUID,
		strconv.FormatUint(receipt.SourceAuthorityRevision, 10), strconv.FormatUint(inventory.Revision, 10), inventory.UUID,
		receipt.RefreshNonce, receipt.Reader.Config.AppService, receipt.Reader.Config.ReaderInstance, receipt.Reader.EpochUUID,
	}, "\x1f")
	sum := sha256.Sum256([]byte(seed))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])
}

func utcClickHouseTime(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}

func (s *Service) appendAndReadbackLectureReaderACKs(ctx context.Context, topology map[string]int, inventory lectureReaderInventory, receipts []lectureReaderReceipt) error {
	if len(receipts) != len(inventory.Readers) || len(receipts) == 0 {
		return stateError("degraded", "public_reader_ack", "reader_ack_set_incomplete")
	}
	columns := []string{
		"ack_uuid", "activation_revision", "activation_uuid", "run_uuid", "source_authority_revision",
		"reader_inventory_revision", "reader_inventory_uuid", "refresh_nonce", "reader_epoch_uuid",
		"app_service", "reader_instance", "surface", "generation", "list_count", "detail_course_id",
		"homepage_count", "sitemap_entry_count", "sitemap_sha256", "refresh_started_at", "refreshed_at",
	}
	rows := make([]map[string]any, 0, len(receipts))
	seen := map[string]bool{}
	for _, receipt := range receipts {
		identity := receipt.Reader.Config.AppService + "\x00" + receipt.Reader.Config.ReaderInstance
		if seen[identity] {
			return stateError("degraded", "public_reader_ack", "reader_ack_identity_duplicate")
		}
		seen[identity] = true
		rows = append(rows, map[string]any{
			"ack_uuid":            stableReceiptUUID(receipt, inventory),
			"activation_revision": receipt.ActivationRevision, "activation_uuid": receipt.ActivationUUID,
			"run_uuid": receipt.RunUUID, "source_authority_revision": receipt.SourceAuthorityRevision,
			"reader_inventory_revision": inventory.Revision, "reader_inventory_uuid": inventory.UUID,
			"refresh_nonce": receipt.RefreshNonce, "reader_epoch_uuid": receipt.Reader.EpochUUID,
			"app_service": receipt.Reader.Config.AppService, "reader_instance": receipt.Reader.Config.ReaderInstance,
			"surface": receipt.Surface, "generation": time.UnixMilli(receipt.GenerationMS).In(KST).Format("2006-01-02 15:04:05.000"),
			"list_count": receipt.ListCount, "detail_course_id": receipt.DetailCourseID,
			"homepage_count": receipt.HomepageCount, "sitemap_entry_count": receipt.SitemapEntryCount,
			"sitemap_sha256":     receipt.SitemapSHA256,
			"refresh_started_at": utcClickHouseTime(receipt.RefreshStartedAt), "refreshed_at": utcClickHouseTime(receipt.RefreshedAt),
		})
	}
	payload, err := encodeClickHouseJSONEachRow(columns, rows)
	if err != nil {
		return stateError("degraded", "public_reader_ack", "reader_ack_encoding_failed")
	}
	first := receipts[0]
	token := "lecture-reader-refresh-" + stableReceiptUUID(first, inventory)
	sql := fmt.Sprintf(`INSERT INTO %s (%s)
		SETTINGS insert_distributed_sync=1,insert_quorum=4,insert_quorum_parallel=0,
		insert_deduplicate=1,insert_deduplication_token=%s FORMAT JSONEachRow`,
		lectureReaderACKTable, clickHouseColumnList(columns), QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt <= publicationMutationReconcileAttempts; attempt++ {
		_, insertErr = s.chPost(ctx, sql, payload, "application/x-ndjson")
		readbackErr := s.readbackLectureReaderACKs(ctx, topology, inventory, receipts)
		if readbackErr == nil {
			return nil
		}
		if insertErr != nil && !isTemporaryClickHouseWriteError(insertErr) {
			return newUpdateReadStateError("public_reader_ack_insert", insertErr)
		}
		if attempt == publicationMutationReconcileAttempts {
			return readbackErr
		}
	}
	return insertErr
}

func (s *Service) readbackLectureReaderACKs(ctx context.Context, topology map[string]int, inventory lectureReaderInventory, receipts []lectureReaderReceipt) error {
	if len(receipts) == 0 {
		return stateError("degraded", "public_reader_ack_readback", "reader_ack_set_empty")
	}
	if err := s.execPublicRefreshCommand(ctx, "SYSTEM SYNC REPLICA "+lectureReaderACKLocalTable+" STRICT", 2*time.Minute); err != nil {
		return newUpdateReadStateError("public_reader_ack_sync", err)
	}
	first := receipts[0]
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,toString(app_service) AS app_service,reader_instance,
		toString(reader_epoch_uuid) AS reader_epoch_uuid,toString(count()) AS row_count,
		toString(uniqExact(ack_uuid)) AS ack_uuids,
		toString(uniqExact(tuple(activation_revision,activation_uuid,run_uuid,source_authority_revision,
		reader_inventory_revision,reader_inventory_uuid,reader_epoch_uuid,app_service,reader_instance,
		surface,generation,list_count,detail_course_id,homepage_count,sitemap_entry_count,sitemap_sha256,
		refresh_started_at,refreshed_at))) AS variants,
		toString(any(ack_uuid)) AS ack_uuid,toString(any(surface)) AS surface,
		toUnixTimestamp64Milli(any(generation)) AS generation_ms,toString(any(list_count)) AS list_count,
		toString(any(detail_course_id)) AS detail_course_id,toString(any(homepage_count)) AS homepage_count,
		toString(any(sitemap_entry_count)) AS sitemap_entry_count,toString(any(sitemap_sha256)) AS sitemap_sha256
		FROM clusterAllReplicas(%s,'lecture_publication','inflearn_public_catalog_reader_refresh_ack_local')
		WHERE activation_revision=%d AND activation_uuid=toUUID(%s) AND run_uuid=toUUID(%s)
		AND source_authority_revision=%d AND reader_inventory_revision=%d AND reader_inventory_uuid=toUUID(%s)
		AND refresh_nonce=toUUID(%s)
		GROUP BY hostname,app_service,reader_instance,reader_epoch_uuid
		ORDER BY hostname,app_service,reader_instance
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(s.Cfg.CHCluster), first.ActivationRevision, QuoteSQLString(first.ActivationUUID),
		QuoteSQLString(first.RunUUID), first.SourceAuthorityRevision, inventory.Revision, QuoteSQLString(inventory.UUID),
		QuoteSQLString(first.RefreshNonce)))
	if err != nil {
		return newUpdateReadStateError("public_reader_ack_readback", err)
	}
	expected := make(map[string]lectureReaderReceipt, len(receipts))
	for _, receipt := range receipts {
		expected[receipt.Reader.Config.AppService+"\x00"+receipt.Reader.Config.ReaderInstance+"\x00"+receipt.Reader.EpochUUID] = receipt
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		identity := asString(row["app_service"]) + "\x00" + asString(row["reader_instance"]) + "\x00" + strings.ToLower(asString(row["reader_epoch_uuid"]))
		receipt, ok := expected[identity]
		rowCount, rowErr := exactUint64(row["row_count"])
		ackUUIDs, ackErr := exactUint64(row["ack_uuids"])
		variants, variantErr := exactUint64(row["variants"])
		listCount, listErr := exactUint64(row["list_count"])
		detailID, detailErr := exactUint64(row["detail_course_id"])
		homepageCount, homepageErr := exactUint64(row["homepage_count"])
		sitemapCount, sitemapErr := exactUint64(row["sitemap_entry_count"])
		key := host + "\x00" + identity
		if topology[host] == 0 || seen[key] || !ok || rowErr != nil || ackErr != nil || variantErr != nil || listErr != nil || detailErr != nil || homepageErr != nil || sitemapErr != nil ||
			rowCount == 0 || ackUUIDs != 1 || variants != 1 || asString(row["ack_uuid"]) != stableReceiptUUID(receipt, inventory) ||
			asString(row["surface"]) != receipt.Surface || asInt64(row["generation_ms"]) != receipt.GenerationMS ||
			listCount != receipt.ListCount || detailID != receipt.DetailCourseID || homepageCount != receipt.HomepageCount ||
			sitemapCount != receipt.SitemapEntryCount || asString(row["sitemap_sha256"]) != receipt.SitemapSHA256 {
			return stateError("degraded", "public_reader_ack_readback", "reader_ack_evidence_mismatch")
		}
		seen[key] = true
	}
	if len(seen) != 4*len(expected) {
		return stateError("degraded", "public_reader_ack_readback", "reader_ack_endpoint_set_incomplete")
	}
	for host := range topology {
		for identity := range expected {
			if !seen[host+"\x00"+identity] {
				return stateError("degraded", "public_reader_ack_readback", "reader_ack_endpoint_set_incomplete")
			}
		}
	}
	return nil
}

func activationFromPublicationPointers(pointers map[string]publicationPointer) (activationEvidence, bool, error) {
	if len(pointers) == 0 {
		return activationEvidence{}, false, nil
	}
	if len(pointers) != 3 {
		return activationEvidence{}, false, stateError("degraded", "public_reader_reconcile", "incomplete_activation_pointer")
	}
	webr, webOK := pointers["webr"]
	mirtype, mirOK := pointers["mirtype"]
	statground, statOK := pointers["statground"]
	if !webOK || !mirOK || !statOK || webr.ActivationRevision == 0 ||
		webr.ActivationRevision != mirtype.ActivationRevision || webr.ActivationRevision != statground.ActivationRevision ||
		webr.ActivationUUID != mirtype.ActivationUUID || webr.ActivationUUID != statground.ActivationUUID ||
		webr.RunUUID != mirtype.RunUUID || webr.RunUUID != statground.RunUUID ||
		webr.ActivationKind != mirtype.ActivationKind || webr.ActivationKind != statground.ActivationKind {
		return activationEvidence{}, false, stateError("degraded", "public_reader_reconcile", "conflicting_activation_pointer")
	}
	return activationEvidence{
		Revision: webr.ActivationRevision, ActivationUUID: webr.ActivationUUID, RunUUID: webr.RunUUID,
		WebRGenerationMS: webr.GenerationMS, MirGenerationMS: mirtype.GenerationMS,
		StatgroundGenerationMS: statground.GenerationMS, Kind: webr.ActivationKind,
	}, true, nil
}

func (s *Service) hasCompleteLectureReaderACK(ctx context.Context, topology map[string]int, inventory lectureReaderInventory, prepared preparedLectureReaderRefresh, activation activationEvidence, sourceAuthorityRevision uint64) (bool, error) {
	if err := s.execPublicRefreshCommand(ctx, "SYSTEM SYNC REPLICA "+lectureReaderACKLocalTable+" STRICT", 2*time.Minute); err != nil {
		return false, newUpdateReadStateError("public_reader_reconcile_ack_sync", err)
	}
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,toString(refresh_nonce) AS refresh_nonce,
		toString(app_service) AS app_service,reader_instance,toString(reader_epoch_uuid) AS reader_epoch_uuid,
		toString(count()) AS row_count,
		toString(uniqExact(tuple(ack_uuid,activation_revision,activation_uuid,run_uuid,source_authority_revision,
		reader_inventory_revision,reader_inventory_uuid,reader_epoch_uuid,app_service,reader_instance,
		surface,generation,list_count,detail_course_id,homepage_count,sitemap_entry_count,sitemap_sha256,
		refresh_started_at,refreshed_at))) AS variants,
		toString(any(surface)) AS surface,toUnixTimestamp64Milli(any(generation)) AS generation_ms,
		toString(any(list_count)) AS list_count,toString(any(detail_course_id)) AS detail_course_id,
		toString(any(homepage_count)) AS homepage_count,toString(any(sitemap_entry_count)) AS sitemap_entry_count,
		toString(any(sitemap_sha256)) AS sitemap_sha256
		FROM clusterAllReplicas(%s,'lecture_publication','inflearn_public_catalog_reader_refresh_ack_local')
		WHERE activation_revision=%d AND activation_uuid=toUUID(%s) AND run_uuid=toUUID(%s)
		AND source_authority_revision=%d AND reader_inventory_revision=%d AND reader_inventory_uuid=toUUID(%s)
		GROUP BY hostname,refresh_nonce,app_service,reader_instance,reader_epoch_uuid
		ORDER BY refresh_nonce,hostname,app_service,reader_instance
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(s.Cfg.CHCluster), activation.Revision, QuoteSQLString(activation.ActivationUUID),
		QuoteSQLString(activation.RunUUID), sourceAuthorityRevision, inventory.Revision, QuoteSQLString(inventory.UUID)))
	if err != nil {
		return false, newUpdateReadStateError("public_reader_reconcile_ack", err)
	}
	expected := make(map[string]preparedLectureReader, len(prepared.Readers))
	for _, reader := range prepared.Readers {
		expected[reader.Config.AppService+"\x00"+reader.Config.ReaderInstance+"\x00"+reader.EpochUUID] = reader
	}
	byNonce := map[string]map[string]bool{}
	invalidNonce := map[string]bool{}
	for _, row := range rows {
		nonce := strings.ToLower(asString(row["refresh_nonce"]))
		host := asString(row["hostname"])
		identity := asString(row["app_service"]) + "\x00" + asString(row["reader_instance"]) + "\x00" + strings.ToLower(asString(row["reader_epoch_uuid"]))
		reader, ok := expected[identity]
		rowCount, rowErr := exactUint64(row["row_count"])
		variants, variantErr := exactUint64(row["variants"])
		listCount, listErr := exactUint64(row["list_count"])
		detailID, detailErr := exactUint64(row["detail_course_id"])
		homepageCount, homepageErr := exactUint64(row["homepage_count"])
		sitemapCount, sitemapErr := exactUint64(row["sitemap_entry_count"])
		digest := asString(row["sitemap_sha256"])
		_, digestErr := hex.DecodeString(digest)
		key := host + "\x00" + identity
		if !isCanonicalUUID(nonce) || topology[host] == 0 || !ok || rowErr != nil || variantErr != nil || listErr != nil || detailErr != nil || homepageErr != nil || sitemapErr != nil ||
			rowCount == 0 || variants != 1 || asString(row["surface"]) != lectureSurfaceForService(reader.Config.AppService) ||
			asInt64(row["generation_ms"]) != generationForReader(activation, reader.Config.AppService) ||
			listCount == 0 || detailID == 0 || homepageCount == 0 || sitemapCount == 0 || len(digest) != 64 ||
			strings.ToLower(digest) != digest || digestErr != nil {
			invalidNonce[nonce] = true
			continue
		}
		if byNonce[nonce] == nil {
			byNonce[nonce] = map[string]bool{}
		}
		if byNonce[nonce][key] {
			invalidNonce[nonce] = true
			continue
		}
		byNonce[nonce][key] = true
	}
	for nonce, seen := range byNonce {
		if invalidNonce[nonce] || len(seen) != 4*len(expected) {
			continue
		}
		complete := true
		for host := range topology {
			for identity := range expected {
				if !seen[host+"\x00"+identity] {
					complete = false
				}
			}
		}
		if complete {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) reconcileCurrentLectureReaders(ctx context.Context, topology map[string]int, cfg lectureReaderConfigFile) error {
	pointers, _, err := s.readPublicationPointer(ctx, "public_reader_reconcile_pointer")
	if err != nil {
		return err
	}
	activation, present, err := activationFromPublicationPointers(pointers)
	if err != nil || !present {
		return err
	}
	authorityRevision, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_reader_reconcile_source_authority")
	if err != nil {
		return err
	}
	prepared, err := s.prepareLectureReaderRefresh(ctx, topology, cfg)
	if err != nil {
		return err
	}
	complete, err := s.hasCompleteLectureReaderACK(ctx, topology, prepared.Inventory, prepared, activation, authorityRevision)
	if err != nil || complete {
		return err
	}
	return s.executePreparedLectureReaderRefresh(ctx, topology, cfg, prepared, activation, authorityRevision)
}
