package inflearn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

const publicFreshnessMaxAgeSeconds = 36 * 60 * 60

var expectedPublicLectureSurfaces = []string{"mirtype", "statground", "webr"}

type publicSurfaceFreshnessMetrics struct {
	Surface            string `json:"surface"`
	PublicExactRows    int    `json:"public_exact_rows"`
	P95AgeSeconds      int    `json:"p95_age_seconds"`
	MaxAgeSeconds      int    `json:"max_age_seconds"`
	Over36Hours        int    `json:"over_36h_rows"`
	OldestGenerationMS int64  `json:"oldest_generation_ms"`
	LatestGenerationMS int64  `json:"latest_generation_ms"`
}

type globalFreshnessMetrics struct {
	GlobalRows     int     `json:"global_rows"`
	GlobalOver60D  int     `json:"global_over_60d_rows"`
	GlobalOver60DR float64 `json:"global_over_60d_ratio"`
}

type refreshViewMetrics struct {
	Database             string `json:"database"`
	View                 string `json:"view"`
	Surface              string `json:"surface"`
	Status               string `json:"status"`
	LastSuccessMS        int64  `json:"last_success_ms"`
	LastRefreshMS        int64  `json:"last_refresh_ms"`
	LastSuccessAgeSecond int64  `json:"last_success_age_seconds"`
	ExceptionPresent     bool   `json:"exception_present"`
}

type publicServingIntegrityMetrics struct {
	Surface                  string `json:"surface"`
	GenerationMS             int64  `json:"generation_ms"`
	MarkerRows               uint64 `json:"marker_rows"`
	NormalizedRows           uint64 `json:"normalized_rows"`
	NormalizedLogicalKeys    uint64 `json:"normalized_logical_keys"`
	PublicRows               uint64 `json:"public_rows"`
	PublicLogicalKeys        uint64 `json:"public_logical_keys"`
	NormalizedFingerprintSum uint64 `json:"normalized_fingerprint_sum"`
	NormalizedFingerprintXOR uint64 `json:"normalized_fingerprint_xor"`
}

func printMachineJSON(payload any) {
	b, err := json.Marshal(payload)
	if err == nil {
		fmt.Println(string(b))
	}
}

func publicRefreshRunUUID() (string, error) {
	runUUID := strings.TrimSpace(os.Getenv("PUBLIC_REFRESH_RUN_UUID"))
	if !isCanonicalUUID(runUUID) {
		return "", stateError("degraded", "public_freshness_verify", "invalid_refresh_receipt")
	}
	return strings.ToLower(runUUID), nil
}

func isCanonicalUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

func (s *Service) VerifyPublicLectureFreshness(ctx context.Context) error {
	runUUID, err := publicRefreshRunUUID()
	if err != nil {
		return err
	}
	topology, err := s.readPublicationTopology(ctx)
	if err != nil {
		return err
	}
	if _, _, err := s.readSourceAuthorityRevision(ctx, topology, "public_freshness_source_authority"); err != nil {
		return err
	}
	pointers, revision, err := s.readPublicationPointer(ctx, "public_freshness_pointer")
	if err != nil {
		return err
	}
	for _, surface := range expectedPublicLectureSurfaces {
		pointer, ok := pointers[surface]
		if !ok || !strings.EqualFold(pointer.RunUUID, runUUID) || pointer.ActivationKind != "publish" {
			return stateError("degraded", "public_freshness_pointer", "current_run_not_activated")
		}
	}
	activationUUID := pointers["webr"].ActivationUUID
	if err := s.verifyPublicServingParity(ctx, runUUID, pointers); err != nil {
		return err
	}

	publicRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`
		WITH active_generations AS (
		  SELECT surface, generation
		  FROM lecture_publication.v_inflearn_public_catalog_generation_latest
		  WHERE run_uuid = toUUID(%s) AND activation_kind = 'publish'
		), public_exact AS (
		  SELECT
		    'webr' AS surface,
		    catalog.course_id AS course_id,
		    catalog.locale AS locale,
		    max(catalog.source_fetched_at) AS source_fetched_at,
		    max(catalog.refresh_batch) AS refresh_batch
		  FROM webr_lecture.v_inflearn_r_lecture_catalog AS catalog
		  GLOBAL INNER JOIN (
		    SELECT generation FROM active_generations WHERE surface = 'webr'
		  ) AS active ON catalog.refresh_batch = active.generation
		  GROUP BY catalog.course_id, catalog.locale
		  UNION ALL
		  SELECT
		    'mirtype' AS surface,
		    catalog.course_id AS course_id,
		    catalog.locale AS locale,
		    max(catalog.source_fetched_at) AS source_fetched_at,
		    max(catalog.refresh_batch) AS refresh_batch
		  FROM mirtype_lecture.v_inflearn_language_lecture_catalog AS catalog
		  GLOBAL INNER JOIN (
		    SELECT generation FROM active_generations WHERE surface = 'mirtype'
		  ) AS active ON catalog.refresh_batch = active.generation
		  GROUP BY catalog.course_id, catalog.locale
		  UNION ALL
		  SELECT
		    'statground' AS surface,
		    catalog.course_id AS course_id,
		    catalog.locale AS locale,
		    max(catalog.fetched_at) AS source_fetched_at,
		    max(catalog.refresh_batch) AS refresh_batch
		  FROM statground_lecture.v_inflearn_workbench_catalog AS catalog
		  GLOBAL INNER JOIN (
		    SELECT generation FROM active_generations WHERE surface = 'statground'
		  ) AS active ON catalog.refresh_batch = active.generation
		  GROUP BY catalog.course_id, catalog.locale
		)
		SELECT
		  surface,
		  count() AS public_exact_rows,
		  toInt64(if(count() = 0, 0, quantileExact(0.95)(age_seconds))) AS p95_age_seconds,
		  toInt64(if(count() = 0, 0, max(age_seconds))) AS max_age_seconds,
		  countIf(age_seconds > 129600) AS over_36h_rows,
		  toInt64(if(count() = 0, 0, min(toUnixTimestamp64Milli(refresh_batch)))) AS oldest_generation_ms,
		  toInt64(if(count() = 0, 0, max(toUnixTimestamp64Milli(refresh_batch)))) AS latest_generation_ms
		FROM (
		  SELECT
		    surface,
		    greatest(dateDiff('second', source_fetched_at, now64(3, 'Asia/Seoul')), 0) AS age_seconds,
		    refresh_batch
		  FROM public_exact
		)
		GROUP BY surface
		ORDER BY surface
		SETTINGS skip_unavailable_shards = 0, max_execution_time = 60, max_threads = 2
	`, QuoteSQLString(runUUID)))
	if err != nil {
		return newUpdateReadStateError("public_exact_freshness_read", err)
	}

	bySurface := make(map[string]publicSurfaceFreshnessMetrics, len(publicRows))
	for _, row := range publicRows {
		item := publicSurfaceFreshnessMetrics{
			Surface:            asString(row["surface"]),
			PublicExactRows:    asInt(row["public_exact_rows"]),
			P95AgeSeconds:      asInt(row["p95_age_seconds"]),
			MaxAgeSeconds:      asInt(row["max_age_seconds"]),
			Over36Hours:        asInt(row["over_36h_rows"]),
			OldestGenerationMS: asInt64(row["oldest_generation_ms"]),
			LatestGenerationMS: asInt64(row["latest_generation_ms"]),
		}
		if item.Surface != "" {
			bySurface[item.Surface] = item
		}
	}
	surfaces := make([]publicSurfaceFreshnessMetrics, 0, len(expectedPublicLectureSurfaces))
	issues := make([]string, 0, 16)
	for _, surface := range expectedPublicLectureSurfaces {
		item, ok := bySurface[surface]
		if !ok {
			issues = append(issues, "public_surface_missing:"+surface)
			continue
		}
		if item.PublicExactRows == 0 {
			issues = append(issues, "public_exact_empty:"+surface)
		}
		if item.MaxAgeSeconds > publicFreshnessMaxAgeSeconds {
			issues = append(issues, "public_max_age_over_36h:"+surface)
		}
		wantGeneration := pointers[surface].GenerationMS
		if item.OldestGenerationMS != wantGeneration || item.LatestGenerationMS != wantGeneration {
			issues = append(issues, "public_generation_mismatch:"+surface)
		}
		surfaces = append(surfaces, item)
	}

	globalRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`
		WITH latest AS (
		  SELECT course_id, locale, max(fetched_at) AS last_fetched_at
		  FROM %s.inflearn_course_snapshot_raw
		  WHERE status_code = 'OK'
		    AND course_id > 0
		    AND notEmpty(trimBoth(locale))
		  GROUP BY course_id, locale
		)
		SELECT
		  count() AS global_rows,
		  countIf(last_fetched_at < now64(3, 'Asia/Seoul') - INTERVAL 60 DAY) AS global_over_60d_rows,
		  if(count() = 0, 0., round(toFloat64(global_over_60d_rows) / toFloat64(count()), 6)) AS global_over_60d_ratio
		FROM latest
		SETTINGS skip_unavailable_shards = 0, max_execution_time = 60, max_threads = 2
	`, chIdent(s.Cfg.CHRawDatabase)))
	if err != nil {
		return newUpdateReadStateError("global_freshness_read", err)
	}
	if len(globalRows) != 1 {
		return stateError("degraded", "global_freshness_read", "unexpected_result")
	}
	global := globalFreshnessMetrics{
		GlobalRows:     asInt(globalRows[0]["global_rows"]),
		GlobalOver60D:  asInt(globalRows[0]["global_over_60d_rows"]),
		GlobalOver60DR: asFloat64(globalRows[0]["global_over_60d_ratio"]),
	}
	printMachineJSON(struct {
		Schema             string                          `json:"schema"`
		Status             string                          `json:"status"`
		RunUUID            string                          `json:"run_uuid"`
		ActivationUUID     string                          `json:"activation_uuid"`
		ActivationRevision uint64                          `json:"activation_revision"`
		Surfaces           []publicSurfaceFreshnessMetrics `json:"surfaces"`
		Global             globalFreshnessMetrics          `json:"global"`
	}{
		Schema: "statground.inflearn.public_freshness.v2", Status: "observed", RunUUID: runUUID,
		ActivationUUID: activationUUID, ActivationRevision: revision, Surfaces: surfaces, Global: global,
	})
	if global.GlobalOver60D > 0 {
		fmt.Printf("::warning title=Inflearn global freshness::over_60d_rows=%d ratio=%.6f (warning only)\n", global.GlobalOver60D, global.GlobalOver60DR)
	}

	refreshRows, err := s.CHQueryRows(ctx, `
		SELECT
		  database,
		  view,
		  status,
		  toInt64(ifNull(toUnixTimestamp(last_success_time), 0)) * 1000 AS last_success_ms,
		  toInt64(ifNull(toUnixTimestamp(last_refresh_time), 0)) * 1000 AS last_refresh_ms,
		  toUInt8(notEmpty(ifNull(exception, ''))) AS exception_present,
		  toUnixTimestamp64Milli(now64(3, 'Asia/Seoul')) AS now_ms
		FROM system.view_refreshes
		WHERE (database, view) IN (
		  ('webr_lecture', 'mv_inflearn_r_lecture_catalog_refresh'),
		  ('mirtype_lecture', 'mv_inflearn_language_lecture_catalog_refresh')
		)
		ORDER BY database, view
	`)
	if err != nil {
		return newUpdateReadStateError("refresh_health_read", err)
	}
	expectedViews := map[string]string{
		"webr_lecture.mv_inflearn_r_lecture_catalog_refresh":           "webr",
		"mirtype_lecture.mv_inflearn_language_lecture_catalog_refresh": "mirtype",
	}
	seenViews := make(map[string]bool, len(expectedViews))
	refresh := make([]refreshViewMetrics, 0, len(refreshRows))
	for _, row := range refreshRows {
		key := asString(row["database"]) + "." + asString(row["view"])
		surface, expected := expectedViews[key]
		if !expected || seenViews[key] {
			issues = append(issues, "refresh_unexpected_or_duplicate:"+key)
			continue
		}
		seenViews[key] = true
		item := refreshViewMetrics{
			Database:         asString(row["database"]),
			View:             asString(row["view"]),
			Surface:          surface,
			Status:           asString(row["status"]),
			LastSuccessMS:    asInt64(row["last_success_ms"]),
			LastRefreshMS:    asInt64(row["last_refresh_ms"]),
			ExceptionPresent: asInt(row["exception_present"]) != 0,
		}
		nowMS := asInt64(row["now_ms"])
		if item.LastSuccessMS > 0 && nowMS > item.LastSuccessMS {
			item.LastSuccessAgeSecond = (nowMS - item.LastSuccessMS) / 1000
		}
		if item.ExceptionPresent {
			issues = append(issues, "refresh_exception:"+key)
		}
		switch item.Status {
		case "Scheduled":
		case "Running", "RunningOnAnotherReplica", "Scheduling", "WaitingForDependencies":
			issues = append(issues, "refresh_busy:"+key)
		case "", "Disabled":
			issues = append(issues, "refresh_not_scheduled:"+key)
		default:
			issues = append(issues, "refresh_status_unexpected:"+key)
		}
		pointer := pointers[surface]
		if item.LastSuccessMS != pointer.RefreshSuccessMS || item.LastRefreshMS != pointer.RefreshEndMS {
			issues = append(issues, "refresh_receipt_mismatch:"+key)
		}
		if item.LastSuccessAgeSecond > publicFreshnessMaxAgeSeconds {
			issues = append(issues, "refresh_last_success_over_36h:"+key)
		}
		refresh = append(refresh, item)
	}
	for key := range expectedViews {
		if !seenViews[key] {
			issues = append(issues, "refresh_missing:"+key)
		}
	}
	sort.Slice(refresh, func(i, j int) bool {
		return refresh[i].Database+"."+refresh[i].View < refresh[j].Database+"."+refresh[j].View
	})
	printMachineJSON(struct {
		Schema  string               `json:"schema"`
		Status  string               `json:"status"`
		RunUUID string               `json:"run_uuid"`
		Views   []refreshViewMetrics `json:"views"`
	}{Schema: "statground.inflearn.refresh_health.v2", Status: "observed", RunUUID: runUUID, Views: refresh})
	if len(issues) > 0 {
		sort.Strings(issues)
		return stateError("degraded", "public_freshness_verify", strings.Join(issues, ","))
	}
	printMachineJSON(map[string]any{
		"schema":              "statground.inflearn.public_freshness_result.v2",
		"status":              "healthy",
		"run_uuid":            runUUID,
		"activation_uuid":     activationUUID,
		"activation_revision": revision,
	})
	return nil
}

func (s *Service) verifyPublicServingParity(ctx context.Context, runUUID string, pointers map[string]publicationPointer) error {
	normalizedRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname, surface,
		toUnixTimestamp64Milli(generation) AS generation_ms,
		toUInt64(count()) AS row_count,
		toUInt64(uniqExact(logical_key)) AS logical_key_count,
		toUInt64(sumWithOverflow(row_fingerprint)) AS fingerprint_sum,
		toUInt64(groupBitXor(row_fingerprint)) AS fingerprint_xor
			FROM clusterAllReplicas(%s, 'lecture_publication', 'v_inflearn_public_catalog_admitted_serving_local')
		GROUP BY hostname, surface, generation ORDER BY surface, hostname
		SETTINGS skip_unavailable_shards = 0, max_execution_time = 120, max_threads = 2`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return newUpdateReadStateError("public_serving_signature_read", err)
	}
	if len(normalizedRows) != 12 {
		return stateError("degraded", "public_serving_signature", "normalized_endpoint_count")
	}
	normalized := make(map[string]candidateStats, 3)
	normalizedHosts := make(map[string]map[string]bool, 3)
	for _, row := range normalizedRows {
		surface := asString(row["surface"])
		host := asString(row["hostname"])
		stats, parseErr := candidateStatsFromRow(map[string]any{
			"rows": row["row_count"], "unique_keys": row["logical_key_count"],
			"fingerprint_sum": row["fingerprint_sum"], "fingerprint_xor": row["fingerprint_xor"],
			"generation_count": 1, "min_generation_ms": row["generation_ms"], "max_generation_ms": row["generation_ms"],
		})
		pointer, ok := pointers[surface]
		if normalizedHosts[surface] == nil {
			normalizedHosts[surface] = map[string]bool{}
		}
		if parseErr != nil || !ok || host == "" || normalizedHosts[surface][host] || stats.Rows == 0 || stats.Rows != stats.Unique ||
			stats.GenerationMS != pointer.GenerationMS {
			return stateError("degraded", "public_serving_signature", "normalized_signature_mismatch")
		}
		if prior, exists := normalized[surface]; exists && !sameCandidateStats(prior, stats) {
			return stateError("degraded", "public_serving_signature", "normalized_signature_mismatch")
		}
		normalizedHosts[surface][host] = true
		normalized[surface] = stats
	}
	for _, surface := range expectedPublicLectureSurfaces {
		if len(normalizedHosts[surface]) != 4 {
			return stateError("degraded", "public_serving_signature", "normalized_replica_loss")
		}
	}

	publicRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`WITH public_serving_counts AS (
		SELECT hostName() AS hostname, 'webr' AS surface, toUInt64(count()) AS row_count,
		  toUInt64(uniqExact(tuple(provider_key, course_id))) AS logical_key_count,
		  toUInt64(uniqExact(refresh_batch)) AS generation_count,
		  toUnixTimestamp64Milli(min(refresh_batch)) AS generation_ms
		FROM clusterAllReplicas(%s, 'webr_lecture', 'v_inflearn_r_lecture_catalog')
		GROUP BY hostname
		UNION ALL
		SELECT hostName() AS hostname, 'mirtype' AS surface, toUInt64(count()) AS row_count,
		  toUInt64(uniqExact(tuple(provider_key, course_id, target_language_code))) AS logical_key_count,
		  toUInt64(uniqExact(refresh_batch)) AS generation_count,
		  toUnixTimestamp64Milli(min(refresh_batch)) AS generation_ms
		FROM clusterAllReplicas(%s, 'mirtype_lecture', 'v_inflearn_language_lecture_catalog')
		GROUP BY hostname
		UNION ALL
		SELECT hostName() AS hostname, 'statground' AS surface, toUInt64(count()) AS row_count,
		  toUInt64(uniqExact(tuple(course_id, display_language))) AS logical_key_count,
		  toUInt64(uniqExact(refresh_batch)) AS generation_count,
		  toUnixTimestamp64Milli(min(refresh_batch)) AS generation_ms
		FROM clusterAllReplicas(%s, 'statground_lecture', 'v_inflearn_workbench_catalog')
		GROUP BY hostname
	)
	SELECT * FROM public_serving_counts ORDER BY surface, hostname
	SETTINGS skip_unavailable_shards = 0, max_execution_time = 120, max_threads = 2`,
		QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(s.Cfg.CHCluster), QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return newUpdateReadStateError("public_consumer_parity_read", err)
	}
	if len(publicRows) != 12 {
		return stateError("degraded", "public_consumer_parity", "public_endpoint_count")
	}
	public := make(map[string]candidateStats, 3)
	publicHosts := make(map[string]map[string]bool, 3)
	for _, row := range publicRows {
		surface := asString(row["surface"])
		host := asString(row["hostname"])
		stats, parseErr := candidateStatsFromRow(map[string]any{
			"rows": row["row_count"], "unique_keys": row["logical_key_count"],
			"fingerprint_sum": 0, "fingerprint_xor": 0,
			"generation_count":  row["generation_count"],
			"min_generation_ms": row["generation_ms"], "max_generation_ms": row["generation_ms"],
		})
		pointer, ok := pointers[surface]
		if publicHosts[surface] == nil {
			publicHosts[surface] = map[string]bool{}
		}
		normalizedStats, normalizedOK := normalized[surface]
		if parseErr != nil || !ok || !normalizedOK || host == "" || publicHosts[surface][host] || stats.Rows == 0 || stats.Rows != stats.Unique ||
			stats.GenerationCount != 1 || stats.GenerationMS != pointer.GenerationMS || stats.Rows != normalizedStats.Rows || stats.Unique != normalizedStats.Unique {
			return stateError("degraded", "public_consumer_parity", "public_signature_mismatch")
		}
		publicHosts[surface][host] = true
		public[surface] = stats
	}
	metrics := make([]publicServingIntegrityMetrics, 0, 3)
	for _, surface := range expectedPublicLectureSurfaces {
		normalizedStats, normalizedOK := normalized[surface]
		publicStats, publicOK := public[surface]
		if !normalizedOK || !publicOK || len(publicHosts[surface]) != 4 {
			return stateError("degraded", "public_consumer_parity", "public_surface_missing")
		}
		pointer := pointers[surface]
		metrics = append(metrics, publicServingIntegrityMetrics{
			Surface: surface, GenerationMS: pointer.GenerationMS, MarkerRows: pointer.Rows,
			NormalizedRows: normalizedStats.Rows, NormalizedLogicalKeys: normalizedStats.Unique,
			PublicRows: publicStats.Rows, PublicLogicalKeys: publicStats.Unique,
			NormalizedFingerprintSum: normalizedStats.FingerprintSum,
			NormalizedFingerprintXOR: normalizedStats.FingerprintXOR,
		})
	}
	printMachineJSON(struct {
		Schema   string                          `json:"schema"`
		Status   string                          `json:"status"`
		RunUUID  string                          `json:"run_uuid"`
		Surfaces []publicServingIntegrityMetrics `json:"surfaces"`
	}{Schema: "statground.inflearn.public_serving_integrity.v1", Status: "verified", RunUUID: runUUID, Surfaces: metrics})
	return nil
}
