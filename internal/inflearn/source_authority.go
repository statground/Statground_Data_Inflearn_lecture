package inflearn

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sourceAuthorityFenceTable = "lecture_publication.inflearn_public_catalog_source_authority_fence"
	sourceAuthorityEventTable = "lecture_publication.inflearn_public_catalog_source_authority"
)

var (
	sourceAuthorityDrainTimeout = 30 * time.Second
	sourceAuthorityDrainPoll    = 250 * time.Millisecond
)

type sourceAuthorityEvent struct {
	CourseID      int
	Locale        string
	Status        string
	SourceFetched time.Time
	AuthorityRev  uint64
	OperationUUID string
	EventUUID     string
	RecordedAt    time.Time
}

func (e sourceAuthorityEvent) restrictive() bool { return e.Status != "PUBLISH" }

func (s *Service) sourceAuthorityClient() (*Service, error) {
	user := strings.TrimSpace(s.Cfg.PublicationCHUser)
	password := s.Cfg.PublicationCHPassword
	writer := strings.TrimSpace(s.Cfg.PublicationWriterID)
	if user == "" || password == "" || writer == "" {
		return nil, stateError("degraded", "source_authority_identity", "missing_dedicated_publisher_identity")
	}
	cfg := s.Cfg
	cfg.CHUser = user
	cfg.CHPassword = password
	cfg.CHDirectReplicaFallback = false
	cfg.CHDirectOutboxFallback = false
	return &Service{Cfg: cfg, HTTPClient: s.HTTPClient}, nil
}

func (s *Service) publishSourceAuthorityEvents(ctx context.Context, courseRows []map[string]any) error {
	if !s.Cfg.PublicationV2Enabled || len(courseRows) == 0 {
		return nil
	}
	s.sourceAuthorityMu.Lock()
	defer s.sourceAuthorityMu.Unlock()

	client, err := s.sourceAuthorityClient()
	if err != nil {
		return err
	}
	topology, err := client.readPublicationTopology(ctx)
	if err != nil {
		return err
	}
	events, err := sourceAuthorityEventsFromCourseRows(courseRows)
	if err != nil {
		return err
	}
	events, err = client.rejectOrRemoveStaleSourceAuthorityEvents(ctx, events)
	if err != nil || len(events) == 0 {
		return err
	}

	currentRevision, _, err := client.readSourceAuthorityRevision(ctx, topology, "source_authority_fence_preflight")
	if err != nil {
		return err
	}
	if currentRevision == ^uint64(0) {
		return stateError("degraded", "source_authority_fence", "authority_revision_exhausted")
	}
	serverMS, err := client.readServerTimeMS(ctx)
	if err != nil {
		return newUpdateReadStateError("source_authority_time", err)
	}
	recordedAt := time.UnixMilli(serverMS).In(KST)
	for _, event := range events {
		if event.SourceFetched.After(recordedAt) {
			recordedAt = event.SourceFetched
		}
	}
	operationUUID := UUIDv7String(recordedAt)
	revision := currentRevision + 1
	for i := range events {
		events[i].AuthorityRev = revision
		events[i].OperationUUID = operationUUID
		events[i].EventUUID = UUIDv7String(recordedAt.Add(time.Duration(i+1) * time.Millisecond))
		events[i].RecordedAt = recordedAt
	}

	if err := client.insertAndReadbackSourceAuthorityFence(ctx, topology, revision, operationUUID, recordedAt); err != nil {
		return err
	}
	// A newer event may have won while this writer acquired its fence. Recheck
	// immediately before the event INSERT so stale content cannot regain admission.
	if _, err := client.rejectOrRemoveStaleSourceAuthorityEvents(ctx, events); err != nil {
		return err
	}
	if err := client.insertAndReadbackSourceAuthorityEvents(ctx, topology, events); err != nil {
		return err
	}
	if err := client.requireLatestSourceAuthorityEvents(ctx, events); err != nil {
		return err
	}
	latestRevision, _, err := client.readSourceAuthorityRevision(ctx, topology, "source_authority_fence_post_event")
	if err != nil {
		return err
	}
	if latestRevision < revision {
		return stateError("degraded", "source_authority_fence_post_event", "authority_revision_regressed")
	}

	restrictive := make([]sourceAuthorityEvent, 0, len(events))
	for _, event := range events {
		if event.restrictive() {
			restrictive = append(restrictive, event)
		}
	}
	if len(restrictive) == 0 {
		return nil
	}
	if err := client.requireSourceAuthorityAdmissionClosed(ctx, topology, restrictive); err != nil {
		return err
	}
	if err := client.waitForSourceAuthorityReadDrain(ctx, topology, recordedAt); err != nil {
		return err
	}
	// The ACK is strict: repeat authority and admission readback after drain so
	// a concurrent stale writer cannot reopen a withdrawn key between checks.
	if err := client.requireLatestSourceAuthorityEvents(ctx, restrictive); err != nil {
		return err
	}
	return client.requireSourceAuthorityAdmissionClosed(ctx, topology, restrictive)
}

func sourceAuthorityEventsFromCourseRows(rows []map[string]any) ([]sourceAuthorityEvent, error) {
	byKey := map[string]sourceAuthorityEvent{}
	for _, row := range rows {
		if row == nil {
			return nil, stateError("degraded", "source_authority_event", "nil_event")
		}
		courseID := asInt(row["course_id"])
		locale := strings.TrimSpace(asString(row["locale"]))
		status := strings.ToUpper(strings.TrimSpace(asString(row["status"])))
		fetched, ok := ParseDT64(asString(row["fetched_at"]))
		minimumSourceTime := time.Date(1971, 1, 1, 0, 0, 0, 0, KST)
		if courseID <= 0 || locale == "" || status == "" || !ok || !fetched.After(minimumSourceTime) {
			return nil, stateError("degraded", "source_authority_event", "nil_event")
		}
		key := strconv.Itoa(courseID) + "\x00" + locale
		event := sourceAuthorityEvent{CourseID: courseID, Locale: locale, Status: status, SourceFetched: fetched}
		if previous, exists := byKey[key]; exists {
			if fetched.Before(previous.SourceFetched) {
				continue
			}
			if fetched.Equal(previous.SourceFetched) && status != previous.Status {
				return nil, stateError("degraded", "source_authority_event", "conflicting_same_time_event")
			}
		}
		byKey[key] = event
	}
	if len(byKey) == 0 {
		return nil, stateError("degraded", "source_authority_event", "nil_event")
	}
	out := make([]sourceAuthorityEvent, 0, len(byKey))
	for _, event := range byKey {
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CourseID != out[j].CourseID {
			return out[i].CourseID < out[j].CourseID
		}
		return out[i].Locale < out[j].Locale
	})
	return out, nil
}

func sourceAuthorityKeyPredicate(events []sourceAuthorityEvent) string {
	parts := make([]string, 0, len(events))
	for _, event := range events {
		parts = append(parts, fmt.Sprintf("(%d,%s)", event.CourseID, QuoteSQLString(event.Locale)))
	}
	return "(course_id,locale) IN (" + strings.Join(parts, ",") + ")"
}

func (s *Service) readLatestSourceAuthorityEvents(ctx context.Context, events []sourceAuthorityEvent) (map[string]map[string]any, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT course_id,locale,authority_revision,
		toString(operation_uuid) AS operation_uuid,toString(event_uuid) AS event_uuid,status,
		toUnixTimestamp64Milli(source_fetched_at) AS source_fetched_ms
		FROM lecture_publication.v_inflearn_public_catalog_source_authority_latest_local
		WHERE %s SETTINGS max_threads=1,max_execution_time=15`, sourceAuthorityKeyPredicate(events)))
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		key := strconv.Itoa(asInt(row["course_id"])) + "\x00" + asString(row["locale"])
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate source authority key")
		}
		out[key] = row
	}
	return out, nil
}

func (s *Service) rejectOrRemoveStaleSourceAuthorityEvents(ctx context.Context, events []sourceAuthorityEvent) ([]sourceAuthorityEvent, error) {
	latest, err := s.readLatestSourceAuthorityEvents(ctx, events)
	if err != nil {
		return nil, newUpdateReadStateError("source_authority_stale_check", err)
	}
	out := make([]sourceAuthorityEvent, 0, len(events))
	for _, event := range events {
		key := strconv.Itoa(event.CourseID) + "\x00" + event.Locale
		row, exists := latest[key]
		if !exists {
			out = append(out, event)
			continue
		}
		latestMS := asInt64(row["source_fetched_ms"])
		if latestMS > event.SourceFetched.UnixMilli() {
			return nil, stateError("degraded", "source_authority_stale_check", "stale_writer_rejected")
		}
		if latestMS == event.SourceFetched.UnixMilli() {
			if asString(row["status"]) != event.Status {
				return nil, stateError("degraded", "source_authority_stale_check", "conflicting_same_time_event")
			}
			continue
		}
		out = append(out, event)
	}
	return out, nil
}

func (s *Service) readSourceAuthorityRevision(ctx context.Context, topology map[string]int, phase string) (uint64, string, error) {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,authority_revision,
		toString(operation_uuid) AS operation_uuid
		FROM clusterAllReplicas(%s,lecture_publication.v_inflearn_public_catalog_source_authority_fence_current_local)
		ORDER BY hostname SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return 0, "", newUpdateReadStateError(phase, err)
	}
	if len(rows) != 4 {
		return 0, "", stateError("degraded", phase, "authority_fence_endpoint_count")
	}
	var revision uint64
	operation := ""
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		value, parseErr := exactUint64(row["authority_revision"])
		op := strings.ToLower(strings.TrimSpace(asString(row["operation_uuid"])))
		if parseErr != nil || value == 0 || op == "" || topology[host] == 0 || seen[host] {
			return 0, "", stateError("degraded", phase, "authority_fence_evidence_mismatch")
		}
		if revision == 0 {
			revision, operation = value, op
		} else if revision != value || operation != op {
			return 0, "", stateError("degraded", phase, "authority_fence_replica_divergence")
		}
		seen[host] = true
	}
	return revision, operation, nil
}

func (s *Service) insertAndReadbackSourceAuthorityFence(ctx context.Context, topology map[string]int, revision uint64, operationUUID string, recordedAt time.Time) error {
	payload, err := encodeClickHouseJSONEachRow(
		[]string{"authority_revision", "operation_uuid", "writer_id", "recorded_at"},
		[]map[string]any{{"authority_revision": revision, "operation_uuid": operationUUID,
			"writer_id": s.Cfg.PublicationWriterID, "recorded_at": FormatCHTime(recordedAt)}},
	)
	if err != nil {
		return err
	}
	token := "source-authority-fence-" + strings.ToLower(operationUUID)
	sql := fmt.Sprintf(`INSERT INTO %s (authority_revision,operation_uuid,writer_id,recorded_at)
		SETTINGS insert_distributed_sync=1,insert_quorum=4,insert_quorum_parallel=0,
		insert_deduplicate=1,insert_deduplication_token=%s FORMAT JSONEachRow`, sourceAuthorityFenceTable, QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt <= publicationMutationReconcileAttempts; attempt++ {
		_, insertErr = s.chPost(ctx, sql, payload, "application/x-ndjson")
		readbackErr := s.readbackSourceAuthorityFence(ctx, topology, revision, operationUUID)
		if readbackErr == nil {
			return nil
		}
		if insertErr != nil && !isTemporaryClickHouseWriteError(insertErr) {
			return newUpdateReadStateError("source_authority_fence_insert", insertErr)
		}
		if attempt == publicationMutationReconcileAttempts {
			return readbackErr
		}
	}
	return insertErr
}

func (s *Service) readbackSourceAuthorityFence(ctx context.Context, topology map[string]int, revision uint64, operationUUID string) error {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,count() AS rows,
		uniqExact(tuple(operation_uuid,writer_id,recorded_at)) AS versions,
		uniqExactIf(operation_uuid,operation_uuid=toUUID(%s)) AS matching_operations
		FROM clusterAllReplicas(%s,lecture_publication.inflearn_public_catalog_source_authority_fence_local)
		WHERE authority_revision=%d GROUP BY hostname ORDER BY hostname
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(operationUUID), QuoteSQLString(s.Cfg.CHCluster), revision))
	if err != nil {
		return newUpdateReadStateError("source_authority_fence_readback", err)
	}
	if len(rows) != 4 {
		return stateError("degraded", "source_authority_fence_readback", "authority_fence_endpoint_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		if topology[host] == 0 || seen[host] || asInt(row["rows"]) != 1 || asInt(row["versions"]) != 1 || asInt(row["matching_operations"]) != 1 {
			return stateError("degraded", "source_authority_fence_readback", "authority_fence_conflict")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) insertAndReadbackSourceAuthorityEvents(ctx context.Context, topology map[string]int, events []sourceAuthorityEvent) error {
	columns := []string{"authority_revision", "operation_uuid", "event_uuid", "course_id", "locale", "status", "source_fetched_at", "recorded_at"}
	rows := make([]map[string]any, 0, len(events))
	for _, event := range events {
		rows = append(rows, map[string]any{
			"authority_revision": event.AuthorityRev, "operation_uuid": event.OperationUUID,
			"event_uuid": event.EventUUID, "course_id": event.CourseID, "locale": event.Locale,
			"status": event.Status, "source_fetched_at": FormatCHTime(event.SourceFetched),
			"recorded_at": FormatCHTime(event.RecordedAt),
		})
	}
	payload, err := encodeClickHouseJSONEachRow(columns, rows)
	if err != nil {
		return err
	}
	token := "source-authority-events-" + strings.ToLower(events[0].OperationUUID)
	sql := fmt.Sprintf(`INSERT INTO %s (%s)
		SETTINGS insert_distributed_sync=1,insert_quorum=4,insert_quorum_parallel=0,
		insert_deduplicate=1,insert_deduplication_token=%s FORMAT JSONEachRow`,
		sourceAuthorityEventTable, clickHouseColumnList(columns), QuoteSQLString(token))
	var insertErr error
	for attempt := 0; attempt <= publicationMutationReconcileAttempts; attempt++ {
		_, insertErr = s.chPost(ctx, sql, payload, "application/x-ndjson")
		readbackErr := s.readbackSourceAuthorityEvents(ctx, topology, events)
		if readbackErr == nil {
			return nil
		}
		if insertErr != nil && !isTemporaryClickHouseWriteError(insertErr) {
			return newUpdateReadStateError("source_authority_event_insert", insertErr)
		}
		if attempt == publicationMutationReconcileAttempts {
			return readbackErr
		}
	}
	return insertErr
}

func (s *Service) readbackSourceAuthorityEvents(ctx context.Context, topology map[string]int, events []sourceAuthorityEvent) error {
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,count() AS rows,
		uniqExact(tuple(event_uuid,course_id,locale,status,source_fetched_at,recorded_at)) AS versions
		FROM clusterAllReplicas(%s,lecture_publication.inflearn_public_catalog_source_authority_local)
		WHERE authority_revision=%d AND operation_uuid=toUUID(%s)
		GROUP BY hostname ORDER BY hostname
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		QuoteSQLString(s.Cfg.CHCluster), events[0].AuthorityRev, QuoteSQLString(events[0].OperationUUID)))
	if err != nil {
		return newUpdateReadStateError("source_authority_event_readback", err)
	}
	if len(rows) != 4 {
		return stateError("degraded", "source_authority_event_readback", "authority_event_endpoint_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		if topology[host] == 0 || seen[host] || asInt(row["rows"]) != len(events) || asInt(row["versions"]) != len(events) {
			return stateError("degraded", "source_authority_event_readback", "authority_event_evidence_mismatch")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) requireLatestSourceAuthorityEvents(ctx context.Context, events []sourceAuthorityEvent) error {
	latest, err := s.readLatestSourceAuthorityEvents(ctx, events)
	if err != nil {
		return newUpdateReadStateError("source_authority_latest_readback", err)
	}
	if len(latest) != len(events) {
		return stateError("degraded", "source_authority_latest_readback", "authority_event_missing")
	}
	for _, event := range events {
		key := strconv.Itoa(event.CourseID) + "\x00" + event.Locale
		row := latest[key]
		if asInt64(row["authority_revision"]) != int64(event.AuthorityRev) ||
			strings.ToLower(asString(row["operation_uuid"])) != strings.ToLower(event.OperationUUID) ||
			asString(row["status"]) != event.Status || asInt64(row["source_fetched_ms"]) != event.SourceFetched.UnixMilli() {
			return stateError("degraded", "source_authority_latest_readback", "stale_writer_rejected")
		}
	}
	return nil
}

func (s *Service) requireSourceAuthorityAdmissionClosed(ctx context.Context, topology map[string]int, events []sourceAuthorityEvent) error {
	predicate := sourceAuthorityKeyPredicate(events)
	rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
		(SELECT count() FROM webr_lecture.v_inflearn_r_lecture_catalog WHERE %s)
		+(SELECT count() FROM mirtype_lecture.v_inflearn_language_lecture_catalog WHERE %s)
		+(SELECT count() FROM statground_lecture.v_inflearn_workbench_catalog WHERE %s) AS admitted_rows
		FROM clusterAllReplicas(%s,system.one) ORDER BY hostname
		SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=30`,
		predicate, predicate, predicate, QuoteSQLString(s.Cfg.CHCluster)))
	if err != nil {
		return newUpdateReadStateError("source_authority_admission_readback", err)
	}
	if len(rows) != 4 {
		return stateError("degraded", "source_authority_admission_readback", "admission_endpoint_count")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		host := asString(row["hostname"])
		if topology[host] == 0 || seen[host] || asInt64(row["admitted_rows"]) != 0 {
			return stateError("degraded", "source_authority_admission_readback", "withdrawal_still_admitted")
		}
		seen[host] = true
	}
	return nil
}

func (s *Service) waitForSourceAuthorityReadDrain(ctx context.Context, topology map[string]int, recordedAt time.Time) error {
	deadline := time.Now().Add(sourceAuthorityDrainTimeout)
	for {
		rows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT hostName() AS hostname,
			countIf((positionCaseInsensitive(query,'v_inflearn_r_lecture_catalog')>0
			  OR positionCaseInsensitive(query,'v_inflearn_language_lecture_catalog')>0
			  OR positionCaseInsensitive(query,'v_inflearn_workbench_catalog')>0)
			  AND positionCaseInsensitive(query,'system.processes')=0
			  AND toUnixTimestamp64Milli(now64(3,'Asia/Seoul'))-toInt64(elapsed*1000)<=%d) AS active_reads
			FROM clusterAllReplicas(%s,system.processes)
			GROUP BY hostname ORDER BY hostname
			SETTINGS skip_unavailable_shards=0,max_threads=1,max_execution_time=15`,
			recordedAt.UnixMilli(), QuoteSQLString(s.Cfg.CHCluster)))
		if err != nil {
			return newUpdateReadStateError("source_authority_read_drain", err)
		}
		if len(rows) != 4 {
			return stateError("degraded", "source_authority_read_drain", "drain_endpoint_count")
		}
		seen := map[string]bool{}
		active := int64(0)
		for _, row := range rows {
			host := asString(row["hostname"])
			if topology[host] == 0 || seen[host] {
				return stateError("degraded", "source_authority_read_drain", "drain_evidence_mismatch")
			}
			seen[host] = true
			active += asInt64(row["active_reads"])
		}
		if active == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return stateError("deferred", "source_authority_read_drain", "inflight_reads_not_drained")
		}
		timer := time.NewTimer(sourceAuthorityDrainPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return stateError("deferred", "source_authority_read_drain", "inflight_reads_not_drained")
		case <-timer.C:
		}
	}
}
