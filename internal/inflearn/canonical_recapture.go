package inflearn

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strconv"
	"strings"
)

type canonicalKoreanSource struct {
	courseID int
	url      string
}

func parseCanonicalKoreanSources(urls []string) ([]canonicalKoreanSource, error) {
	if len(urls) == 0 || len(urls) > 4 {
		return nil, fmt.Errorf("canonical recapture requires one to four course URLs")
	}
	sources := make([]canonicalKoreanSource, 0, len(urls))
	seen := make(map[int]bool, len(urls))
	for _, source := range urls {
		u, err := url.Parse(source)
		if err != nil || u.Scheme != "https" || u.Host != "www.inflearn.com" || u.User != nil || u.Fragment != "" {
			return nil, fmt.Errorf("expected an HTTPS Inflearn course URL")
		}
		slug, ok := strings.CutPrefix(u.Path, "/course/")
		id, locale := ParseCourseIDAndLocale(source)
		if !ok || slug == "" || strings.Contains(slug, "/") || id < 1 || locale != "ko" || u.RawQuery != "cid="+strconv.Itoa(id) {
			return nil, fmt.Errorf("expected a canonical Korean course path with an explicit cid")
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate course ID %d", id)
		}
		seen[id] = true
		sources = append(sources, canonicalKoreanSource{courseID: id, url: source})
	}
	return sources, nil
}

// RunCanonicalKoreanRecapture fills missing source keys during pre-publication
// recovery. Proceed only while every newest Korean record has the known
// legacy Japanese URL mismatch. Historical canonical records are retained.
func (s *Service) RunCanonicalKoreanRecapture(ctx context.Context, urls []string) error {
	sources, err := parseCanonicalKoreanSources(urls)
	if err != nil {
		return err
	}
	if !s.UseClickHouseIngest() || s.UseLocalState() || !s.Cfg.CHInsertDistributedSync ||
		s.Cfg.CHDirectReplicaFallback || s.Cfg.CHDirectOutboxFallback || s.Cfg.PublicationV2Enabled {
		return fmt.Errorf("canonical recapture requires synchronous ClickHouse ingestion without fallback while publication is inactive")
	}
	if err := s.ValidateIngest(ctx); err != nil {
		return err
	}
	keys := make([]string, 0, len(sources))
	ids := make([]string, 0, len(sources))
	for _, source := range sources {
		id := strconv.Itoa(source.courseID)
		ids = append(ids, id)
		keys = append(keys, fmt.Sprintf("(%s,%s)", id, QuoteSQLString(source.url)))
	}
	raw := chIdent(s.Cfg.CHRawDatabase) + ".inflearn_course_snapshot_raw"
	dim := chIdent(s.Cfg.CHServiceDatabase) + ".inflearn_course_dim"
	const limits = "SETTINGS max_threads=1,max_execution_time=18,skip_unavailable_shards=0,max_rows_to_read=4000000,max_bytes_to_read=671088640"
	where := "WHERE locale = 'ko' AND (course_id, source_url) IN (" + strings.Join(keys, ",") + ")"
	cohort := "WHERE locale = 'ko' AND course_id IN (" + strings.Join(ids, ",") + ")"
	readBaseline := func() (map[int]string, error) {
		rows, err := s.CHQueryRows(ctx, `SELECT course_id, toString(max(fetched_at)) AS newest,
			max(fetched_at) > maxIf(fetched_at, NOT startsWith(source_url,'https://www.inflearn.com/ja/course/')) AS latest_is_mismatch
			FROM `+raw+" "+cohort+" AND status_code = 'OK' GROUP BY course_id "+limits)
		if err != nil {
			return nil, fmt.Errorf("canonical raw preflight: %w", err)
		}
		if len(rows) != len(sources) {
			return nil, fmt.Errorf("canonical recapture could not verify the previous source for every selected course")
		}
		baseline := make(map[int]string, len(rows))
		for _, row := range rows {
			id := asInt(row["course_id"])
			if asInt(row["latest_is_mismatch"]) != 1 {
				return nil, fmt.Errorf("course %d no longer has the latest legacy URL mismatch; reconcile the batch before retrying", id)
			}
			baseline[id] = asString(row["newest"])
		}
		for _, source := range sources {
			if _, ok := ParseDT64(baseline[source.courseID]); !ok {
				return nil, fmt.Errorf("missing baseline timestamp for course %d", source.courseID)
			}
		}
		return baseline, nil
	}
	baseline, err := readBaseline()
	if err != nil {
		return err
	}
	fetchedAt := NowDT64()
	var batch CourseRows
	expectedRaw := make(map[int]int, len(sources))
	for _, source := range sources {
		previous, _ := ParseDT64(baseline[source.courseID])
		if !fetchedAt.After(previous) {
			return fmt.Errorf("recapture time would not supersede the previous source for course %d", source.courseID)
		}
		course, err := s.fetchCourseData(ctx, source.url, fetchedAt)
		if err != nil {
			return fmt.Errorf("fetch canonical course %d: %w", source.courseID, err)
		}
		if course.CourseID != source.courseID || course.Locale != "ko" || course.SourceURL != source.url {
			return fmt.Errorf("canonical source identity changed for course %d", source.courseID)
		}
		rows := buildCourseRows(course, fetchedAt)
		if len(rows.SnapshotRaw) == 0 || len(rows.CourseDim) != 1 || strings.TrimSpace(asString(rows.CourseDim[0]["title"])) == "" {
			return fmt.Errorf("canonical course %d lacks a complete source payload", source.courseID)
		}
		for _, row := range rows.SnapshotRaw {
			if asInt(row["course_id"]) != source.courseID || row["locale"] != "ko" || row["source_url"] != source.url || row["status_code"] != "OK" {
				return fmt.Errorf("canonical snapshot identity changed for course %d", source.courseID)
			}
		}
		expectedRaw[source.courseID] = len(rows.SnapshotRaw)
		batch.Append(rows)
	}
	current, err := readBaseline()
	if err != nil {
		return err
	}
	if !maps.Equal(baseline, current) {
		return fmt.Errorf("a selected source changed during fetch; reconcile before writing")
	}
	fetched := FormatCHTime(fetchedAt)
	fmt.Printf("[canonical-recapture] write intent fetched_at=%s courses=%d raw=%d\n", fetched, len(sources), len(batch.SnapshotRaw))
	if err := batch.InsertAll(ctx, s); err != nil {
		return fmt.Errorf("canonical recapture write requires reconciliation at %s: %w", fetched, err)
	}
	stamp := " AND fetched_at = toDateTime64(" + QuoteSQLString(fetched) + ",3,'Asia/Seoul')"
	for _, proof := range []struct {
		name, query string
		count       func(int) int
	}{
		{"raw", "SELECT course_id, count() AS n FROM " + raw + " " + where + stamp + " AND status_code = 'OK' GROUP BY course_id " + limits,
			func(id int) int { return expectedRaw[id] }},
		{"dimension", "SELECT course_id, count() AS n FROM " + dim + " " + cohort + stamp + " AND title != '' GROUP BY course_id " + limits,
			func(id int) int { return 1 }},
	} {
		rows, err := s.CHQueryRows(ctx, proof.query)
		if err != nil || len(rows) != len(sources) {
			return fmt.Errorf("canonical %s delivery could not be verified at %s: %v", proof.name, fetched, err)
		}
		for _, row := range rows {
			id := asInt(row["course_id"])
			if expectedRaw[id] == 0 || asInt(row["n"]) != proof.count(id) {
				return fmt.Errorf("canonical %s count does not match course %d at %s", proof.name, id, fetched)
			}
		}
	}
	fmt.Printf("[canonical-recapture] verified fetched_at=%s courses=%d raw=%d dimensions=%d\n", fetched, len(sources), len(batch.SnapshotRaw), len(batch.CourseDim))
	return nil
}
