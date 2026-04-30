package inflearn

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type localCrawlerState struct {
	SchemaVersion int                         `json:"schema_version"`
	UpdatedAt     string                      `json:"updated_at"`
	Checkpoints   map[string]Checkpoint       `json:"checkpoints"`
	Courses       map[string]localCourseState `json:"courses"`
}

type localCourseState struct {
	CourseID      int    `json:"course_id"`
	Locale        string `json:"locale"`
	SourceURL     string `json:"source_url"`
	FirstSeenAt   string `json:"first_seen_at"`
	LastFetchedAt string `json:"last_fetched_at"`
}

func (s *Service) UseLocalState() bool {
	switch strings.ToLower(strings.TrimSpace(s.Cfg.StateBackend)) {
	case "", "local", "file", "github_cache", "github-cache":
		return true
	default:
		return false
	}
}

func (s *Service) localStatePath() string {
	dir := strings.TrimSpace(s.Cfg.StateDir)
	if dir == "" {
		dir = ".statground_state"
	}
	return filepath.Join(dir, "inflearn_crawler_state.json")
}

func emptyLocalCrawlerState() localCrawlerState {
	return localCrawlerState{
		SchemaVersion: 1,
		Checkpoints:   map[string]Checkpoint{},
		Courses:       map[string]localCourseState{},
	}
}

func (s *Service) loadLocalState() (localCrawlerState, error) {
	path := s.localStatePath()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return emptyLocalCrawlerState(), nil
		}
		return localCrawlerState{}, err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return emptyLocalCrawlerState(), nil
	}
	st := emptyLocalCrawlerState()
	if err := json.Unmarshal(b, &st); err != nil {
		return localCrawlerState{}, fmt.Errorf("failed to decode local crawler state %s: %w", path, err)
	}
	if st.Checkpoints == nil {
		st.Checkpoints = map[string]Checkpoint{}
	}
	if st.Courses == nil {
		st.Courses = map[string]localCourseState{}
	}
	if st.SchemaVersion == 0 {
		st.SchemaVersion = 1
	}
	return st, nil
}

func (s *Service) saveLocalState(st localCrawlerState) error {
	path := s.localStatePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	st.SchemaVersion = 1
	st.UpdatedAt = FormatCHTime(NowDT64())
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Service) localGetCheckpoint(source string) (Checkpoint, error) {
	st, err := s.loadLocalState()
	if err != nil {
		return Checkpoint{}, err
	}
	cp, ok := st.Checkpoints[source]
	if !ok {
		return Checkpoint{}, nil
	}
	return cp, nil
}

func (s *Service) localSetCheckpoint(source string, cp Checkpoint) error {
	st, err := s.loadLocalState()
	if err != nil {
		return err
	}
	if st.Checkpoints == nil {
		st.Checkpoints = map[string]Checkpoint{}
	}
	st.Checkpoints[source] = cp
	return s.saveLocalState(st)
}

func localCourseKey(courseID int, locale string) string {
	locale = strings.ToLower(strings.TrimSpace(locale))
	if locale == "" {
		locale = "ko"
	}
	return fmt.Sprintf("%d|%s", courseID, locale)
}

func (s *Service) localCourseExists(courseID int, locale string) (bool, error) {
	if courseID <= 0 {
		return false, nil
	}
	st, err := s.loadLocalState()
	if err != nil {
		return false, err
	}
	_, ok := st.Courses[localCourseKey(courseID, locale)]
	return ok, nil
}

func (s *Service) localRememberCourseRows(rows CourseRows) error {
	if len(rows.SnapshotRaw) == 0 && len(rows.CourseDim) == 0 {
		return nil
	}
	st, err := s.loadLocalState()
	if err != nil {
		return err
	}
	if st.Courses == nil {
		st.Courses = map[string]localCourseState{}
	}
	remember := func(courseID int, locale, sourceURL, fetchedAt string) {
		if courseID <= 0 {
			return
		}
		locale = strings.ToLower(strings.TrimSpace(locale))
		if locale == "" {
			locale = "ko"
		}
		fetchedAt = strings.TrimSpace(fetchedAt)
		if fetchedAt == "" {
			fetchedAt = FormatCHTime(NowDT64())
		}
		key := localCourseKey(courseID, locale)
		cur := st.Courses[key]
		cur.CourseID = courseID
		cur.Locale = locale
		if strings.TrimSpace(sourceURL) != "" {
			cur.SourceURL = strings.TrimSpace(sourceURL)
		}
		if strings.TrimSpace(cur.FirstSeenAt) == "" {
			cur.FirstSeenAt = fetchedAt
		}
		cur.LastFetchedAt = fetchedAt
		st.Courses[key] = cur
	}
	for _, row := range rows.SnapshotRaw {
		remember(asInt(row["course_id"]), asString(row["locale"]), asString(row["source_url"]), asString(row["fetched_at"]))
	}
	for _, row := range rows.CourseDim {
		remember(asInt(row["course_id"]), asString(row["locale"]), "", asString(row["fetched_at"]))
	}
	return s.saveLocalState(st)
}

func (s *Service) localPickUpdateURLs(limit int) ([]updatePick, error) {
	if limit <= 0 {
		return nil, nil
	}
	st, err := s.loadLocalState()
	if err != nil {
		return nil, err
	}
	items := make([]localCourseState, 0, len(st.Courses))
	for _, course := range st.Courses {
		if course.CourseID <= 0 || strings.TrimSpace(course.SourceURL) == "" {
			continue
		}
		items = append(items, course)
	}
	sort.Slice(items, func(i, j int) bool {
		ti := localStateTime(items[i].LastFetchedAt)
		tj := localStateTime(items[j].LastFetchedAt)
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if items[i].CourseID != items[j].CourseID {
			return items[i].CourseID < items[j].CourseID
		}
		return items[i].Locale < items[j].Locale
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]updatePick, 0, len(items))
	for _, item := range items {
		out = append(out, updatePick{CourseID: item.CourseID, Locale: item.Locale, SourceURL: item.SourceURL})
	}
	return out, nil
}

func localStateTime(raw string) time.Time {
	if t, ok := ParseDT64(raw); ok {
		return t
	}
	return EpochKST()
}
