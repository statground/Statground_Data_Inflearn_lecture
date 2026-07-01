package inflearn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const translationPromptContract = "inflearn-course-display-translation-v1:no-links:json-only"
const curriculumTranslationPromptContract = "inflearn-course-curriculum-translation-v1:no-links:json-only"

type displayTranslationCandidate struct {
	CourseID          int
	SourceLocale      string
	Title             string
	Description       string
	CategoryMainTitle string
	CategorySubTitle  string
	LevelCode         string
	Keywords          string
}

type displayTranslationResult struct {
	Title             string `json:"title"`
	Description       string `json:"description"`
	CategoryMainTitle string `json:"category_main_title"`
	CategorySubTitle  string `json:"category_sub_title"`
	LevelCode         string `json:"level_code"`
	Keywords          string `json:"keywords"`
}

type displayCurriculumTranslationCandidate struct {
	CourseID     int
	SourceLocale string
	SectionID    int
	SectionTitle string
	UnitID       int
	UnitTitle    string
}

type displayCurriculumTranslationResult struct {
	SectionTitle string `json:"section_title"`
	UnitTitle    string `json:"unit_title"`
}

func LoadTranslationConfig() (Config, error) {
	host := envFirst("CH_HOST", "CLICKHOUSE_HOST")
	targets := splitCSV(envDefault("INFLEARN_TRANSLATION_TARGET_LANGS", "ko,en,ja,zh-Hans,zh-Hant,es,fr,de,pt-BR,ru,id,vi,th,ms,fil,hi,ar,it,nl,pl,sv,tr,uk"))
	cfg := Config{
		CHHost:                     host,
		CHPort:                     parsePort(envFirst("CH_PORT", "CLICKHOUSE_PORT"), 8123),
		CHUser:                     envFirstDefault([]string{"CH_USER", "CLICKHOUSE_USER"}, "default"),
		CHPassword:                 envFirstDefault([]string{"CH_PASSWORD", "CLICKHOUSE_PASSWORD"}, ""),
		CHDatabase:                 envFirstDefault([]string{"CH_DATABASE", "CLICKHOUSE_DATABASE"}, "Data_Lecture_Inflearn_Service"),
		CHRawDatabase:              envDefault("CH_RAW_DATABASE", "Data_Lecture_Inflearn_Service"),
		CHServiceDatabase:          envDefault("CH_SERVICE_DATABASE", "Data_Lecture_Inflearn_Service"),
		CHSecure:                   parseBool(envFirstDefault([]string{"CH_SECURE", "CLICKHOUSE_SECURE"}, "0")),
		CHCluster:                  envDefault("CH_CLUSTER", "statground_cluster"),
		CHInsertChunkSize:          parsePositiveInt(envDefault("CH_INSERT_CHUNK_SIZE", "100"), 100),
		CHInsertTimeout:            parseSecondsDefault(envDefault("CH_INSERT_TIMEOUT_SECONDS", "300"), 5*time.Minute),
		CHInsertDistributedSync:    parseBool(envDefault("CH_INSERT_DISTRIBUTED_SYNC", "false")),
		CHDirectReplicaFallback:    parseBool(envDefault("CH_DIRECT_REPLICA_FALLBACK", "true")),
		CHDirectOutboxFallback:     parseBool(envDefault("CH_DIRECT_OUTBOX_FALLBACK", "true")),
		CHOutboxDatabase:           envDefault("CH_OUTBOX_DATABASE", "Data_Lecture_Inflearn_Log"),
		CHOutboxTable:              envDefault("CH_OUTBOX_TABLE", "inflearn_direct_insert_outbox"),
		CHOutboxReplayLimit:        parsePositiveInt(envDefault("CH_OUTBOX_REPLAY_LIMIT", "50"), 50),
		UserAgent:                  envDefault("CRAWLER_USER_AGENT", "Mozilla/5.0 (compatible; StatgroundCrawler/2.0; +https://www.statground.net)"),
		TranslationTargetLanguages: normalizeTranslationTargets(targets),
		TranslationCourseIDs:       parseTranslationCourseIDs(envDefault("INFLEARN_TRANSLATION_COURSE_IDS", "")),
		TranslationBatchSize:       parsePositiveInt(envDefault("INFLEARN_TRANSLATION_BATCH_SIZE", "12"), 12),
		TranslationMaxPerRun:       parsePositiveInt(envDefault("INFLEARN_TRANSLATION_MAX_PER_RUN", "80"), 80),
		TranslationTable:           envDefault("INFLEARN_TRANSLATION_TABLE", "Data_Lecture_Inflearn_Service.inflearn_course_display_translation"),
		TranslationCurriculumTable: envDefault("INFLEARN_CURRICULUM_TRANSLATION_TABLE", "Data_Lecture_Inflearn_Service.inflearn_course_curriculum_display_translation"),
		TranslationProvider:        strings.ToLower(strings.TrimSpace(envDefault("INFLEARN_TRANSLATION_PROVIDER", ""))),
		TranslationEndpoint:        strings.TrimSpace(envDefault("INFLEARN_TRANSLATION_ENDPOINT", "")),
		TranslationAPIKey:          envFirst("INFLEARN_TRANSLATION_API_KEY", "OPENAI_API_KEY", "GH_MODELS_API_KEY", "GITHUB_MODELS_API_KEY", "OPENROUTER_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY"),
		TranslationModel:           strings.TrimSpace(envDefault("INFLEARN_TRANSLATION_MODEL", "")),
		TranslationDryRun:          parseBool(envDefault("INFLEARN_TRANSLATION_DRY_RUN", "0")),
	}
	if cfg.CHHost == "" {
		return Config{}, fmt.Errorf("missing required env: CH_HOST or CLICKHOUSE_HOST")
	}
	if cfg.TranslationProvider == "" {
		cfg.TranslationProvider = inferTranslationProvider()
	}
	if cfg.TranslationEndpoint == "" {
		cfg.TranslationEndpoint = defaultTranslationEndpoint(cfg.TranslationProvider)
	}
	if cfg.TranslationModel == "" {
		cfg.TranslationModel = defaultTranslationModel(cfg.TranslationProvider)
	}
	if cfg.TranslationAPIKey == "" && !cfg.TranslationDryRun {
		return Config{}, fmt.Errorf("missing translation API key; set INFLEARN_TRANSLATION_API_KEY, OPENAI_API_KEY, or GH_MODELS_API_KEY")
	}
	if len(cfg.TranslationTargetLanguages) == 0 {
		return Config{}, fmt.Errorf("INFLEARN_TRANSLATION_TARGET_LANGS resolved to empty list")
	}
	return cfg, nil
}

func (s *Service) RunTranslateDisplay(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("service is nil")
	}
	if err := s.ValidateClickHouseIngest(ctx); err != nil {
		return err
	}
	remaining := s.Cfg.TranslationMaxPerRun
	totalInserted := 0
	totalCurriculumInserted := 0
	for _, target := range s.Cfg.TranslationTargetLanguages {
		if remaining <= 0 {
			break
		}
		limit := s.Cfg.TranslationBatchSize
		if limit > remaining {
			limit = remaining
		}
		candidates, err := s.pickDisplayTranslationCandidates(ctx, target, limit)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			fmt.Printf("[translate] target=%s candidates=0\n", target)
			continue
		}
		rows := make([]map[string]any, 0, len(candidates))
		for _, candidate := range candidates {
			sourceHash := candidate.SourceHash()
			if s.Cfg.TranslationDryRun {
				fmt.Printf("[translate:dry-run] target=%s course_id=%d source_locale=%s source_hash=%d title=%q\n", target, candidate.CourseID, candidate.SourceLocale, sourceHash, candidate.Title)
				remaining--
				continue
			}
			translated, err := s.translateDisplayCandidate(ctx, target, candidate)
			if err != nil {
				fmt.Printf("[warn] translate failed target=%s course_id=%d error=%s\n", target, candidate.CourseID, sanitizeTranslationError(err))
				continue
			}
			now := FormatCHTime(NowDT64())
			rows = append(rows, map[string]any{
				"course_id":           candidate.CourseID,
				"source_locale":       candidate.SourceLocale,
				"target_language":     target,
				"source_hash":         sourceHash,
				"translated_at":       now,
				"title":               compactTranslationText(translated.Title, 500),
				"description":         compactTranslationText(translated.Description, 4000),
				"category_main_title": compactTranslationText(translated.CategoryMainTitle, 300),
				"category_sub_title":  compactTranslationText(translated.CategorySubTitle, 300),
				"level_code":          compactTranslationText(localizeTranslationLevel(target, translated.LevelCode), 120),
				"keywords":            compactTranslationText(translated.Keywords, 1200),
				"model":               s.Cfg.TranslationModel,
				"prompt_hash":         H64(translationPromptContract),
				"generation_status":   "success",
				"generation_error":    "",
				"ingested_at":         now,
			})
			remaining--
		}
		if len(rows) > 0 {
			if err := s.insertDisplayTranslations(ctx, rows); err != nil {
				return err
			}
			totalInserted += len(rows)
		}
		fmt.Printf("[translate] target=%s inserted=%d remaining=%d\n", target, len(rows), remaining)
	}
	for _, target := range s.Cfg.TranslationTargetLanguages {
		if remaining <= 0 {
			break
		}
		limit := s.Cfg.TranslationBatchSize
		if limit > remaining {
			limit = remaining
		}
		candidates, err := s.pickDisplayCurriculumTranslationCandidates(ctx, target, limit)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			fmt.Printf("[translate-curriculum] target=%s candidates=0\n", target)
			continue
		}
		rows := make([]map[string]any, 0, len(candidates))
		for _, candidate := range candidates {
			sourceHash := candidate.SourceHash()
			if s.Cfg.TranslationDryRun {
				fmt.Printf("[translate-curriculum:dry-run] target=%s course_id=%d section_id=%d unit_id=%d source_locale=%s source_hash=%d title=%q\n", target, candidate.CourseID, candidate.SectionID, candidate.UnitID, candidate.SourceLocale, sourceHash, candidate.UnitTitle)
				remaining--
				continue
			}
			translated, err := s.translateCurriculumCandidate(ctx, target, candidate)
			if err != nil {
				fmt.Printf("[warn] translate curriculum failed target=%s course_id=%d unit_id=%d error=%s\n", target, candidate.CourseID, candidate.UnitID, sanitizeTranslationError(err))
				continue
			}
			now := FormatCHTime(NowDT64())
			rows = append(rows, map[string]any{
				"course_id":         candidate.CourseID,
				"source_locale":     candidate.SourceLocale,
				"target_language":   target,
				"section_id":        candidate.SectionID,
				"unit_id":           candidate.UnitID,
				"source_hash":       sourceHash,
				"translated_at":     now,
				"section_title":     compactTranslationText(translated.SectionTitle, 500),
				"unit_title":        compactTranslationText(translated.UnitTitle, 500),
				"model":             s.Cfg.TranslationModel,
				"prompt_hash":       H64(curriculumTranslationPromptContract),
				"generation_status": "success",
				"generation_error":  "",
				"ingested_at":       now,
			})
			remaining--
		}
		if len(rows) > 0 {
			if err := s.insertCurriculumTranslations(ctx, rows); err != nil {
				return err
			}
			totalCurriculumInserted += len(rows)
		}
		fmt.Printf("[translate-curriculum] target=%s inserted=%d remaining=%d\n", target, len(rows), remaining)
	}
	fmt.Printf("[done] display_translations_inserted=%d curriculum_translations_inserted=%d\n", totalInserted, totalCurriculumInserted)
	return nil
}

func (c displayTranslationCandidate) SourceHash() uint64 {
	return H64(strings.Join([]string{
		c.SourceLocale,
		c.Title,
		c.Description,
		c.CategoryMainTitle,
		c.CategorySubTitle,
		c.LevelCode,
		c.Keywords,
	}, "\x1f"))
}

func (c displayCurriculumTranslationCandidate) SourceHash() uint64 {
	return H64(strings.Join([]string{
		c.SourceLocale,
		strconv.Itoa(c.SectionID),
		c.SectionTitle,
		strconv.Itoa(c.UnitID),
		c.UnitTitle,
	}, "\x1f"))
}

func (s *Service) pickDisplayTranslationCandidates(ctx context.Context, target string, limit int) ([]displayTranslationCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	target = normalizeTranslationTarget(target)
	sourcePreference := "multiIf(c.locale = 'ko', 0, c.locale = 'en', 1, 9)"
	if target == "en" {
		sourcePreference = "multiIf(c.locale = 'en', 0, c.locale = 'ko', 1, 9)"
	}
	courseIDFilter := ""
	if len(s.Cfg.TranslationCourseIDs) > 0 {
		courseIDFilter = fmt.Sprintf("AND course_id IN (%s)", translationCourseIDListSQL(s.Cfg.TranslationCourseIDs))
	}
	textExpr := "concat(title, ' ', description, ' ', category_main_title, ' ', category_sub_title, ' ', keywords)"
	nativePredicate := translationNativePredicateSQL(target, "title", textExpr)
	sql := fmt.Sprintf(`
        WITH latest AS (
          SELECT
            course_id AS course_id,
            locale AS locale,
            argMax(status, fetched_at) AS status_latest,
            argMax(title, fetched_at) AS title,
            argMax(description, fetched_at) AS description,
            argMax(category_main_title, fetched_at) AS category_main_title,
            argMax(category_sub_title, fetched_at) AS category_sub_title,
            argMax(level_code, fetched_at) AS level_code,
            argMax(keywords, fetched_at) AS keywords,
            argMax(published_at, fetched_at) AS published_at,
            argMax(last_updated_at, fetched_at) AS last_updated_at,
            max(fetched_at) AS max_fetched_at
          FROM %s.inflearn_course_dim
          WHERE locale IN ('ko', 'en')
            %s
          GROUP BY course_id, locale
          HAVING status_latest = 'PUBLISH' AND title != ''
        ),
        native_target AS (
          SELECT course_id AS native_course_id
          FROM latest
          WHERE %s
          GROUP BY native_course_id
        ),
        existing AS (
          SELECT course_id AS existing_course_id
          FROM %s
          WHERE toString(target_language) = %s
            AND toString(generation_status) = 'success'
          GROUP BY existing_course_id
        ),
        scored AS (
          SELECT
            c.course_id AS course_id,
            c.locale AS locale,
            c.title AS title,
            c.description AS description,
            c.category_main_title AS category_main_title,
            c.category_sub_title AS category_sub_title,
            c.level_code AS level_code,
            c.keywords AS keywords,
            greatest(
              if(c.published_at <= toDateTime64('1971-01-01 00:00:00.000', 3, 'Asia/Seoul'), toDateTime64('1970-01-01 00:00:00.000', 3, 'Asia/Seoul'), c.published_at),
              if(c.last_updated_at <= toDateTime64('1971-01-01 00:00:00.000', 3, 'Asia/Seoul'), toDateTime64('1970-01-01 00:00:00.000', 3, 'Asia/Seoul'), c.last_updated_at),
              c.max_fetched_at
            ) AS latest_activity_at,
            c.max_fetched_at AS max_fetched_at,
            %s AS source_rank
          FROM latest AS c
          LEFT JOIN native_target AS n ON n.native_course_id = c.course_id
          LEFT JOIN existing AS e ON e.existing_course_id = toUInt32(c.course_id)
          WHERE n.native_course_id = 0 AND e.existing_course_id = 0
        ),
        per_course AS (
          SELECT
            course_id,
            argMin(
              tuple(locale, title, description, category_main_title, category_sub_title, level_code, keywords),
              tuple(source_rank, -toUnixTimestamp64Milli(latest_activity_at), -toUnixTimestamp64Milli(max_fetched_at), -toInt64(course_id))
            ) AS best,
            max(latest_activity_at) AS order_latest_activity_at,
            max(max_fetched_at) AS order_max_fetched_at
          FROM scored
          GROUP BY course_id
        )
        SELECT
          course_id,
          tupleElement(best, 1) AS locale,
          tupleElement(best, 2) AS title,
          tupleElement(best, 3) AS description,
          tupleElement(best, 4) AS category_main_title,
          tupleElement(best, 5) AS category_sub_title,
          tupleElement(best, 6) AS level_code,
          tupleElement(best, 7) AS keywords
        FROM per_course
        ORDER BY order_latest_activity_at DESC, order_max_fetched_at DESC, course_id DESC
        LIMIT %d
        SETTINGS max_execution_time = 20, timeout_overflow_mode = 'break', max_threads = 4
    `, chIdent(s.Cfg.CHServiceDatabase), courseIDFilter, nativePredicate, chTablePath(s.Cfg.TranslationTable), QuoteSQLString(target), sourcePreference, limit)
	if parseBool(envDefault("INFLEARN_TRANSLATION_DEBUG_SQL", "0")) {
		fmt.Printf("[debug_sql] target=%s\n%s\n", target, sql)
	}
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	out := make([]displayTranslationCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, displayTranslationCandidate{
			CourseID:          asInt(row["course_id"]),
			SourceLocale:      asString(row["locale"]),
			Title:             asString(row["title"]),
			Description:       asString(row["description"]),
			CategoryMainTitle: asString(row["category_main_title"]),
			CategorySubTitle:  asString(row["category_sub_title"]),
			LevelCode:         asString(row["level_code"]),
			Keywords:          asString(row["keywords"]),
		})
	}
	return out, nil
}

func (s *Service) translateDisplayCandidate(ctx context.Context, target string, candidate displayTranslationCandidate) (displayTranslationResult, error) {
	target = normalizeTranslationTarget(target)
	input := map[string]any{
		"target_language":      target,
		"target_language_name": targetLanguageName(target),
		"title":                candidate.Title,
		"description":          candidate.Description,
		"category_main_title":  candidate.CategoryMainTitle,
		"category_sub_title":   candidate.CategorySubTitle,
		"level_code":           candidate.LevelCode,
		"keywords":             candidate.Keywords,
	}
	inputJSON, _ := json.Marshal(input)
	prompt := "Translate the Inflearn course metadata into the requested target language. Preserve brand names, programming language names, library names, and proper nouns when appropriate. Do not add hyperlinks or markdown. Return strict JSON with keys title, description, category_main_title, category_sub_title, level_code, keywords only.\n\nINPUT:\n" + string(inputJSON)
	payload := map[string]any{
		"model": s.Cfg.TranslationModel,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a precise course metadata translator for a multilingual education search interface."},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.2,
	}
	body, _ := json.Marshal(payload)
	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.Cfg.TranslationEndpoint, bytes.NewReader(body))
	if err != nil {
		return displayTranslationResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.Cfg.UserAgent)
	req.Header.Set("Authorization", "Bearer "+s.Cfg.TranslationAPIKey)
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return displayTranslationResult{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return displayTranslationResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return displayTranslationResult{}, fmt.Errorf("translation provider http %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return displayTranslationResult{}, err
	}
	if len(parsed.Choices) == 0 {
		return displayTranslationResult{}, fmt.Errorf("translation provider returned no choices")
	}
	content := extractJSONObject(parsed.Choices[0].Message.Content)
	var out displayTranslationResult
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		return displayTranslationResult{}, err
	}
	if strings.TrimSpace(out.Title) == "" {
		return displayTranslationResult{}, fmt.Errorf("translation title is empty")
	}
	return out, nil
}

func (s *Service) pickDisplayCurriculumTranslationCandidates(ctx context.Context, target string, limit int) ([]displayCurriculumTranslationCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	target = normalizeTranslationTarget(target)
	sourcePreference := "multiIf(c.locale = 'ko', 0, c.locale = 'en', 1, 9)"
	if target == "en" {
		sourcePreference = "multiIf(c.locale = 'en', 0, c.locale = 'ko', 1, 9)"
	}
	courseIDFilter := ""
	if len(s.Cfg.TranslationCourseIDs) > 0 {
		courseIDFilter = fmt.Sprintf("AND course_id IN (%s)", translationCourseIDListSQL(s.Cfg.TranslationCourseIDs))
	}
	textExpr := "concat(section_title, ' ', unit_title)"
	nativePredicate := translationNativePredicateSQL(target, "unit_title", textExpr)
	sql := fmt.Sprintf(`
        WITH latest AS (
          SELECT
            course_id AS course_id,
            locale AS locale,
            section_id AS section_id,
            unit_id AS unit_id,
            argMax(section_title, fetched_at) AS section_title,
            argMax(unit_title, fetched_at) AS unit_title,
            max(fetched_at) AS max_fetched_at
          FROM %s.inflearn_course_curriculum_unit
          WHERE locale IN ('ko', 'en')
            %s
          GROUP BY course_id, locale, section_id, unit_id
          HAVING unit_title != ''
        ),
        native_target AS (
          SELECT course_id AS native_course_id, section_id AS native_section_id, unit_id AS native_unit_id
          FROM latest
          WHERE %s
          GROUP BY native_course_id, native_section_id, native_unit_id
        ),
        existing AS (
          SELECT course_id AS existing_course_id, section_id AS existing_section_id, unit_id AS existing_unit_id
          FROM %s
          WHERE toString(target_language) = %s
            AND toString(generation_status) = 'success'
          GROUP BY existing_course_id, existing_section_id, existing_unit_id
        ),
        scored AS (
          SELECT
            c.course_id AS course_id,
            c.locale AS locale,
            c.section_id AS section_id,
            c.unit_id AS unit_id,
            c.section_title AS section_title,
            c.unit_title AS unit_title,
            c.max_fetched_at AS max_fetched_at,
            %s AS source_rank
          FROM latest AS c
          LEFT JOIN native_target AS n
            ON n.native_course_id = toUInt32(c.course_id)
           AND n.native_section_id = toUInt32(c.section_id)
           AND n.native_unit_id = toUInt32(c.unit_id)
          LEFT JOIN existing AS e
            ON e.existing_course_id = toUInt32(c.course_id)
           AND e.existing_section_id = toUInt32(c.section_id)
           AND e.existing_unit_id = toUInt32(c.unit_id)
          WHERE n.native_course_id = 0 AND e.existing_course_id = 0
        ),
        per_unit AS (
          SELECT
            course_id,
            section_id,
            unit_id,
            argMin(
              tuple(locale, section_title, unit_title),
              tuple(source_rank, -toUnixTimestamp64Milli(max_fetched_at), -toInt64(course_id), -toInt64(section_id), -toInt64(unit_id))
            ) AS best,
            max(max_fetched_at) AS order_max_fetched_at
          FROM scored
          GROUP BY course_id, section_id, unit_id
        )
        SELECT
          course_id,
          tupleElement(best, 1) AS locale,
          section_id,
          tupleElement(best, 2) AS section_title,
          unit_id,
          tupleElement(best, 3) AS unit_title
        FROM per_unit
        ORDER BY order_max_fetched_at DESC, course_id DESC, section_id ASC, unit_id ASC
        LIMIT %d
        SETTINGS max_execution_time = 20, timeout_overflow_mode = 'break', max_threads = 4
    `, chIdent(s.Cfg.CHServiceDatabase), courseIDFilter, nativePredicate, chTablePathWithDefault(s.Cfg.TranslationCurriculumTable, "Data_Lecture_Inflearn_Service", "inflearn_course_curriculum_display_translation"), QuoteSQLString(target), sourcePreference, limit)
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	out := make([]displayCurriculumTranslationCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, displayCurriculumTranslationCandidate{
			CourseID:     asInt(row["course_id"]),
			SourceLocale: asString(row["locale"]),
			SectionID:    asInt(row["section_id"]),
			SectionTitle: asString(row["section_title"]),
			UnitID:       asInt(row["unit_id"]),
			UnitTitle:    asString(row["unit_title"]),
		})
	}
	return out, nil
}

func (s *Service) translateCurriculumCandidate(ctx context.Context, target string, candidate displayCurriculumTranslationCandidate) (displayCurriculumTranslationResult, error) {
	target = normalizeTranslationTarget(target)
	input := map[string]any{
		"target_language":      target,
		"target_language_name": targetLanguageName(target),
		"section_title":        candidate.SectionTitle,
		"unit_title":           candidate.UnitTitle,
	}
	inputJSON, _ := json.Marshal(input)
	prompt := "Translate the Inflearn curriculum section and unit title into the requested target language. Preserve brand names, programming language names, library names, and proper nouns when appropriate. Do not add hyperlinks or markdown. Return strict JSON with keys section_title and unit_title only.\n\nINPUT:\n" + string(inputJSON)
	payload := map[string]any{
		"model": s.Cfg.TranslationModel,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a precise course curriculum translator for a multilingual education search interface."},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.2,
	}
	body, _ := json.Marshal(payload)
	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.Cfg.TranslationEndpoint, bytes.NewReader(body))
	if err != nil {
		return displayCurriculumTranslationResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.Cfg.UserAgent)
	req.Header.Set("Authorization", "Bearer "+s.Cfg.TranslationAPIKey)
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return displayCurriculumTranslationResult{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return displayCurriculumTranslationResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return displayCurriculumTranslationResult{}, fmt.Errorf("translation provider http %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return displayCurriculumTranslationResult{}, err
	}
	if len(parsed.Choices) == 0 {
		return displayCurriculumTranslationResult{}, fmt.Errorf("translation provider returned no choices")
	}
	content := extractJSONObject(parsed.Choices[0].Message.Content)
	var out displayCurriculumTranslationResult
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		return displayCurriculumTranslationResult{}, err
	}
	if strings.TrimSpace(out.UnitTitle) == "" {
		return displayCurriculumTranslationResult{}, fmt.Errorf("translation unit title is empty")
	}
	if strings.TrimSpace(out.SectionTitle) == "" {
		out.SectionTitle = candidate.SectionTitle
	}
	return out, nil
}

func (s *Service) insertDisplayTranslations(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	database, table := chTablePartsWithDefault(s.Cfg.TranslationTable, "Data_Lecture_Inflearn_Service", "inflearn_course_display_translation")
	return s.insertClickHouseRows(ctx, database, table, inflearnCourseDisplayTranslationColumns, rows)
}

func (s *Service) insertCurriculumTranslations(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	database, table := chTablePartsWithDefault(s.Cfg.TranslationCurriculumTable, "Data_Lecture_Inflearn_Service", "inflearn_course_curriculum_display_translation")
	return s.insertClickHouseRows(ctx, database, table, inflearnCourseCurriculumDisplayTranslationColumns, rows)
}

func normalizeTranslationTargets(targets []string) []string {
	out := make([]string, 0, len(targets))
	seen := map[string]struct{}{}
	for _, target := range targets {
		target = normalizeTranslationTarget(target)
		if target == "" {
			continue
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}

func normalizeTranslationTarget(raw string) string {
	raw = strings.TrimSpace(raw)
	lower := strings.ToLower(raw)
	switch {
	case raw == "":
		return ""
	case lower == "zh" || strings.HasPrefix(lower, "zh-"):
		if strings.Contains(lower, "tw") || strings.Contains(lower, "hk") || strings.Contains(lower, "mo") || strings.Contains(lower, "hant") {
			return "zh-Hant"
		}
		return "zh-Hans"
	case lower == "pt" || strings.HasPrefix(lower, "pt-"):
		return "pt-BR"
	case lower == "tl" || strings.HasPrefix(lower, "tl-") || lower == "fil" || strings.HasPrefix(lower, "fil-"):
		return "fil"
	}
	for _, target := range []string{"ko", "en", "ja", "es", "fr", "de", "ru", "id", "vi", "th", "ms", "fil", "hi", "ar", "it", "nl", "pl", "sv", "tr", "uk"} {
		if lower == strings.ToLower(target) || strings.HasPrefix(lower, strings.ToLower(target)+"-") {
			return target
		}
	}
	return ""
}

func parseTranslationCourseIDs(raw string) []int {
	parts := splitCSV(raw)
	out := make([]int, 0, len(parts))
	seen := map[int]struct{}{}
	for _, part := range parts {
		id, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func translationCourseIDListSQL(ids []int) string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		out = append(out, strconv.Itoa(id))
	}
	if len(out) == 0 {
		return "0"
	}
	return strings.Join(out, ", ")
}

func translationNativePredicateSQL(target, titleExpr, textExpr string) string {
	titleHangul := translationScriptCountSQL(titleExpr, "가-힣")
	titleKana := translationScriptCountSQL(titleExpr, "ぁ-んァ-ン")
	titleCyrillic := translationScriptCountSQL(titleExpr, "А-Яа-яЁёІіЇїЄєҐґ")
	titleArabic := translationScriptCountSQL(titleExpr, "؀-ۿ")
	titleDevanagari := translationScriptCountSQL(titleExpr, "ऀ-ॿ")
	titleThai := translationScriptCountSQL(titleExpr, "ก-๿")
	titleLatin := translationScriptCountSQL(titleExpr, "A-Za-zÀ-ÿĀ-žĐđıİŞşĞğÇçÖöÜüÑñ")
	textHangul := translationScriptCountSQL(textExpr, "가-힣")
	textKana := translationScriptCountSQL(textExpr, "ぁ-んァ-ン")
	textHan := translationScriptCountSQL(textExpr, "一-龥")
	textCyrillic := translationScriptCountSQL(textExpr, "А-Яа-яЁёІіЇїЄєҐґ")
	textArabic := translationScriptCountSQL(textExpr, "؀-ۿ")
	textDevanagari := translationScriptCountSQL(textExpr, "ऀ-ॿ")
	textThai := translationScriptCountSQL(textExpr, "ก-๿")
	switch normalizeTranslationTarget(target) {
	case "ko":
		return fmt.Sprintf("%s >= 4 AND %s = 0 AND %s = 0 AND %s <= 4 AND (%s * 3) >= %s", titleHangul, titleKana, textKana, textHan, titleHangul, titleLatin)
	case "ja":
		return fmt.Sprintf("%s >= 2 AND %s = 0", textKana, textHangul)
	case "zh-Hans", "zh-Hant":
		return fmt.Sprintf("%s >= 2 AND %s = 0 AND %s = 0", textHan, textHangul, textKana)
	case "ru", "uk":
		return fmt.Sprintf("%s >= 2", titleCyrillic)
	case "ar":
		return fmt.Sprintf("%s >= 2", titleArabic)
	case "hi":
		return fmt.Sprintf("%s >= 2", titleDevanagari)
	case "th":
		return fmt.Sprintf("%s >= 2", titleThai)
	default:
		return fmt.Sprintf("%s >= 3 AND (%s + %s + %s + %s + %s + %s + %s) = 0", titleLatin, textHangul, textKana, textHan, textCyrillic, textArabic, textDevanagari, textThai)
	}
}

func translationScriptCountSQL(textExpr, charClass string) string {
	return fmt.Sprintf("lengthUTF8(replaceRegexpAll(%s, '[^%s]', ''))", textExpr, charClass)
}

func targetLanguageName(target string) string {
	switch normalizeTranslationTarget(target) {
	case "ko":
		return "Korean"
	case "en":
		return "English"
	case "ja":
		return "Japanese"
	case "zh-Hans":
		return "Simplified Chinese"
	case "zh-Hant":
		return "Traditional Chinese"
	case "es":
		return "Spanish"
	case "fr":
		return "French"
	case "de":
		return "German"
	case "pt-BR":
		return "Brazilian Portuguese"
	case "ru":
		return "Russian"
	case "id":
		return "Indonesian"
	case "vi":
		return "Vietnamese"
	case "th":
		return "Thai"
	case "ms":
		return "Malay"
	case "fil":
		return "Filipino"
	case "hi":
		return "Hindi"
	case "ar":
		return "Arabic"
	case "it":
		return "Italian"
	case "nl":
		return "Dutch"
	case "pl":
		return "Polish"
	case "sv":
		return "Swedish"
	case "tr":
		return "Turkish"
	case "uk":
		return "Ukrainian"
	default:
		return target
	}
}

func localizeTranslationLevel(target, raw string) string {
	value := strings.TrimSpace(raw)
	key := strings.ToLower(strings.NewReplacer(" ", "", "_", "", "-", "").Replace(value))
	if key == "" {
		return value
	}
	levels := map[string]map[string]string{
		"beginner": {
			"ko": "초급", "en": "Beginner", "ja": "初級", "zh-Hans": "初级", "zh-Hant": "初級",
		},
		"intermediate": {
			"ko": "중급", "en": "Intermediate", "ja": "中級", "zh-Hans": "中级", "zh-Hant": "中級",
		},
		"advanced": {
			"ko": "고급", "en": "Advanced", "ja": "上級", "zh-Hans": "高级", "zh-Hant": "高級",
		},
		"alllevels": {
			"ko": "전체 수준", "en": "All levels", "ja": "全レベル", "zh-Hans": "所有级别", "zh-Hant": "所有級別",
		},
	}
	if byLang, ok := levels[key]; ok {
		if label := byLang[normalizeTranslationTarget(target)]; label != "" {
			return label
		}
	}
	return value
}

func inferTranslationProvider() string {
	switch {
	case strings.TrimSpace(osEnv("GH_MODELS_API_KEY")) != "" || strings.TrimSpace(osEnv("GITHUB_MODELS_API_KEY")) != "":
		return "github_models"
	case strings.TrimSpace(osEnv("OPENROUTER_API_KEY")) != "":
		return "openrouter"
	case strings.TrimSpace(osEnv("GROQ_API_KEY")) != "":
		return "groq"
	case strings.TrimSpace(osEnv("CEREBRAS_API_KEY")) != "":
		return "cerebras"
	default:
		return "openai"
	}
}

func defaultTranslationEndpoint(provider string) string {
	switch provider {
	case "github_models":
		return "https://models.github.ai/inference/chat/completions"
	case "openrouter":
		return "https://openrouter.ai/api/v1/chat/completions"
	case "groq":
		return "https://api.groq.com/openai/v1/chat/completions"
	case "cerebras":
		return "https://api.cerebras.ai/v1/chat/completions"
	default:
		return "https://api.openai.com/v1/chat/completions"
	}
}

func defaultTranslationModel(provider string) string {
	switch provider {
	case "github_models":
		return "openai/gpt-4.1"
	case "groq":
		return "llama-3.1-8b-instant"
	case "cerebras":
		return "llama3.1-8b"
	default:
		return "gpt-4.1-mini"
	}
}

func chTablePath(raw string) string {
	return chTablePathWithDefault(raw, "Data_Lecture_Inflearn_Service", "inflearn_course_display_translation")
}

func chTablePathWithDefault(raw, defaultDB, defaultTable string) string {
	db, table := chTablePartsWithDefault(raw, defaultDB, defaultTable)
	return chIdent(db) + "." + chIdent(table)
}

func chTablePartsWithDefault(raw, defaultDB, defaultTable string) (string, string) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) != 2 {
		return defaultDB, defaultTable
	}
	db := strings.TrimSpace(parts[0])
	table := strings.TrimSpace(parts[1])
	if db == "" || table == "" {
		return defaultDB, defaultTable
	}
	return db, table
}

func extractJSONObject(raw string) string {
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end >= start {
		return raw[start : end+1]
	}
	return raw
}

func compactTranslationText(raw string, limit int) string {
	raw = strings.TrimSpace(strings.Join(strings.Fields(raw), " "))
	raw = strings.ReplaceAll(raw, "http://", "")
	raw = strings.ReplaceAll(raw, "https://", "")
	if limit > 0 && len([]rune(raw)) > limit {
		runes := []rune(raw)
		raw = string(runes[:limit])
	}
	return raw
}

func sanitizeTranslationError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "unknown"
	}
	if len(msg) > 160 {
		msg = msg[:160]
	}
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(msg)
}

func osEnv(name string) string {
	return strings.TrimSpace(envDefault(name, ""))
}
