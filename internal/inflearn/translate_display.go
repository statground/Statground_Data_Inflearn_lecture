package inflearn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const translationPromptContract = "inflearn-course-display-translation-v1:no-links:json-only"

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
		UserAgent:                  envDefault("CRAWLER_USER_AGENT", "Mozilla/5.0 (compatible; StatgroundCrawler/2.0; +https://www.statground.net)"),
		TranslationTargetLanguages: normalizeTranslationTargets(targets),
		TranslationBatchSize:       parsePositiveInt(envDefault("INFLEARN_TRANSLATION_BATCH_SIZE", "12"), 12),
		TranslationMaxPerRun:       parsePositiveInt(envDefault("INFLEARN_TRANSLATION_MAX_PER_RUN", "80"), 80),
		TranslationTable:           envDefault("INFLEARN_TRANSLATION_TABLE", "Data_Lecture_Inflearn_Service.inflearn_course_display_translation"),
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
	remaining := s.Cfg.TranslationMaxPerRun
	totalInserted := 0
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
				"level_code":          compactTranslationText(translated.LevelCode, 120),
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
	fmt.Printf("[done] display_translations_inserted=%d\n", totalInserted)
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

func (s *Service) pickDisplayTranslationCandidates(ctx context.Context, target string, limit int) ([]displayTranslationCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	target = normalizeTranslationTarget(target)
	sourcePreference := "multiIf(c.locale = 'ko', 0, c.locale = 'en', 1, 9)"
	if target == "en" {
		sourcePreference = "multiIf(c.locale = 'en', 0, c.locale = 'ko', 1, 9)"
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
          WHERE target_language = %s
            AND generation_status = 'success'
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
          LEFT JOIN existing AS e ON e.existing_course_id = c.course_id
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
    `, chIdent(s.Cfg.CHServiceDatabase), nativePredicate, chTablePath(s.Cfg.TranslationTable), QuoteSQLString(target), sourcePreference, limit)
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

func (s *Service) insertDisplayTranslations(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	sql := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", chTablePath(s.Cfg.TranslationTable))
	insertCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err := s.chPost(insertCtx, sql, buf.Bytes(), "application/x-ndjson")
	return err
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

func translationNativePredicateSQL(target, titleExpr, textExpr string) string {
	hangul := translationScriptCountSQL(titleExpr, "가-힣")
	kana := translationScriptCountSQL(titleExpr, "ぁ-んァ-ン")
	han := translationScriptCountSQL(titleExpr, "一-龥")
	cyrillic := translationScriptCountSQL(titleExpr, "А-Яа-яЁёІіЇїЄєҐґ")
	arabic := translationScriptCountSQL(titleExpr, "؀-ۿ")
	devanagari := translationScriptCountSQL(titleExpr, "ऀ-ॿ")
	thai := translationScriptCountSQL(titleExpr, "ก-๿")
	latin := translationScriptCountSQL(titleExpr, "A-Za-zÀ-ÿĀ-žĐđıİŞşĞğÇçÖöÜüÑñ")
	switch normalizeTranslationTarget(target) {
	case "ko":
		return fmt.Sprintf("%s >= 4 AND %s = 0 AND %s <= 2 AND (%s * 3) >= %s", hangul, kana, han, hangul, latin)
	case "ja":
		return fmt.Sprintf("%s >= 2", kana)
	case "zh-Hans", "zh-Hant":
		return fmt.Sprintf("%s >= 2 AND %s = 0 AND %s = 0", han, hangul, kana)
	case "ru", "uk":
		return fmt.Sprintf("%s >= 2", cyrillic)
	case "ar":
		return fmt.Sprintf("%s >= 2", arabic)
	case "hi":
		return fmt.Sprintf("%s >= 2", devanagari)
	case "th":
		return fmt.Sprintf("%s >= 2", thai)
	default:
		return fmt.Sprintf("%s >= 3 AND (%s + %s + %s + %s + %s + %s + %s) = 0", latin, hangul, kana, han, cyrillic, arabic, devanagari, thai)
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
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) != 2 {
		return chIdent("Data_Lecture_Inflearn_Service") + "." + chIdent("inflearn_course_display_translation")
	}
	return chIdent(parts[0]) + "." + chIdent(parts[1])
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
