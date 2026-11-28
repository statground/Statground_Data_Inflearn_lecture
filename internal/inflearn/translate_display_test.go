package inflearn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func translationTestService(t *testing.T, answer any) *Service {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		content, _ := json.Marshal(answer)
		response, _ := json.Marshal(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": string(content)}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
	t.Cleanup(server.Close)
	return &Service{
		Cfg:        Config{TranslationEndpoint: server.URL, TranslationModel: "test", TranslationAPIKey: "test"},
		HTTPClient: server.Client(),
	}
}

func TestDisplayTranslationRejectsMissingNonemptySourceFields(t *testing.T) {
	candidate := displayTranslationCandidate{Title: "원문", Description: "설명"}
	svc := translationTestService(t, displayTranslationResult{Title: "Translated"})
	_, err := svc.translateDisplayCandidate(context.Background(), "en", candidate)
	if err == nil || !stringsContains(err.Error(), "description") {
		t.Fatalf("missing translated description must not be marked success: %v", err)
	}
}

func TestDisplayTranslationAllowsAbsentOptionalSourceFields(t *testing.T) {
	candidate := displayTranslationCandidate{Title: "원문"}
	svc := translationTestService(t, displayTranslationResult{Title: "Translated"})
	got, err := svc.translateDisplayCandidate(context.Background(), "en", candidate)
	if err != nil || got.Title != "Translated" {
		t.Fatalf("translation of a title-only source must remain valid: got=%+v err=%v", got, err)
	}
}

func TestCurriculumTranslationRejectsMissingSectionTitle(t *testing.T) {
	candidate := displayCurriculumTranslationCandidate{SectionTitle: "목차", UnitTitle: "강의"}
	svc := translationTestService(t, displayCurriculumTranslationResult{UnitTitle: "Lecture"})
	_, err := svc.translateCurriculumCandidate(context.Background(), "en", candidate)
	if err == nil || !stringsContains(err.Error(), "section title") {
		t.Fatalf("missing translated section title must not be replaced with the source: %v", err)
	}
}

func TestTranslationCandidatesRetryIncompleteCurrentSourceOnly(t *testing.T) {
	queries := []string{}
	svc := &Service{
		Cfg: Config{CHHost: "localhost", CHPort: 8123, CHServiceDatabase: "Data_Lecture_Inflearn_Service"},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(request.Body)
			queries = append(queries, string(body))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[]}`)), Header: make(http.Header)}, nil
		})},
	}
	if _, err := svc.pickDisplayTranslationCandidates(context.Background(), "en", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.pickDisplayCurriculumTranslationCandidates(context.Background(), "en", 1); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 {
		t.Fatalf("got %d candidate queries, want 2", len(queries))
	}
	for index, required := range [][]string{
		{"e.existing_source_hash = c.source_hash", "tupleElement(e.translated, 1) = ''", "c.description != '' AND tupleElement(e.translated, 2) = ''"},
		{"e.existing_source_hash = c.source_hash", "c.section_title != '' AND tupleElement(e.translated, 1) = ''", "tupleElement(e.translated, 2) = ''"},
	} {
		for _, fragment := range required {
			if !strings.Contains(queries[index], fragment) {
				t.Errorf("candidate query %d missing %q", index, fragment)
			}
		}
	}
}

func TestTranslationFailureConsumesAttemptBudgetAndFailsRun(t *testing.T) {
	attempts := 0
	svc := &Service{
		Cfg: Config{
			CHHost: "db.invalid", CHPort: 8123, CHUser: "fixture", CHServiceDatabase: "lecture", TranslationTable: "lecture.display",
			TranslationCurriculumTable: "lecture.curriculum", TranslationEndpoint: "https://provider.invalid",
			TranslationTargetLanguages: []string{"en", "ko", "ja"}, TranslationBatchSize: 1, TranslationMaxPerRun: 2,
		},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			response := func(status int, body string) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			}
			if request.URL.Hostname() == "provider.invalid" {
				attempts++
				return response(http.StatusUnauthorized, "")
			}
			query, _ := io.ReadAll(request.Body)
			if strings.Contains(string(query), "SELECT 1 AS ok") {
				return response(http.StatusOK, `{"data":[{"ok":1}]}`)
			}
			if strings.Contains(string(query), "INSERT INTO") {
				t.Fatal("failed translation must not write rows")
			}
			return response(http.StatusOK, `{"data":[{"course_id":1,"locale":"en","title":"Source","section_id":1,"unit_id":1,"section_title":"Section","unit_title":"Unit"}]}`)
		})},
	}
	if err := svc.RunTranslateDisplay(context.Background()); err == nil || !strings.Contains(err.Error(), "2 attempted rows") {
		t.Fatalf("failed translation run must report incomplete progress: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("provider requests = %d, want 2", attempts)
	}
}

func TestRotatedTranslationTargets(t *testing.T) {
	targets := []string{"ko", "en", "ja"}
	for _, test := range []struct {
		at   time.Time
		want []string
	}{
		{time.Unix(0, 0), []string{"ko", "en", "ja"}},
		{time.Unix(int64(12*time.Hour/time.Second), 0), []string{"en", "ja", "ko"}},
		{time.Unix(int64(24*time.Hour/time.Second), 0), []string{"ja", "ko", "en"}},
		{time.Unix(int64(36*time.Hour/time.Second), 0), []string{"ko", "en", "ja"}},
	} {
		if got := rotatedTranslationTargets(targets, test.at); !reflect.DeepEqual(got, test.want) {
			t.Errorf("at %s: got %v, want %v", test.at, got, test.want)
		}
	}
	if got := rotatedTranslationTargets(nil, time.Now()); got != nil {
		t.Errorf("empty targets: got %v", got)
	}
	if !reflect.DeepEqual(targets, []string{"ko", "en", "ja"}) {
		t.Errorf("input targets changed: %v", targets)
	}
}

func TestSplitTranslationBudget(t *testing.T) {
	first := time.Unix(0, 0)
	second := first.Add(12 * time.Hour)
	for _, test := range []struct {
		max               int
		at                time.Time
		display, syllabus int
	}{
		{0, first, 0, 0},
		{1, first, 1, 0},
		{1, second, 0, 1},
		{2, first, 1, 1},
		{80, first, 40, 40},
		{81, second, 40, 41},
	} {
		display, syllabus := splitTranslationBudget(test.max, test.at)
		if display != test.display || syllabus != test.syllabus {
			t.Errorf("max=%d at=%s: got %d/%d, want %d/%d", test.max, test.at, display, syllabus, test.display, test.syllabus)
		}
	}
}

func TestTranslationConfigMatchesRProjectProviderOrder(t *testing.T) {
	t.Setenv("CH_HOST", "localhost")
	t.Setenv("INFLEARN_TRANSLATION_PROVIDER", "")
	t.Setenv("INFLEARN_TRANSLATION_API_KEY", "")
	t.Setenv("INFLEARN_TRANSLATION_ENDPOINT", "")
	t.Setenv("INFLEARN_TRANSLATION_MODEL", "")
	t.Setenv("INFLEARN_TRANSLATION_DRY_RUN", "0")
	t.Setenv("OPENAI_API_KEY", "unused-provider")
	t.Setenv("OPENROUTER_API_KEY", "openrouter-fixture")
	t.Setenv("GROQ_API_KEY", "groq-fixture")
	t.Setenv("CEREBRAS_API_KEY", "cerebras-fixture")
	cfg, err := LoadTranslationConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TranslationProvider != "openrouter" || cfg.TranslationModel != "google/gemini-2.5-flash-lite" ||
		!reflect.DeepEqual(cfg.translationFallbacks, []translationRoute{{"groq", "groq-fixture"}, {"cerebras", "cerebras-fixture"}}) {
		t.Fatal("provider order or model differs from the R-project translation convention")
	}
	t.Setenv("OPENROUTER_API_KEY", "")
	cfg, err = LoadTranslationConfig()
	if err != nil || cfg.TranslationProvider != "groq" || len(cfg.translationFallbacks) != 1 {
		t.Fatal("unset provider should be skipped")
	}
	t.Setenv("GROQ_API_KEY", "")
	t.Setenv("CEREBRAS_API_KEY", "")
	if _, err := LoadTranslationConfig(); err == nil {
		t.Fatal("direct OpenAI must not be inferred")
	}
}

func TestTranslationFallbackSpendsBudgetAndRecordsActualModel(t *testing.T) {
	svc := &Service{Cfg: Config{
		TranslationProvider: "openrouter", TranslationModel: "primary-model", TranslationAPIKey: "first-fixture",
		translationFallbacks: []translationRoute{{"groq", "second-fixture"}, {"cerebras", "third-fixture"}},
	}}
	var providers []string
	translate := func(provider *Service) (string, error) {
		providers = append(providers, provider.Cfg.TranslationProvider)
		if provider.Cfg.TranslationProvider == "openrouter" {
			return "", io.ErrUnexpectedEOF
		}
		if provider.Cfg.TranslationProvider != "groq" || provider.Cfg.TranslationAPIKey != "second-fixture" ||
			provider.Cfg.TranslationEndpoint != "https://api.groq.com/openai/v1/chat/completions" {
			t.Fatal("fallback used the wrong endpoint or credential")
		}
		return "translation", nil
	}
	remaining := 2
	result, model, err := tryTranslation(svc, &remaining, translate)
	if err != nil || result != "translation" || model != "openai/gpt-oss-20b" || remaining != 0 ||
		!reflect.DeepEqual(providers, []string{"openrouter", "groq"}) {
		t.Fatal("fallback must use one remaining request and report the actual model")
	}
	providers = nil
	remaining = 1
	if _, _, err := tryTranslation(svc, &remaining, translate); err == nil || remaining != 0 || len(providers) != 1 {
		t.Fatal("translation fallback exceeded the remaining request budget")
	}
}

func TestTranslationConfigUsesOnlySelectedProviderKey(t *testing.T) {
	for _, key := range []string{"INFLEARN_TRANSLATION_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY"} {
		t.Setenv(key, "")
	}
	t.Setenv("CH_HOST", "localhost")
	t.Setenv("INFLEARN_TRANSLATION_DRY_RUN", "0")
	t.Setenv("INFLEARN_TRANSLATION_PROVIDER", "openrouter")
	t.Setenv("OPENAI_API_KEY", "wrong-provider")
	if _, err := LoadTranslationConfig(); err == nil || !strings.Contains(err.Error(), "OPENROUTER_API_KEY") {
		t.Fatalf("an OpenAI key must not authorize OpenRouter: %v", err)
	}
	t.Setenv("OPENROUTER_API_KEY", "selected-provider")
	cfg, err := LoadTranslationConfig()
	if err != nil || cfg.TranslationAPIKey != "selected-provider" {
		t.Fatalf("selected provider key was not used: key=%q err=%v", cfg.TranslationAPIKey, err)
	}
	t.Setenv("INFLEARN_TRANSLATION_API_KEY", "explicit-override")
	cfg, err = LoadTranslationConfig()
	if err != nil || cfg.TranslationAPIKey != "explicit-override" {
		t.Fatalf("explicit override was not used: key=%q err=%v", cfg.TranslationAPIKey, err)
	}
	t.Setenv("INFLEARN_TRANSLATION_PROVIDER", "github_models")
	if _, err := LoadTranslationConfig(); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("retired GitHub Models must fail closed: %v", err)
	}
}

func TestTranslationNativePredicateKoreanChecksFullDisplayText(t *testing.T) {
	sql := translationNativePredicateSQL("ko", "title", "display_text")
	for _, want := range []string{
		"replaceRegexpAll(title, '[^가-힣]'",
		"replaceRegexpAll(display_text, '[^ぁ-んァ-ン]'",
		"replaceRegexpAll(display_text, '[^一-龥]'",
	} {
		if !stringsContains(sql, want) {
			t.Fatalf("korean native predicate should contain %q: %s", want, sql)
		}
	}
}

func TestTranslationCourseIDListSQL(t *testing.T) {
	ids := parseTranslationCourseIDs("324573, 324573, bad, 0, 326644")
	if got := translationCourseIDListSQL(ids); got != "324573, 326644" {
		t.Fatalf("translationCourseIDListSQL = %q", got)
	}
}

func TestLocalizeTranslationLevel(t *testing.T) {
	if got := localizeTranslationLevel("ko", "Beginner"); got != "초급" {
		t.Fatalf("Beginner in Korean = %q", got)
	}
	if got := localizeTranslationLevel("zh-TW", "Advanced"); got != "高級" {
		t.Fatalf("Advanced in zh-TW = %q", got)
	}
}

func stringsContains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return needle == ""
}
