package inflearn

import (
	"reflect"
	"testing"
	"time"
)

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

func TestInferTranslationProviderMatchesKeyPriority(t *testing.T) {
	for _, key := range []string{"OPENAI_API_KEY", "GH_MODELS_API_KEY", "GITHUB_MODELS_API_KEY", "OPENROUTER_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY"} {
		t.Setenv(key, "")
	}
	t.Setenv("OPENROUTER_API_KEY", "test-openrouter")
	t.Setenv("GH_MODELS_API_KEY", "test-gh")
	t.Setenv("OPENAI_API_KEY", "test-openai")
	if got := inferTranslationProvider(); got != "openai" {
		t.Fatalf("provider with OPENAI key selected first = %q", got)
	}
	t.Setenv("OPENAI_API_KEY", "")
	if got := inferTranslationProvider(); got != "github_models" {
		t.Fatalf("provider with GH key selected first = %q", got)
	}
	t.Setenv("GH_MODELS_API_KEY", "")
	if got := inferTranslationProvider(); got != "openrouter" {
		t.Fatalf("provider with OpenRouter key = %q", got)
	}
	if got := defaultTranslationModel("openrouter"); got != "openai/gpt-4.1-mini" {
		t.Fatalf("OpenRouter model = %q", got)
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
