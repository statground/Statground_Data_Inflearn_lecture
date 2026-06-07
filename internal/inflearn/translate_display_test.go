package inflearn

import "testing"

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
