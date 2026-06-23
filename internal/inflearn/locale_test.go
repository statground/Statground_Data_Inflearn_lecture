package inflearn

import (
	"strings"
	"testing"
)

func TestParseLocaleFromURLRecognizesLanguagePathPrefix(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"https://www.inflearn.com/course/r-data?cid=1", "ko"},
		{"https://www.inflearn.com/en/course/r-data?cid=1", "en"},
		{"https://www.inflearn.com/ja/course/r-data?cid=1", "ja"},
		{"https://www.inflearn.com/zh-Hans/course/r-data?cid=1", "zh-Hans"},
		{"https://www.inflearn.com/pt-BR/course/r-data?cid=1", "pt-BR"},
		{"https://www.inflearn.com/course/r-data?cid=1&locale=ja", "ja"},
	}
	for _, tt := range tests {
		if got := ParseLocaleFromURL(tt.raw); got != tt.want {
			t.Fatalf("ParseLocaleFromURL(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

func TestCourseExistsSQLRequiresKoreanTextForKoreanLocale(t *testing.T) {
	koSQL := courseExistsSQL("Data_Lecture_Inflearn_Service", 324587, "ko")
	if !strings.Contains(koSQL, "HAVING match(title, '[가-힣]')") {
		t.Fatalf("ko courseExistsSQL must require Korean display text:\n%s", koSQL)
	}
	jaSQL := courseExistsSQL("Data_Lecture_Inflearn_Service", 324587, "ja")
	if strings.Contains(jaSQL, "[가-힣]") {
		t.Fatalf("non-ko courseExistsSQL should not require Korean text:\n%s", jaSQL)
	}
	if !strings.Contains(jaSQL, "locale = 'ja'") {
		t.Fatalf("ja courseExistsSQL should keep ja locale:\n%s", jaSQL)
	}
}
