package inflearn

import (
	"encoding/json"
	"testing"
)

func TestRecentCourseURLsFromSearchPayload(t *testing.T) {
	payload := map[string]any{
		"data": map[string]any{
			"items": []any{
				map[string]any{
					"id": json.Number("342604"),
					"course": map[string]any{
						"slug": "mastering-harness-en",
					},
				},
				map[string]any{
					"course": map[string]any{
						"id":   json.Number("342533"),
						"slug": "무료 타입스크립트",
					},
				},
				map[string]any{
					"id": json.Number("341961"),
					"course": map[string]any{
						"url": "/course/interview-guide",
					},
				},
			},
		},
	}

	got := recentCourseURLsFromSearchPayload(payload)
	want := []string{
		"https://www.inflearn.com/course/mastering-harness-en?cid=342604",
		"https://www.inflearn.com/course/%EB%AC%B4%EB%A3%8C%20%ED%83%80%EC%9E%85%EC%8A%A4%ED%81%AC%EB%A6%BD%ED%8A%B8?cid=342533",
		"https://www.inflearn.com/course/interview-guide?cid=341961",
	}
	if len(got) != len(want) {
		t.Fatalf("recentCourseURLsFromSearchPayload length = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("url[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRecentSearchURLUsesRECENTDefaults(t *testing.T) {
	svc := NewService(Config{
		RecentSearchBaseURL: "https://course-api.inflearn.com/",
		RecentHeadPageSize:  100,
		RecentHeadSort:      "RECENT",
		RecentHeadTypes:     "ONLINE",
	})
	got := svc.recentSearchURL(2)
	for _, want := range []string{
		"https://course-api.inflearn.com/client/api/v2/courses/search?",
		"pageNumber=2",
		"pageSize=100",
		"sort=RECENT",
		"types=ONLINE",
		"isBot=false",
	} {
		if !stringsContains(got, want) {
			t.Fatalf("recentSearchURL should contain %q: %s", want, got)
		}
	}
}
