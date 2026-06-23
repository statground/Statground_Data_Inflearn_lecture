package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"statground_inflearn/internal/inflearn"
)

func main() {
	cfg, err := inflearn.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raw := os.Getenv("INFLEARN_COURSE_URLS")
	urls := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	svc := inflearn.NewService(cfg)
	if err := svc.RunCollectURLs(context.Background(), urls); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
