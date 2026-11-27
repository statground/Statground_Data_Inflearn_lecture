package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"statground_inflearn/internal/inflearn"
)

func main() {
	canonicalKoRecapture := flag.Bool("canonical-ko-recapture", false, "collect up to four missing canonical Korean sources in ClickHouse")
	flag.Parse()
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
	ctx := context.Background()
	if *canonicalKoRecapture {
		svc.HTTPClient.CheckRedirect = checkCanonicalRecaptureRedirect
		err = svc.RunCanonicalKoreanRecapture(ctx, urls)
	} else {
		err = svc.RunCollectURLs(ctx, urls)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkCanonicalRecaptureRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > 4 || req.URL.Scheme != "https" || req.URL.Host != via[0].URL.Host {
		return fmt.Errorf("recapture redirect changed the source endpoint")
	}
	switch req.URL.Host {
	case "www.inflearn.com":
		if strings.HasPrefix(req.URL.Path, "/course/") && inflearn.ParseLocaleFromURL(req.URL.String()) == "ko" {
			return nil
		}
	case "course-api.inflearn.com":
		return nil
	}
	return fmt.Errorf("recapture redirect left the selected Korean source path")
}
