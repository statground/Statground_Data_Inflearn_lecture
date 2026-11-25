package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"statground_inflearn/internal/inflearn"
)

func main() {
	checkOnly := flag.Bool("check-only", false, "read publication prerequisites without writing or refreshing")
	flag.Parse()
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := inflearn.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	svc := inflearn.NewService(cfg)
	if *checkOnly {
		preflight, checkErr := svc.CheckPublicLectureViews(context.Background())
		if checkErr != nil {
			fmt.Fprintln(os.Stderr, checkErr)
			os.Exit(1)
		}
		if err := json.NewEncoder(os.Stdout).Encode(preflight); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	receipt, err := svc.RefreshPublicLectureViews(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if outputPath := os.Getenv("GITHUB_OUTPUT"); outputPath != "" {
		output, openErr := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if openErr != nil {
			fmt.Fprintln(os.Stderr, openErr)
			os.Exit(1)
		}
		if _, writeErr := fmt.Fprintf(output, "publication_run_uuid=%s\npublication_activation_uuid=%s\npublication_activation_revision=%d\n",
			receipt.RunUUID, receipt.ActivationUUID, receipt.ActivationRevision); writeErr != nil {
			_ = output.Close()
			fmt.Fprintln(os.Stderr, writeErr)
			os.Exit(1)
		}
		if closeErr := output.Close(); closeErr != nil {
			fmt.Fprintln(os.Stderr, closeErr)
			os.Exit(1)
		}
	}
}
