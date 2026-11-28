package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"statground_inflearn/internal/inflearn"
)

func main() {
	checkOnly := flag.Bool("check-only", false, "read publication prerequisites without writing or refreshing")
	checkReadersOnly := flag.Bool("check-readers-only", false, "read the DB reader inventory and authenticated process states")
	resumePath := flag.String("resume-first-publication", "", "private checkpoint for an interrupted first publication")
	recoverReaders := flag.Bool("recover-first-publication-readers", false, "reconcile unactivated readers under the checkpoint lease")
	flag.Parse()
	if flag.NArg() != 0 || (*checkReadersOnly && (*checkOnly || *resumePath != "")) ||
		(*recoverReaders && (*checkOnly || *checkReadersOnly || *resumePath == "")) {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := inflearn.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	svc := inflearn.NewService(cfg)
	var checkpoint inflearn.FirstPublicationCheckpoint
	if *resumePath != "" {
		checkpoint, err = readCheckpoint(*resumePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if *recoverReaders {
		if err := svc.RecoverFirstPublicationReaders(context.Background(), checkpoint); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(`{"status":"reader_reconciliation_completed"}`)
		return
	}
	if *checkOnly || *checkReadersOnly {
		var preflight any
		var checkErr error
		if *checkReadersOnly {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			preflight, checkErr = svc.CheckPublicationReaders(ctx)
			cancel()
		} else if *resumePath != "" {
			preflight, checkErr = svc.CheckFirstPublicationResume(context.Background(), checkpoint)
		} else {
			preflight, checkErr = svc.CheckPublicLectureViews(context.Background())
		}
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
	var receipt inflearn.PublicRefreshReceipt
	if *resumePath != "" {
		receipt, err = svc.ResumeFirstPublicLectureViews(context.Background(), checkpoint)
	} else {
		receipt, err = svc.RefreshPublicLectureViews(context.Background())
	}
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

func readCheckpoint(path string) (inflearn.FirstPublicationCheckpoint, error) {
	var checkpoint inflearn.FirstPublicationCheckpoint
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return checkpoint, fmt.Errorf("invalid first-publication checkpoint file")
	}
	file, err := os.Open(path)
	if err != nil {
		return checkpoint, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return checkpoint, fmt.Errorf("first-publication checkpoint changed during open")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return checkpoint, fmt.Errorf("invalid first-publication checkpoint JSON")
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return checkpoint, fmt.Errorf("unexpected content after first-publication checkpoint")
	}
	return checkpoint, nil
}
