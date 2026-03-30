package main

import (
	"context"
	"fmt"
	"os"

	"statground_inflearn/internal/inflearn"
)

func main() {
	cfg, err := inflearn.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	svc := inflearn.NewService(cfg)
	if err := svc.RunCollectNew(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
