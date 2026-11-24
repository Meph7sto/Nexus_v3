package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Wei-Shaw/sub2api/cmd"
	"github.com/Wei-Shaw/sub2api/internal/worker"
)

var Version string

func main() {
	showVersion := flag.Bool("version", false, "Show Worker version information")
	flag.Parse()
	version := cmd.ResolveVersion(Version)
	if *showVersion {
		fmt.Printf("Nexus Worker %s\n", version)
		return
	}
	cfg, err := worker.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Version = version
	s, err := worker.New(cfg, nil)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := s.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
