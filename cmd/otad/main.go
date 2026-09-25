// Command otad runs the local OTA orchestrator daemon: an in-process simulated
// device fleet, a JSON file state store, the rollout state machine and the
// HTTP API (default :8080).
//
//	go run ./cmd/otad -addr :8080 -state ./data/state.json
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/api"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	statePath := flag.String("state", "data/state.json", "path to the JSON state snapshot")
	batchWait := flag.Duration("batch-wait", 3*time.Second, "per-batch health-gate wait window")
	poll := flag.Duration("poll", 100*time.Millisecond, "decision loop interval")
	ratio := flag.Float64("ratio", 0.8, "default success-ratio gate")
	flag.Parse()

	logger := log.New(os.Stdout, "otad ", log.LstdFlags|log.Lmicroseconds)

	st, err := store.New(*statePath)
	if err != nil {
		logger.Fatalf("store: %v", err)
	}
	fl := fleet.New()
	o := orchestrator.New(st, fl, orchestrator.Config{
		BatchWait:    *batchWait,
		PollInterval: *poll,
		SuccessRatio: *ratio,
		Logger:       logger,
	}, time.Now)
	fl.SetSink(directSink{o})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := o.Start(ctx); err != nil {
		logger.Fatalf("start orchestrator: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewServer(o),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Printf("listening on %s (state=%s, batch-wait=%s)", *addr, *statePath, *batchWait)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Printf("shutting down...")
	shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shCtx)
	if err := o.Shutdown(shCtx); err != nil {
		logger.Printf("shutdown: %v", err)
	}
	logger.Printf("stopped")
}

// directSink connects the simulated fleet's asynchronous reports to the
// orchestrator's report ingress (same in-process entrypoint as HTTP reports).
type directSink struct{ o orchestrator.Orchestrator }

func (s directSink) Deliver(ctx context.Context, r domain.Report) error {
	_, err := s.o.HandleReport(ctx, r)
	return err
}
