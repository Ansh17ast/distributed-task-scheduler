package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/health"
	"distributed-scheduler/internal/logging"
)

func main() {
	workerID := flag.String("worker-id", "worker-1", "Worker node ID")
	coordAddr := flag.String("coordinator", "127.0.0.1:9001", "Coordinator gRPC address")
	slots := flag.Int("slots", 4, "Available execution slots")
	httpPort := flag.Int("http-port", 8011, "Worker health HTTP port")
	flag.Parse()

	cfg := config.DefaultWorkerConfig(*workerID, *coordAddr)
	cfg.MaxSlots = int32(*slots)
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid worker configuration: %v\n", err)
		os.Exit(1)
	}

	logger := logging.NewLogger(cfg.LogLevel)
	ctx := logging.WithWorkerID(context.Background(), cfg.WorkerID)

	logger.InfoContext(ctx, "starting worker daemon",
		"worker_id", cfg.WorkerID,
		"coordinator", cfg.CoordinatorEndpoint,
		"max_slots", cfg.MaxSlots,
		"http_port", *httpPort,
	)

	// Worker Health Server
	healthServer := health.NewServer(cfg.WorkerID, func() map[string]interface{} {
		return map[string]interface{}{
			"worker_id":   cfg.WorkerID,
			"max_slots":   cfg.MaxSlots,
			"coordinator": cfg.CoordinatorEndpoint,
			"version":     "0.1.0-alpha",
		}
	})

	mux := http.NewServeMux()
	healthServer.RegisterRoutes(mux)

	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: mux,
	}

	go func() {
		logger.InfoContext(ctx, "worker health server listening", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorContext(ctx, "worker http server failed", "error", err)
		}
	}()

	// Await OS termination signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	logger.InfoContext(ctx, "shutting down worker", "signal", sig.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.ErrorContext(ctx, "error during worker http shutdown", "error", err)
	}
	logger.InfoContext(ctx, "worker shutdown complete")
}
