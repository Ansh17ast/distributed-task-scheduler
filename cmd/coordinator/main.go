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
	nodeID := flag.String("node-id", "coord-1", "Coordinator node ID")
	grpcPort := flag.Int("grpc-port", 9001, "gRPC server port")
	httpPort := flag.Int("http-port", 8001, "HTTP health and metrics port")
	raftBind := flag.String("raft-bind", "127.0.0.1:7001", "Raft bind address")
	flag.Parse()

	cfg := config.DefaultCoordinatorConfig(*nodeID, *grpcPort, *httpPort, *raftBind)
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger := logging.NewLogger(cfg.LogLevel)
	ctx := logging.WithTraceID(context.Background(), fmt.Sprintf("boot-%s-%d", *nodeID, time.Now().Unix()))

	logger.InfoContext(ctx, "starting coordinator node",
		"node_id", cfg.NodeID,
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"raft_bind", cfg.RaftBindAddr,
	)

	// Initialize Health & Readiness Server
	healthServer := health.NewServer(cfg.NodeID, func() map[string]interface{} {
		return map[string]interface{}{
			"node_id":   cfg.NodeID,
			"grpc_port": cfg.GRPCPort,
			"version":   "0.1.0-alpha",
		}
	})

	mux := http.NewServeMux()
	healthServer.RegisterRoutes(mux)

	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: mux,
	}

	go func() {
		logger.InfoContext(ctx, "http health server listening", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorContext(ctx, "http server failed", "error", err)
		}
	}()

	// Await OS termination signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	logger.InfoContext(ctx, "shutting down coordinator", "signal", sig.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.ErrorContext(ctx, "error during http shutdown", "error", err)
	}
	logger.InfoContext(ctx, "coordinator shutdown complete")
}
