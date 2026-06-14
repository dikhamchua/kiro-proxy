package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	upstreamURL := getEnv("UPSTREAM_URL", "http://localhost:20128")
	listenAddr := getEnv("LISTEN_HOST", "127.0.0.1") + ":" + getEnv("LISTEN_PORT", "8081")
	maxRetries := 2
	if v := os.Getenv("MAX_RETRIES"); v == "0" {
		maxRetries = 0
	} else if v == "3" {
		maxRetries = 3
	}

	logger := log.New(os.Stdout, "[kiro-proxy] ", log.LstdFlags|log.Lmsgprefix)

	proxy := &ProxyHandler{
		UpstreamURL: upstreamURL,
		MaxRetries:  maxRetries,
		Logger:      logger,
		Client: &http.Client{
			Timeout: 0, // no timeout for streaming
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/", proxy)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	logger.Printf("Starting kiro-proxy middleware")
	logger.Printf("  Upstream: %s", upstreamURL)
	logger.Printf("  Listen:   %s", listenAddr)
	logger.Printf("  Retries:  %d", maxRetries)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
