package api

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
)

func NewServer(rds *broker.RedisStore) http.Handler {
	mux := http.NewServeMux()

	var handler http.Handler = mux

	return handler
}

func GracefulShutDown(wg *sync.WaitGroup, ctx context.Context, httpServer *http.Server) error {
	defer wg.Done()
	<-ctx.Done()

	shutdownCtx := context.Background()
	shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)

	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down http server %s\n", err)
	}

	return nil
}

func Run(
	ctx context.Context,
	w io.Writer,
	getenv func(string) string,
) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	rds, err := broker.Redis_Connect()

	if err != nil {
		return fmt.Errorf("Failed to connect to redis: %s\n", err)
	}

	srvr := NewServer(rds)

	httpServer := &http.Server{
		Addr:    net.JoinHostPort("localhost", "8080"),
		Handler: srvr,
	}

	go func() {
		fmt.Println("Server is up")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "error listening and serving: %s\n", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		go GracefulShutDown(&wg, ctx, httpServer)
	}()

	wg.Wait()
	return nil
}
