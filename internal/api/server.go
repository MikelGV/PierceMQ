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
)

var wg sync.WaitGroup

func NewServer() http.Handler {
	mux := http.NewServeMux()

	var handler http.Handler = mux

	return handler
}

func GracefulShutDown(ctx context.Context, httpServer *http.Server) error {
	go func() {
		defer wg.Done()
		<-ctx.Done()

		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)

		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error shutting down http server %s\n", err)
		}
	}()

	return nil
}

func Run(
	ctx context.Context,
	w io.Writer,
	getenv func(string) string,
) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	srvr := NewServer()

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

	wg.Add(1)

	GracefulShutDown(ctx, httpServer)

	wg.Wait()
	return nil
}
