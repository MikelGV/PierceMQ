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
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
)

func NewServer(
	rds *broker.RedisStore,
	config *config.Config,
	stores *storage.Stores,
) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := rds.Conn.Ping(ctx).Err(); err != nil {
			http.Error(w, fmt.Sprintf("redis: %v", err), http.StatusServiceUnavailable)
			return
		}
		if stores != nil && stores.Write != nil {
			if err := stores.Write.Conn.PingContext(ctx); err != nil {
				http.Error(w, fmt.Sprintf("db write: %v", err), http.StatusServiceUnavailable)
				return
			}
		}
		if stores != nil && stores.Read != nil {
			if err := stores.Read.Conn.PingContext(ctx); err != nil {
				http.Error(w, fmt.Sprintf("db read: %v", err), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

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

	rds, err := broker.Redis_Connect(config.Env.RedisURI)

	if err != nil {
		return fmt.Errorf("Failed to connect to redis: %s\n", err)
	}

	// Fail fast when Postgres (via PgBouncer) is unreachable so the
	// container restarts instead of serving without a database.
	stores, err := storage.Connect(ctx, config.Env.DB_URL, config.Env.DB_READ_URL)
	if err != nil {
		rds.Conn.Close()
		return fmt.Errorf("Failed to connect to db: %s\n", err)
	}

	srvr := NewServer(
		rds,
		&config.Config{},
		stores,
	)

	httpServer := &http.Server{
		Addr:    net.JoinHostPort(config.Env.Host, config.Env.Port),
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
		defer rds.Conn.Close()
		defer stores.Close()
		go GracefulShutDown(&wg, ctx, httpServer)
	}()

	wg.Wait()
	return nil
}
