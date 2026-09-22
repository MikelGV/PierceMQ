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

	"github.com/MikelGV/PierceMQ/internal/api/routes"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
)

func NewServer(d routes.Deps) http.Handler {
	mux := http.NewServeMux()

	routes.AddRoutes(mux, d)

	var handler http.Handler = mux
	// Here we set up the middleware like cors or things like that.

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

	srvr := NewServer(routes.Deps{
		Redis:  rds,
		Config: &config.Env,
		Stores: stores,
		Users:  users.New(stores.Write.Conn, stores.Read.Conn),
		Keys:   auth.New(stores.Write.Conn, stores.Read.Conn),
		Jobs:   jobs.New(stores.Write.Conn, stores.Read.Conn),
	})

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

	// NOTE: the defers must fire when GracefulShutDown RETURNS (at
	// shutdown), not when this goroutine is spawned. A nested `go`
	// here would run them immediately at boot, closing Redis and both
	// DB pools while the server is still running (every /healthz after
	// boot would 503).
	go func() {
		defer rds.Conn.Close()
		defer stores.Close()
		GracefulShutDown(&wg, ctx, httpServer)
	}()

	wg.Wait()
	return nil
}
