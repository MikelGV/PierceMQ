// Command web serves the PierceMQ dashboard (templ + Tailwind + htmx).
//
// Env: PORT (default 3000), API_BASE_URL (default http://localhost:8000).
// The browser talks only to this service; it proxies to the API server-side
// with the session JWT, which never leaves an HttpOnly cookie.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/MikelGV/PierceMQ/internal/web"
)

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	apiBase := getenv("API_BASE_URL", "http://localhost:8000")
	addr := net.JoinHostPort("0.0.0.0", getenv("PORT", "3000"))
	fmt.Printf("web dashboard up on %s (api %s)\n", addr, apiBase)

	if err := web.NewServer(apiBase).Run(ctx, addr); err != nil {
		fmt.Fprintf(os.Stderr, "web exited: %s\n", err)
		os.Exit(1)
	}
}
