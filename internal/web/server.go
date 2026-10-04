// Package web is the PierceMQ dashboard: a server-rendered UI (templ +
// Tailwind, htmx for live fragments) running as its own service on :3000.
//
// The browser never talks to the API directly and never sees the JWT: every
// page handler proxies to the API server-side with
// Authorization: Bearer <jwt from the pmq_jwt HttpOnly cookie> set at login.
// CSRF posture is SameSite=Lax cookies + plain form POSTs (no separate token
// for this MVP); the cookie is never readable from JS.
package web

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed static
var staticFS embed.FS

// Server proxies dashboard traffic to the API.
type Server struct {
	apiBase string
	http    *http.Client
	mux     *http.ServeMux
}

// NewServer builds the dashboard handler proxying to apiBase
// (e.g. http://api:8000).
func NewServer(apiBase string) *Server {
	s := &Server{
		apiBase: strings.TrimRight(apiBase, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
		mux:     http.NewServeMux(),
	}
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	sub, err := fs.Sub(staticFS, "static")
	if err == nil {
		s.mux.Handle("/static/", http.StripPrefix("/static/",
			http.FileServer(http.FS(sub))))
	}
	s.mux.HandleFunc("/login", s.routeLogin)
	s.mux.HandleFunc("/register", s.routeRegister)
	s.mux.HandleFunc("/logout", s.handleLogout)
	s.mux.HandleFunc("/jobs/new", s.requireSession(s.handleJobNew))
	s.mux.HandleFunc("/jobs", s.requireSession(s.routeJobs))
	s.mux.HandleFunc("/jobs/", s.requireSession(s.routeJobSub))
	s.mux.HandleFunc("/partials/stats", s.requireSession(s.handlePartialStats))
	s.mux.HandleFunc("/partials/jobs", s.requireSession(s.handlePartialJobs))
	s.mux.HandleFunc("/", s.requireSession(s.handleDashboard))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Run serves until ctx is done, then drains gracefully.
func (s *Server) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "web shutdown: %s\n", err)
		}
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("web listen: %w", err)
	}
	return nil
}
