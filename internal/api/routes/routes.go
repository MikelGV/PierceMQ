package routes

import (
	"net/http"
	"time"

	"github.com/MikelGV/PierceMQ/internal/api/handlers"
	"github.com/MikelGV/PierceMQ/internal/api/middleware"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
	storageauth "github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
)

// Deps is the single wiring struct for route registration. It keeps
// storage.Stores lean (Write/Read only) while carrying the domain stores
// (already constructed via users.New / jobs.New / auth.New) alongside infra
// handles.
type Deps struct {
	Redis  *broker.RedisStore
	Config *config.Config
	Stores *storage.Stores
	Users  *users.UsersStore
	Keys   *storageauth.Store
	Jobs   *jobs.JobsStore
}

func jwtTTL(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.JWTTTLHours > 0 {
		return time.Duration(cfg.JWTTTLHours) * time.Hour
	}
	return 24 * time.Hour
}

func jwtSecret(cfg *config.Config) string {
	if cfg != nil && cfg.JWTSecret != "" {
		return cfg.JWTSecret
	}
	return config.Env.JWTSecret
}

func AddRoutes(mux *http.ServeMux, d Deps) {
	mux.HandleFunc("/healthz", handlers.NewHealthHandler(d.Redis, d.Stores))
	mux.HandleFunc("/v1/auth/register", handlers.NewRegisterHandler(d.Users))
	mux.HandleFunc("/v1/auth/login", handlers.NewLoginHandler(d.Users, jwtSecret(d.Config), jwtTTL(d.Config)))

	authed := func(h http.Handler) http.HandlerFunc {
		return middleware.RequireAuth(d.Users, d.Keys, jwtSecret(d.Config), h).ServeHTTP
	}
	mux.HandleFunc("/v1/jobs", authed(handlers.NewEnqueueHandler(d.Jobs, d.Redis)))
	mux.HandleFunc("/v1/jobs/", authed(handlers.NewJobsHandler(d.Jobs)))
	mux.HandleFunc("/v1/keys", authed(handlers.NewAPIKeysHandler(d.Keys)))
	mux.HandleFunc("/v1/keys/revoke", authed(handlers.NewAPIKeyRevokeHandler(d.Keys)))
}
