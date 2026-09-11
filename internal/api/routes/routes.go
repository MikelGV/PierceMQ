package routes

import (
	"net/http"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
)

// Deps is the single wiring struct for route registration. It keeps
// storage.Stores lean (Write/Read only) while carrying the domain stores
// (already constructed via users.New / jobs.New) alongside infra handles.
type Deps struct {
	Redis  *broker.RedisStore
	Config *config.Config
	Stores *storage.Stores
	Users  *users.UsersStore
	Jobs   *jobs.JobsStore
}

func AddRoutes(mux *http.ServeMux, d Deps) {
	mux.HandleFunc("/healthz", NewHealthHandler(d.Redis, d.Stores))
}
