package handlers

import (
	"net/http"
	"strconv"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
)

// validListStatuses is the §9 filter vocabulary: every job_status value, plus
// "" for unfiltered.
var validListStatuses = map[string]bool{
	"":                        true,
	string(task.JobScheduled): true,
	string(task.JobPending):   true,
	string(task.JobQueued):    true,
	string(task.JobRunning):   true,
	string(task.JobCompleted): true,
	string(task.JobFailed):    true,
	string(task.JobCancelled): true,
}

// NewJobsListHandler serves GET /v1/jobs?status=&limit=&offset= (§9.4-9.5).
// Limit defaults to 50 and caps at 100; offset defaults to 0. Unknown status
// or non-integer paging is a 400.
func NewJobsListHandler(store *jobs.JobsStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if _, ok := auth.UserIDFromContext(r.Context()); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		q := r.URL.Query()
		status := q.Get("status")
		if !validListStatuses[status] {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown status filter"})
			return
		}
		limit := 50
		if raw := q.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be an integer"})
				return
			}
			limit = n
		}
		offset := 0
		if raw := q.Get("offset"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "offset must be a non-negative integer"})
				return
			}
			offset = n
		}

		list, err := store.ListJobs(r.Context(), task.JobStatus(status), limit, offset)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "list jobs failed"})
			return
		}
		out := make([]map[string]any, 0, len(list))
		for _, job := range list {
			out = append(out, jobResponse(job))
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
	}
}
