package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/web/views"
)

// statsFromMap converts GET /v1/stats for the dashboard cards.
func statsFromMap(m map[string]any) views.Stats {
	return views.Stats{
		Pending: numVal(m, "pending"), Queued: numVal(m, "queued"),
		Processing: numVal(m, "processing"), Completed: numVal(m, "completed"),
		Failed: numVal(m, "failed"), Scheduled: numVal(m, "scheduled"),
		Cancelled:    numVal(m, "cancelled"),
		OldestAgeSec: numVal(m, "pending_oldest_age_sec"),
		Workers:      numVal(m, "total_workers"),
		QueueDepth:   numVal(m, "queue_depth"),
	}
}

func strVal(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func numVal(m map[string]any, key string) int64 {
	switch n := m[key].(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

func jobFromMap(m map[string]any) views.Job {
	j := views.Job{
		ID: strVal(m, "job_id"), Status: strVal(m, "status"),
		Type: strVal(m, "type"), Queue: strVal(m, "queue_name"),
		Priority:  int(numVal(m, "priority")),
		Attempt:   int(numVal(m, "attempt_count")),
		MaxRetry:  int(numVal(m, "max_retry")),
		CreatedAt: strVal(m, "created_at"), StartedAt: strVal(m, "started_at"),
		CompletedAt: strVal(m, "completed_at"), ScheduledAt: strVal(m, "scheduled_at"),
		LastError: strVal(m, "last_error"),
		Cron:      strVal(m, "cron"), CronTZ: strVal(m, "cron_tz"),
		SeriesID: strVal(m, "series_id"),
	}
	if b, ok := m["is_recurring"].(bool); ok {
		j.IsRecurring = b
	}
	if idem, _ := m["idempotency_key"].(string); idem != "" {
		_ = idem
	}
	switch p := m["payload"].(type) {
	case map[string]any:
		if b, err := json.MarshalIndent(p, "", "  "); err == nil {
			j.PayloadStr = string(b)
		}
	case string:
		j.PayloadStr = p
	case nil:
	default:
		j.PayloadStr = fmt.Sprint(p)
	}
	return j
}

func jobsFromList(m map[string]any) []views.Job {
	raw, _ := m["jobs"].([]any)
	out := make([]views.Job, 0, len(raw))
	for _, item := range raw {
		if jm, ok := item.(map[string]any); ok {
			out = append(out, jobFromMap(jm))
		}
	}
	return out
}

func eventsFromList(m map[string]any) []views.JobEvent {
	raw, _ := m["events"].([]any)
	out := make([]views.JobEvent, 0, len(raw))
	for _, item := range raw {
		em, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, views.JobEvent{
			ID: strVal(em, "event_id"), Old: strVal(em, "old_status"),
			New: strVal(em, "new_status"), Worker: strVal(em, "worker_id"),
			Reason: strVal(em, "reason"), At: strVal(em, "occurred_at"),
		})
	}
	return out
}

// listQuery clamps dashboard paging params shared by full and partial renders.
func listQuery(r *http.Request) (status, seriesID string, limit, offset int) {
	q := r.URL.Query()
	status = q.Get("status")
	seriesID = q.Get("series_id")
	limit = 25
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = min(n, 100)
	}
	if n, err := strconv.Atoi(q.Get("offset")); err == nil && n >= 0 {
		offset = n
	}
	return status, seriesID, limit, offset
}

// fetchDashboard pulls stats + the current job page; 401 funnels to login.
func (s *Server) fetchDashboard(r *http.Request) (views.Stats, []views.Job, string, error) {
	status, seriesID, limit, offset := listQuery(r)
	var statsOut, listOut map[string]any
	if _, err := s.apiDo(r, http.MethodGet, "/v1/stats", nil, &statsOut); err != nil {
		return views.Stats{}, nil, "", err
	}
	path := "/v1/jobs?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	if status != "" {
		path += "&status=" + status
	}
	if seriesID != "" {
		path += "&series_id=" + seriesID
	}
	if _, err := s.apiDo(r, http.MethodGet, path, nil, &listOut); err != nil {
		return views.Stats{}, nil, "", err
	}
	return statsFromMap(statsOut), jobsFromList(listOut), status, nil
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		w.WriteHeader(http.StatusNotFound)
		_ = views.ErrorPage(http.StatusNotFound, "page not found").Render(r.Context(), w)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stats, jobs, status, err := s.fetchDashboard(r)
	if err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			redirectLogin(w, r)
			return
		}
		if apiStatusOf(err) == http.StatusBadRequest {
			// Bad filter (e.g. bogus status): render empty with a notice.
			_, _, limit, offset := listQuery(r)
			_ = views.Dashboard(views.Stats{}, nil,
				views.JobFilter{Status: status, Limit: limit, Offset: offset},
				"invalid filter").Render(r.Context(), w)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		_ = views.ErrorPage(http.StatusBadGateway, "dashboard unavailable: "+err.Error()).Render(r.Context(), w)
		return
	}
	_, seriesID, limit, offset := listQuery(r)
	filter := views.JobFilter{
		Status: status, SeriesID: seriesID,
		Limit: limit, Offset: offset, HasMore: len(jobs) == limit,
	}
	_ = views.Dashboard(stats, jobs, filter, "").Render(r.Context(), w)
}

func (s *Server) routeJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		http.Redirect(w, r, "/", http.StatusSeeOther)
	case http.MethodPost:
		s.handleJobCreate(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) routeJobSub(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/jobs/")
	if !ok || rest == "" || strings.Contains(rest, "/../") {
		w.WriteHeader(http.StatusNotFound)
		_ = views.ErrorPage(http.StatusNotFound, "page not found").Render(r.Context(), w)
		return
	}
	id, sub, _ := strings.Cut(rest, "/")
	switch {
	case sub == "" && r.Method == http.MethodGet:
		s.handleJobDetail(w, r, id)
	case sub == "retry" && r.Method == http.MethodPost:
		s.handleJobRetry(w, r, id)
	case sub == "cancel" && r.Method == http.MethodPost:
		s.handleJobCancel(w, r, id)
	default:
		w.WriteHeader(http.StatusNotFound)
		_ = views.ErrorPage(http.StatusNotFound, "page not found").Render(r.Context(), w)
	}
}

func (s *Server) handleJobDetail(w http.ResponseWriter, r *http.Request, id string) {
	var jobOut, eventsOut map[string]any
	if _, err := s.apiDo(r, http.MethodGet, "/v1/jobs/"+id, nil, &jobOut); err != nil {
		switch apiStatusOf(err) {
		case http.StatusUnauthorized:
			redirectLogin(w, r)
		case http.StatusNotFound:
			w.WriteHeader(http.StatusNotFound)
			_ = views.ErrorPage(http.StatusNotFound, "job not found").Render(r.Context(), w)
		default:
			w.WriteHeader(http.StatusBadGateway)
			_ = views.ErrorPage(http.StatusBadGateway, "job unavailable").Render(r.Context(), w)
		}
		return
	}
	// Events are best-effort: the header renders even if the timeline fails.
	if _, err := s.apiDo(r, http.MethodGet, "/v1/jobs/"+id+"/events", nil, &eventsOut); err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			redirectLogin(w, r)
			return
		}
		eventsOut = nil
	}
	_ = views.JobDetail(jobFromMap(jobOut), eventsFromList(eventsOut)).Render(r.Context(), w)
}

func (s *Server) handleJobRetry(w http.ResponseWriter, r *http.Request, id string) {
	var out map[string]any
	if _, err := s.apiDo(r, http.MethodPost, "/v1/jobs/"+id+"/retry", nil, &out); err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			redirectLogin(w, r)
			return
		}
		http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request, id string) {
	path := "/v1/jobs/" + id
	if r.URL.Query().Get("series") == "true" {
		path += "?series=true"
	}
	var out map[string]any
	if _, err := s.apiDo(r, http.MethodDelete, path, nil, &out); err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			redirectLogin(w, r)
			return
		}
		http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
		return
	}
	if r.URL.Query().Get("series") == "true" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

// handleJobNew renders the enqueue form.
func (s *Server) handleJobNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	_ = views.JobNew(views.JobForm{}, "").Render(r.Context(), w)
}

// handleJobCreate validates the form server-side and enqueues via the API.
func (s *Server) handleJobCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = views.JobNew(views.JobForm{}, "invalid form").Render(r.Context(), w)
		return
	}
	form := views.JobForm{
		Type:           strings.TrimSpace(r.FormValue("type")),
		QueueName:      strings.TrimSpace(r.FormValue("queue_name")),
		Payload:        strings.TrimSpace(r.FormValue("payload")),
		Priority:       strings.TrimSpace(r.FormValue("priority")),
		MaxRetry:       strings.TrimSpace(r.FormValue("max_retry")),
		ScheduledAt:    strings.TrimSpace(r.FormValue("scheduled_at")),
		Cron:           strings.TrimSpace(r.FormValue("cron")),
		CronTZ:         strings.TrimSpace(r.FormValue("cron_tz")),
		IdempotencyKey: strings.TrimSpace(r.FormValue("idempotency_key")),
	}
	fail := func(msg string) {
		w.WriteHeader(http.StatusBadRequest)
		_ = views.JobNew(form, msg).Render(r.Context(), w)
	}
	if form.Type == "" {
		fail("type is required")
		return
	}
	if form.Payload == "" {
		fail("payload is required (JSON object)")
		return
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(form.Payload), &payload); err != nil {
		fail("payload must be a JSON object")
		return
	}
	body := map[string]any{"type": form.Type, "payload": payload}
	if form.QueueName != "" {
		body["queue_name"] = form.QueueName
	}
	if form.Priority != "" {
		n, err := strconv.Atoi(form.Priority)
		if err != nil {
			fail("priority must be an integer")
			return
		}
		body["priority"] = n
	}
	if form.MaxRetry != "" {
		n, err := strconv.Atoi(form.MaxRetry)
		if err != nil || n < 0 {
			fail("max retries must be a non-negative integer")
			return
		}
		body["max_retry"] = n
	}
	if form.ScheduledAt != "" {
		body["scheduled_at"] = form.ScheduledAt
	}
	if form.Cron != "" {
		body["cron"] = form.Cron
	}
	if form.CronTZ != "" {
		body["cron_tz"] = form.CronTZ
	}
	var headers map[string]string
	if form.IdempotencyKey != "" {
		headers = map[string]string{"Idempotency-Key": form.IdempotencyKey}
	}
	var out map[string]any
	if _, err := s.apiDoWithHeaders(r, http.MethodPost, "/v1/jobs", headers, body, &out); err != nil {
		switch apiStatusOf(err) {
		case http.StatusUnauthorized:
			redirectLogin(w, r)
			return
		case http.StatusConflict, http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			var ae *apiError
			msg := "enqueue failed"
			if errors.As(err, &ae) && ae.Msg != "" {
				msg = ae.Msg
			}
			fail(msg)
			return
		default:
			w.WriteHeader(http.StatusBadGateway)
			_ = views.JobNew(form, "enqueue failed: "+err.Error()).Render(r.Context(), w)
			return
		}
	}
	id, _ := out["job_id"].(string)
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

// handlePartialStats renders just the stat cards for htmx polling.
func (s *Server) handlePartialStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var out map[string]any
	if _, err := s.apiDo(r, http.MethodGet, "/v1/stats", nil, &out); err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			htmxRedirect(w, "/login")
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		_ = views.StatsError().Render(r.Context(), w)
		return
	}
	_ = views.StatCards(statsFromMap(out)).Render(r.Context(), w)
}

// handlePartialJobs renders the job table for htmx filter/poll swaps.
func (s *Server) handlePartialJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status, seriesID, limit, offset := listQuery(r)
	path := "/v1/jobs?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	if status != "" {
		path += "&status=" + status
	}
	if seriesID != "" {
		path += "&series_id=" + seriesID
	}
	var out map[string]any
	if _, err := s.apiDo(r, http.MethodGet, path, nil, &out); err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			htmxRedirect(w, "/login")
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		_ = views.JobsError().Render(r.Context(), w)
		return
	}
	jobs := jobsFromList(out)
	filter := views.JobFilter{
		Status: status, SeriesID: seriesID,
		Limit: limit, Offset: offset, HasMore: len(jobs) == limit,
	}
	_ = views.JobTable(jobs, filter).Render(r.Context(), w)
}
