package views

import (
	"strconv"
	"strings"
)

// Display models shared by handlers and templates. They mirror the API
// shapes; handlers convert API maps into these.
type Stats struct {
	Pending, Queued, Processing, Completed, Failed, Scheduled, Cancelled int64
	OldestAgeSec, Workers, QueueDepth                                    int64
}

type Job struct {
	ID, Status, Type, Queue           string
	Priority, Attempt, MaxRetry       int
	CreatedAt, StartedAt, CompletedAt string
	ScheduledAt, LastError            string
	PayloadStr                        string
	IsRecurring                       bool
	Cron, CronTZ, SeriesID            string
}

type JobEvent struct {
	ID, Old, New, Worker, Reason, At string
}

// JobForm carries the enqueue form values back on validation errors so the
// user doesn't lose their input.
type JobForm struct {
	Type, QueueName, Payload string
	Priority, MaxRetry       string
	ScheduledAt, Cron, CronTZ string
}

// QueueTypes are the canonical job types offered by the enqueue form.
var QueueTypes = []string{"email", "file_processing", "exec_processing"}

// QueueOption is one canonical stream a job can be routed to. Name is the
// queue_name the API expects, Type is the job type owning it.
type QueueOption struct {
	Name string
	Type string
}

// QueueOptions lists the six canonical queues in display order. Mirrors
// internal/queue: every entry is "<type>-high" or "<type>-low".
func QueueOptions() []QueueOption {
	out := make([]QueueOption, 0, len(QueueTypes)*2)
	for _, t := range QueueTypes {
		out = append(out,
			QueueOption{Name: t + "-high", Type: t},
			QueueOption{Name: t + "-low", Type: t},
		)
	}
	return out
}

// DefaultQueue is the queue a type routes to when the form leaves it empty,
// matching the API default of "<type>-high".
func DefaultQueue(jobType string) string {
	if jobType == "" {
		jobType = QueueTypes[0]
	}
	return jobType + "-high"
}

// QueueType strips the -high/-low suffix from a canonical queue name.
func QueueType(queue string) string {
	q := strings.TrimSpace(strings.ToLower(queue))
	if t := strings.TrimSuffix(q, "-high"); t != q {
		return t
	}
	return strings.TrimSuffix(q, "-low")
}

// IsCanonicalQueue reports whether name is one of the six known streams.
func IsCanonicalQueue(name string) bool {
	n := strings.TrimSpace(strings.ToLower(name))
	for _, q := range QueueOptions() {
		if q.Name == n {
			return true
		}
	}
	return false
}

// IsLowQueue reports whether a canonical queue routes to the low stream.
func IsLowQueue(queue string) bool {
	return strings.HasSuffix(strings.TrimSpace(strings.ToLower(queue)), "-low")
}

// SelectedQueue is the queue marked in the form: the submitted value when
// present, else the type's default high stream.
func SelectedQueue(form JobForm) string {
	if q := strings.TrimSpace(form.QueueName); q != "" {
		return q
	}
	return DefaultQueue(form.Type)
}

// PriorityLabel renders the routing tier a stored priority selects. Mirrors
// queue.StreamFor, where only the sign of priority matters.
func PriorityLabel(priority int) string {
	if priority > 0 {
		return "High"
	}
	return "Low"
}

// JobFilter carries the dashboard list state for tabs and paging.
type JobFilter struct {
	Status, SeriesID string
	Limit, Offset    int
	HasMore          bool
}

// Query renders the filter as a query string for htmx polling and pager links.
func (f JobFilter) Query() string {
	q := "limit=" + strconv.Itoa(f.Limit) + "&offset=" + strconv.Itoa(f.Offset)
	if f.Status != "" {
		q += "&status=" + f.Status
	}
	if f.SeriesID != "" {
		q += "&series_id=" + f.SeriesID
	}
	return q
}

// WithOffset returns the filter at a different page.
func (f JobFilter) WithOffset(offset int) JobFilter {
	f.Offset = offset
	return f
}

// StatusBadge returns badge classes per job status.
func StatusBadge(status string) string {
	base := "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium "
	switch status {
	case "pending", "queued":
		return base + "bg-amber-500/15 text-amber-300 ring-1 ring-amber-500/30"
	case "running":
		return base + "bg-sky-500/15 text-sky-300 ring-1 ring-sky-500/30"
	case "completed":
		return base + "bg-emerald-500/15 text-emerald-300 ring-1 ring-emerald-500/30"
	case "failed":
		return base + "bg-red-500/15 text-red-300 ring-1 ring-red-500/30"
	case "scheduled":
		return base + "bg-violet-500/15 text-violet-300 ring-1 ring-violet-500/30"
	case "cancelled":
		return base + "bg-zinc-500/15 text-zinc-400 ring-1 ring-zinc-500/30"
	default:
		return base + "bg-zinc-500/15 text-zinc-300 ring-1 ring-zinc-500/30"
	}
}

// ShortID trims a UUID for table display.
func ShortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// Cancellable mirrors the API: only not-yet-running jobs may cancel.
func Cancellable(status string) bool {
	return status == "scheduled" || status == "pending" || status == "queued"
}

// Retryable mirrors the API: only failed jobs may retry.
func Retryable(status string) bool {
	return status == "failed"
}

// Num formats a counter for cards and tables.
func Num(n int64) string {
	return strconv.FormatInt(n, 10)
}

// Age formats a head-of-line wait in seconds.
func Age(sec int64) string {
	if sec < 60 {
		return strconv.FormatInt(sec, 10) + "s"
	}
	if m := sec / 60; m < 60 {
		return strconv.FormatInt(m, 10) + "m"
	}
	return strconv.FormatInt(sec/3600, 10) + "h"
}

// TabClass styles the active dashboard filter tab.
func TabClass(active bool) string {
	base := "rounded-md px-3 py-1.5 text-sm font-medium "
	if active {
		return base + "bg-zinc-100 text-zinc-900"
	}
	return base + "text-zinc-400 hover:bg-zinc-800 hover:text-zinc-200"
}

// tabLink builds a status-tab URL, preserving the series scope and page size.
func tabLink(tab string, f JobFilter) string {
	q := "/?limit=" + strconv.Itoa(f.Limit) + "&offset=0"
	if tab != "" {
		q += "&status=" + tab
	}
	if f.SeriesID != "" {
		q += "&series_id=" + f.SeriesID
	}
	return q
}

// FilterTabs is the dashboard status vocabulary.
var FilterTabs = []string{"", "scheduled", "pending", "queued", "running", "completed", "failed", "cancelled"}
