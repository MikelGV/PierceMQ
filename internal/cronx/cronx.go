// Package cronx wraps cron expression parsing for recurring job series.
//
// Expressions are 5-field standard cron (minute hour dom month dow) plus
// @-descriptors (@hourly, @daily, ...); 6-field seconds are rejected.
// The schedule is evaluated in the series timezone so wall-clock times like
// "9:00 America/New_York" stay put across DST.
package cronx

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// DefaultTZ is used when a series carries no timezone.
const DefaultTZ = "UTC"

// Location resolves tz (IANA name, empty = UTC).
func Location(tz string) (*time.Location, error) {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("cronx: invalid timezone %q: %w", tz, err)
	}
	return loc, nil
}

// Parse validates a 5-field cron expression.
func Parse(expr string) (cron.Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("cronx: empty cron expression")
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("cronx: invalid cron expression %q: %w", expr, err)
	}
	return sched, nil
}

// NextAfter returns the first fire time strictly after t, evaluated in tz.
func NextAfter(expr, tz string, t time.Time) (time.Time, error) {
	sched, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	loc, err := Location(tz)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(t.In(loc)), nil
}
