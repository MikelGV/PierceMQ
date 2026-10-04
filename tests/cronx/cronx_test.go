package cronx_test

import (
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/cronx"
	"github.com/stretchr/testify/require"
)

func TestNextAfter(t *testing.T) {
	base := time.Date(2026, 10, 4, 12, 34, 0, 0, time.UTC)

	t.Run("every minute fires at the next minute", func(t *testing.T) {
		next, err := cronx.NextAfter("* * * * *", "UTC", base)
		require.NoError(t, err)
		require.Equal(t, time.Date(2026, 10, 4, 12, 35, 0, 0, time.UTC), next.UTC())
	})

	t.Run("descriptors work", func(t *testing.T) {
		next, err := cronx.NextAfter("@hourly", "", base)
		require.NoError(t, err)
		require.Equal(t, time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC), next.UTC())
	})

	t.Run("timezone shifts wall-clock firing", func(t *testing.T) {
		// Base 12:34 UTC is 08:34 EDT; next 09:00 New York is 13:00 UTC
		// the same day.
		next, err := cronx.NextAfter("0 9 * * *", "America/New_York", base)
		require.NoError(t, err)
		require.Equal(t, "2026-10-04T13:00:00Z", next.UTC().Format(time.RFC3339))
	})

	t.Run("invalid expression errors", func(t *testing.T) {
		_, err := cronx.NextAfter("not a cron", "UTC", base)
		require.Error(t, err)
	})

	t.Run("six-field expression errors", func(t *testing.T) {
		_, err := cronx.NextAfter("0 * * * * *", "UTC", base)
		require.Error(t, err)
	})

	t.Run("unknown timezone errors", func(t *testing.T) {
		_, err := cronx.NextAfter("* * * * *", "Mars/Olympus", base)
		require.Error(t, err)
	})
}
