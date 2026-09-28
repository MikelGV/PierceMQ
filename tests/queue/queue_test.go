package queue_test

import (
	"testing"

	"github.com/MikelGV/PierceMQ/internal/queue"
)

func TestStreamFor(t *testing.T) {
	cases := []struct {
		queue    string
		priority int16
		stream   string
		group    string
	}{
		{"email-high", 0, queue.EmailHighStream, queue.EmailGroupHigh},
		{"email-low", 99, queue.EmailLowStream, queue.EmailGroupLow},
		{"email", 1, queue.EmailHighStream, queue.EmailGroupHigh},
		{"email", 0, queue.EmailLowStream, queue.EmailGroupLow},
		{"file_processing", 5, queue.FileHighStream, queue.FileGroupHigh},
		{"file-low", 0, queue.FileLowStream, queue.FileGroupLow},
		{"exec", 0, queue.ExecLowStream, queue.ExecGroupLow},
		{"exec_processing-high", 0, queue.ExecHighStream, queue.ExecGroupHigh},
	}
	for _, c := range cases {
		s, g, err := queue.StreamFor(c.queue, c.priority)
		if err != nil {
			t.Fatalf("StreamFor(%q,%d): %v", c.queue, c.priority, err)
		}
		if s != c.stream || g != c.group {
			t.Fatalf("StreamFor(%q,%d) = (%q,%q), want (%q,%q)",
				c.queue, c.priority, s, g, c.stream, c.group)
		}
	}
	if _, _, err := queue.StreamFor("video-high", 1); err == nil {
		t.Fatal("StreamFor(video-high) should fail")
	}
}
