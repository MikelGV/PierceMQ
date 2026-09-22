package queue

import "testing"

func TestStreamFor(t *testing.T) {
	cases := []struct {
		queue    string
		priority int16
		stream   string
		group    string
	}{
		{"email-high", 0, EmailHighStream, EmailGroupHigh},
		{"email-low", 99, EmailLowStream, EmailGroupLow},
		{"email", 1, EmailHighStream, EmailGroupHigh},
		{"email", 0, EmailLowStream, EmailGroupLow},
		{"file_processing", 5, FileHighStream, FileGroupHigh},
		{"file-low", 0, FileLowStream, FileGroupLow},
		{"exec", 0, ExecLowStream, ExecGroupLow},
		{"exec_processing-high", 0, ExecHighStream, ExecGroupHigh},
	}
	for _, c := range cases {
		s, g, err := StreamFor(c.queue, c.priority)
		if err != nil {
			t.Fatalf("StreamFor(%q,%d): %v", c.queue, c.priority, err)
		}
		if s != c.stream || g != c.group {
			t.Fatalf("StreamFor(%q,%d) = (%q,%q), want (%q,%q)",
				c.queue, c.priority, s, g, c.stream, c.group)
		}
	}
	if _, _, err := StreamFor("video-high", 1); err == nil {
		t.Fatal("StreamFor(video-high) should fail")
	}
}
