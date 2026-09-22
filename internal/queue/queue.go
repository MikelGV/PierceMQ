package queue

import (
	"fmt"
	"strings"
)

// Stream / group constants — canonical source. broker and worker import these
// (queue is leaf, no imports, acyclic).
const (
	EmailHighStream string = "stream:queue:email:high"
	EmailLowStream  string = "stream:queue:email:low"
	FileHighStream  string = "stream:queue:file_processing:high"
	FileLowStream   string = "stream:queue:file_processing:low"
	ExecHighStream  string = "stream:queue:exec_processing:high"
	ExecLowStream   string = "stream:queue:exec_processing:low"

	EmailGroupHigh string = "email-workers-high"
	EmailGroupLow  string = "email-workers-low"
	FileGroupHigh  string = "file_processing-workers-high"
	FileGroupLow   string = "file_processing-workers-low"
	ExecGroupHigh string = "exec-workers-workers-high"
	ExecGroupLow   string = "exec-workers-workers-low"
)

// StreamFor resolves a queue_name (+priority) to its Redis stream + consumer
// group. Queue names are "<type>-<high|low>" (e.g. "email-high"); bare type
// names ("email") default to high when priority > 0, low otherwise.
// Priority > 0 selects the high stream, 0 and below the low stream.
func StreamFor(queueName string, priority int16) (stream, group string, err error) {
	name := strings.ToLower(strings.TrimSpace(queueName))
	high := priority > 0

	// Explicit suffix wins over priority.
	if strings.HasSuffix(name, "-high") {
		high = true
		name = strings.TrimSuffix(name, "-high")
	} else if strings.HasSuffix(name, "-low") {
		high = false
		name = strings.TrimSuffix(name, "-low")
	}

	switch name {
	case "email":
		if high {
			return EmailHighStream, EmailGroupHigh, nil
		}
		return EmailLowStream, EmailGroupLow, nil
	case "file", "file_processing":
		if high {
			return FileHighStream, FileGroupHigh, nil
		}
		return FileLowStream, FileGroupLow, nil
	case "exec", "exec_processing":
		if high {
			return ExecHighStream, ExecGroupHigh, nil
		}
		return ExecLowStream, ExecGroupLow, nil
	default:
		return "", "", fmt.Errorf("queue: unknown queue %q", queueName)
	}
}
