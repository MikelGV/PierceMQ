package queue

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
	ExecGroupHigh  string = "exec-workers-workers-high"
	ExecGroupLow   string = "exec-workers-workers-low"
)
