package task

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type TaskRequest struct {
	Id      int
	Type    string
	Payload map[string]any
	Attempt int
}

type Job struct {
	ID          string
	PAYLOAD     any
	TYPE        string
	ATTEMPT     int
	MAX_RETRIES int
}

/**
* To fields has to be changed once i write the custom encode decode i think
**/
// Marshals payload
func (t TaskRequest) ToFields() map[string]any {
	payloadBytes, err := json.Marshal(t.Payload)
	if err != nil {
		return nil
	}

	return map[string]any{
		"type":    t.Type,
		"payload": string(payloadBytes),
		"attempt": t.Attempt,
	}
}

// Unmarshal from Redis
func FromFields(fields map[string]any) (*TaskRequest, error) {
	taskType, ok := fields["type"].(string)

	if !ok {
		return nil, fmt.Errorf("missing task type")
	}

	payloadStr, ok := fields["payload"].(string)
	if !ok {
		return nil, fmt.Errorf("missing task payload")
	}

	var payload map[string]any

	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return nil, err
	}

	attempt := 0
	if a, ok := fields["attempt"]; ok {
		switch v := a.(type) {
		case string:
			attempt, _ = strconv.Atoi(v)
		case int64:
			attempt = int(v)
		case float64:
			attempt = int(v)
		}
	}

	return &TaskRequest{
		Type:    taskType,
		Payload: payload,
		Attempt: attempt,
	}, nil
}
