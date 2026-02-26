package task

import (
	"encoding/json"
	"fmt"
)

type TaskRequest struct {
	Id      int
	Type    string
	Payload map[string]any
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

	return &TaskRequest{
		Type:    taskType,
		Payload: payload,
	}, nil
}
