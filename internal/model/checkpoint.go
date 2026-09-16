package model

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// ErrCheckpointTooLarge is returned by a CheckpointStore implementation when
// a checkpoint's serialized state exceeds the configured size limit.
// It lives in model so both the persistence layer and runner can share it.
var ErrCheckpointTooLarge = errors.New("execution checkpoint exceeds configured size limit")

// CheckpointQueueItem is the minimal persisted representation of one
// in-flight engine work item. It deliberately contains only the node name
// and the n8n-style item payload needed to execute that node.
type CheckpointQueueItem struct {
	NodeName    string     `json:"nodeName"`
	Input       NodeOutput `json:"input,omitempty"`
	OperationID string     `json:"operationId,omitempty"`
}

// CheckpointWait captures an in-flight Wait node. The current node is kept in
// Pending so a restart can either continue waiting for the remaining duration
// or, if the deadline has already passed, execute the node immediately.
type CheckpointWait struct {
	Until time.Time `json:"until"`
}

// ExecutionCheckpointState is the bounded, engine-derived state required to
// resume a workflow. Large JSON payloads are transparently gzip-compressed
// inside the checkpoint JSON. The in-memory representation remains unchanged,
// so recovery semantics do not change.
type ExecutionCheckpointState struct {
	Version       int                       `json:"version"`
	LastCompleted string                    `json:"lastCompletedNode,omitempty"`
	Pending       []CheckpointQueueItem     `json:"pending"`
	NodeOutputs   map[string]map[string]any `json:"nodeOutputs,omitempty"`
	NodeAttempts  map[string]int            `json:"nodeAttempts,omitempty"`
	Steps         int                       `json:"steps"`
	Status        ExecutionStatus           `json:"status"`
	Wait          *CheckpointWait           `json:"wait,omitempty"`
}

// checkpointCompressedField stores gzip+base64 JSON. Base64 keeps the outer
// checkpoint valid JSON/JSONB while gzip removes the large repeated JSON
// overhead commonly produced by model lists and node outputs.
type checkpointCompressedField string

func compressCheckpointJSON(v any) (checkpointCompressedField, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return "", err
	}
	if _, err := zw.Write(raw); err != nil {
		_ = zw.Close()
		return "", err
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	// Do not expand small/incompressible data. The raw form is normally
	// smaller for tiny values and remains backwards-compatible.
	if buf.Len() >= len(raw) {
		return checkpointCompressedField(base64.RawStdEncoding.EncodeToString(raw)), nil
	}
	return checkpointCompressedField("gz:" + base64.RawStdEncoding.EncodeToString(buf.Bytes())), nil
}

func decompressCheckpointJSON(s checkpointCompressedField, dst any) error {
	text := string(s)
	if len(text) >= 3 && text[:3] == "gz:" {
		b, err := base64.RawStdEncoding.DecodeString(text[3:])
		if err != nil {
			return err
		}
		r, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(r)
		closeErr := r.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return json.Unmarshal(raw, dst)
	}
	b, err := base64.RawStdEncoding.DecodeString(text)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// MarshalJSON compresses only the potentially large fields. All other fields
// keep their normal JSON representation. This is intentionally transparent to
// the store: it still writes one ordinary JSONB document per execution.
func (s ExecutionCheckpointState) MarshalJSON() ([]byte, error) {
	pending, err := compressCheckpointJSON(s.Pending)
	if err != nil {
		return nil, err
	}
	outputs, err := compressCheckpointJSON(s.NodeOutputs)
	if err != nil {
		return nil, err
	}

	type wire struct {
		Version               int                       `json:"version"`
		LastCompleted         string                    `json:"lastCompletedNode,omitempty"`
		PendingCompressed     checkpointCompressedField `json:"pendingCompressed"`
		NodeOutputsCompressed checkpointCompressedField `json:"nodeOutputsCompressed,omitempty"`
		NodeAttempts          map[string]int            `json:"nodeAttempts,omitempty"`
		Steps                 int                       `json:"steps"`
		Status                ExecutionStatus           `json:"status"`
		Wait                  *CheckpointWait           `json:"wait,omitempty"`
	}
	w := wire{
		Version:               s.Version,
		LastCompleted:         s.LastCompleted,
		PendingCompressed:     pending,
		NodeOutputsCompressed: outputs,
		NodeAttempts:          s.NodeAttempts,
		Steps:                 s.Steps,
		Status:                s.Status,
		Wait:                  s.Wait,
	}
	return json.Marshal(w)
}

// UnmarshalJSON accepts the compact representation above and also accepts the
// previous uncompressed checkpoint shape, allowing old rows to be recovered.
func (s *ExecutionCheckpointState) UnmarshalJSON(data []byte) error {
	var probe struct {
		Version               int                       `json:"version"`
		LastCompleted         string                    `json:"lastCompletedNode,omitempty"`
		Pending               []CheckpointQueueItem     `json:"pending"`
		NodeOutputs           map[string]map[string]any `json:"nodeOutputs,omitempty"`
		PendingCompressed     checkpointCompressedField `json:"pendingCompressed"`
		NodeOutputsCompressed checkpointCompressedField `json:"nodeOutputsCompressed,omitempty"`
		NodeAttempts          map[string]int            `json:"nodeAttempts,omitempty"`
		Steps                 int                       `json:"steps"`
		Status                ExecutionStatus           `json:"status"`
		Wait                  *CheckpointWait           `json:"wait,omitempty"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}

	*s = ExecutionCheckpointState{
		Version:       probe.Version,
		LastCompleted: probe.LastCompleted,
		NodeAttempts:  probe.NodeAttempts,
		Steps:         probe.Steps,
		Status:        probe.Status,
		Wait:          probe.Wait,
	}

	if probe.PendingCompressed != "" {
		if err := decompressCheckpointJSON(probe.PendingCompressed, &s.Pending); err != nil {
			return err
		}
	} else {
		s.Pending = probe.Pending
	}
	if probe.NodeOutputsCompressed != "" {
		if err := decompressCheckpointJSON(probe.NodeOutputsCompressed, &s.NodeOutputs); err != nil {
			return err
		}
	} else {
		s.NodeOutputs = probe.NodeOutputs
	}
	return nil
}

// ExecutionCheckpoint is the durable wrapper around the engine state. One
// row exists per execution and is updated in place; no checkpoint history is
// retained.
type ExecutionCheckpoint struct {
	ExecutionID       string                   `json:"executionId"`
	WorkflowID        string                   `json:"workflowId"`
	WorkflowHash      string                   `json:"workflowHash"`
	WorkflowUpdatedAt time.Time                `json:"workflowUpdatedAt,omitempty"`
	Mode              string                   `json:"mode"`
	StartedAt         time.Time                `json:"startedAt,omitempty"`
	State             ExecutionCheckpointState `json:"state"`
	UpdatedAt         time.Time                `json:"updatedAt"`
	LeaseOwner        string                   `json:"-"`
	LeaseUntil        time.Time                `json:"-"`
}
