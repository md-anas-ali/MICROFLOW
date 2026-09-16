package model

import "time"

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
// resume a workflow without replaying already completed graph transitions.
// It is intentionally not a serialization of engine.RunContext.
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
