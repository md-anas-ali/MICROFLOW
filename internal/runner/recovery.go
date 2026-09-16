package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"microflow/internal/model"
)

const checkpointVersion = 1

var ErrCheckpointTooLarge = errors.New("execution checkpoint exceeds configured size limit")
var ErrInvalidCheckpoint = errors.New("invalid execution checkpoint")

// CheckpointStore is the narrow persistence contract used by Runner/Manager.
// PostgreSQL is the production implementation; no second database or queue is
// introduced.
type CheckpointStore interface {
	SaveExecutionCheckpoint(context.Context, *model.ExecutionCheckpoint) error
	DeleteExecutionCheckpoint(context.Context, string) error
	ClaimRecoverableExecutionCheckpoints(context.Context, string, int, time.Duration) ([]*model.ExecutionCheckpoint, error)
	ClearExecutionCheckpointLease(context.Context, string, string) error
	CleanupOrphanedExecutionCheckpoints(context.Context, int, time.Time) (int64, error)
}

func checkpointMaxBytes() int {
	const def = 512 * 1024
	if v := os.Getenv("MICROFLOW_MAX_CHECKPOINT_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 64*1024 && n <= 4*1024*1024 {
			return n
		}
	}
	return def
}

func recoveryLease() time.Duration {
	const def = 1 * time.Hour
	if v := os.Getenv("MICROFLOW_RECOVERY_LEASE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 60 && n <= 24*60*60 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func recoveryBatchSize(maxQueued int) int {
	if maxQueued < 1 {
		return 1
	}
	if maxQueued > 16 {
		return 16
	}
	return maxQueued
}

func workflowHash(wf *model.Workflow) (string, error) {
	b, err := json.Marshal(wf)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func validateCheckpoint(cp *model.ExecutionCheckpoint, wf *model.Workflow, scratchRoot string) error {
	if cp == nil || cp.State.Version != checkpointVersion || cp.ExecutionID == "" || cp.WorkflowID == "" || cp.WorkflowID != wf.ID {
		return ErrInvalidCheckpoint
	}
	h, err := workflowHash(wf)
	if err != nil || !strings.EqualFold(h, cp.WorkflowHash) {
		return fmt.Errorf("%w: workflow definition changed", ErrInvalidCheckpoint)
	}
	if cp.State.Steps < 0 || cp.State.Steps > 5000 || len(cp.State.Pending) > 4096 || len(cp.State.NodeAttempts) > 2048 || len(cp.State.NodeOutputs) > 2048 {
		return fmt.Errorf("%w: bounded state limits exceeded", ErrInvalidCheckpoint)
	}
	if cp.State.Status != model.StatusQueued && cp.State.Status != model.StatusRunning && cp.State.Status != model.StatusWaiting && cp.State.Status != model.StatusSuccess && cp.State.Status != model.StatusError && cp.State.Status != model.StatusCancelled {
		return fmt.Errorf("%w: unknown execution status", ErrInvalidCheckpoint)
	}
	if cp.State.Wait != nil {
		if cp.State.Status != model.StatusWaiting || len(cp.State.Pending) == 0 {
			return fmt.Errorf("%w: invalid waiting state", ErrInvalidCheckpoint)
		}
		if n, ok := wf.Nodes[cp.State.Pending[0].NodeName]; !ok || n.Type != model.TypeWait {
			return fmt.Errorf("%w: waiting checkpoint does not point to a wait node", ErrInvalidCheckpoint)
		}
	}
	for _, item := range cp.State.Pending {
		if item.NodeName == "" {
			return fmt.Errorf("%w: empty pending node", ErrInvalidCheckpoint)
		}
		if _, ok := wf.Nodes[item.NodeName]; !ok {
			return fmt.Errorf("%w: pending node %q no longer exists", ErrInvalidCheckpoint, item.NodeName)
		}
		for _, branch := range item.Input {
			for _, it := range branch {
				for _, br := range it.Binary {
					if !validScratchRef(br.FileName, scratchRoot, cp.ExecutionID) {
						return fmt.Errorf("%w: missing/unsafe binary reference", ErrInvalidCheckpoint)
					}
					if br.SizeBytes < 0 {
						return fmt.Errorf("%w: negative binary size", ErrInvalidCheckpoint)
					}
				}
			}
		}
	}
	return nil
}

func validScratchRef(name, scratchRoot, executionID string) bool {
	if name == "" {
		return true
	}
	base := filepath.Join(scratchRoot, executionID)
	cleanBase, _ := filepath.Abs(base)
	cleanName, _ := filepath.Abs(name)
	if cleanBase == "" || cleanName == "" {
		return false
	}
	rel, err := filepath.Rel(cleanBase, cleanName)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}
	_, err = os.Stat(cleanName)
	return err == nil
}
