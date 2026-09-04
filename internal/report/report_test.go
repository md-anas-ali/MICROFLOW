package report

import (
	"strings"
	"testing"
	"time"

	"microflow/internal/model"
)

func sampleWorkflow() *model.Workflow {
	return &model.Workflow{
		ID:   "wf1",
		Name: "Test Workflow",
		Nodes: map[string]*model.Node{
			"Trend Finder": {ID: "n1", Name: "Trend Finder", Type: model.TypeCode},
			"Script Gen":   {ID: "n2", Name: "Script Gen", Type: model.TypeHTTPRequest, RetryOnFail: true, MaxTries: 3},
			"Upload":       {ID: "n3", Name: "Upload", Type: model.TypeYouTube},
		},
		Connections: map[string][]model.Connection{
			"Trend Finder": {{SourceName: "Trend Finder", TargetName: "Script Gen"}},
			"Script Gen":   {{SourceName: "Script Gen", TargetName: "Upload"}},
		},
	}
}

func TestBuild_SuccessfulExecution(t *testing.T) {
	wf := sampleWorkflow()
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	finish := start.Add(3 * time.Second)
	ex := &model.Execution{
		ID:         "exec1",
		WorkflowID: "wf1",
		Status:     model.StatusSuccess,
		StartedAt:  start,
		FinishedAt: &finish,
		NodeRuns: []model.NodeRunResult{
			{
				NodeName:  "Trend Finder",
				Status:    model.StatusSuccess,
				StartedAt: start,
				Duration:  1800 * time.Millisecond,
				Attempt:   1,
				Output:    model.NodeOutput{{{JSON: map[string]any{"topic": "cats"}}}},
			},
			{
				NodeName:  "Script Gen",
				Status:    model.StatusSuccess,
				StartedAt: start.Add(2 * time.Second),
				Duration:  1200 * time.Millisecond,
				Attempt:   1,
				Input:     model.NodeOutput{{{JSON: map[string]any{"topic": "cats"}}}},
				Output:    model.NodeOutput{{{JSON: map[string]any{"script": "hello"}}}},
			},
		},
	}

	rpt := Build(wf, ex)

	if rpt.Summary.ExecutionID != "exec1" || rpt.Summary.WorkflowName != "Test Workflow" {
		t.Fatalf("summary identity fields wrong: %+v", rpt.Summary)
	}
	if rpt.Summary.ExecutionStatus != "SUCCESS" {
		t.Fatalf("expected SUCCESS, got %q", rpt.Summary.ExecutionStatus)
	}
	if rpt.Summary.TotalNodes != 2 || rpt.Summary.SuccessfulNodes != 2 || rpt.Summary.FailedNodes != 0 {
		t.Fatalf("wrong node counts: %+v", rpt.Summary)
	}
	// Upload never ran (not in NodeRuns) -- should count as not-executed,
	// not be silently missing.
	if rpt.Summary.NotExecutedNodes != 1 {
		t.Fatalf("expected 1 not-executed node (Upload), got %d", rpt.Summary.NotExecutedNodes)
	}
	if rpt.Summary.MainError != none {
		t.Fatalf("expected no main error, got %q", rpt.Summary.MainError)
	}

	n0 := rpt.Nodes[0]
	if n0.NodeID != "n1" || n0.NodeType != string(model.TypeCode) {
		t.Fatalf("node id/type not resolved from workflow: %+v", n0)
	}
	if n0.PreviousNodes != none {
		t.Fatalf("expected no previous node for the first node, got %q", n0.PreviousNodes)
	}
	if n0.NextNodes != "Script Gen" {
		t.Fatalf("expected next node Script Gen, got %q", n0.NextNodes)
	}
	if n0.Error != none || n0.ErrorType != none {
		t.Fatalf("successful node should report None for error fields: %+v", n0)
	}

	n1 := rpt.Nodes[1]
	if n1.PreviousNodes != "Trend Finder" {
		t.Fatalf("expected previous node Trend Finder, got %q", n1.PreviousNodes)
	}
	if !n1.RetryOnFail || n1.MaxTries != "3" {
		t.Fatalf("expected retry config carried over from workflow node: %+v", n1)
	}

	full := rpt.FormatFullText()
	if !strings.Contains(full, "MICROFLOW — FULL EXECUTION REPORT") {
		t.Fatalf("full report missing header:\n%s", full)
	}
	if !strings.Contains(full, "NODE 1") || !strings.Contains(full, "NODE 2") {
		t.Fatalf("full report missing node blocks:\n%s", full)
	}
	if !strings.Contains(full, "Total Nodes: 2") {
		t.Fatalf("full report missing summary counts:\n%s", full)
	}
}

func TestBuild_FailedExecution(t *testing.T) {
	wf := sampleWorkflow()
	start := time.Now()
	ex := &model.Execution{
		ID:         "exec2",
		WorkflowID: "wf1",
		Status:     model.StatusError,
		StartedAt:  start,
		Error:      "engine: no executor registered for node \"Upload\"",
		NodeRuns: []model.NodeRunResult{
			{NodeName: "Trend Finder", Status: model.StatusSuccess, StartedAt: start, Duration: time.Second, Attempt: 1},
			{
				NodeName:  "Script Gen",
				Status:    model.StatusError,
				StartedAt: start.Add(time.Second),
				Duration:  500 * time.Millisecond,
				Attempt:   1,
				Error:     "http request \"Script Gen\": configuration error (HTTP 401) -- check the credential/API key configured for this node: unauthorized",
			},
		},
	}

	rpt := Build(wf, ex)

	if rpt.Summary.FailedNodes != 1 || len(rpt.Summary.FailedNodeNames) != 1 || rpt.Summary.FailedNodeNames[0] != "Script Gen" {
		t.Fatalf("expected 1 failed node named Script Gen: %+v", rpt.Summary)
	}
	if rpt.Summary.MainError == none {
		t.Fatalf("expected a main error to be populated")
	}

	failed := rpt.Nodes[1]
	if failed.Status != "FAILED" {
		t.Fatalf("expected FAILED status, got %q", failed.Status)
	}
	if failed.HTTPStatus != "401" {
		t.Fatalf("expected HTTP status 401 extracted from error text, got %q", failed.HTTPStatus)
	}
	if failed.ErrorType != "AuthenticationError" {
		t.Fatalf("expected AuthenticationError classification, got %q", failed.ErrorType)
	}
	if failed.ErrorMessage == none || failed.ErrorMessage == "" {
		t.Fatalf("expected a non-empty error message")
	}

	errText := rpt.FormatErrorText()
	if !strings.Contains(errText, "MICROFLOW — ERROR REPORT") {
		t.Fatalf("error report missing header:\n%s", errText)
	}
	if strings.Contains(errText, "NODE 1") {
		t.Fatalf("error report should not include the successful node:\n%s", errText)
	}
	if !strings.Contains(errText, "Script Gen") {
		t.Fatalf("error report should include the failed node:\n%s", errText)
	}
}

func TestBuild_SecretMasking(t *testing.T) {
	wf := sampleWorkflow()
	start := time.Now()
	ex := &model.Execution{
		ID:         "exec3",
		WorkflowID: "wf1",
		Status:     model.StatusError,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{
				NodeName:  "Script Gen",
				Status:    model.StatusError,
				StartedAt: start,
				Duration:  time.Second,
				Attempt:   1,
				Input: model.NodeOutput{{{JSON: map[string]any{
					"Authorization": "Bearer sk-1234567890abcdef",
					"apiKey":        "AKIA1234567890",
					"note":          "safe field",
				}}}},
				Error: "http request \"Script Gen\": failed, Authorization: Bearer sk-abcdefghijklmno, TOKEN=abcdef123456",
			},
		},
	}

	rpt := Build(wf, ex)
	n := rpt.Nodes[0]

	// Structured JSON: secret-named keys are replaced outright.
	inItem := n.Input[0][0].JSON
	if inItem["Authorization"] != redactedPlaceholder {
		t.Fatalf("expected Authorization key redacted, got %v", inItem["Authorization"])
	}
	if inItem["apiKey"] != redactedPlaceholder {
		t.Fatalf("expected apiKey redacted, got %v", inItem["apiKey"])
	}
	if inItem["note"] != "safe field" {
		t.Fatalf("expected unrelated field preserved, got %v", inItem["note"])
	}

	// Free text: secret-shaped values are scrubbed even without a
	// matching key name (they're embedded inline in the error string).
	if strings.Contains(n.Error, "sk-abcdefghijklmno") {
		t.Fatalf("raw sk- secret leaked into error text: %q", n.Error)
	}
	if !strings.Contains(n.Error, redactedPlaceholder) {
		t.Fatalf("expected [REDACTED] placeholder in error text: %q", n.Error)
	}

	full := rpt.FormatFullText()
	if strings.Contains(full, "sk-1234567890abcdef") || strings.Contains(full, "sk-abcdefghijklmno") || strings.Contains(full, "AKIA1234567890") {
		t.Fatalf("raw secret leaked into formatted text report:\n%s", full)
	}
}

func TestBuild_LargeOutputTruncatedInText(t *testing.T) {
	wf := sampleWorkflow()
	start := time.Now()
	big := strings.Repeat("x", maxInlineBytes*3)
	ex := &model.Execution{
		ID:         "exec4",
		WorkflowID: "wf1",
		Status:     model.StatusSuccess,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{
				NodeName:  "Trend Finder",
				Status:    model.StatusSuccess,
				StartedAt: start,
				Duration:  time.Second,
				Attempt:   1,
				Output:    model.NodeOutput{{{JSON: map[string]any{"blob": big}}}},
			},
		},
	}

	rpt := Build(wf, ex)

	// JSON export must never truncate -- full data always available.
	raw, err := rpt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	if !strings.Contains(string(raw), big) {
		t.Fatalf("JSON export must contain the full untruncated output")
	}

	// Text report truncates with a note, keeping the report bounded.
	full := rpt.FormatFullText()
	if len(full) >= len(big) {
		t.Fatalf("expected the text report to be much smaller than the raw blob (got %d bytes)", len(full))
	}
	if !strings.Contains(full, "truncated") {
		t.Fatalf("expected a truncation note in the text report")
	}
}

func TestBuild_MissingWorkflowDegradesGracefully(t *testing.T) {
	start := time.Now()
	ex := &model.Execution{
		ID:         "exec5",
		WorkflowID: "deleted-wf",
		Status:     model.StatusSuccess,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{NodeName: "Some Node", Status: model.StatusSuccess, StartedAt: start, Duration: time.Second, Attempt: 1},
		},
	}

	rpt := Build(nil, ex) // workflow unavailable

	if rpt.Summary.WorkflowName != none {
		t.Fatalf("expected None workflow name when workflow unavailable, got %q", rpt.Summary.WorkflowName)
	}
	n := rpt.Nodes[0]
	if n.NodeID != none || n.NodeType != none || n.PreviousNodes != none || n.NextNodes != none {
		t.Fatalf("expected None for every workflow-derived field, got %+v", n)
	}
	if n.NodeName != "Some Node" {
		t.Fatalf("execution-native fields must still be populated: %+v", n)
	}

	// Must not panic and must still produce a usable report.
	if rpt.FormatFullText() == "" || rpt.FormatErrorText() == "" {
		t.Fatalf("expected non-empty reports even without a workflow")
	}
}

func TestBuild_NilExecution(t *testing.T) {
	rpt := Build(sampleWorkflow(), nil)
	if rpt.Summary.ExecutionID != none {
		t.Fatalf("expected None for a nil execution, got %q", rpt.Summary.ExecutionID)
	}
	if len(rpt.Nodes) != 0 {
		t.Fatalf("expected no nodes for a nil execution")
	}
}
