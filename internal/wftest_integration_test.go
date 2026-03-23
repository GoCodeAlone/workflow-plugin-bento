package internal

import (
	"testing"

	"github.com/GoCodeAlone/workflow/wftest"
)

// TestBentoPlugin_ProcessStep verifies that the step.bento step type is
// invoked during pipeline execution and that recorded call inputs match
// the trigger data passed to ExecutePipeline.
func TestBentoPlugin_ProcessStep(t *testing.T) {
	rec := wftest.RecordStep("step.bento")
	rec.WithOutput(map[string]any{"result": "processed"})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  process-data:
    trigger:
      type: manual
    steps:
      - name: transform
        type: step.bento
        config:
          processors:
            - bloblang: 'root = this'
`), rec)

	result := h.ExecutePipeline("process-data", map[string]any{"data": "hello"})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}

	if rec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.bento, got %d", rec.CallCount())
	}

	calls := rec.Calls()
	if calls[0].Input["data"] != "hello" {
		t.Errorf("expected input data=hello, got %v", calls[0].Input["data"])
	}
}

// TestBentoPlugin_OutputPassthrough verifies that the output returned by the
// mock step handler is surfaced as the pipeline result output.
func TestBentoPlugin_OutputPassthrough(t *testing.T) {
	want := map[string]any{"status": "ok", "count": float64(42)}
	rec := wftest.RecordStep("step.bento")
	rec.WithOutput(want)

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  passthrough:
    trigger:
      type: manual
    steps:
      - name: enrich
        type: step.bento
        config:
          processors: "mapping: 'root = this'"
`), rec)

	result := h.ExecutePipeline("passthrough", map[string]any{"key": "value"})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}

	if result.Output["status"] != "ok" {
		t.Errorf("expected output status=ok, got %v", result.Output["status"])
	}
	if result.Output["count"] != float64(42) {
		t.Errorf("expected output count=42, got %v", result.Output["count"])
	}
}

// TestBentoPlugin_MultipleSteps verifies call recording across multiple
// step.bento steps in a single pipeline. Each step should be recorded once
// and its config should reflect the YAML defined for that step.
func TestBentoPlugin_MultipleSteps(t *testing.T) {
	rec := wftest.RecordStep("step.bento")
	rec.WithOutput(map[string]any{"transformed": true})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  multi-step:
    trigger:
      type: manual
    steps:
      - name: step-one
        type: step.bento
        config:
          processors: "mapping: 'root = this'"
      - name: step-two
        type: step.bento
        config:
          processors: "mapping: 'root.transformed = true'"
`), rec)

	result := h.ExecutePipeline("multi-step", map[string]any{"input": "data"})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}

	if rec.CallCount() != 2 {
		t.Errorf("expected 2 calls to step.bento (one per step), got %d", rec.CallCount())
	}

	calls := rec.Calls()
	// First call: step-one should receive the original trigger data.
	if calls[0].Input["input"] != "data" {
		t.Errorf("step-one: expected input input=data, got %v", calls[0].Input["input"])
	}
	// Second call: step-two should see output from step-one (transformed=true).
	if calls[1].Input["transformed"] != true {
		t.Errorf("step-two: expected transformed=true in input, got %v", calls[1].Input)
	}
}
