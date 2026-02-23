package internal

import (
	"context"
	"testing"
	"time"

	// Import pure Bento components to register bloblang and other processors.
	_ "github.com/warpstreamlabs/bento/v4/public/components/pure"
)

func TestNewProcessorStep_ValidConfig_StringProcessors(t *testing.T) {
	// AddProcessorYAML expects a single processor object (YAML map), not a list.
	config := map[string]any{
		"processors": `bloblang: "root = this"`,
	}

	s, err := newProcessorStep("test-step", config)
	if err != nil {
		t.Fatalf("newProcessorStep() returned error: %v", err)
	}
	if s == nil {
		t.Fatal("newProcessorStep() returned nil")
	}
}

func TestNewProcessorStep_ValidConfig_MapProcessors(t *testing.T) {
	config := map[string]any{
		"processors": map[string]any{
			"bloblang": "root = this",
		},
	}

	s, err := newProcessorStep("test-step", config)
	if err != nil {
		t.Fatalf("newProcessorStep() returned error: %v", err)
	}
	if s == nil {
		t.Fatal("newProcessorStep() returned nil")
	}
}

func TestNewProcessorStep_NoProcessors_IsAllowed(t *testing.T) {
	// nil processors = pass-through; constructor should succeed.
	s, err := newProcessorStep("test-step", map[string]any{})
	if err != nil {
		t.Fatalf("newProcessorStep() with no processors returned error: %v", err)
	}
	if s == nil {
		t.Fatal("newProcessorStep() returned nil")
	}
	if s.processors != "" {
		t.Errorf("processors = %q, want empty string for pass-through", s.processors)
	}
}

func TestNewProcessorStep_InvalidProcessorsType_ReturnsError(t *testing.T) {
	config := map[string]any{
		"processors": 42, // invalid type
	}

	s, err := newProcessorStep("test-step", config)
	if err == nil {
		t.Fatal("newProcessorStep() with invalid processors type should return error")
	}
	if s != nil {
		t.Error("newProcessorStep() with invalid processors type should return nil step")
	}
}

func TestProcessorStep_Name(t *testing.T) {
	s, _ := newProcessorStep("my-step", map[string]any{})
	if s.name != "my-step" {
		t.Errorf("processorStep.name = %q, want %q", s.name, "my-step")
	}
}

func TestProcessorStep_Execute_PassThrough(t *testing.T) {
	// With no processors configured, the step should return the merged input unchanged.
	s, _ := newProcessorStep("test", map[string]any{})

	triggerData := map[string]any{"event": "click"}
	current := map[string]any{"count": 1}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, triggerData, nil, current, nil)
	if err != nil {
		t.Fatalf("Execute() returned error: %v", err)
	}
	if result == nil {
		t.Fatal("Execute() returned nil result")
	}

	// Both trigger data and current should be present in the output.
	if result.Output["event"] != "click" {
		t.Errorf("Output[event] = %v, want %q", result.Output["event"], "click")
	}
	if result.Output["count"] != 1 {
		t.Errorf("Output[count] = %v, want 1", result.Output["count"])
	}
}

func TestProcessorStep_Execute_CurrentOverridesTriggerData(t *testing.T) {
	// When both triggerData and current have the same key, current wins.
	s, _ := newProcessorStep("test", map[string]any{})

	triggerData := map[string]any{"key": "from-trigger"}
	current := map[string]any{"key": "from-current"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, triggerData, nil, current, nil)
	if err != nil {
		t.Fatalf("Execute() returned error: %v", err)
	}

	if result.Output["key"] != "from-current" {
		t.Errorf("Output[key] = %v, want %q (current should override trigger)", result.Output["key"], "from-current")
	}
}

func TestProcessorStep_Execute_EmptyInputs(t *testing.T) {
	s, _ := newProcessorStep("test", map[string]any{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute() with nil inputs returned error: %v", err)
	}
	if result == nil {
		t.Fatal("Execute() returned nil result")
	}
}

func TestProcessorStep_Execute_WithBloblangPassthrough(t *testing.T) {
	// AddProcessorYAML expects a single processor object (YAML map), not a list.
	config := map[string]any{
		"processors": `bloblang: "root = this"`,
	}

	s, err := newProcessorStep("test", config)
	if err != nil {
		t.Fatalf("newProcessorStep() returned error: %v", err)
	}

	input := map[string]any{"value": "hello"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, input, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute() with bloblang pass-through returned error: %v", err)
	}
	if result == nil {
		t.Fatal("Execute() returned nil result")
	}
	if result.Output == nil {
		t.Fatal("Execute() result.Output is nil")
	}
	if result.Output["value"] != "hello" {
		t.Errorf("Output[value] = %v, want %q", result.Output["value"], "hello")
	}
}

func TestProcessorStep_Execute_ContextCancelled(t *testing.T) {
	config := map[string]any{
		"processors": `- bloblang: "root = this"`,
	}

	s, _ := newProcessorStep("test", config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := s.Execute(ctx, map[string]any{"key": "val"}, nil, nil, nil)
	// May succeed (if stream runs fast enough) or return context.Canceled — both are acceptable.
	// We just verify it does not panic or block forever.
	_ = err
}

func TestProcessorStep_Execute_StopPipelineIsFalse(t *testing.T) {
	s, _ := newProcessorStep("test", map[string]any{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, map[string]any{"x": 1}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute() returned error: %v", err)
	}

	if result.StopPipeline {
		t.Error("StopPipeline should be false for normal step execution")
	}
}
