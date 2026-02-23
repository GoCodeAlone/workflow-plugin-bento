package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestNewProcessorStep(t *testing.T) {
	tests := []struct {
		name         string
		stepName     string
		config       map[string]any
		wantErr      bool
		wantProcYAML string
	}{
		{
			name:     "with processors as string",
			stepName: "test-step",
			config: map[string]any{
				"processors": "bloblang: 'root = this'",
			},
			wantErr:      false,
			wantProcYAML: "bloblang: 'root = this'",
		},
		{
			name:     "with processors as map",
			stepName: "test-step-map",
			config: map[string]any{
				"processors": map[string]any{
					"bloblang": "root = this",
				},
			},
			wantErr: false,
		},
		{
			name:     "no processors",
			stepName: "test-step-empty",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			name:     "processors as invalid type",
			stepName: "test-step-invalid",
			config: map[string]any{
				"processors": 123, // invalid type
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newProcessorStep(tt.stepName, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("newProcessorStep() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if s.name != tt.stepName {
					t.Errorf("expected name %q, got %q", tt.stepName, s.name)
				}
				if tt.wantProcYAML != "" && s.processors != tt.wantProcYAML {
					t.Errorf("expected processors %q, got %q", tt.wantProcYAML, s.processors)
				}
			}
		})
	}
}

func TestProcessorStep_ExecutePassthrough(t *testing.T) {
	// Step with no processors should pass data through unchanged
	s, err := newProcessorStep("passthrough", map[string]any{})
	if err != nil {
		t.Fatalf("newProcessorStep() error = %v", err)
	}

	ctx := context.Background()
	triggerData := map[string]any{"trigger": "value"}
	current := map[string]any{"current": "data"}

	result, err := s.Execute(ctx, triggerData, nil, current, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.Output == nil {
		t.Fatal("expected non-nil output")
	}

	// Should have merged trigger + current
	if result.Output["trigger"] != "value" {
		t.Errorf("expected trigger=value, got %v", result.Output["trigger"])
	}
	if result.Output["current"] != "data" {
		t.Errorf("expected current=data, got %v", result.Output["current"])
	}
}

func TestProcessorStep_ExecuteWithBloblang(t *testing.T) {
	tests := []struct {
		name       string
		processors string
		input      map[string]any
		wantOutput map[string]any
		wantErr    bool
	}{
		{
			name:       "simple pass-through mapping",
			processors: "mapping: 'root = this'",
			input:      map[string]any{"key": "value"},
			wantOutput: map[string]any{"key": "value"},
			wantErr:    false,
		},
		{
			name:       "field transformation",
			processors: "mapping: 'root.output = this.input.uppercase()'",
			input:      map[string]any{"input": "hello"},
			wantOutput: map[string]any{"output": "HELLO"},
			wantErr:    false,
		},
		{
			name:       "add computed field",
			processors: "mapping: |\n  root = this\n  root.computed = this.a + this.b",
			input:      map[string]any{"a": float64(5), "b": float64(3)},
			wantOutput: map[string]any{"a": float64(5), "b": float64(3), "computed": float64(8)},
			wantErr:    false,
		},
		{
			name:       "constant output",
			processors: "mapping: 'root.status = \"processed\"'",
			input:      map[string]any{"data": "test"},
			wantOutput: map[string]any{"status": "processed"},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newProcessorStep("test", map[string]any{
				"processors": tt.processors,
			})
			if err != nil {
				t.Fatalf("newProcessorStep() error = %v", err)
			}

			ctx := context.Background()
			result, err := s.Execute(ctx, tt.input, nil, map[string]any{}, nil)

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if result.Output == nil {
					t.Fatal("expected non-nil output")
				}

				// Compare outputs
				for k, want := range tt.wantOutput {
					got := result.Output[k]
					if !equalValues(got, want) {
						t.Errorf("output[%q] = %v (type %T), want %v (type %T)", k, got, got, want, want)
					}
				}
			}
		})
	}
}

func TestProcessorStep_ExecuteWithInvalidBloblang(t *testing.T) {
	s, err := newProcessorStep("test", map[string]any{
		"processors": "mapping: 'root = invalid syntax here {'",
	})
	if err != nil {
		// Invalid Bloblang may be caught at construction time
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := s.Execute(ctx, map[string]any{"data": "test"}, nil, map[string]any{}, nil)

	if err == nil {
		t.Error("Execute() expected error for invalid Bloblang syntax, got nil")
		if result != nil {
			t.Logf("unexpected result: %+v", result)
		}
	}
}

func TestProcessorStep_ExecuteMergesInputs(t *testing.T) {
	s, err := newProcessorStep("test", map[string]any{
		"processors": "mapping: 'root = this'",
	})
	if err != nil {
		t.Fatalf("newProcessorStep() error = %v", err)
	}

	ctx := context.Background()
	triggerData := map[string]any{"from_trigger": "trigger_val"}
	current := map[string]any{"from_current": "current_val"}

	result, err := s.Execute(ctx, triggerData, nil, current, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Both should be present in output
	if result.Output["from_trigger"] != "trigger_val" {
		t.Errorf("expected from_trigger in output, got %v", result.Output)
	}
	if result.Output["from_current"] != "current_val" {
		t.Errorf("expected from_current in output, got %v", result.Output)
	}
}

func TestProcessorStep_ExecuteWithNonJSONOutput(t *testing.T) {
	// Processor that outputs plain text instead of JSON
	s, err := newProcessorStep("test", map[string]any{
		"processors": "mapping: 'root = \"plain text output\"'",
	})
	if err != nil {
		t.Fatalf("newProcessorStep() error = %v", err)
	}

	ctx := context.Background()
	result, err := s.Execute(ctx, map[string]any{"input": "test"}, nil, map[string]any{}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Non-JSON output should be stored under "output" key
	if result.Output["output"] != "plain text output" {
		t.Errorf("expected output key with plain text, got %v", result.Output)
	}
}

func TestProcessorStep_ExecuteWithContextCancel(t *testing.T) {
	s, err := newProcessorStep("test", map[string]any{
		"processors": "mapping: 'root = this'",
	})
	if err != nil {
		t.Fatalf("newProcessorStep() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = s.Execute(ctx, map[string]any{"data": "test"}, nil, map[string]any{}, nil)

	if err == nil {
		t.Error("Execute() expected error with cancelled context")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("Execute() expected context.Canceled, got: %v", err)
	}
}

func TestProcessorStep_ExecuteWithMultipleProcessors(t *testing.T) {
	// Combined mapping that chains transformations
	s, err := newProcessorStep("test", map[string]any{
		"processors": "mapping: |\n  root.step1 = this.input.uppercase()\n  root.step2 = root.step1 + \" PROCESSED\"",
	})
	if err != nil {
		t.Fatalf("newProcessorStep() error = %v", err)
	}

	ctx := context.Background()
	result, err := s.Execute(ctx, map[string]any{"input": "hello"}, nil, map[string]any{}, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.Output["step2"] != "HELLO PROCESSED" {
		t.Errorf("expected chained processing result, got %v", result.Output)
	}
}

// equalValues compares two values for equality, handling JSON number conversion.
func equalValues(a, b any) bool {
	// Try JSON round-trip to normalize types
	aJSON, _ := json.Marshal(a)
	bJSON, _ := json.Marshal(b)

	var aNorm, bNorm any
	_ = json.Unmarshal(aJSON, &aNorm)
	_ = json.Unmarshal(bJSON, &bNorm)

	aStr := string(aJSON)
	bStr := string(bJSON)

	return aStr == bStr
}
