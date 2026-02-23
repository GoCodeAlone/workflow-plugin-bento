package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/warpstreamlabs/bento/v4/public/components/pure"
)

// mockTriggerCallback captures trigger invocations for testing.
type mockTriggerCallback struct {
	mu    sync.Mutex
	calls []triggerCall
}

type triggerCall struct {
	action string
	data   map[string]any
}

func (m *mockTriggerCallback) Call(action string, data map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy data to avoid race conditions
	dataCopy := make(map[string]any, len(data))
	for k, v := range data {
		dataCopy[k] = v
	}

	m.calls = append(m.calls, triggerCall{
		action: action,
		data:   dataCopy,
	})
	return nil
}

func (m *mockTriggerCallback) GetCalls() []triggerCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]triggerCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func TestNewBentoTrigger(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "empty config",
			config:  map[string]any{},
			wantErr: false,
		},
		{
			name: "valid single subscription",
			config: map[string]any{
				"subscriptions": []any{
					map[string]any{
						"input": map[string]any{
							"generate": map[string]any{
								"mapping": "root = {}",
								"count":   1,
							},
						},
						"workflow": "test-workflow",
						"action":   "on_message",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid subscriptions type",
			config: map[string]any{
				"subscriptions": "not-a-list",
			},
			wantErr: true,
		},
		{
			name: "subscription missing input",
			config: map[string]any{
				"subscriptions": []any{
					map[string]any{
						"workflow": "test-workflow",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "subscription input not a map",
			config: map[string]any{
				"subscriptions": []any{
					map[string]any{
						"input": "not-a-map",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := &mockTriggerCallback{}
			_, err := newBentoTrigger(tt.config, cb.Call)
			if (err != nil) != tt.wantErr {
				t.Errorf("newBentoTrigger() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBentoTrigger_ParseSubscriptions(t *testing.T) {
	cb := &mockTriggerCallback{}

	config := map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": "root = {}",
					},
				},
				"workflow": "workflow1",
				"action":   "custom_action",
			},
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": "root = {}",
					},
				},
				"workflow": "workflow2",
				// action omitted, should default to "trigger"
			},
		},
	}

	trigger, err := newBentoTrigger(config, cb.Call)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	if len(trigger.subscriptions) != 2 {
		t.Errorf("expected 2 subscriptions, got %d", len(trigger.subscriptions))
	}

	if trigger.subscriptions[0].workflow != "workflow1" {
		t.Errorf("expected workflow1, got %s", trigger.subscriptions[0].workflow)
	}
	if trigger.subscriptions[0].action != "custom_action" {
		t.Errorf("expected custom_action, got %s", trigger.subscriptions[0].action)
	}

	if trigger.subscriptions[1].action != "trigger" {
		t.Errorf("expected default action 'trigger', got %s", trigger.subscriptions[1].action)
	}
}

func TestBentoTrigger_StartStop(t *testing.T) {
	cb := &mockTriggerCallback{}

	trigger, err := newBentoTrigger(map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping":  `root = {"id": count("c")}`,
						"count":    2,
						"interval": "50ms",
					},
				},
				"workflow": "test-workflow",
				"action":   "process",
			},
		},
	}, cb.Call)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait for messages to be processed
	time.Sleep(300 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := trigger.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	calls := cb.GetCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 callback invocations, got %d", len(calls))
	}

	for i, call := range calls {
		if call.action != "process" {
			t.Errorf("call[%d]: expected action 'process', got %q", i, call.action)
		}
		if call.data["workflow"] != "test-workflow" {
			t.Errorf("call[%d]: expected workflow 'test-workflow', got %v", i, call.data["workflow"])
		}
		if call.data["body"] == nil {
			t.Errorf("call[%d]: expected non-nil body", i)
		}
	}
}

func TestBentoTrigger_MultipleSubscriptions(t *testing.T) {
	cb := &mockTriggerCallback{}

	trigger, err := newBentoTrigger(map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": `root = {"source": "input1"}`,
						"count":   1,
					},
				},
				"workflow": "workflow1",
				"action":   "action1",
			},
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": `root = {"source": "input2"}`,
						"count":   1,
					},
				},
				"workflow": "workflow2",
				"action":   "action2",
			},
		},
	}, cb.Call)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait for all messages
	time.Sleep(200 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := trigger.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	calls := cb.GetCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 callback invocations, got %d", len(calls))
	}

	// Verify both workflows were triggered
	workflows := make(map[string]bool)
	actions := make(map[string]bool)

	for _, call := range calls {
		if wf, ok := call.data["workflow"].(string); ok {
			workflows[wf] = true
		}
		actions[call.action] = true
	}

	if !workflows["workflow1"] || !workflows["workflow2"] {
		t.Errorf("expected both workflows to be triggered, got %v", workflows)
	}
	if !actions["action1"] || !actions["action2"] {
		t.Errorf("expected both actions to be called, got %v", actions)
	}
}

func TestBentoTrigger_NoSubscriptions(t *testing.T) {
	cb := &mockTriggerCallback{}

	trigger, err := newBentoTrigger(map[string]any{}, cb.Call)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Errorf("Start() with no subscriptions should not error, got %v", err)
	}

	// Should complete quickly
	stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := trigger.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	calls := cb.GetCalls()
	if len(calls) != 0 {
		t.Errorf("expected 0 callbacks with no subscriptions, got %d", len(calls))
	}
}

func TestBentoTrigger_CallbackError(t *testing.T) {
	// Callback that returns error
	var callbackErr error
	errorCb := func(action string, data map[string]any) error {
		return callbackErr
	}

	trigger, err := newBentoTrigger(map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": `root = {"test": "data"}`,
						"count":   1,
					},
				},
				"workflow": "test",
			},
		},
	}, errorCb)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	callbackErr = nil // First call succeeds

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Should stop cleanly even if callback had errors
	if err := trigger.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func TestBentoTrigger_StopWithoutStart(t *testing.T) {
	cb := &mockTriggerCallback{}
	trigger, err := newBentoTrigger(map[string]any{}, cb.Call)
	if err != nil {
		t.Fatalf("newBentoTrigger() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Stop without Start — done channel never closed, expect timeout or nil
	err = trigger.Stop(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	t.Errorf("Stop() without Start: expected nil or DeadlineExceeded, got %v", err)
}
